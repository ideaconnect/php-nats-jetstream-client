<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Cancellation;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\IncomingChunkResult;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Core\SubscriptionQueue;
use IDCT\NATS\Exception\SlowConsumerException;
use IDCT\NATS\Tests\Support\HeldUpDelivery;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\async;

/**
 * An operation's read takes what is already queued for its own subscription before it reads or waits for another
 * fiber's read (#179). An earlier read can leave the operation's message queued behind a delivery it has not finished,
 * held up in the handler of a lower sid that awaits - which is the very handler the operation runs in when a handler
 * polls a SubscriptionQueue of its own, or one awaiting an HTTP or database call in another fiber. The operation used
 * to read the socket with its message already queued, and waited for the server's next bytes or its deadline, so a
 * handler ahead of it that outlasted the deadline made it time out: {@see SubscriptionQueue::next()} with a 1 s timeout
 * returned null, and the message reached the queue only once the handler returned.
 *
 * The regression tests here fail on the code before the fix - the operation times out with its result queued the whole
 * time - and pass on it. The guards hold on both: the own-sid delivery leaves the other subscriptions' messages queued,
 * fails the poll on an overflow of its own subscription, is skipped while that sid's own delivery is under way or a
 * disconnect() is closing, and does not touch request()'s reply-inbox read.
 */
final class OperationTakesItsQueuedMessageTest extends TestCase
{
    use ReconnectScenarios;

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{string}> */
    public static function polls(): iterable
    {
        yield 'next()' => ['next'];
        yield 'fetchAll(1)' => ['fetchAll'];
    }

    /**
     * #179 itself. A user handler on "trigger" (the lower sid) polls the "jobs" queue, and the chunk the read delivers
     * holds both: trigger ahead of job-1, so job-1 is queued behind trigger's delivery, which is the delivery this very
     * handler runs in. The poll returns job-1 from inside the handler, within half a second of its 1 s timeout, without
     * a socket read. Before the fix the poll read the socket with nothing more to come and returned null at its timeout,
     * the message queued the whole time.
     */
    #[DataProvider('polls')]
    public function testAHandlerPollingItsOwnQueueGetsTheMessageQueuedBehindIt(string $poll): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->watchedClient($watched);
        $box = new class {
            public ?SubscriptionQueue $queue = null;
            public mixed $payload = 'UNSET';
            public int $readsDuring = -1;
            public float $elapsed = -1.0;
        };
        // Subscribed first, so its sid is the lower one and its message is delivered ahead of the queue's.
        $triggerSid = $client->subscribe('trigger', static function () use ($box, $watched, $poll): void {
            $queue = $box->queue;
            if ($queue === null) {
                return;
            }

            $readsBefore = $watched->reads;
            $start = hrtime(true);
            if ($poll === 'next') {
                $box->payload = $queue->setTimeout(1.0)->next()?->payload;
            } else {
                $messages = $queue->setTimeout(1.0)->fetchAll(1);
                $box->payload = $messages === [] ? null : $messages[0]->payload;
            }
            $box->elapsed = (hrtime(true) - $start) / 1e9;
            $box->readsDuring = $watched->reads - $readsBefore;
        })->await();
        $box->queue = $client->subscribeQueue('jobs')->await();

        $transport->pushFrame(
            ReconnectingTransport::msgFrame('trigger', $triggerSid, 't')
            . ReconnectingTransport::msgFrame('jobs', $box->queue->sid, 'job-1'),
        );
        $client->processIncoming(new TimeoutCancellation(3))->await();

        self::assertSame('job-1', $box->payload, 'the handler got the message queued behind it');
        self::assertLessThan(0.5, $box->elapsed, sprintf('the poll took %.3f s of its 1 s timeout', $box->elapsed));
        self::assertSame(0, $box->readsDuring, 'the poll delivered its queued message without reading the socket');
    }

    /** @return iterable<string, array{string}> */
    public static function operations(): iterable
    {
        yield 'next()' => ['next'];
        yield 'fetchAll(1)' => ['fetchAll'];
        yield 'fetchBatch(1)' => ['fetchBatch'];
    }

    /**
     * The same shape with the handler in another fiber: the application's read loop takes a chunk holding, ahead of the
     * operation's message, a message for "slow" (the lowest sid) whose handler awaits an HTTP or a database call. The
     * operation's message is queued behind that delivery. The operation takes it at once, before the handler is let go,
     * and without reading the socket. The order decides it: {@see HeldUpDelivery::$held} is still true when the result
     * is in. Before the fix the operation read the socket and waited for the server, and the handler - let go only by
     * the read taking the socket, which the operation no longer makes - outlasted the operation's deadline.
     */
    #[DataProvider('operations')]
    public function testAnOperationTakesItsMessageQueuedBehindAHandlerThatAwaitsWithoutReading(string $operation): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->watchedClient($watched);
        // The fetch writes a PING behind its inbox's SUB (#175), whose PONG would end the application's parked read
        // and make it read again: a read the count below would take for the operation's. The server leaves PINGs
        // after the handshake unanswered, so the one chunk is the pull's answer, as the shape requires.
        $transport->answerPings = false;
        $hold = new HeldUpDelivery(fallbackSeconds: 2.0);
        $slowSid = $client->subscribe('slow', $hold->handler())->await();
        $slowFrame = ReconnectingTransport::msgFrame('slow', $slowSid, 's');
        $server = new class {
            public int $pulls = 0;
        };
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport, $slowFrame, $server): array {
            if ($replyTo === null) {
                return [];
            }

            if (str_starts_with($subject, '$JS.API.CONSUMER.MSG.NEXT.')) {
                // Only the first pull finds a message; the server holds later ones until they expire.
                return ++$server->pulls === 1
                    ? [$slowFrame . ReconnectingTransport::msgFrame('evt.s', (int) $transport->sidFor($replyTo), 'm1', '$JS.ACK.S.C.1.1.1.0.0')]
                    : [];
            }

            return $transport->replyFrame($replyTo, 'ok');
        };
        $queue = $operation !== 'fetchBatch' ? $client->subscribeQueue('jobs')->await() : null;

        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop);
        $this->awaitRead($watched);

        if ($queue !== null) {
            // The chunk holds the operation's message behind slow's, and the application's read is held up in slow.
            $transport->pushFrame($slowFrame . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
            $hold->began->getFuture()->await(new TimeoutCancellation(2));
        }

        $readsBefore = $watched->reads;
        [$payloads, $elapsed] = $this->runOperation($operation, $client, $queue);
        $heldWhenDone = $hold->held;
        $reads = $watched->reads - $readsBefore;
        $hold->end('the end of the test');
        $stop->cancel();

        self::assertSame([$operation === 'fetchBatch' ? 'm1' : 'job-1'], $payloads, 'the operation got its queued message');
        self::assertTrue($heldWhenDone, 'the operation returned before the handler ahead of it was let go');
        self::assertSame(0, $reads, 'the operation took its queued message without a socket read of its own');
        self::assertLessThan(1.0, $elapsed, sprintf('the operation took %.3f s', $elapsed));
    }

    /**
     * Only the operation's own subscription is delivered: the chunk holds "trigger" (the lowest sid), then "other", then
     * the poll's "jobs". trigger's handler polls jobs and gets job-1 while "other", queued between them, stays undelivered
     * - delivered only once trigger's handler returns and the read's own delivery pass reaches it. The poll reorders its
     * own message ahead of the handler it runs in, nothing else.
     */
    public function testTheOwnSidDeliveryLeavesTheOtherSubscriptionsMessagesQueued(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->connectClient($transport);
        $order = new class {
            /** @var list<string> */
            public array $list = [];
            public ?SubscriptionQueue $queue = null;
        };
        $triggerSid = $client->subscribe('trigger', static function () use ($order): void {
            $queue = $order->queue;
            if ($queue === null) {
                return;
            }

            $message = $queue->setTimeout(1.0)->next();
            $order->list[] = 'poll:' . ($message === null ? 'null' : $message->payload);
        })->await();
        $otherSid = $client->subscribe('other', static function (NatsMessage $message) use ($order): void {
            $order->list[] = 'other:' . $message->payload;
        })->await();
        $order->queue = $client->subscribeQueue('jobs')->await();

        $transport->pushFrame(
            ReconnectingTransport::msgFrame('trigger', $triggerSid, 't')
            . ReconnectingTransport::msgFrame('other', $otherSid, 'o')
            . ReconnectingTransport::msgFrame('jobs', $order->queue->sid, 'job-1'),
        );
        $client->processIncoming(new TimeoutCancellation(3))->await();

        self::assertSame(['poll:job-1', 'other:o'], $order->list, 'the poll took only jobs; other was delivered after the handler returned');
    }

    /**
     * An overflow of the operation's own subscription met this way still fails the operation, as it did when the read
     * delivered the message (SlowConsumerPolicy::Error). The chunk holds "trigger" and then two messages for its own
     * queue, whose buffer of one overflows on the second: the poll from trigger's handler throws the overflow for its
     * own sid.
     */
    public function testAnOverflowOfTheOwnSubscriptionMetThisWayFailsThePoll(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->watchedClient(new WatchedTransport($transport), [
            'maxPendingMessagesPerSubscription' => 1,
            'slowConsumerPolicy' => SlowConsumerPolicy::Error,
        ]);
        $box = new class {
            public ?SubscriptionQueue $queue = null;
            public ?string $thrown = null;
        };
        $triggerSid = $client->subscribe('trigger', static function () use ($box): void {
            $queue = $box->queue;
            if ($queue === null) {
                return;
            }

            try {
                $queue->setTimeout(1.0)->next();
            } catch (SlowConsumerException $overflow) {
                $box->thrown = 'overflow sid ' . $overflow->sid;
            }
        })->await();
        $box->queue = $client->subscribeQueue('jobs')->await();
        // Leave the connection-level queue unbounded so both j1 and j2 reach the poll's delivery; the queue's own
        // polling buffer (one message) is what overflows when the poll delivers them.
        $client->markSubscriptionUnbounded($box->queue->sid);

        $transport->pushFrame(
            ReconnectingTransport::msgFrame('trigger', $triggerSid, 't')
            . ReconnectingTransport::msgFrame('jobs', $box->queue->sid, 'j1')
            . ReconnectingTransport::msgFrame('jobs', $box->queue->sid, 'j2'),
        );

        try {
            $client->processIncoming(new TimeoutCancellation(3))->await();
        } catch (\Throwable) {
            // The handler's throw leaves the read, which is the application's own: not the subject of this test.
        }

        self::assertSame('overflow sid ' . $box->queue->sid, $box->thrown, 'the poll threw its own subscription\'s overflow');
    }

    /**
     * The own-sid delivery is skipped while that sid's own delivery is under way further up the stack: it never invokes
     * a handler on top of itself. With the sid marked as being dispatched, deliverQueuedForOwnSid() delivers nothing and
     * leaves the message queued for the suspended delivery to continue with.
     */
    public function testTheOwnSidDeliveryIsSkippedWhileThatSidIsBeingDelivered(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $sid = $connection->subscribe('jobs', static function (): void {})->await();
        $queued = $this->queueMessageForSid($connection, $sid, 'job-1');

        // As if a handler of this sid were on the stack (a drop report's error listener that polls, say).
        $this->setPrivate($connection, 'dispatchingSids', [$sid => true]);
        $delivered = $this->invokePrivate($connection, 'deliverQueuedForOwnSid', $sid, false, false);

        self::assertFalse($delivered, 'nothing is delivered while the sid is being delivered');
        self::assertFalse($queued->isEmpty(), 'the message stays queued for the suspended delivery');
    }

    /**
     * And skipped while a disconnect() is closing the connection: its queued messages are the close's to discard, not an
     * operation's to deliver ({@see NatsConnection::leftoversBelongToAClose()}). With close-intent set, the delivery is a
     * no-op and the message is left for the close.
     */
    public function testTheOwnSidDeliveryIsSkippedWhileADisconnectIsClosing(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $sid = $connection->subscribe('jobs', static function (): void {})->await();
        $queued = $this->queueMessageForSid($connection, $sid, 'job-1');

        // disconnect() sets this before it closes the socket; the state stays Open here, so it is not a drain.
        $this->setPrivate($connection, 'closing', true);
        $delivered = $this->invokePrivate($connection, 'deliverQueuedForOwnSid', $sid, false, false);

        self::assertFalse($delivered, 'nothing is delivered while a disconnect() is closing');
        self::assertFalse($queued->isEmpty(), 'the message is left for the close to discard');
    }

    /**
     * request() is unchanged: its reply comes on the one mux inbox every request shares, so it reads with no own sid and
     * never takes a queued delivery ahead of its reply. A request whose reply is queued behind a held-up handler in the
     * application's read still gets it from that read, in order.
     */
    public function testRequestIsUnchangedByTheOwnSidDelivery(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->watchedClient($watched);
        $hold = new HeldUpDelivery(fallbackSeconds: 1.0);
        $slowSid = $client->subscribe('slow', $hold->handler())->await();
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport, $slowSid): array {
            if ($replyTo === null) {
                return [];
            }

            if ($subject === 'svc.warm') {
                return $transport->replyFrame($replyTo, 'ok');
            }

            // The reply comes behind slow's message, which holds the application's read up.
            return [ReconnectingTransport::msgFrame('slow', $slowSid, 's') . implode('', $transport->replyFrame($replyTo, 'reply-1'))];
        };
        // Sets the reply inbox up before the application's read starts.
        $client->request('svc.warm', 'x', 1_000)->await();

        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop);
        $this->awaitRead($watched);

        $reply = $client->request('svc.echo', 'hi', 3_000)->await(new TimeoutCancellation(3));
        $hold->end('the end of the test');
        $stop->cancel();

        self::assertSame('reply-1', $reply->payload, 'the request got its reply, delivered behind the held-up handler');
    }

    /** @return iterable<string, array{bool}> */
    public static function failOperationsOption(): iterable
    {
        yield 'slowConsumerErrorsFailOperations off' => [false];
        yield 'slowConsumerErrorsFailOperations on' => [true];
    }

    /**
     * An own-sid overflow met by the take follows deliverPending()'s rules for the option that governs it: it is thrown
     * to the poll either way, and reported through the error listener as well only with
     * {@see NatsOptions::$slowConsumerErrorsFailOperations}, as a SubscriptionQueue's overflow always was before that
     * option existed. The first message stays in the buffer; the second is the one dropped.
     */
    #[DataProvider('failOperationsOption')]
    public function testAnOwnSidOverflowTakenThisWayIsThrownAndReportedOnlyWithTheOption(bool $option): void
    {
        $transport = new ReconnectingTransport();
        $reported = [];
        $client = $this->watchedClient(new WatchedTransport($transport), [
            'maxPendingMessagesPerSubscription' => 1,
            'slowConsumerPolicy' => SlowConsumerPolicy::Error,
            'slowConsumerErrorsFailOperations' => $option,
            'errorListener' => static function (\Throwable $error) use (&$reported): void {
                if ($error instanceof SlowConsumerException) {
                    $reported[] = 'overflow sid ' . $error->sid;
                }
            },
        ]);
        $box = new class {
            public ?SubscriptionQueue $queue = null;
            public ?string $thrown = null;
        };
        $triggerSid = $client->subscribe('trigger', static function () use ($box): void {
            $queue = $box->queue;
            if ($queue === null) {
                return;
            }

            try {
                $queue->setTimeout(1.0)->next();
            } catch (SlowConsumerException $overflow) {
                $box->thrown = 'overflow sid ' . $overflow->sid;
            }
        })->await();
        $box->queue = $client->subscribeQueue('jobs')->await();
        $client->markSubscriptionUnbounded($box->queue->sid);

        $transport->pushFrame(
            ReconnectingTransport::msgFrame('trigger', $triggerSid, 't')
            . ReconnectingTransport::msgFrame('jobs', $box->queue->sid, 'j1')
            . ReconnectingTransport::msgFrame('jobs', $box->queue->sid, 'j2'),
        );
        $client->processIncoming(new TimeoutCancellation(3))->await();

        self::assertSame('overflow sid ' . $box->queue->sid, $box->thrown, 'the poll threw its own overflow');
        self::assertSame($option ? ['overflow sid ' . $box->queue->sid] : [], $reported, 'reported as well only with the option');
        self::assertSame(1, $box->queue->droppedCount());
        self::assertSame('j1', $box->queue->fetch()?->payload, 'the first message stayed in the buffer');
    }

    /**
     * An own-sid handler that throws fails the read that took the message, leaves the rest of that subscription queued
     * and the sid dirty, and the next take continues with it: a handler is never left a message it could have had, and
     * one that throws does not cost the messages behind it.
     */
    public function testAnOwnSidHandlerFailureFailsTheReadAndLeavesTheRestQueued(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $connection = $this->connectWatchedConnection($watched);
        $seen = [];
        $sid = $connection->subscribe('own', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = $message->payload;
            if ($message->payload === 'a') {
                throw new \RuntimeException('boom');
            }
        })->await();
        $this->queueMessageForSid($connection, $sid, 'a');
        $this->queueMessageForSid($connection, $sid, 'b');

        $readsBefore = $watched->reads;
        try {
            $connection->readIncomingForOperation(new TimeoutCancellation(1), $sid)->await();
            self::fail('the own-sid handler failure should fail the read');
        } catch (\RuntimeException $e) {
            self::assertSame('boom', $e->getMessage());
        }

        self::assertSame(['a'], $seen);
        self::assertSame(0, $watched->reads - $readsBefore, 'no socket read');
        /** @var array<int, \SplQueue<NatsMessage>> $pending */
        $pending = $this->getPrivate($connection, 'pendingMessages');
        self::assertSame(1, $pending[$sid]->count(), 'b stays queued');
        self::assertArrayHasKey($sid, $this->getPrivate($connection, 'pendingDirty'), 'the sid stays dirty');
        self::assertSame([], $this->getPrivate($connection, 'dispatchingSids'));

        $start = hrtime(true);
        $result = $connection->readIncomingForOperation(new TimeoutCancellation(1), $sid)->await();
        $elapsed = (hrtime(true) - $start) / 1e9;
        self::assertInstanceOf(IncomingChunkResult::class, $result);
        self::assertSame(0, $result->frames);
        self::assertFalse($result->consumedBytes);
        self::assertSame(['a', 'b'], $seen, 'the next take continued with the rest of the subscription');
        self::assertLessThan(0.5, $elapsed);
        self::assertArrayNotHasKey($sid, $this->getPrivate($connection, 'pendingDirty'));
        self::assertSame(0, $watched->reads - $readsBefore, 'still no socket read');
    }

    /**
     * After an own-sid take only the other subscription stays dirty, its wake untouched; the own sid is clean, its empty
     * queue kept for reuse (#139), its wake fired, and no sid is left marked as being dispatched.
     */
    public function testTheDirtySetAndTheWakesAfterAnOwnSidTake(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $connection = $this->connectWatchedConnection($watched);
        $seen = [];
        $ownSid = $connection->subscribe('own', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'own:' . $message->payload;
        })->await();
        $otherSid = $connection->subscribe('other', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'other:' . $message->payload;
        })->await();
        $this->queueMessageForSid($connection, $otherSid, 'o');
        $this->queueMessageForSid($connection, $ownSid, 'x');
        /** @var Cancellation $otherWake */
        $otherWake = $this->invokePrivate($connection, 'nextDeliveryTo', $otherSid);

        $readsBefore = $watched->reads;
        $result = $connection->readIncomingForOperation(new TimeoutCancellation(1), $ownSid)->await();

        self::assertSame(0, $result->frames);
        self::assertFalse($result->consumedBytes);
        self::assertSame(['own:x'], $seen, 'only the own sid was delivered');
        self::assertSame(0, $watched->reads - $readsBefore, 'no socket read');
        /** @var array<int, bool> $dirty */
        $dirty = $this->getPrivate($connection, 'pendingDirty');
        self::assertSame([$otherSid => true], $dirty, 'the other sid stays dirty, the own sid does not');
        /** @var array<int, \SplQueue<NatsMessage>> $pending */
        $pending = $this->getPrivate($connection, 'pendingMessages');
        self::assertArrayHasKey($ownSid, $pending, 'the empty queue is kept for reuse (#139)');
        self::assertTrue($pending[$ownSid]->isEmpty());
        self::assertSame(1, $pending[$otherSid]->count());
        self::assertSame([], $this->getPrivate($connection, 'dispatchingSids'));
        /** @var array<int, DeferredCancellation> $wakes */
        $wakes = $this->getPrivate($connection, 'deliveryWakes');
        self::assertArrayNotHasKey($ownSid, $wakes, 'the own sid wake fired');
        self::assertArrayHasKey($otherSid, $wakes, 'the other sid wake is untouched');
        self::assertFalse($otherWake->isRequested(), 'the other sid wake was not fired');
    }

    /** @return iterable<string, array{string, string}> */
    public static function chunkShapes(): iterable
    {
        // t = trigger (the polling handler), j = jobs (its queue), o = other. The first list is the order of
        // subscription (sid order), the second the order on the wire.
        yield 'sids t<j, wire [t j]' => ['t,j', 't,j'];
        yield 'sids t<j, wire [j t]' => ['t,j', 'j,t'];
        yield 'sids j<t, wire [t j]' => ['j,t', 't,j'];
        yield 'sids j<t, wire [j t]' => ['j,t', 'j,t'];
        yield 'sids o<t<j, wire [o t j] (trigger in the middle)' => ['o,t,j', 'o,t,j'];
        yield 'sids o<t<j, wire [j o t] (trigger last)' => ['o,t,j', 'j,o,t'];
        yield 'sids t<o<j, wire [o j t] (trigger last on the wire, first by sid)' => ['t,o,j', 'o,j,t'];
        yield 'sids j<o<t, wire [t o j]' => ['j,o,t', 't,o,j'];
        yield 'sids o<j<t, wire [t j o]' => ['o,j,t', 't,j,o'];
    }

    /**
     * #179 whatever the positions: the handler on "trigger" polls the "jobs" queue, and the queue's message is in the
     * same chunk as the handler's own, wherever each sits by sid and on the wire. The poll gets its message within half
     * a second of its 1 s timeout and without a socket read in every shape.
     */
    #[DataProvider('chunkShapes')]
    public function testAHandlerPollingItsQueueGetsItsMessageWhateverThePositions(string $sidOrder, string $wireOrder): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->watchedClient($watched);
        $box = new class {
            public ?SubscriptionQueue $queue = null;
            public mixed $payload = 'UNSET';
            public int $readsDuring = -1;
            public float $elapsed = -1.0;
        };
        $sids = [];
        foreach (explode(',', $sidOrder) as $who) {
            $sids[$who] = match ($who) {
                't' => $client->subscribe('trigger', static function () use ($box, $watched): void {
                    $queue = $box->queue;
                    if ($queue === null) {
                        return;
                    }

                    $readsBefore = $watched->reads;
                    $start = hrtime(true);
                    $box->payload = $queue->setTimeout(1.0)->next()?->payload;
                    $box->elapsed = (hrtime(true) - $start) / 1e9;
                    $box->readsDuring = $watched->reads - $readsBefore;
                })->await(),
                'o' => $client->subscribe('other', static function (): void {})->await(),
                'j' => ($box->queue = $client->subscribeQueue('jobs')->await())->sid,
                default => throw new \LogicException('unknown token ' . $who),
            };
        }

        $chunk = '';
        foreach (explode(',', $wireOrder) as $who) {
            $chunk .= match ($who) {
                't' => ReconnectingTransport::msgFrame('trigger', $sids['t'], 't'),
                'o' => ReconnectingTransport::msgFrame('other', $sids['o'], 'o'),
                'j' => ReconnectingTransport::msgFrame('jobs', $sids['j'], 'job-1'),
                default => throw new \LogicException('unknown token ' . $who),
            };
        }
        $transport->pushFrame($chunk);
        $client->processIncoming(new TimeoutCancellation(3))->await();

        self::assertSame('job-1', $box->payload, sprintf('shape %s / %s', $sidOrder, $wireOrder));
        self::assertLessThan(0.5, $box->elapsed, sprintf('the poll took %.3f s', $box->elapsed));
        self::assertSame(0, $box->readsDuring, 'no socket read was needed');
    }

    /**
     * The boundary of the take while a reconnect is under way (#179, by design): the take sits after the read's wait for
     * a reconnect, next to the in-order leftover delivery, so a poll whose message is already queued does not take it
     * during an outage - it waits for the reconnect within its own timeout, as before 2.13.0, and takes the message once
     * the connection is back. Here the message is queued behind a handler held up in the application's read, the
     * connection drops with dials refused, and a poll with a 0.3 s timeout returns null while the connection is still
     * Connecting and the handler still held; once dials are accepted, a poll returns the message.
     */
    public function testAnOwnSidPollDuringAReconnectWaitsForTheReconnect(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->watchedClient($watched, [
            'reconnectEnabled' => true,
            'maxReconnectAttempts' => 1_000,
            'reconnectDelayMs' => 5,
            'reconnectMaxDelayMs' => 20,
            'reconnectJitterMs' => 0,
            'waitForReconnect' => true,
        ]);
        $hold = new HeldUpDelivery(fallbackSeconds: 5.0);
        $slowSid = $client->subscribe('slow', $hold->handler())->await();
        $queue = $client->subscribeQueue('jobs')->await();

        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop);
        $this->awaitRead($watched);
        $transport->pushFrame(ReconnectingTransport::msgFrame('slow', $slowSid, 's') . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
        $hold->began->getFuture()->await(new TimeoutCancellation(2));
        // Another fiber's read starts the reconnect; dials are refused, so the connection stays Connecting.
        $this->startRecoveryInBackground($client, $transport);
        /** @var array<int, bool> $dirty */
        $dirty = $this->getPrivate($this->connectionOf($client), 'pendingDirty');
        self::assertArrayHasKey($queue->sid, $dirty, 'job-1 is queued for the poll when it starts');

        $duringOutage = $queue->setTimeout(0.3)->next()?->payload;
        $stillHeld = $hold->held;
        $state = $client->state();

        self::assertNull($duringOutage, 'the take does not run ahead of the reconnect wait: the poll waits for the reconnect');
        self::assertSame(ConnectionState::Connecting, $state, 'the connection is still reconnecting');
        self::assertTrue($stillHeld, 'the handler ahead of the message was still held');

        // Once the connection is back, the poll takes the message that was queued before the drop.
        $transport->acceptDials();
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open, 3.0);
        $afterReconnect = $queue->setTimeout(2.0)->next()?->payload;
        $hold->end('the end of the test');
        $stop->cancel();

        self::assertSame('job-1', $afterReconnect, 'the message queued before the drop is taken once the connection is back');
    }

    /** A NatsConnection connected through $watched, for the reflection-level own-sid tests. */
    private function connectWatchedConnection(WatchedTransport $watched): NatsConnection
    {
        $connection = $this->own(new NatsConnection(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: 2_000,
            pingIntervalSeconds: 0,
        ), $watched));
        $this->opened[] = $connection;
        $connection->connect()->await();

        return $connection;
    }

    private function connectionOf(NatsClient $client): NatsConnection
    {
        /** @var NatsConnection $connection */
        $connection = $this->getPrivate($client, 'connection');

        return $connection;
    }

    /**
     * The polling buffer of $queue, by reflection: for an assertion on what the own-sid delivery left queued.
     *
     * @return \SplQueue<NatsMessage>
     */
    private function queueMessageForSid(NatsConnection $connection, int $sid, string $payload): \SplQueue
    {
        /** @var array<int, \SplQueue<NatsMessage>> $pending */
        $pending = $this->getPrivate($connection, 'pendingMessages');
        /** @var \SplQueue<NatsMessage> $queue */
        $queue = $pending[$sid] ?? new \SplQueue();
        $queue->enqueue(new NatsMessage('jobs', $sid, null, $payload));
        $pending[$sid] = $queue;
        $this->setPrivate($connection, 'pendingMessages', $pending);

        /** @var array<int, bool> $dirty */
        $dirty = $this->getPrivate($connection, 'pendingDirty');
        $dirty[$sid] = true;
        $this->setPrivate($connection, 'pendingDirty', $dirty);

        return $queue;
    }

    /**
     * A client connected through $watched.
     *
     * @param array<string, mixed> $overrides NatsOptions arguments.
     */
    private function watchedClient(WatchedTransport $watched, array $overrides = []): NatsClient
    {
        $client = $this->own(new NatsClient(new NatsOptions(...array_merge([
            'connectTimeoutMs' => 500,
            'requestTimeoutMs' => 2_000,
            'pingIntervalSeconds' => 0,
        ], $overrides)), $watched));
        $this->opened[] = $client;
        $client->connect()->await();

        return $client;
    }

    /** Waits until a read takes the socket of $watched. */
    private function awaitRead(WatchedTransport $watched): void
    {
        /** @var DeferredFuture<null> $reading */
        $reading = new DeferredFuture();
        $watched->onRead = static function () use ($reading): void {
            if (!$reading->isComplete()) {
                $reading->complete();
            }
        };
        $reading->getFuture()->await(new TimeoutCancellation(2));
        $watched->onRead = null;
    }

    /**
     * Starts the application's own read loop, processIncoming() over and over on a fiber of its own, until $stop is
     * cancelled or a read fails.
     */
    private function startApplicationReadLoop(NatsClient $client, DeferredCancellation $stop): void
    {
        async(static function () use ($client, $stop): void {
            while (!$stop->isCancelled()) {
                try {
                    $client->processIncoming($stop->getCancellation())->await();
                } catch (\Throwable) {
                    return;
                }
            }
        })->ignore();
    }

    /**
     * Runs $operation and returns its payloads and the seconds it took.
     *
     * @return array{list<string>, float}
     */
    private function runOperation(string $operation, NatsClient $client, ?SubscriptionQueue $queue): array
    {
        $start = hrtime(true);
        $payloads = match ($operation) {
            'next' => (static function () use ($queue): array {
                $message = self::queueOf($queue)->setTimeout(3.0)->next();

                return $message === null ? [] : [$message->payload];
            })(),
            'fetchAll' => self::payloads(self::queueOf($queue)->setTimeout(3.0)->fetchAll(1)),
            'fetchBatch' => self::payloads($client->jetStream()->fetchBatch('S', 'C', 1, 2_000)->await()),
            default => throw new \LogicException('Unknown operation ' . $operation),
        };

        return [$payloads, (hrtime(true) - $start) / 1e9];
    }

    private static function queueOf(?SubscriptionQueue $queue): SubscriptionQueue
    {
        self::assertNotNull($queue);

        return $queue;
    }

    /**
     * @param list<NatsMessage> $messages
     * @return list<string>
     */
    private static function payloads(array $messages): array
    {
        return array_map(static fn(NatsMessage $message): string => $message->payload, $messages);
    }

    private function setPrivate(object $object, string $property, mixed $value): void
    {
        (new \ReflectionProperty($object, $property))->setValue($object, $value);
    }

    private function getPrivate(object $object, string $property): mixed
    {
        return (new \ReflectionProperty($object, $property))->getValue($object);
    }

    private function invokePrivate(object $object, string $method, mixed ...$args): mixed
    {
        return (new \ReflectionMethod($object, $method))->invoke($object, ...$args);
    }
}
