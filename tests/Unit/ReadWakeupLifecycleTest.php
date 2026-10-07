<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Tests\Support\CountingBuffer;
use IDCT\NATS\Tests\Support\HeldUpDelivery;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;

/**
 * The wake-up of an operation's read ({@see ReadWakeupTest}) through the life of the subscription it belongs to, and
 * beside other pollers. It fires however the delivery ends, the subscription's removal in the same pass included; it
 * is only ever a reason to look again; and deliveries keep the wire order across subscriptions.
 *
 *  - A subscription armed to auto-unsubscribe, or drained, is removed in the pass that delivers its last message, and
 *    its state goes with it. A count of its deliveries noted before the wait would read as its noted value again after
 *    that pass, and a poll comparing counts would read the socket with its message in the queue. The wake-up is an
 *    object that the pass fires once, so the poll returns.
 *  - The handler of a SubscriptionQueue can suspend: under DropOldest its drop report calls the error listener, which
 *    may await. The wake-up fires once the pass is over, and the poll gets its message then.
 *  - A dispatch held up in the write of a PONG, before the poll's message is queued, is not overtaken: the lower sid's
 *    handler runs first, as on the wire, and the poll returns once the dispatch reaches its message. Once the poll's
 *    message is already queued, behind a held-up error listener, the poll takes it ahead of another subscription's
 *    message queued with it (#179): order is only ever kept within a subscription.
 *  - A second poller of the same queue, or a lifecycle event while a poll is parked, leaves the poll with nothing
 *    wrong: it looks again and waits on, or ends with the connection, and never spins.
 */
final class ReadWakeupLifecycleTest extends TestCase
{
    use ReconnectScenarios;

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{string, ?int, bool}> */
    public static function pollsArmedToAutoUnsubscribe(): iterable
    {
        yield 'next(), auto-unsubscribe after 1, its first message' => ['next', 1, false];
        yield 'fetchAll(1), auto-unsubscribe after 1, its first message' => ['fetchAll', 1, false];
        yield 'next(), auto-unsubscribe after 2, one delivered earlier' => ['next', 2, true];
        yield 'guard: next(), no auto-unsubscribe' => ['next', null, false];
    }

    /**
     * The application's read is held up in a handler of a lower sid that awaits, with the poll's message queued behind
     * it, and the queue is armed to auto-unsubscribe once that message is in. The poll takes the queued message at once,
     * without a socket read and before the handler ahead of it is let go (#179), and the delivery that hands it over
     * removes the subscription and its state, delivered count included. A poll comparing delivered counts found the
     * count back at the value it had noted; the wake-up is an object the pass fires once, so the own-sid delivery ends
     * the poll cleanly even as the subscription goes.
     */
    #[DataProvider('pollsArmedToAutoUnsubscribe')]
    public function testAPollArmedToAutoUnsubscribeGetsItsMessageQueuedBehindAHandlerThatAwaits(string $poll, ?int $max, bool $earlier): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->watchedClient($watched);
        $hold = new HeldUpDelivery(fallbackSeconds: 2.0);
        $slowSid = $client->subscribe('slow', $hold->handler())->await();
        $queue = $client->subscribeQueue('jobs')->await();
        if ($max !== null) {
            $client->unsubscribe($queue->sid, $max)->await();
        }

        if ($earlier) {
            $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-0'));
            self::assertSame('job-0', $queue->setTimeout(1.0)->next()?->payload);
        }

        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop);
        $this->awaitRead($watched);
        $transport->pushFrame(ReconnectingTransport::msgFrame('slow', $slowSid, 's') . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
        $hold->began->getFuture()->await(new TimeoutCancellation(2));

        $readsBefore = $watched->reads;
        $start = hrtime(true);
        $result = $poll === 'next'
            ? $queue->setTimeout(3.0)->next()?->payload
            : self::payloads($queue->setTimeout(3.0)->fetchAll(1));
        $elapsed = $this->secondsSince($start);
        $heldWhenDone = $hold->held;
        $reads = $watched->reads - $readsBefore;
        $hold->end('the end of the test');
        $stop->cancel();

        self::assertSame($poll === 'next' ? 'job-1' : ['job-1'], $result);
        self::assertTrue($heldWhenDone, 'the poll returned before the handler ahead of it was let go (#179)');
        self::assertSame(0, $reads, 'the poll delivered its queued message without reading the socket');
        self::assertLessThan(1.0, $elapsed, sprintf('the poll took %.3f s of its 3 s timeout', $elapsed));
        self::assertSame($max === null, $client->isSubscriptionActive($queue->sid), 'the auto-unsubscribe removed the subscription');
    }

    /** @return iterable<string, array{string, ?int}> */
    public static function pollsArmedToAutoUnsubscribeAndAProcessIncoming(): iterable
    {
        yield 'next(), auto-unsubscribe after 1' => ['next', 1];
        yield 'fetchAll(1), auto-unsubscribe after 1' => ['fetchAll', 1];
        yield 'guard: next(), no auto-unsubscribe' => ['next', null];
        yield 'guard: fetchAll(1), no auto-unsubscribe' => ['fetchAll', null];
    }

    /**
     * The hop window with the queue armed to auto-unsubscribe after one message: the message is on the socket, and a
     * processIncoming() starts 0 to 16 event-loop hops before or after the poll. Where that read delivers the message,
     * and removes the subscription, between the poll's look and the start of its read, the poll still returns at once.
     */
    #[DataProvider('pollsArmedToAutoUnsubscribeAndAProcessIncoming')]
    public function testAPollArmedToAutoUnsubscribeEndsPromptlyWhateverTheHopsToAProcessIncoming(string $poll, ?int $max): void
    {
        $late = [];
        for ($hops = -16; $hops <= 16; $hops++) {
            $transport = new ReconnectingTransport();
            $client = $this->connectClient($transport);
            $queue = $client->subscribeQueue('jobs')->await();
            if ($max !== null) {
                $client->unsubscribe($queue->sid, $max)->await();
            }
            $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));

            $run = new class {
                /** @var Future<list<string>>|null */
                public ?Future $poll = null;
            };
            $startPoll = static function () use ($run, $poll, $queue): void {
                $run->poll = async(static fn(): array => $poll === 'next'
                    ? self::payloads(array_filter([$queue->setTimeout(1.5)->next()]))
                    : self::payloads($queue->setTimeout(1.5)->fetchAll(1)));
            };
            $startOther = static function () use ($client): void {
                $client->processIncoming(new TimeoutCancellation(3))->ignore();
            };
            [$first, $second] = $hops < 0 ? [$startPoll, $startOther] : [$startOther, $startPoll];
            $start = hrtime(true);
            $first();
            $made = new DeferredFuture();
            self::afterHops(abs($hops), static function () use ($second, $made): void {
                $second();
                $made->complete();
            });
            $made->getFuture()->await();
            self::assertNotNull($run->poll);
            $result = $run->poll->await();
            $elapsed = $this->secondsSince($start);
            if ($result !== ['job-1'] || $elapsed >= 0.5) {
                $late[] = sprintf('processIncoming() %d hop(s) %s the poll: %s after %.3f s', abs($hops), $hops < 0 ? 'after' : 'before', json_encode($result), $elapsed);
            }

            $client->disconnect()->await();
        }

        self::report(sprintf('%s, auto-unsubscribe %s: %d of 33 runs late%s', $poll, $max ?? 'none', count($late), $late === [] ? '' : "\n    " . implode("\n    ", $late)));
        self::assertSame([], $late);
    }

    /**
     * The same removal through drainSubscription(): the application's read is held up in a handler of a lower sid with
     * the poll's message queued behind it, the poll's queue is drained, and the poll starts 0 to 40 hops after the
     * drain. The drain's flush delivers the message and then removes the subscription; a poll whose read looked after
     * that still returns the message at once.
     */
    public function testAPollGetsItsMessageWhenItsSubscriptionIsDrainedAroundIt(): void
    {
        $late = [];
        for ($hops = 0; $hops <= 40; $hops++) {
            $transport = new ReconnectingTransport();
            $watched = new WatchedTransport($transport);
            $client = $this->watchedClient($watched);
            $hold = new HeldUpDelivery(fallbackSeconds: 2.0);
            $slowSid = $client->subscribe('slow', $hold->handler())->await();
            $queue = $client->subscribeQueue('jobs')->await();
            $stop = new DeferredCancellation();
            $this->startApplicationReadLoop($client, $stop);
            $this->awaitRead($watched);
            $transport->pushFrame(ReconnectingTransport::msgFrame('slow', $slowSid, 's') . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
            $hold->began->getFuture()->await(new TimeoutCancellation(2));

            $drained = $client->drainSubscription($queue->sid);
            $drained->ignore();
            $run = new class {
                /** @var Future<?string>|null */
                public ?Future $poll = null;
            };
            $made = new DeferredFuture();
            $start = hrtime(true);
            self::afterHops($hops, static function () use ($run, $made, $queue): void {
                $run->poll = async(static fn(): ?string => $queue->setTimeout(3.0)->next()?->payload);
                $made->complete();
            });
            $made->getFuture()->await();
            self::assertNotNull($run->poll);
            $payload = $run->poll->await();
            $elapsed = $this->secondsSince($start);
            $hold->end('the end of the run');
            $stop->cancel();
            if ($payload !== 'job-1' || $elapsed >= 1.0) {
                $late[] = sprintf('the poll %d hop(s) after drainSubscription(): %s after %.3f s', $hops, $payload ?? 'null', $elapsed);
            }

            $client->disconnect()->await();
        }

        self::report(sprintf('drainSubscription() around the poll: %d of 41 runs late%s', count($late), $late === [] ? '' : "\n    " . implode("\n    ", $late)));
        self::assertSame([], $late);
    }

    /** @return iterable<string, array{?int}> */
    public static function autoUnsubscribeThroughAReconnect(): iterable
    {
        yield 'auto-unsubscribe after 1' => [1];
        yield 'guard: no auto-unsubscribe' => [null];
    }

    /**
     * The delivery after a reconnect with the queue armed to auto-unsubscribe after one message: the application's read
     * holds the socket when the connection drops at +20 ms, dials are refused until +80 ms, and the server sends the
     * poll's message right after the replayed SUB. The reconnect's delivery hands the message over and removes the
     * subscription, and the poll waiting for the reconnect returns the message.
     */
    #[DataProvider('autoUnsubscribeThroughAReconnect')]
    public function testAPollArmedToAutoUnsubscribeGetsWhatTheDeliveryAfterAReconnectBrings(?int $max): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->connectClient($transport);
        $queue = $client->subscribeQueue('jobs')->await();
        if ($max !== null) {
            $client->unsubscribe($queue->sid, $max)->await();
        }
        $server = new class {
            public bool $sent = false;
        };
        $transport->afterWrite = static function () use ($transport, $server): void {
            if ($server->sent || $transport->epoch() < 1) {
                return;
            }

            // The reconnect has replayed the SUB.
            $sid = $transport->sidFor('jobs');
            if ($sid === null) {
                return;
            }

            $server->sent = true;
            $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $sid, 'job-1'));
        };
        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop);
        delay(0.01);
        $transport->refuseDials();
        EventLoop::delay(0.02, static fn() => $transport->dropConnection());
        EventLoop::delay(0.08, static fn() => $transport->acceptDials());

        $start = hrtime(true);
        $result = $queue->setTimeout(3.0)->next()?->payload;
        $elapsed = $this->secondsSince($start);
        $stop->cancel();

        self::assertSame(1, $transport->epoch(), 'the connection was reconnected once');
        self::assertTrue($server->sent, 'the server sent the message right after the replayed SUB');
        self::assertSame('job-1', $result);
        self::assertLessThan(1.0, $elapsed, sprintf('the poll took %.3f s of its 3 s timeout', $elapsed));
    }

    /** @return iterable<string, array{bool}> */
    public static function dropReportListeners(): iterable
    {
        yield 'the listener returns once a read takes the socket' => [true];
        yield 'the listener returns after 0.3 s' => [false];
    }

    /**
     * The own handler of a SubscriptionQueue can suspend: under DropOldest its drop report calls the error listener,
     * after it took the oldest message out of the buffer and before it put the new one in. A listener that awaits (an
     * HTTP alert, an async logger) leaves the queue's delivery under way with the buffer empty, so a poll that comes in
     * finds nothing and reads the socket. The wake-up fires once the pass is over, and the poll returns the message
     * then; it used to return it only at its 3 s timeout.
     */
    #[DataProvider('dropReportListeners')]
    public function testAPollGetsTheMessageWhoseDropReportIsHeldUp(bool $endedByARead): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $hold = new HeldUpDelivery(fallbackSeconds: $endedByARead ? 2.0 : 0.3);
        $client = $this->watchedClient($watched, [
            'maxPendingMessagesPerSubscription' => 1,
            'errorListener' => static function (\Throwable $error) use ($hold): void {
                if (str_contains($error->getMessage(), 'dropped oldest')) {
                    $hold->holdUp();
                }
            },
        ]);
        $queue = $client->subscribeQueue('jobs')->await();

        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop);
        $this->awaitRead($watched);
        // job-0 fills the queue's buffer, which nobody polls yet; the loop's read then waits on the socket again.
        $readsBefore = $watched->reads;
        $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-0'));
        $this->waitUntil(static fn(): bool => $watched->reads > $readsBefore);
        // job-1 overflows it: the drop report's listener holds the delivery up, with the buffer empty.
        $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
        $hold->began->getFuture()->await(new TimeoutCancellation(2));
        if ($endedByARead) {
            $watched->onRead = $hold->endWhenARead();
        }

        $start = hrtime(true);
        $result = $queue->setTimeout(3.0)->next()?->payload;
        $elapsed = $this->secondsSince($start);
        $endedBy = $hold->endedBy;
        $hold->end('the end of the test');
        $stop->cancel();

        self::assertSame($endedByARead ? 'a read taking the socket' : 'its own time running out', $endedBy);
        self::assertSame(1, $queue->droppedCount(), 'job-0 was dropped for job-1');
        self::assertSame('job-1', $result);
        self::assertLessThan(1.0, $elapsed, sprintf('the poll took %.3f s of its 3 s timeout', $elapsed));
    }

    /**
     * Deliveries keep the wire order across subscriptions while a dispatch is held up BEFORE the poll's message is
     * queued. The chunk holds a message for "first", the lower sid, whose handler only records it, then a server PING,
     * then the poll's message; the dispatch answers the PING inline and waits for the write of its PONG, held up like
     * one on a socket under backpressure, so the poll's message is not even queued yet. The poll does not overtake that
     * wait: it finds nothing of its own to take (#179), reads the socket, and once the dispatch goes on "first" is
     * delivered first and the poll returns its message, a second in rather than at its 3 s timeout.
     */
    public function testDeliveriesKeepTheWireOrderWhileADispatchIsHeldUp(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->watchedClient($watched);
        $order = new class {
            /** @var list<string> */
            public array $list = [];
        };
        $firstSid = $client->subscribe('first', static function (NatsMessage $message) use ($order): void {
            $order->list[] = 'first:' . $message->payload;
        })->await();
        $queue = $client->subscribeQueue('jobs')->await();
        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop);
        $this->awaitRead($watched);

        $lead = ReconnectingTransport::msgFrame('first', $firstSid, 'a');
        $job = ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1');
        $transport->stallNextWriteContaining('PONG', 1.0);
        $transport->pushFrame($lead . "PING\r\n" . $job);
        $this->waitUntil(static fn(): bool => $transport->writesStalled() > 0);

        $start = hrtime(true);
        $message = $queue->setTimeout(3.0)->next();
        $order->list[] = 'poll:' . ($message->payload ?? 'null');
        $elapsed = $this->secondsSince($start);
        $transport->releaseStalledWrites();
        $stop->cancel();

        self::assertSame(['first:a', 'poll:job-1'], $order->list, 'the lower sid was delivered first, in wire order');
        self::assertGreaterThan(0.5, $elapsed, 'the poll waited for the dispatch to go on');
        self::assertLessThan(2.0, $elapsed, sprintf('the poll returned %.3f s in, its timeout being 3 s', $elapsed));
    }

    /**
     * When the poll's message is ALREADY queued behind a held-up dispatch, the poll takes it ahead of another
     * subscription's message queued with it (#179): the chunk holds "first" (the lower sid), a permissions -ERR whose
     * error listener awaits, and the poll's message, so the whole chunk is queued before the listener holds the pass up
     * - "first" and the poll's job both waiting for the pass to go on. The poll takes its own job at once, without a
     * socket read, while "first" stays queued for the held-up pass to deliver when it resumes. Order is only ever kept
     * within a subscription, so taking the poll's own message ahead of another subscription's reorders no handler calls
     * between them. Before #179 the poll read the socket and returned its message only once the pass went on, "first"
     * ahead of it.
     */
    public function testAnOwnSidPollTakesItsQueuedMessageAheadOfAStillQueuedLowerSid(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $hold = new HeldUpDelivery(fallbackSeconds: 1.0);
        $client = $this->watchedClient($watched, ['errorListener' => static fn() => $hold->holdUp()]);
        $order = new class {
            /** @var list<string> */
            public array $list = [];
        };
        $firstSid = $client->subscribe('first', static function (NatsMessage $message) use ($order): void {
            $order->list[] = 'first:' . $message->payload;
        })->await();
        $queue = $client->subscribeQueue('jobs')->await();
        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop);
        $this->awaitRead($watched);

        $lead = ReconnectingTransport::msgFrame('first', $firstSid, 'a');
        $job = ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1');
        $transport->pushFrame($lead . "-ERR 'Permissions Violation for Subscription to \"foo.bar\"'\r\n" . $job);
        $hold->began->getFuture()->await(new TimeoutCancellation(2));

        $readsBefore = $watched->reads;
        $start = hrtime(true);
        $message = $queue->setTimeout(3.0)->next();
        $order->list[] = 'poll:' . ($message->payload ?? 'null');
        $elapsed = $this->secondsSince($start);
        $heldWhenDone = $hold->held;
        $reads = $watched->reads - $readsBefore;
        $hold->end('the end of the test');
        // The held-up pass delivers "first" one or more event-loop hops after it is let go: wait on the recorded
        // order rather than a fixed delay.
        $this->waitUntil(static fn(): bool => in_array('first:a', $order->list, true));
        $stop->cancel();

        self::assertSame('poll:job-1', $order->list[0], 'the poll took its own message first, ahead of the still-queued lower sid');
        self::assertContains('first:a', $order->list, 'the lower sid is delivered when the held-up pass goes on');
        self::assertTrue($heldWhenDone, 'the poll returned before the held-up pass went on');
        self::assertSame(0, $reads, 'the poll delivered its queued message without reading the socket');
        self::assertLessThan(1.0, $elapsed, sprintf('the poll returned %.3f s in, its timeout being 3 s', $elapsed));
    }

    /** @return iterable<string, array{bool}> */
    public static function withAndWithoutAnApplicationLoop(): iterable
    {
        yield 'no other reader' => [false];
        yield "the application's read loop" => [true];
    }

    /**
     * Two fibers poll one queue, and one message arrives 50 ms in. One poll returns it at once; the other is woken,
     * finds nothing, and waits on to its 2 s timeout without spinning: the socket reads of the whole wait are bounded,
     * and so is how often the polls look at the queue's buffer, which is swapped for a {@see CountingBuffer}. Both
     * polls share the buffer, so the count is theirs together: one check each as they start, and two each when the
     * message comes - the poll that takes it finds the buffer full at both, the woken one finds it empty at both,
     * pauses a millisecond between them, and then waits on to its timeout without looking again - six in all. A
     * woken poll whose next wait ended at once again and again (a wake-up left fired and handed to its next read, a
     * wait behind the application's read that returned without waiting) would look twice per pause of a millisecond,
     * close to a thousand times or more over its wait as the loop's timer paces it, with the socket unread. Only a
     * wait that returns to the poll is counted: a read looping on a timer inside the connection, returning to neither
     * poll and never reading the socket, would escape the count, and no path of a poll on an open connection has such
     * a loop. A count of the callbacks the event loop registers over the wait, which would have caught it, was tried
     * and dropped: it is process-wide, and a reconnect attempt of a client an earlier test left behind registers its
     * 500 handshake polls in a burst that landed in this wait on the CI runners. The test used to bound the CPU time
     * of the whole wait (under 0.5 s) instead, which depended on the environment: under Xdebug with coverage on a
     * busy CI runner the wait's own cost reached it.
     */
    #[DataProvider('withAndWithoutAnApplicationLoop')]
    public function testTwoPollsOfOneQueueWithOneMessage(bool $loop): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->watchedClient($watched);
        $queue = $client->subscribeQueue('jobs')->await();
        $buffer = CountingBuffer::install($queue);
        $stop = new DeferredCancellation();
        if ($loop) {
            $this->startApplicationReadLoop($client, $stop);
            $this->awaitRead($watched);
        }
        $readsBefore = $watched->reads;
        $checksAtDelivery = 0;
        EventLoop::delay(0.05, static function () use ($transport, $queue, $buffer, &$checksAtDelivery): void {
            $checksAtDelivery = $buffer->checks;
            $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
        });

        $start = hrtime(true);
        $polls = [];
        foreach ([1, 2] as $i) {
            $polls[] = async(function () use ($queue, $start): array {
                $message = $queue->setTimeout(2.0)->next();

                return [$message?->payload, $this->secondsSince($start)];
            });
        }
        $results = \Amp\Future\await($polls);
        $checks = $buffer->checks;
        $reads = $watched->reads - $readsBefore;
        $stop->cancel();

        $got = array_values(array_filter($results, static fn(array $result): bool => $result[0] === 'job-1'));
        $none = array_values(array_filter($results, static fn(array $result): bool => $result[0] === null));
        self::report(sprintf('two polls, loop %s: %s; %d socket reads; the polls checked the buffer %d times at the delivery, %d in all', $loop ? 'yes' : 'no', json_encode($results), $reads, $checksAtDelivery, $checks));
        self::assertCount(1, $got, 'one poll got the message');
        self::assertCount(1, $none, 'the other got nothing');
        self::assertLessThan(0.5, $got[0][1], 'the message was returned at once');
        self::assertGreaterThan(1.9, $none[0][1], 'the other poll waited to its timeout');
        self::assertGreaterThan(0, $checksAtDelivery, 'the polls checked the buffer as they started: the count sees their looks');
        self::assertLessThan(10, $reads, 'the poll that went back to waiting read the socket a bounded number of times');
        self::assertLessThan(60, $checks, sprintf('the poll that went back to waiting did not spin: the two polls checked the buffer %d times in all, six when no wait ends at once', $checks));
    }

    /** @return iterable<string, array{string, bool}> */
    public static function lifecycleEvents(): iterable
    {
        foreach ([false, true] as $loop) {
            $suffix = $loop ? ", the application's read loop" : ', no other reader';
            yield 'disconnect()' . $suffix => ['disconnect', $loop];
            yield 'drain() with the message in flight' . $suffix => ['drain', $loop];
            yield 'drainSubscription() with the message in flight' . $suffix => ['drainSubscription', $loop];
            yield 'unsubscribe() of the queue' . $suffix => ['unsubscribe', $loop];
            yield 'a reconnect, the message after the replay' . $suffix => ['reconnect', $loop];
        }
    }

    /**
     * A poll parked with a 1.5 s timeout when a lifecycle event happens 50 ms in. A close ends it with the connection's
     * error at once. A drain, of the connection or of the queue, delivers the message the server sends before its PONG,
     * and the poll returns it at once. A reconnect that the server answers with the message right after the replayed
     * SUB delivers it the same way. An unsubscribe() of the queue wakes the poll, which finds nothing, and waits on to
     * its timeout with nothing to wake it again: a bounded number of reads, no spin.
     */
    #[DataProvider('lifecycleEvents')]
    public function testAParkedPollThroughALifecycleEvent(string $event, bool $loop): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->watchedClient($watched);
        $queue = $client->subscribeQueue('jobs')->await();
        $stop = new DeferredCancellation();
        if ($loop) {
            $this->startApplicationReadLoop($client, $stop);
            $this->awaitRead($watched);
        }
        $readsBefore = $watched->reads;
        $transport->afterWrite = static function (string $bytes) use ($transport, $queue): void {
            // drain() and drainSubscription() flush with a PING: the message in flight comes ahead of the PONG.
            if (str_contains($bytes, 'UNSUB ' . $queue->sid)) {
                $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
            }
        };
        $side = new class {
            public string $note = '';
        };
        EventLoop::delay(0.05, static function () use ($event, $client, $transport, $queue, $side): void {
            async(static function () use ($event, $client, $transport, $queue, $side): void {
                try {
                    match ($event) {
                        'disconnect' => $client->disconnect()->await(),
                        'drain' => $client->drain()->await(),
                        'drainSubscription' => $client->drainSubscription($queue->sid)->await(),
                        'unsubscribe' => $queue->unsubscribe()->await(),
                        'reconnect' => self::dropAndAnswerAfterTheReplay($transport, $queue->sid),
                        default => throw new \LogicException('Unknown event ' . $event),
                    };
                } catch (\Throwable $e) {
                    $side->note = 'the event threw ' . $e::class . ': ' . $e->getMessage();
                }
            })->ignore();
        });

        $start = hrtime(true);
        $error = null;
        try {
            $result = $queue->setTimeout(1.5)->next()?->payload;
        } catch (\Throwable $e) {
            $result = null;
            $error = $e::class . ': ' . $e->getMessage();
        }
        $elapsed = $this->secondsSince($start);
        delay(0.05);
        $reads = $watched->reads - $readsBefore;
        $stop->cancel();

        self::report(sprintf('%s, loop %s: %s after %.3f s%s, %d socket read(s)%s', $event, $loop ? 'yes' : 'no', $result ?? 'null', $elapsed, $error === null ? '' : ', threw ' . $error, $reads, $side->note === '' ? '' : ' [' . $side->note . ']'));
        self::assertSame('', $side->note);
        if ($event === 'disconnect') {
            self::assertNotNull($error, 'the poll ends with the connection');
            self::assertLessThan(1.0, $elapsed, sprintf('the poll ended %.3f s in, its timeout being 1.5 s', $elapsed));
        } elseif ($event === 'unsubscribe') {
            self::assertNull($error);
            self::assertNull($result, 'nothing can come for a removed subscription');
            self::assertGreaterThan(1.4, $elapsed, 'the poll waited to its timeout');
            self::assertLessThan(10, $reads, 'a bounded number of reads, no spin');
        } else {
            self::assertNull($error);
            self::assertSame('job-1', $result);
            self::assertLessThan(1.0, $elapsed, sprintf('the poll returned %.3f s in, its timeout being 1.5 s', $elapsed));
        }
    }

    /** @return iterable<string, array{int, bool}> */
    public static function concurrentRequests(): iterable
    {
        foreach ([5, 20] as $n) {
            yield $n . ' requests, no other reader' => [$n, false];
            yield $n . " requests, the application's read loop" => [$n, true];
        }
    }

    /**
     * N requests wait on the reply inbox they all share, and the server answers them one at a time, 20 ms apart, in
     * reverse order. Each gets its own reply as it comes, within 0.3 s of the answer: a request's wake-up is its own
     * reply, so another request's reply does not end its read, and the reads of the socket stay few.
     */
    #[DataProvider('concurrentRequests')]
    public function testConcurrentRequestsGetTheirRepliesAsTheyCome(int $n, bool $loop): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->watchedClient($watched);
        $inboxes = new class {
            /** @var array<string, string> Reply subject of each request, by its payload. */
            public array $byPayload = [];
        };
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport, $inboxes): array {
            if ($replyTo === null) {
                return [];
            }

            if ($subject === 'svc.warm') {
                return $transport->replyFrame($replyTo, 'ok');
            }

            $inboxes->byPayload[$payload] = $replyTo;

            return [];
        };
        $client->request('svc.warm', 'x', 1_000)->await();
        $stop = new DeferredCancellation();
        if ($loop) {
            $this->startApplicationReadLoop($client, $stop);
            $this->awaitRead($watched);
        }

        $start = hrtime(true);
        $requests = [];
        for ($i = 0; $i < $n; $i++) {
            $payload = 'q' . $i;
            $requests[$payload] = $client->request('svc.slow', $payload, 5_000)->map(fn(NatsMessage $message): array => [$message->payload, $this->secondsSince($start)]);
        }
        $this->waitUntil(static fn(): bool => count($inboxes->byPayload) === $n);
        $readsBefore = $watched->reads;
        $order = array_reverse(array_keys($inboxes->byPayload));
        foreach ($order as $k => $payload) {
            $replyTo = $inboxes->byPayload[$payload];
            EventLoop::delay(0.02 * ($k + 1), static function () use ($transport, $replyTo, $payload): void {
                $transport->pushFrame(ReconnectingTransport::msgFrame($replyTo, (int) $transport->sidFor($replyTo), 'r-' . $payload));
            });
        }
        $results = \Amp\Future\await($requests);
        $reads = $watched->reads - $readsBefore;
        $stop->cancel();

        $wrong = [];
        foreach ($results as $payload => [$reply, $at]) {
            $due = 0.02 * ((int) array_search($payload, $order, true) + 1);
            if ($reply !== 'r-' . $payload || $at > $due + 0.3) {
                $wrong[] = sprintf('%s: %s at %.3f s, due at %.3f s', $payload, $reply, $at, $due);
            }
        }
        self::report(sprintf('%d requests, loop %s: %d socket reads for %d replies%s', $n, $loop ? 'yes' : 'no', $reads, $n, $wrong === [] ? '' : "\n    " . implode("\n    ", $wrong)));
        self::assertSame([], $wrong, 'every request got its own reply as it came');
        self::assertLessThan(4 * $n + 10, $reads, 'the reads of the socket stayed few');
    }

    /**
     * Drops the connection with dials refused for 60 ms, and has the server send job-1 on the replayed SUB as soon as
     * the reconnect has written it.
     */
    private static function dropAndAnswerAfterTheReplay(ReconnectingTransport $transport, int $sid): void
    {
        $transport->afterWrite = static function () use ($transport, $sid): void {
            if ($transport->epoch() >= 1 && $transport->sidFor('jobs') !== null) {
                $transport->afterWrite = null;
                $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $sid, 'job-1'));
            }
        };
        $transport->refuseDials();
        $transport->dropConnection();
        EventLoop::delay(0.06, static fn() => $transport->acceptDials());
    }

    /**
     * A client connected through $watched, so that the test sees each read of the socket start.
     *
     * @param array<string, mixed> $overrides NatsOptions arguments.
     */
    private function watchedClient(WatchedTransport $watched, array $overrides = []): NatsClient
    {
        $client = new NatsClient(new NatsOptions(...array_merge([
            'connectTimeoutMs' => 500,
            'requestTimeoutMs' => 2_000,
            'reconnectEnabled' => true,
            'maxReconnectAttempts' => 1_000,
            'reconnectDelayMs' => 5,
            'reconnectMaxDelayMs' => 20,
            'reconnectJitterMs' => 0,
            'pingIntervalSeconds' => 0,
            'waitForReconnect' => true,
        ], $overrides)), $watched);
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

    /** Makes $call after $hops event-loop hops: a callback queued that many times in a row. */
    private static function afterHops(int $hops, \Closure $call): void
    {
        if ($hops === 0) {
            $call();

            return;
        }

        EventLoop::queue(static function () use ($hops, $call): void {
            self::afterHops($hops - 1, $call);
        });
    }

    /**
     * @param array<NatsMessage> $messages
     * @return list<string>
     */
    private static function payloads(array $messages): array
    {
        return array_values(array_map(static fn(NatsMessage $message): string => $message->payload, $messages));
    }

    /** Prints a line of the measurements to STDERR when the READ_WAKEUP_VERBOSE environment variable is set. */
    private static function report(string $line): void
    {
        $verbose = getenv('READ_WAKEUP_VERBOSE');
        if ($verbose !== false && $verbose !== '' && $verbose !== '0') {
            fwrite(STDERR, $line . "\n");
        }
    }
}
