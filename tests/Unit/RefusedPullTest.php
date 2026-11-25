<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\PermissionViolationException;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\Tests\Support\CompletionHoldingTransport;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\HeldFailingWriteTransport;
use IDCT\NATS\Tests\Support\HeldUpDelivery;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\WatchedTransport;
use IDCT\NATS\Transport\TransportClosedException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\delay;

/**
 * A pull whose publish the server refuses for lack of permission fails at once (#260). nats-server answers a PUB to
 * "$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>" by a user who may not publish there with
 * "-ERR 'Permissions Violation for Publish to "<subject>"'" and keeps the connection open: the pull reaches no
 * consumer, so no message and no status ever come for it. fetchBatch() publishes with an inbox of its own, not through
 * request(), so 2.30.0's refusal of requests did not reach it: it waited out its expiry plus a second and threw the
 * JetStreamException(408) of an empty pull, and a worker pulling in a loop stayed idle for ever without an error
 * (ideaconnect/symfony-nats-messenger#130). The connection now watches the pull's subject while the fetch runs
 * ({@see NatsConnection::watchPublishRefusal()}), and the fetch fails with a {@see PermissionViolationException} that
 * names the subject, which is not a JetStreamException, so a caller that reads a 408 as "nothing to do" cannot take it
 * for one. Messages delivered before the refusal are returned. A refusal read by another fiber's read is covered in
 * MuxInboxDropWakeupTest.
 *
 * Over `tests/Support/FakeTransport.php`, whose onWrite answers each PUB the client writes: with the messages a test
 * gives for the subject, delivered on the reply inbox, then with the -ERR for a refused subject. Each pull has a 5 s
 * expiry, so the fetch's budget is 6 s. Each regression test fails on the code before the fix, the fetch ending only
 * at that deadline: with the 408, or, with messages delivered, with them.
 *
 * The orderings of the review of #264 run over `tests/Support/ReconnectingTransport.php`, seen through
 * `tests/Support/WatchedTransport.php` and `tests/Support/CompletionHoldingTransport.php`, with an application read
 * loop as the other reader: a refusal read while the fetch is suspended in a write whose bytes reached the server
 * (the PUB's, the UNSUB's), and a refusal read behind frames for the fetch that wait in a delivery held up by an
 * earlier subscription's handler (`tests/Support/HeldUpDelivery.php`, the gate). Each asserts the fetch's result while
 * the write or the gate is still held, so the order of events decides, not time. Each pull there has a 3 s expiry,
 * so a 4 s budget.
 */
final class RefusedPullTest extends TestCase
{
    use OwnsTestResources;

    private const STREAM = 'ORDERS';
    private const PULL = '$JS.API.CONSUMER.MSG.NEXT.ORDERS.worker';
    private const OTHER_PULL = '$JS.API.CONSUMER.MSG.NEXT.ORDERS.other';
    /** The scripted server's INFO, naming a cluster member the client has not heard of: a DiscoveredServers event. */
    private const INFO_WITH_A_NEW_MEMBER = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true,"connect_urls":["127.0.0.1:14299"]}' . "\r\n";

    protected function tearDown(): void
    {
        $this->releaseOwnedResources();
    }

    /** @return iterable<string, array{string}> */
    public static function fetches(): iterable
    {
        yield 'fetchBatch()' => ['fetchBatch'];
        yield 'fetchNext()' => ['fetchNext'];
    }

    /**
     * The issue's shape: the pull fails within 2.5 s of its 6 s budget with the server's refusal, which names the
     * subject and the permission needed, and leaves the connection open, the inbox unsubscribed and no watch behind.
     * It used to fail at the deadline with JetStreamException(408) 'No messages received within timeout'.
     */
    #[DataProvider('fetches')]
    public function testAPullWhosePublishTheServerRefusesFailsAtOnce(string $fetch): void
    {
        $transport = self::server([self::PULL]);
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 5_000), $transport));
        $client->connect()->await();

        $start = hrtime(true);
        $failure = self::failureOf(self::fetch($fetch, $client->jetStream(), 'worker'));
        $elapsed = (hrtime(true) - $start) / 1e9;

        self::assertInstanceOf(PermissionViolationException::class, $failure, sprintf('%s: %s', $fetch, self::describe($failure)));
        self::assertSame(self::PULL, $failure->getSubject());
        self::assertSame(
            'The server refused the pull to "' . self::PULL . '": Permissions Violation for Publish to "' . self::PULL
            . '". No message was delivered for it. Grant this connection\'s user permission to publish to "' . self::PULL . '".',
            $failure->getMessage(),
            'the message carries the server\'s text, without its quotes, and names the permission needed',
        );
        self::assertLessThan(2.5, $elapsed, sprintf('%s failed after %.3f s, with a 6 s budget', $fetch, $elapsed));
        self::assertSame(ConnectionState::Open, $client->state(), 'a refused pull leaves the connection open');
        self::assertCount(1, self::linesStartingWith($transport, 'UNSUB '), 'the fetch unsubscribed its inbox');
        self::assertNoWatchLeft($client);
    }

    /**
     * A refusal that names another consumer's pull, read before the pull's message, leaves the pull alone: it gets its
     * message. A regression that ended every pull in flight on any publish refusal would fail it with the refusal.
     */
    public function testARefusalOfAnotherSubjectLeavesThePullAlone(): void
    {
        $transport = self::server([], [self::PULL => ['first']], errorsBefore: [self::PULL => [self::OTHER_PULL]]);
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 5_000), $transport));
        $client->connect()->await();

        $messages = $client->jetStream()->fetchBatch(self::STREAM, 'worker', 1, 5_000)->await();

        self::assertSame(['first'], self::payloads($messages));
        self::assertNoWatchLeft($client);
    }

    /**
     * Of two pulls in flight, the one to the refused subject fails at once, and the other gets the message that comes
     * after the -ERR. A pull made afterwards works: nothing was latched, and the connection stayed open.
     */
    public function testOnlyThePullToTheRefusedSubjectFails(): void
    {
        $transport = self::server([self::PULL], [self::OTHER_PULL => ['other']]);
        $inner = $transport->onWrite;
        self::assertNotNull($inner);
        /** @var list<string> $held */
        $held = [];
        $holding = true;
        // The other consumer's message is held back until the refused pull is written, and then comes after its -ERR.
        $transport->onWrite = static function (string $bytes) use ($inner, &$held, &$holding): array {
            $frames = $inner($bytes);
            if ($holding && str_contains($bytes, 'PUB ' . self::OTHER_PULL . ' ')) {
                $held = $frames;

                return [];
            }
            if ($holding && str_contains($bytes, 'PUB ' . self::PULL . ' ')) {
                $holding = false;

                return [...$frames, ...$held];
            }

            return $frames;
        };
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 5_000), $transport));
        $client->connect()->await();
        $js = $client->jetStream();

        $start = hrtime(true);
        $other = $js->fetchBatch(self::STREAM, 'other', 1, 5_000);
        $other->ignore();
        $refused = $js->fetchBatch(self::STREAM, 'worker', 1, 5_000);
        $refused->ignore();

        $failure = self::failureOf($refused);
        self::assertInstanceOf(PermissionViolationException::class, $failure, self::describe($failure));
        self::assertSame(['other'], self::payloads($other->await()));
        self::assertLessThan(2.5, (hrtime(true) - $start) / 1e9, 'both ended well before their 6 s budget');
        self::assertSame(['other'], self::payloads($js->fetchBatch(self::STREAM, 'other', 1, 5_000)->await()));
        self::assertNoWatchLeft($client);
    }

    /**
     * A pull that holds messages when its publish is refused returns them at once, as at its deadline: they are real,
     * and the server counted them as delivered. Here the server delivers one message of a batch of 5 and then refuses
     * a publish to the subject. The fetch used to return the same message at its 6 s deadline.
     */
    public function testAPullWithMessagesReturnsThemAtOnceWhenItsPublishIsRefused(): void
    {
        $transport = self::server([], [self::PULL => ['first']], errorsAfter: [self::PULL => [self::PULL]]);
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 5_000), $transport));
        $client->connect()->await();

        $start = hrtime(true);
        $messages = $client->jetStream()->fetchBatch(self::STREAM, 'worker', 5, 5_000)->await();
        $elapsed = (hrtime(true) - $start) / 1e9;

        self::assertSame(['first'], self::payloads($messages));
        self::assertLessThan(2.5, $elapsed, sprintf('returned after %.3f s, with a 6 s budget', $elapsed));
        self::assertNoWatchLeft($client);
    }

    /**
     * A pull nothing refuses still ends as an empty pull at its deadline: the watch changes nothing for it. A guard: it
     * keeps the 408 a caller reads as "nothing to do" from turning into the refusal. On the code before the fix it
     * fails only at the check of the watch state, which does not exist there.
     */
    public function testAnUnansweredPullStillEndsAsAnEmptyPull(): void
    {
        $transport = self::server([]);
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 5_000), $transport));
        $client->connect()->await();

        $failure = self::failureOf($client->jetStream()->fetchBatch(self::STREAM, 'worker', 1, 200));

        self::assertInstanceOf(JetStreamException::class, $failure, self::describe($failure));
        self::assertSame(408, $failure->getCode());
        self::assertNoWatchLeft($client);
    }

    /**
     * The pull's PUB has reached the server, which refused it, but the fetch's writer is not resumed yet: the PUB's
     * completion is held. The application's read loop, the only reader, brings the -ERR. The fetch, suspended in its
     * publish, fails with the refusal while the PUB's completion is still held, within 0.5 s of its 4 s budget, and
     * leaves the connection open, its inbox unsubscribed and no watch behind. It used to wait in the publish until the
     * budget ran out and then throw the JetStreamException (408) of an empty pull: the refusal woke the reads waiting
     * for the inbox, and the fetch was not reading (review of #264).
     */
    public function testAPullSuspendedInItsPublishFailsAtOnceWhenAnotherFibersReadBringsTheRefusal(): void
    {
        [$server, $watched, $holding, $client] = $this->connectOverTheScriptedServer();
        $server->responder = static fn(string $subject, ?string $replyTo): array => $subject === self::PULL ? [self::refusal(self::PULL)] : [];
        [$stop, $loop] = $this->startReadLoop($client);
        $this->awaitRead($watched);

        try {
            $holding->holdCompletionOfNextWriteContaining('PUB ' . self::PULL . ' ', 5.0);
            $start = hrtime(true);
            [, $failure] = self::settle($client->jetStream()->fetchBatch(self::STREAM, 'worker', 1, 3_000));
            $elapsed = (hrtime(true) - $start) / 1e9;

            // Before the PUB's writer is let go.
            self::assertSame(1, $holding->completionsHeld(), "the PUB's completion was still held when the fetch ended");
            self::assertInstanceOf(PermissionViolationException::class, $failure, sprintf('the fetch ended after %.3f s with %s', $elapsed, self::describe($failure)));
            self::assertSame(self::PULL, $failure->getSubject());
            self::assertLessThan(0.5, $elapsed, sprintf('the fetch failed after %.3f s, with a 4 s budget', $elapsed));
            self::assertSame(ConnectionState::Open, $client->state(), 'a refused pull leaves the connection open');
            self::assertContains('UNSUB ' . self::fetchSid($server), $server->controlLinesStartingWith('UNSUB '), 'the fetch unsubscribed its inbox');
            self::assertNoWatchLeft($client);
        } finally {
            $holding->releaseHeldCompletions();
            $stop->cancel();
            $loop->await();
        }
    }

    /**
     * The fetch meets the refusal in its own read, and the UNSUB it then writes for its inbox reaches the server, but
     * the UNSUB's completion is held. The fetch fails with the refusal within 0.5 s of its 4 s budget, with the inbox
     * released locally, and the UNSUB then goes out in the background, its completion still held: once the refusal is
     * known the fetch no longer waits for that completion. It used to wait for it within its budget, and threw the
     * right exception only at the budget's end (review of #264).
     */
    public function testARefusedPullDoesNotWaitForItsUnsubToComplete(): void
    {
        [$server, , $holding, $client] = $this->connectOverTheScriptedServer();
        $server->responder = static fn(string $subject, ?string $replyTo): array => $subject === self::PULL ? [self::refusal(self::PULL)] : [];

        try {
            $holding->holdCompletionOfNextWriteContaining('UNSUB ', 5.0);
            $start = hrtime(true);
            [, $failure] = self::settle($client->jetStream()->fetchBatch(self::STREAM, 'worker', 1, 3_000));
            $elapsed = (hrtime(true) - $start) / 1e9;

            // Before the UNSUB's writer is let go: the fetch did not wait for it.
            self::assertInstanceOf(PermissionViolationException::class, $failure, sprintf('the fetch ended after %.3f s with %s', $elapsed, self::describe($failure)));
            self::assertSame(self::PULL, $failure->getSubject());
            self::assertLessThan(0.5, $elapsed, sprintf('the fetch failed after %.3f s, with a 4 s budget', $elapsed));
            $sid = self::fetchSid($server);
            $subscriptions = (new \ReflectionProperty(NatsConnection::class, 'subscriptionMeta'))->getValue(self::connectionOf($client));
            self::assertIsArray($subscriptions);
            self::assertArrayNotHasKey($sid, $subscriptions, 'the inbox was released locally');
            self::assertNoWatchLeft($client);
            // The UNSUB goes out in the background, and its completion is still held.
            $this->waitUntil(static fn(): bool => in_array('UNSUB ' . $sid, $server->controlLinesStartingWith('UNSUB '), true) && $holding->completionsHeld() === 1);
        } finally {
            $holding->releaseHeldCompletions();
        }
    }

    /** @return iterable<string, array{list<string>}> */
    public static function framesQueuedAheadOfTheRefusal(): iterable
    {
        yield 'a message' => [['message']];
        yield 'a 409 Consumer Deleted' => [['status']];
        yield 'a message, then a 409 Consumer Deleted' => [['message', 'status']];
    }

    /**
     * The application's read takes one chunk with a message for an earlier subscription, frames for the fetch's
     * inbox and the -ERR refusing the pull. Its delivery is held up in the earlier subscription's handler (the gate),
     * so the fetch's frames wait in its queue, behind the handler, when the -ERR has woken the fetch's read, parked on
     * the socket. The fetch delivers what is queued for its inbox before it settles the refusal, and the usual rules
     * then decide, while the gate is still held: a message is returned (a partial batch of 1 for 5), a 409 alone is
     * thrown as the JetStreamException(409) it is, and a message with a 409 behind it is returned with the terminal
     * callback called once. It used to release the inbox, the queued frames with it, and throw the
     * PermissionViolationException: the message and the terminal status were lost (review of #264).
     *
     * The chunk opens with an INFO that names a new cluster member, and the application's DiscoveredServers listener
     * holds its dispatch there until the fetch's read has taken the socket. A server PING held at its PONG's completion
     * used to stage that: the application's dispatch now queues the frames behind a PING before it waits for the PONG
     * when the chunk carries a delivery for a pull in flight (third review of #264), so a PING holds nothing back.
     *
     * @param list<string> $queued
     */
    #[DataProvider('framesQueuedAheadOfTheRefusal')]
    public function testWhatIsQueuedForThePullAheadOfTheRefusalIsDeliveredBeforeItIsSettled(array $queued): void
    {
        /** @var DeferredFuture<null> $letTheDispatchGo */
        $letTheDispatchGo = new DeferredFuture();
        $dispatchHeld = false;
        $this->resources()->onRelease(static function () use ($letTheDispatchGo): void {
            if (!$letTheDispatchGo->isComplete()) {
                $letTheDispatchGo->complete();
            }
        }, 'held DiscoveredServers listener');
        [$server, $watched, , $client] = $this->connectOverTheScriptedServer(
            connectionListener: static function (ConnectionEvent $event) use ($letTheDispatchGo, &$dispatchHeld): void {
                if ($event !== ConnectionEvent::DiscoveredServers) {
                    return;
                }

                $dispatchHeld = true;
                $letTheDispatchGo->getFuture()->await(new TimeoutCancellation(5));
            },
        );
        // Nothing answers the pull: what comes for it comes in the chunk below.
        $server->responder = static fn(string $subject, ?string $replyTo): array => [];
        $gate = new HeldUpDelivery(5.0);
        $this->resources()->onRelease(static function () use ($gate): void {
            $gate->end('the test resource scope');
        }, 'held handler');
        // Subscribed ahead of the fetch's inbox, so its sid is the lower one: the inbox's delivery waits behind its handler.
        $slowSid = $client->subscribe('slow', $gate->handler())->await();
        [$stop, $loop] = $this->startReadLoop($client);
        $this->awaitRead($watched);

        try {
            /** @var list<array{int, string}> $terminal */
            $terminal = [];
            $fetch = $client->jetStream()->fetchBatch(
                self::STREAM,
                'worker',
                5,
                3_000,
                [],
                static function (int $code, string $description) use (&$terminal): void {
                    $terminal[] = [$code, $description];
                },
            );
            $fetch->ignore();
            $this->waitUntil(static fn(): bool => $server->controlLinesStartingWith('PUB ' . self::PULL . ' ') !== []);
            $replyTo = self::replySubjectOfThePull($server);
            $inboxSid = (int) $server->sidFor($replyTo);
            self::assertGreaterThan($slowSid, $inboxSid);

            $frames = ReconnectingTransport::msgFrame('slow', $slowSid, 's') . self::framesForThePull($queued, $replyTo, $inboxSid);

            // The application's read takes the chunk, and its dispatch is held in the DiscoveredServers listener of the
            // INFO ahead of the frames until the fetch's read has taken the socket. Then the dispatch goes on, meets the
            // -ERR, and the delivery stops in the gate's handler.
            $reading = self::nextRead($watched);
            $server->pushFrame(self::INFO_WITH_A_NEW_MEMBER . $frames . self::refusal(self::PULL));
            $this->waitUntil(static function () use ($reading, &$dispatchHeld): bool {
                return $reading->isComplete() && $dispatchHeld;
            });
            $watched->onRead = null;
            $letTheDispatchGo->complete();
            $gate->began->getFuture()->await(new TimeoutCancellation(2));

            [$messages, $failure] = self::settle($fetch);

            // Before the gate is released.
            self::assertTrue($gate->held, "the delivery was still held up in the earlier subscription's handler when the fetch ended");
            self::assertTheResultOfThePull($queued, $messages, $failure, $terminal);
            self::assertNoWatchLeft($client);
        } finally {
            $gate->end('the test');
            if (!$letTheDispatchGo->isComplete()) {
                $letTheDispatchGo->complete();
            }
            $stop->cancel();
            $loop->await();
        }
    }

    /**
     * The application's read takes one chunk with a server PING and, behind it, frames for the fetch's inbox, and the
     * PONG answering the PING reaches the server with its completion held. The fetch's read then takes the socket and
     * brings the -ERR refusing the pull in the server's next chunk. While the PONG's completion is still held, the
     * fetch of 5 returns the message (a partial batch), throws the JetStreamException(409), or returns the message and
     * calls the terminal callback once with the 409: the application's dispatch answers the PING in wire order and
     * queues the frames behind it before it waits for the PONG, since the chunk carries a delivery for a pull in flight
     * (third review of #264). It used to write the PONG in place, the frames behind it not queued while it waited:
     * the fetch settled the refusal without them, threw the PermissionViolationException, lost the message and never
     * called the terminal callback, even once the PONG was let go. Fails on the code before the fix with that
     * exception.
     *
     * @param list<string> $queued
     */
    #[DataProvider('framesQueuedAheadOfTheRefusal')]
    public function testWhatAnApplicationReadBroughtInBehindAPingIsKeptWhenThePullsOwnReadBringsTheRefusal(array $queued): void
    {
        [$server, $watched, $holding, $client] = $this->connectOverTheScriptedServer();
        $server->responder = static fn(string $subject, ?string $replyTo): array => [];
        [$stop, $loop] = $this->startReadLoop($client);
        $this->awaitRead($watched);

        try {
            /** @var list<array{int, string}> $terminal */
            $terminal = [];
            $fetch = $client->jetStream()->fetchBatch(
                self::STREAM,
                'worker',
                5,
                3_000,
                [],
                static function (int $code, string $description) use (&$terminal): void {
                    $terminal[] = [$code, $description];
                },
            );
            $fetch->ignore();
            $this->waitUntil(static fn(): bool => $server->controlLinesStartingWith('PUB ' . self::PULL . ' ') !== []);
            $replyTo = self::replySubjectOfThePull($server);
            $inboxSid = (int) $server->sidFor($replyTo);

            // The application's read, on the socket, takes the chunk, and the PONG for its PING is held.
            $holding->holdCompletionOfNextWriteContaining('PONG', 5.0);
            $reading = self::nextRead($watched);
            $start = hrtime(true);
            $server->pushFrame("PING\r\n" . self::framesForThePull($queued, $replyTo, $inboxSid));
            $this->waitUntil(static fn(): bool => $holding->completionsHeld() === 1);
            // The fetch's read takes the socket next, unless the fetch has its result already (a terminal status).
            $this->waitUntil(static fn(): bool => $reading->isComplete() || $fetch->isComplete());
            $watched->onRead = null;
            $server->pushFrame(self::refusal(self::PULL));
            [$messages, $failure] = self::settle($fetch);
            $elapsed = (hrtime(true) - $start) / 1e9;

            // Before the PONG's writer is let go.
            self::assertSame(1, $holding->completionsHeld(), "the PONG's completion was still held when the fetch ended");
            self::assertTheResultOfThePull($queued, $messages, $failure, $terminal);
            self::assertLessThan(0.5, $elapsed, sprintf('the fetch ended after %.3f s, with a 4 s budget', $elapsed));
            self::assertNoWatchLeft($client);
        } finally {
            $holding->releaseHeldCompletions();
            $stop->cancel();
            $loop->await();
        }
    }

    /** @return iterable<string, array{string}> */
    public static function refusalAroundAPing(): iterable
    {
        yield 'the -ERR ahead of the PING' => ['ahead'];
        yield 'the -ERR behind the PING' => ['behind'];
    }

    /**
     * The fetch's own read, the only reader, takes one chunk with the -ERR refusing the pull and a server PING, and
     * the PONG answering that PING reaches the server with its completion held. The read records the refusal and
     * stops waiting for the PONG's completion: the fetch fails with the refusal within 0.5 s of its 4 s budget while
     * that completion is still held, and the PONG went out ahead of the fetch's UNSUB, in wire order. It used to wait
     * in the dispatch for the PONG's completion, past its budget, and threw the refusal only once the write was let go
     * (review of #264). Fails on the code before the fix with the fetch still pending when the hold ends.
     */
    #[DataProvider('refusalAroundAPing')]
    public function testARefusedPullDoesNotWaitForThePongItsOwnReadOwesTheServer(string $order): void
    {
        [$server, , $holding, $client] = $this->connectOverTheScriptedServer();
        // Nothing answers the pull: what comes for it comes in the chunk below.
        $server->responder = static fn(string $subject, ?string $replyTo): array => [];

        $fetch = $client->jetStream()->fetchBatch(self::STREAM, 'worker', 1, 3_000);
        $fetch->ignore();
        $this->waitUntil(static fn(): bool => $server->controlLinesStartingWith('PUB ' . self::PULL . ' ') !== []);
        $holding->holdCompletionOfNextWriteContaining('PONG', 5.0);
        $start = hrtime(true);
        $server->pushFrame($order === 'ahead' ? self::refusal(self::PULL) . "PING\r\n" : "PING\r\n" . self::refusal(self::PULL));
        [, $failure] = self::settle($fetch);
        $elapsed = (hrtime(true) - $start) / 1e9;

        // Before the PONG's writer is let go.
        self::assertSame(1, $holding->completionsHeld(), "the PONG's completion was still held when the fetch ended");
        self::assertInstanceOf(PermissionViolationException::class, $failure, sprintf('the fetch ended after %.3f s with %s', $elapsed, self::describe($failure)));
        self::assertSame(self::PULL, $failure->getSubject());
        self::assertLessThan(0.5, $elapsed, sprintf('the fetch failed after %.3f s, with a 4 s budget', $elapsed));
        self::assertNoWatchLeft($client);
        $sid = self::fetchSid($server);
        $this->waitUntil(static fn(): bool => in_array('UNSUB ' . $sid, $server->controlLines(), true));
        $lines = $server->controlLines();
        self::assertLessThan(
            array_search('UNSUB ' . $sid, $lines, true),
            array_search('PONG', $lines, true),
            'the PONG went out ahead of the UNSUB the fetch wrote after it: ' . implode(' | ', $lines),
        );

        $holding->releaseHeldCompletions();
        self::assertSame(ConnectionState::Open, $client->state(), 'a refused pull leaves the connection open');
    }

    /**
     * The fetch's own read takes one chunk with a server PING, then a message and a 409 Consumer Deleted for the
     * fetch's inbox, and the PONG answering the PING reaches the server with its completion held. The read queues the
     * message and the 409 before it waits for the PONG. The application's read, the second reader, then brings the -ERR
     * refusing the pull, which ends the fetch's wait for the PONG: while the PONG's completion is still held, the fetch
     * of 5 returns the message and calls the terminal callback once with the 409 (review of #264). The frames behind
     * the PING used to be queued only once the PONG's write completed: a fetch that stopped waiting for its read when
     * the refusal came would have released its inbox without them, the message and the terminal status lost. Fails on
     * the code before the fix with the fetch still pending when the hold ends.
     */
    public function testWhatTheFetchsReadBroughtInBehindAPingIsKeptWhenAnotherFibersReadBringsTheRefusal(): void
    {
        [$server, $watched, $holding, $client] = $this->connectOverTheScriptedServer();
        $server->responder = static fn(string $subject, ?string $replyTo): array => [];
        /** @var list<array{int, string}> $terminal */
        $terminal = [];
        $fetch = $client->jetStream()->fetchBatch(
            self::STREAM,
            'worker',
            5,
            3_000,
            [],
            static function (int $code, string $description) use (&$terminal): void {
                $terminal[] = [$code, $description];
            },
        );
        $fetch->ignore();
        $this->waitUntil(static fn(): bool => $server->controlLinesStartingWith('PUB ' . self::PULL . ' ') !== []);
        $replyTo = self::replySubjectOfThePull($server);
        $inboxSid = (int) $server->sidFor($replyTo);

        // The fetch's read, the only reader, takes the chunk; its dispatch answers the PING and queues the frames.
        $holding->holdCompletionOfNextWriteContaining('PONG', 5.0);
        $start = hrtime(true);
        $server->pushFrame(
            "PING\r\n"
            . ReconnectingTransport::msgFrame($replyTo, $inboxSid, 'm-1', '$JS.ACK.ORDERS.worker.1.1.1.0.0')
            . ReconnectingTransport::hmsgFrame($replyTo, $inboxSid, "NATS/1.0 409 Consumer Deleted\r\n\r\n", ''),
        );
        $this->waitUntil(static fn(): bool => $holding->completionsHeld() === 1);
        self::assertFalse($fetch->isComplete(), 'the fetch waits for the PONG it owes the server');

        // The application's read takes the socket, and the server's next chunk refuses the pull.
        [, $loop] = $this->startReadLoop($client);
        $this->awaitRead($watched);
        $server->pushFrame(self::refusal(self::PULL));
        [$messages, $failure] = self::settle($fetch);
        $elapsed = (hrtime(true) - $start) / 1e9;

        // Before the PONG's writer is let go.
        self::assertSame(1, $holding->completionsHeld(), "the PONG's completion was still held when the fetch ended");
        self::assertNull($failure, sprintf('the fetch ended after %.3f s with %s', $elapsed, self::describe($failure)));
        self::assertSame(['m-1'], self::payloads($messages));
        self::assertSame([[409, 'Consumer Deleted']], $terminal, 'the terminal callback is called once, with the 409');
        self::assertLessThan(0.5, $elapsed, sprintf('the fetch returned after %.3f s, with a 4 s budget', $elapsed));
        self::assertNoWatchLeft($client);
        self::assertFalse($loop->isComplete());
    }

    /** @return iterable<string, array{bool}> */
    public static function connectionsWhosePongFailsLate(): iterable
    {
        yield 'a connection left open' => [false];
        yield 'a connection the user closed meanwhile' => [true];
    }

    /**
     * The PONG the fetch's read owes the server is held back, and fails only after the fetch, which the refusal in the
     * same chunk ended, stopped waiting for it. The connection notices the failure on its own: the error listener gets
     * it, and the connection, with reconnect off, ends Closed, as when the read that met the PING saw the PONG fail
     * (#171). With nobody left waiting for the write, the failure used to have nowhere to go (review of #264). Not for a
     * connection the user closed meanwhile: its PONG went with it, and the listener hears nothing of it. Fails on the
     * code before the fix, the fetch waiting for the held write.
     */
    #[DataProvider('connectionsWhosePongFailsLate')]
    public function testAPongThatFailsAfterThePullStoppedWaitingForItEndsTheConnection(bool $closedMeanwhile): void
    {
        $server = new ReconnectingTransport();
        $held = new HeldFailingWriteTransport($server);
        /** @var list<\Throwable> $errors */
        $errors = [];
        $client = $this->own(new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: 5_000,
            reconnectEnabled: false,
            errorListener: static function (\Throwable $error) use (&$errors): void {
                $errors[] = $error;
            },
        ), $held));
        $client->connect()->await();
        $server->responder = static fn(string $subject, ?string $replyTo): array => [];
        $pongError = new TransportClosedException('the socket would not take the PONG');
        $this->resources()->onRelease(static function () use ($held, $pongError): void {
            $held->failHeldWrite($pongError);
        }, 'held PONG');

        $fetch = $client->jetStream()->fetchBatch(self::STREAM, 'worker', 1, 3_000);
        $fetch->ignore();
        $this->waitUntil(static fn(): bool => $server->controlLinesStartingWith('PUB ' . self::PULL . ' ') !== []);
        $held->holdNextWriteContaining('PONG');
        $server->pushFrame(self::refusal(self::PULL) . "PING\r\n");
        [, $failure] = self::settle($fetch);

        self::assertTrue($held->writeHeld(), 'the PONG was still held back when the fetch ended');
        self::assertInstanceOf(PermissionViolationException::class, $failure, self::describe($failure));
        $stateWhenTheFetchEnded = $client->state();
        self::assertSame(ConnectionState::Open, $stateWhenTheFetchEnded);

        if ($closedMeanwhile) {
            $client->disconnect()->await();
        }
        $held->failHeldWrite($pongError);
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Closed);
        // The failure's handling runs from queued callbacks: give them their turn.
        delay(0.05);
        if ($closedMeanwhile) {
            self::assertNotContains($pongError, $errors, 'a PONG that failed with the closed connection is not reported');
        } else {
            self::assertContains($pongError, $errors, 'the error listener heard of the failed PONG');
        }
    }

    /** @return iterable<string, array{string}> */
    public static function closesThatComeBeforeTheLatePongFailure(): iterable
    {
        yield "the socket's EOF" => ['eof'];
        yield 'a fatal -ERR' => ['fatal'];
    }

    /**
     * As above, but the connection closes on its own before the PONG fails, reconnect off: a read meets the socket's EOF,
     * or a fatal -ERR, and the connection announces Closed once. The PONG's failure then announces nothing more, and is
     * not reported: the connection it was written on, and its close, are over (third review of #264). It used to
     * recover that connection once more, its generation unchanged, and announce a second Closed for the same socket,
     * which runs an application's cleanup twice. Fails on the code before the fix with two Closed events.
     */
    #[DataProvider('closesThatComeBeforeTheLatePongFailure')]
    public function testAPongThatFailsAfterTheConnectionClosedOnItsOwnAnnouncesNoSecondClose(string $close): void
    {
        $server = new ReconnectingTransport();
        $held = new HeldFailingWriteTransport($server);
        $recorder = new LifecycleRecorder();
        $client = $this->own(new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: 5_000,
            reconnectEnabled: false,
            connectionListener: $recorder->connectionListener(),
            errorListener: $recorder->errorListener(),
        ), $held));
        $client->connect()->await();
        $server->responder = static fn(string $subject, ?string $replyTo): array => [];
        $pongError = new TransportClosedException('the socket would not take the PONG');
        $this->resources()->onRelease(static function () use ($held, $pongError): void {
            $held->failHeldWrite($pongError);
        }, 'held PONG');

        $fetch = $client->jetStream()->fetchBatch(self::STREAM, 'worker', 1, 3_000);
        $fetch->ignore();
        $this->waitUntil(static fn(): bool => $server->controlLinesStartingWith('PUB ' . self::PULL . ' ') !== []);
        $held->holdNextWriteContaining('PONG');
        $server->pushFrame(self::refusal(self::PULL) . "PING\r\n");
        [, $failure] = self::settle($fetch);
        self::assertTrue($held->writeHeld(), 'the PONG was still held back when the fetch ended');
        self::assertInstanceOf(PermissionViolationException::class, $failure, self::describe($failure));

        // The connection ends on its own while the PONG is still held back.
        if ($close === 'eof') {
            $server->dropConnection();
        } else {
            $server->pushFrame("-ERR 'Stale Connection'\r\n");
        }
        try {
            $client->processIncoming()->await(new TimeoutCancellation(2));
        } catch (\Throwable) {
            // The EOF, or the -ERR: what ends the connection.
        }
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Closed);
        self::assertSame(1, $recorder->closedEvents(), 'the connection announced its close');

        $held->failHeldWrite($pongError);
        // The failure's handling runs from queued callbacks: give them their turn.
        delay(0.05);
        self::assertSame(1, $recorder->closedEvents(), 'the late PONG failure announced no second close: ' . implode(', ', array_map(static fn(ConnectionEvent $event): string => $event->name, $recorder->events)));
        self::assertSame([], $recorder->errorsContaining('the socket would not take the PONG'), 'nor was it reported');
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * The connection drains while a pull is in flight, and the pull's own read takes a chunk with the -ERR refusing the
     * pull and a server PING, which the drain answers with a PONG bounded by its budget. That PONG reaches the server
     * with its completion held. The fetch, with a 1.2 s budget, fails with the refusal within 0.5 s while the completion
     * is still held, and the drain, with a 3.5 s budget, keeps the write: once it is let go and the drain's flush
     * answered, the drain closes the connection. It used to write that PONG in place, bounded by the drain's budget
     * alone: the refused fetch stayed pending past its own deadline until the write completed, or the drain's budget
     * ran out (third review of #264). Fails on the code before the fix, the fetch ending only at the drain's 3.5 s
     * deadline.
     */
    public function testARefusedPullDoesNotWaitForThePongItsReadOwesTheServerWhileTheConnectionDrains(): void
    {
        [$server, , $holding, $client] = $this->connectOverTheScriptedServer(requestTimeoutMs: 3_500);
        $server->responder = static fn(string $subject, ?string $replyTo): array => [];
        // The drain's flush is answered by the test, once the fetch has ended.
        $server->answerPings = false;

        $fetch = $client->jetStream()->fetchBatch(self::STREAM, 'worker', 1, 200);
        $fetch->ignore();
        $this->waitUntil(static fn(): bool => $server->controlLinesStartingWith('PUB ' . self::PULL . ' ') !== []);
        $pingsBefore = count($server->controlLinesStartingWith('PING'));
        $drain = $client->drain();
        $drain->ignore();
        $this->resources()->ownOperation($drain, static function () use ($server): void {
            $server->pushFrame("PONG\r\n");
        }, 'drain');
        // The drain has written its flush's PING and waits for the fetch's read, on the socket, to let it read.
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Draining && count($server->controlLinesStartingWith('PING')) > $pingsBefore);

        $holding->holdCompletionOfNextWriteContaining('PONG', 5.0);
        $start = hrtime(true);
        $server->pushFrame(self::refusal(self::PULL) . "PING\r\n");
        [, $failure] = self::settle($fetch);
        $elapsed = (hrtime(true) - $start) / 1e9;

        // Before the PONG's writer is let go.
        self::assertSame(1, $holding->completionsHeld(), "the PONG's completion was still held when the fetch ended");
        self::assertInstanceOf(PermissionViolationException::class, $failure, sprintf('the fetch ended after %.3f s with %s', $elapsed, self::describe($failure)));
        self::assertSame(self::PULL, $failure->getSubject());
        self::assertLessThan(0.5, $elapsed, sprintf('the fetch failed after %.3f s, with a 1.2 s budget', $elapsed));
        self::assertSame(ConnectionState::Draining, $client->state(), 'the drain goes on');
        self::assertNoWatchLeft($client);

        $holding->releaseHeldCompletions();
        $server->pushFrame("PONG\r\n");
        $drain->await(new TimeoutCancellation(5));
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /** @return iterable<string, array{array<string, mixed>, int, string}> */
    public static function pullsWhoseRefusalIsNotMatched(): iterable
    {
        yield 'the defaults: an empty pull at its deadline' => [[], 408, 'No messages received within timeout'];
        yield 'a 100 ms idle_heartbeat: two silent intervals' => [['idle_heartbeat' => 100_000_000], 0, 'missed idle heartbeats'];
    }

    /**
     * The limit of #259 for pulls: an -ERR naming a subject the client cannot match to the pull's - here the subject an
     * account mapping would show - refuses nothing, and the pull ends by its usual rules for one nothing answers: with
     * the defaults as an empty pull, the JetStreamException (408), at its deadline; with an idle_heartbeat as the
     * JetStreamException (code 0) for missed heartbeats, once two intervals pass with nothing, well before it. A guard
     * of what the README, the changelog and fetchBatch() say (review of #264).
     *
     * @param array<string, mixed> $pull
     */
    #[DataProvider('pullsWhoseRefusalIsNotMatched')]
    public function testAPullWhoseRefusalTheClientCannotMatchEndsByItsUsualRules(array $pull, int $code, string $message): void
    {
        $transport = self::server([], errorsBefore: [self::PULL => ['orders.mapped.next']]);
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 5_000), $transport));
        $client->connect()->await();

        $start = hrtime(true);
        $failure = self::failureOf($client->jetStream()->fetchBatch(self::STREAM, 'worker', 1, 500, $pull));
        $elapsed = (hrtime(true) - $start) / 1e9;

        self::assertInstanceOf(JetStreamException::class, $failure, self::describe($failure));
        self::assertSame($code, $failure->getCode());
        self::assertStringContainsString($message, $failure->getMessage());
        if ($code === 0) {
            self::assertLessThan(1.0, $elapsed, sprintf('the missed heartbeats ended the pull after %.3f s, with a 1.5 s budget', $elapsed));
        } else {
            self::assertGreaterThan(0.45, $elapsed, sprintf('the empty pull ended after %.3f s, at its deadline', $elapsed));
        }
        self::assertNoWatchLeft($client);
    }

    /**
     * Guard of the watch itself: the first -ERR refusing the watched subject, bare here, is the one recorded, and the
     * callback is called once, however many refusals of the subject follow; an -ERR refusing another subject marks
     * nothing. Ending the watch leaves nothing behind. Fails on a variant that records, and calls back, at every
     * refusal of the subject.
     */
    public function testTheFirstRefusalOfAWatchedSubjectIsRecordedAndItsCallbackIsCalledOnce(): void
    {
        $transport = self::server([]);
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 5_000), $transport));
        $client->connect()->await();
        $calls = 0;
        $client->watchPublishRefusal(901, self::PULL, static function () use (&$calls): void {
            $calls++;
        });

        $transport->pushReadChunk(self::refusal(self::OTHER_PULL));
        self::assertGreaterThan(0, $client->processIncoming()->await());
        self::assertNull($client->publishRefusalOf(901), 'a refusal of another subject marks nothing');
        self::assertSame(0, $calls);

        $transport->pushReadChunk("-ERR 'Permissions Violation for Publish to " . self::PULL . "'\r\n" . self::refusal(self::PULL));
        self::assertGreaterThan(0, $client->processIncoming()->await());
        self::assertSame('Permissions Violation for Publish to ' . self::PULL, trim((string) $client->publishRefusalOf(901), "' "), 'the first refusal is the one recorded');
        self::assertSame(1, $calls, 'the callback is called once');

        $client->unwatchPublishRefusal(901);
        self::assertNull($client->publishRefusalOf(901));
        self::assertNoWatchLeft($client);
    }

    /**
     * Guard: a refusal callback that throws breaks neither the dispatch of the -ERR nor another watch on the subject:
     * both refusals are recorded, the other callback is still called, the read that brought the -ERR returns as usual
     * and the connection stays open. Fails with the callback's exception, from the read, without the guard around it.
     */
    public function testARefusalCallbackThatThrowsLeavesTheDispatchAndTheOtherWatchesAlone(): void
    {
        $transport = self::server([]);
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 5_000), $transport));
        $client->connect()->await();
        $calls = 0;
        $client->watchPublishRefusal(901, self::PULL, static function (): void {
            throw new \RuntimeException('a refusal callback that throws');
        });
        $client->watchPublishRefusal(902, self::PULL, static function () use (&$calls): void {
            $calls++;
        });

        $transport->pushReadChunk(self::refusal(self::PULL));
        self::assertGreaterThan(0, $client->processIncoming()->await());

        self::assertNotNull($client->publishRefusalOf(901), 'the refusal is recorded for the watch whose callback threw');
        self::assertNotNull($client->publishRefusalOf(902), 'and for the watch after it');
        self::assertSame(1, $calls, 'the callback after the one that threw is called');
        self::assertSame(ConnectionState::Open, $client->state());

        $client->unwatchPublishRefusal(901);
        $client->unwatchPublishRefusal(902);
        self::assertNoWatchLeft($client);
    }

    /**
     * A client over the scripted server, seen through a WatchedTransport, which tells when a read takes the socket,
     * and a CompletionHoldingTransport, which holds a writer after its bytes reached the server. No heartbeat, and no
     * reconnect: nothing writes or reads but the test's operations. The holds are let go when the test ends, whatever
     * ended it.
     *
     * @param (\Closure(ConnectionEvent, ?\Throwable): void)|null $connectionListener
     * @return array{ReconnectingTransport, WatchedTransport, CompletionHoldingTransport, NatsClient}
     */
    private function connectOverTheScriptedServer(?\Closure $connectionListener = null, int $requestTimeoutMs = 5_000): array
    {
        $server = new ReconnectingTransport();
        $watched = new WatchedTransport($server);
        $holding = new CompletionHoldingTransport($watched);
        $this->resources()->onRelease($holding->releaseHeldCompletions(...), 'held write completions');
        $client = $this->own(new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: $requestTimeoutMs,
            reconnectEnabled: false,
            connectionListener: $connectionListener,
        ), $holding));
        $client->connect()->await();

        return [$server, $watched, $holding, $client];
    }

    /**
     * The frames $queued names for the fetch's inbox, in order: a message 'm-1', a 409 Consumer Deleted.
     *
     * @param list<string> $queued
     */
    private static function framesForThePull(array $queued, string $replyTo, int $inboxSid): string
    {
        $frames = '';
        foreach ($queued as $kind) {
            $frames .= $kind === 'message'
                ? ReconnectingTransport::msgFrame($replyTo, $inboxSid, 'm-1', '$JS.ACK.ORDERS.worker.1.1.1.0.0')
                : ReconnectingTransport::hmsgFrame($replyTo, $inboxSid, "NATS/1.0 409 Consumer Deleted\r\n\r\n", '');
        }

        return $frames;
    }

    /**
     * What a fetch of 5 whose inbox got the frames $queued names ends with, refused or not: a message alone is returned
     * (a partial batch), a 409 alone is thrown as the JetStreamException(409) it is, and a message with a 409 behind
     * it is returned with the terminal callback called once.
     *
     * @param list<string> $queued
     * @param list<NatsMessage> $messages
     * @param list<array{int, string}> $terminal
     */
    private static function assertTheResultOfThePull(array $queued, array $messages, ?\Throwable $failure, array $terminal): void
    {
        if ($queued === ['status']) {
            self::assertInstanceOf(JetStreamException::class, $failure, self::describe($failure));
            self::assertSame(409, $failure->getCode());
            self::assertStringContainsString('Consumer Deleted', $failure->getMessage());
        } else {
            self::assertNull($failure, self::describe($failure));
            self::assertSame(['m-1'], self::payloads($messages));
        }
        self::assertSame($queued === ['message', 'status'] ? [[409, 'Consumer Deleted']] : [], $terminal, 'the terminal callback');
    }

    /**
     * An application loop that keeps a read on the socket, as a consumer's does, and reads on past whatever a read
     * throws. Stop it with the cancellation, then await the future. Owned by the test, with the cancellation as what
     * ends it, so that a test that fails before it stops the loop still leaves nothing reading.
     *
     * @return array{DeferredCancellation, Future<void>}
     */
    private function startReadLoop(NatsClient $client): array
    {
        $stop = new DeferredCancellation();
        $loop = async(static function () use ($client, $stop): void {
            while (!$stop->isCancelled()) {
                try {
                    $client->processIncoming($stop->getCancellation())->await();
                } catch (CancelledException) {
                    return;
                } catch (\Throwable) {
                    delay(0.001);
                }
            }
        });

        return [$stop, $this->resources()->ownOperation($loop, $stop->cancel(...), 'application read loop')];
    }

    /** Waits until a read takes the socket of $watched. */
    private function awaitRead(WatchedTransport $watched): void
    {
        self::nextRead($watched)->getFuture()->await(new TimeoutCancellation(2));
        $watched->onRead = null;
    }

    /**
     * Completed when the next read takes the socket of $watched.
     *
     * @return DeferredFuture<null>
     */
    private static function nextRead(WatchedTransport $watched): DeferredFuture
    {
        /** @var DeferredFuture<null> $reading */
        $reading = new DeferredFuture();
        $watched->onRead = static function () use ($reading): void {
            if (!$reading->isComplete()) {
                $reading->complete();
            }
        };

        return $reading;
    }

    /** @param \Closure(): bool $condition */
    private function waitUntil(\Closure $condition, float $seconds = 2.0): void
    {
        $deadline = hrtime(true) + (int) ($seconds * 1e9);
        while (!$condition()) {
            if (hrtime(true) > $deadline) {
                self::fail('condition not reached within ' . $seconds . ' s');
            }

            delay(0.001);
        }
    }

    /**
     * Awaits $fetch, within 5 s, past its 4 s budget: its messages and what it threw.
     *
     * @param Future<list<NatsMessage>> $fetch
     * @return array{list<NatsMessage>, ?\Throwable}
     */
    private static function settle(Future $fetch): array
    {
        try {
            return [$fetch->await(new TimeoutCancellation(5)), null];
        } catch (\Throwable $e) {
            return [[], $e];
        }
    }

    /** The reply subject the fetch's pull went out with. */
    private static function replySubjectOfThePull(ReconnectingTransport $server): string
    {
        $line = $server->controlLinesStartingWith('PUB ' . self::PULL . ' ')[0];
        if (preg_match('/^PUB \S+ (\S+) \d+$/', $line, $parts) !== 1) {
            self::fail('not a pull with a reply subject: ' . $line);
        }

        return $parts[1];
    }

    /** The sid of the fetch's inbox, from its SUB. */
    private static function fetchSid(ReconnectingTransport $server): int
    {
        $subs = $server->controlLinesStartingWith('SUB _INBOX.JS.FETCH.');
        self::assertCount(1, $subs);
        $parts = explode(' ', $subs[0]);

        return (int) $parts[count($parts) - 1];
    }

    private static function connectionOf(NatsClient $client): NatsConnection
    {
        $connection = (new \ReflectionProperty(NatsClient::class, 'connection'))->getValue($client);
        self::assertInstanceOf(NatsConnection::class, $connection);

        return $connection;
    }

    /**
     * A server that answers every SUB silently and every PUB with, in order: the -ERRs refusing the subjects
     * $errorsBefore gives for that subject, the messages $deliveries gives for it, delivered on its reply inbox, the
     * -ERRs refusing the subjects $errorsAfter gives for it, and the -ERR refusing it when it is in $refused, as
     * nats-server writes them.
     *
     * @param list<string> $refused
     * @param array<string, list<string>> $deliveries
     * @param array<string, list<string>> $errorsAfter
     * @param array<string, list<string>> $errorsBefore
     */
    private static function server(array $refused, array $deliveries = [], array $errorsAfter = [], array $errorsBefore = []): FakeTransport
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        /** @var array<string, int> $sids */
        $sids = [];
        $transport->onWrite = static function (string $bytes) use ($refused, $deliveries, $errorsAfter, $errorsBefore, &$sids): array {
            $frames = [];
            $lines = explode("\r\n", $bytes);
            for ($index = 0, $count = count($lines); $index < $count; $index++) {
                $parts = explode(' ', $lines[$index]);
                if ($parts[0] === 'SUB' && count($parts) >= 3) {
                    $sids[$parts[1]] = (int) $parts[count($parts) - 1];

                    continue;
                }
                if ($parts[0] !== 'PUB' || count($parts) < 3) {
                    continue;
                }

                // The payload line.
                $index++;
                $subject = $parts[1];
                $replyTo = count($parts) === 4 ? $parts[2] : null;
                foreach ($errorsBefore[$subject] ?? [] as $other) {
                    $frames[] = self::refusal($other);
                }
                if ($replyTo !== null && isset($sids[$replyTo])) {
                    foreach ($deliveries[$subject] ?? [] as $payload) {
                        $frames[] = sprintf("MSG %s %d %d\r\n%s\r\n", $replyTo, $sids[$replyTo], strlen($payload), $payload);
                    }
                }
                foreach ($errorsAfter[$subject] ?? [] as $other) {
                    $frames[] = self::refusal($other);
                }
                if (in_array($subject, $refused, true)) {
                    $frames[] = self::refusal($subject);
                }
            }

            return $frames;
        };

        return $transport;
    }

    private static function refusal(string $subject): string
    {
        return "-ERR 'Permissions Violation for Publish to \"" . $subject . "\"'\r\n";
    }

    /** @return Future<list<NatsMessage>> */
    private static function fetch(string $operation, JetStreamContext $js, string $consumer): Future
    {
        return $operation === 'fetchNext'
            ? $js->fetchNext(self::STREAM, $consumer, 5_000)->map(static fn(NatsMessage $message): array => [$message])
            : $js->fetchBatch(self::STREAM, $consumer, 1, 5_000);
    }

    /** @param Future<mixed> $future */
    private static function failureOf(Future $future): ?\Throwable
    {
        try {
            $future->await();
        } catch (\Throwable $e) {
            return $e;
        }

        return null;
    }

    private static function describe(?\Throwable $error): string
    {
        return $error === null ? 'no failure' : $error::class . ': ' . $error->getMessage();
    }

    /**
     * @param list<NatsMessage> $messages
     * @return list<string>
     */
    private static function payloads(array $messages): array
    {
        return array_map(static fn(NatsMessage $message): string => $message->payload, $messages);
    }

    /** @return list<string> */
    private static function linesStartingWith(FakeTransport $transport, string $prefix): array
    {
        $lines = [];
        foreach ($transport->writes as $write) {
            foreach (explode("\r\n", $write) as $line) {
                if (str_starts_with($line, $prefix)) {
                    $lines[] = $line;
                }
            }
        }

        return $lines;
    }

    /** No watch, refusal or refusal callback is left for a pull that has ended. */
    private static function assertNoWatchLeft(NatsClient $client): void
    {
        $connection = self::connectionOf($client);
        foreach (['publishRefusalWatches', 'publishRefusals', 'publishRefusalCallbacks'] as $property) {
            $state = (new \ReflectionProperty(NatsConnection::class, $property))->getValue($connection);
            self::assertSame([], $state, $property . ' is empty once every pull has ended');
        }
    }
}
