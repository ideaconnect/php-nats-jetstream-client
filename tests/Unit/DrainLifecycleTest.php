<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\AuthenticationException;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\UncancellableDialTransport;
use IDCT\NATS\Tests\Support\ThrowingLogger;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;

/**
 * drain() and drainSubscription() where they meet reconnects and closes: every drain ends Closed with
 * one Closed event, connect() is refused for as long as a drain runs, a reconnect a drain or a
 * disconnect() stops really stops, nothing a drain discards goes unreported, and a subscription removed
 * by the client is never left subscribed on the server by a reconnect.
 */
final class DrainLifecycleTest extends TestCase
{
    use ReconnectScenarios;

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    public function testDrainOfAnOpenConnectionEndsWithAClosedEvent(): void
    {
        $recorder = new LifecycleRecorder();
        $connection = $this->connect(new ReconnectingTransport(), connectionListener: $recorder->connectionListener());

        $connection->drain()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Closed], $recorder->events);
    }

    public function testDrainThatWaitedForAReconnectEndsWithOneClosedEvent(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, connectionListener: $recorder->connectionListener());
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $this->acceptDialsAfter($transport, 0.05);
        $connection->drain()->await();

        self::assertSame(
            [ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected, ConnectionEvent::Closed],
            $recorder->events,
        );
        $reader->await();
    }

    public function testDrainThatRanOutOfTimeWaitingForTheReconnectEndsWithOneClosedEvent(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, requestTimeoutMs: 100, connectionListener: $recorder->connectionListener());
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $connection->drain()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Closed], $recorder->events);
        $reader->await();
    }

    public function testDrainThatCouldNotWaitForTheReconnectEndsWithOneClosedEvent(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, waitForReconnect: false, connectionListener: $recorder->connectionListener());
        $reader = $this->startRecoveryInBackground($connection, $transport);

        try {
            $connection->drain()->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Cannot drain while reconnecting: the connection was closed instead', $e->getMessage());
        }

        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Closed], $recorder->events);
        $reader->await();
    }

    public function testDrainWaitingForAReconnectThatGivesUpEndsWithOneClosedEvent(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, maxReconnectAttempts: 3, connectionListener: $recorder->connectionListener());
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $connection->drain()->await();

        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Closed], $recorder->events);

        try {
            $reader->await();
            self::fail('the reader that ran the recovery gets its failure');
        } catch (ConnectionException $e) {
            self::assertSame('Reconnect attempts exhausted', $e->getMessage());
        }
    }

    /**
     * A logger that throws while the reconnect that gave up reports the publishes it discards cannot
     * break the cleanup that follows: the connection still ends Closed, announced once, and the drain
     * waiting for that reconnect resolves.
     */
    public function testDrainWaitingForAReconnectThatGivesUpEndsClosedEvenWhenTheLoggerThrows(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $logger = new class extends AbstractLogger {
            /** @param array<mixed> $context */
            public function log($level, string|\Stringable $message, array $context = []): void
            {
                if (str_contains((string) $message, 'Reconnect exhausted')) {
                    throw new \RuntimeException('logger failed');
                }
            }
        };
        $connection = new NatsConnection(
            new NatsOptions(
                connectTimeoutMs: 500,
                requestTimeoutMs: 2_000,
                reconnectEnabled: true,
                maxReconnectAttempts: 2,
                reconnectDelayMs: 5,
                reconnectMaxDelayMs: 20,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
                connectionListener: $recorder->connectionListener(),
                logger: $logger,
            ),
            $transport,
        );
        $this->opened[] = $connection;
        $connection->connect()->await();
        $reader = $this->startRecoveryInBackground($connection, $transport);
        $connection->publish('events', 'buffered')->await();

        $connection->drain()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(1, $recorder->closedEvents());

        try {
            $reader->await();
            self::fail('the reader that ran the recovery gets its failure');
        } catch (ConnectionException $e) {
            self::assertSame('Reconnect attempts exhausted', $e->getMessage());
        }
    }

    /**
     * A shutdown that stops waiting for a drain and disconnect()s: the drain still resolves, and the
     * close is announced once, not by both.
     */
    public function testDisconnectDuringADrainEndsWithOneClosedEvent(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, connectionListener: $recorder->connectionListener());
        // The drain's flush waits for a PONG that never comes.
        $transport->answerPings = false;
        $drain = $connection->drain();
        $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Draining);
        delay(0.02);

        $connection->disconnect()->await();
        $drain->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Closed], $recorder->events);
    }

    /**
     * The Closed event comes once the drain is over, so a supervisor that reconnects on Closed can do it
     * straight from its listener.
     */
    public function testClosedListenerCanReconnectStraightAfterADrain(): void
    {
        $transport = new ReconnectingTransport();
        $holder = new class {
            public ?NatsConnection $connection = null;
            public bool $reconnecting = false;
            public ?\Throwable $failure = null;
        };
        $listener = static function (ConnectionEvent $event) use ($holder): void {
            $connection = $holder->connection;
            if ($event !== ConnectionEvent::Closed || $connection === null || $holder->reconnecting) {
                return;
            }

            $holder->reconnecting = true;
            try {
                $connection->connect()->await();
            } catch (\Throwable $e) {
                $holder->failure = $e;
            }
        };
        $connection = $this->connect($transport, connectionListener: $listener);
        $holder->connection = $connection;

        $connection->drain()->await();

        self::assertTrue($holder->reconnecting, 'the drain announced the close');
        self::assertNull($holder->failure);
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $transport->epoch());
    }

    /** @return iterable<string, array{string}> */
    public static function listenersOfAReconnectThatGaveUp(): iterable
    {
        yield 'error listener, on the exhaustion report' => ['error'];
        yield 'connection listener, on Closed' => ['closed'];
    }

    /**
     * A drain() from a listener of a reconnect that just gave up finds the connection already Closed: it
     * fails like any drain of a closed connection, and does not close it - or announce it - a second time.
     */
    #[DataProvider('listenersOfAReconnectThatGaveUp')]
    public function testDrainFromAListenerOfAReconnectThatGaveUpFailsWithoutClosingAgain(string $listener): void
    {
        $transport = new ReconnectingTransport();
        $holder = new class {
            public ?NatsConnection $connection = null;
            public bool $attempted = false;
            public ?string $outcome = null;
            public int $closedEvents = 0;
        };
        $drainOnce = static function () use ($holder): void {
            $connection = $holder->connection;
            if ($connection === null || $holder->attempted) {
                return;
            }

            $holder->attempted = true;
            try {
                $connection->drain()->await();
                $holder->outcome = 'drained';
            } catch (\Throwable $e) {
                $holder->outcome = $e->getMessage();
            }
        };
        $connection = $this->connect(
            $transport,
            maxReconnectAttempts: 2,
            connectionListener: static function (ConnectionEvent $event) use ($holder, $listener, $drainOnce): void {
                if ($event !== ConnectionEvent::Closed) {
                    return;
                }

                $holder->closedEvents++;
                if ($listener === 'closed') {
                    $drainOnce();
                }
            },
            errorListener: static function (\Throwable $error) use ($listener, $drainOnce): void {
                if ($listener === 'error' && str_starts_with($error->getMessage(), 'Reconnect exhausted')) {
                    $drainOnce();
                }
            },
        );
        $holder->connection = $connection;
        $reader = $this->startRecoveryInBackground($connection, $transport);
        // Reported as discarded when the reconnect gives up.
        $connection->publish('events', 'buffered')->await();

        try {
            $reader->await();
            self::fail('the reader that ran the recovery gets its failure');
        } catch (ConnectionException $e) {
            self::assertSame('Reconnect attempts exhausted', $e->getMessage());
        }

        self::assertSame('Connection is not open', $holder->outcome);
        self::assertSame(1, $holder->closedEvents);
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    public function testSecondDrainOfAnOpenConnectionFails(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 200);
        $transport->answerPings = false;
        $first = $connection->drain();
        $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Draining);

        try {
            $connection->drain()->await();
            self::fail('expected the second drain to fail');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }

        $first->await();
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /** A very small request timeout still gives drain() a 100 ms budget. */
    public function testDrainBudgetIsAtLeastOneHundredMilliseconds(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 1);
        // The drain waits for its PONG until the budget ends.
        $transport->answerPings = false;

        $start = hrtime(true);
        $connection->drain()->await();
        $elapsed = $this->secondsSince($start);

        self::assertGreaterThanOrEqual(0.09, $elapsed);
        self::assertLessThan(0.4, $elapsed);
    }

    public function testConnectIsRefusedWhileADrainWaitsForAReconnect(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $reader = $this->startRecoveryInBackground($connection, $transport);
        $drain = $connection->drain();
        delay(0.02);

        $this->acceptDialsAfter($transport, 0.1);
        try {
            $connection->connect()->await();
            self::fail('expected connect() to be refused while the drain runs');
        } catch (ConnectionException $e) {
            self::assertSame('Cannot connect: drain in progress', $e->getMessage());
        }

        $drain->await();
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(1, $connection->statistics()->reconnects, 'only the reconnect the drain waited for');
        $reader->await();
    }

    /**
     * A drain whose budget ran out waiting for the reconnect stops it, then runs its backlog pass. A
     * connect() meanwhile - a supervisor reacting to the connection being Closed - is refused: the
     * drain's teardown would destroy the connection it opened. Once the drain is over the connection
     * can be reopened, and it stays open.
     */
    public function testConnectIsRefusedWhileADrainThatRanOutOfTimeWindsDown(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 150);
        $supervisor = new class {
            public bool $attempted = false;
            public ?ConnectionState $state = null;
            public ?\Throwable $failure = null;
        };
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use ($connection, $transport, $supervisor): void {
            if ($message->payload === 'boom') {
                throw new \RuntimeException('handler failed');
            }

            // Delivered by the drain's backlog pass, once its budget is spent and the reconnect stopped:
            // a supervisor reacts to the connection being Closed while this handler is still busy.
            EventLoop::delay(0.02, static function () use ($connection, $transport, $supervisor): void {
                $transport->acceptDials();
                $supervisor->attempted = true;
                $supervisor->state = $connection->state();
                try {
                    $connection->connect()->await();
                } catch (\Throwable $e) {
                    $supervisor->failure = $e;
                }
            });
            delay(0.2);
        })->await();
        // 'slow' stays queued behind the handler failure.
        $transport->pushFrame(ReconnectingTransport::msgFrame('updates', $sid, 'boom') . ReconnectingTransport::msgFrame('updates', $sid, 'slow'));
        $this->expectHandlerFailure($connection);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $connection->drain()->await();

        self::assertTrue($supervisor->attempted);
        self::assertSame(ConnectionState::Closed, $supervisor->state, 'the stopped reconnect had already ended');
        self::assertInstanceOf(ConnectionException::class, $supervisor->failure);
        self::assertSame('Cannot connect: drain in progress', $supervisor->failure->getMessage());
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(0, $transport->epoch(), 'nothing was dialled while the drain ran');

        $connection->connect()->await();
        delay(0.05);
        self::assertSame(ConnectionState::Open, $connection->state());
        $connection->publish('events', 'after')->await();
        self::assertSame(['PUB events 5'], $transport->controlLinesStartingWith('PUB events', 1));
        $reader->await();
    }

    /**
     * A publish while a drain whose budget ran out closes the transport fails: the reconnect is
     * stopping, so nothing would ever send it. It is not buffered to outlive the drain and go out on a
     * later connection.
     */
    public function testPublishWhileATimedOutDrainClosesTheTransportFailsInsteadOfOutlivingTheDrain(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 100);
        $reader = $this->startRecoveryHeldMidDial($connection, $transport);
        // The drain's own close is slow (the reconnect already closed the old session), and a publish
        // lands while it is under way.
        $transport->closeDelay = 0.2;
        $publisher = new class {
            public ?ConnectionState $state = null;
            public bool $published = false;
            public ?\Throwable $failure = null;
        };
        $transport->beforeClose = static function () use ($connection, $transport, $publisher): void {
            $transport->beforeClose = null;
            EventLoop::queue(static function () use ($connection, $publisher): void {
                $publisher->state = $connection->state();
                try {
                    $connection->publish('events', 'late')->await();
                    $publisher->published = true;
                } catch (\Throwable $e) {
                    $publisher->failure = $e;
                }
            });
        };

        $connection->drain()->await();

        self::assertSame(ConnectionState::Connecting, $publisher->state, 'the publish landed while the drain closed the transport');
        self::assertFalse($publisher->published);
        self::assertInstanceOf(ConnectionException::class, $publisher->failure);
        self::assertSame('Connection is not open', $publisher->failure->getMessage());

        // Nothing is left behind to go out on a later connection.
        $transport->closeDelay = 0.0;
        $transport->releaseDial();
        $reader->await();
        $connection->connect()->await();
        $nextReader = $this->startRecoveryInBackground($connection, $transport);
        $transport->acceptDials();
        $this->waitUntilOpen($connection);
        self::assertSame([], $transport->controlLinesStartingWith('PUB events'));
        $nextReader->await();
    }

    /**
     * A drain whose budget ran out waiting for the reconnect still runs its backlog pass, and that pass
     * stops at the deadline like the drain of an open connection: the rest is reported as undelivered
     * instead of holding the drain far past its budget.
     */
    public function testDrainThatRanOutOfTimeStopsItsBacklogPassAtTheDeadline(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, requestTimeoutMs: 150, errorListener: $recorder->errorListener());
        $handled = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use ($handled): void {
            if ($message->payload === 'boom') {
                throw new \RuntimeException('handler failed');
            }

            $handled->payloads[] = $message->payload;
            // 20 ms of synchronous work per message.
            usleep(20_000);
        })->await();
        $chunk = ReconnectingTransport::msgFrame('updates', $sid, 'boom');
        for ($i = 1; $i <= 20; $i++) {
            $chunk .= ReconnectingTransport::msgFrame('updates', $sid, 'm' . $i);
        }
        $transport->pushFrame($chunk);
        $this->expectHandlerFailure($connection);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $start = hrtime(true);
        $connection->drain()->await();

        self::assertLessThan(0.3, $this->secondsSince($start));
        self::assertSame(['m1'], $handled->payloads, 'only the head of the pass once the budget is spent');
        self::assertContains('drain deadline exceeded: 19 buffered message(s) were not delivered before close', $recorder->errors);
        $reader->await();
    }

    /**
     * The reconnect a drain waits for completes only once its Reconnected listener returns. A slow
     * listener can hold it past the drain budget with the connection already back: the drain still
     * drains that connection - it unsubscribes there - instead of just closing it.
     */
    public function testDrainOutlastedByASlowReconnectedListenerStillDrainsTheReopenedConnection(): void
    {
        $transport = new ReconnectingTransport();
        $listener = new class {
            public bool $busy = true;
        };
        $connection = $this->connect(
            $transport,
            requestTimeoutMs: 150,
            connectionListener: static function (ConnectionEvent $event) use ($listener): void {
                // Still busy when the drain's budget runs out.
                while ($event === ConnectionEvent::Reconnected && $listener->busy) {
                    delay(0.01);
                }
            },
        );
        $sid = $connection->subscribe('updates', static function (): void {})->await();
        $reader = $this->startRecoveryInBackground($connection, $transport);
        $transport->acceptDials();

        $connection->drain()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(1, $transport->epoch());
        self::assertSame(['UNSUB ' . $sid], $transport->controlLinesStartingWith('UNSUB', 1));
        $listener->busy = false;
        $reader->await();
    }

    /**
     * A drain whose budget ran out stops the reconnect it waited for at once, mid-backoff too, so the
     * connection can be reopened right after the drain.
     */
    public function testConnectRightAfterADrainThatRanOutOfTimeSucceeds(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 100, reconnectDelayMs: 1_000, reconnectMaxDelayMs: 1_000);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $connection->drain()->await();
        $transport->acceptDials();

        $start = hrtime(true);
        $connection->connect()->await();

        self::assertLessThan(0.2, $this->secondsSince($start));
        self::assertSame(ConnectionState::Open, $connection->state());
        $reader->await();
    }

    /**
     * A reconnect attempt that fails after the user closed the connection ends the reconnect there: no
     * backoff first, and no "attempts exhausted" failure or second Closed event for the user's close.
     */
    public function testReconnectAttemptFailingAfterADisconnectStopsWithoutBackingOffOrReportingExhaustion(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect(
            $transport,
            maxReconnectAttempts: 1,
            connectionListener: $recorder->connectionListener(),
            reconnectDelayMs: 1_000,
            reconnectMaxDelayMs: 1_000,
        );
        $reader = $this->startRecoveryHeldMidDial($connection, $transport);

        $connection->disconnect()->await();
        $transport->refuseDials();
        $transport->releaseDial();

        $start = hrtime(true);
        // Resolves: the reconnect was stopped, it did not fail.
        $reader->await();

        self::assertLessThan(0.2, $this->secondsSince($start));
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(1, $recorder->closedEvents());
    }

    /** The same when the user closes during the backoff that follows the last attempt. */
    public function testDisconnectDuringTheLastReconnectBackoffDoesNotReportExhaustion(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect(
            $transport,
            maxReconnectAttempts: 1,
            connectionListener: $recorder->connectionListener(),
            reconnectDelayMs: 1_000,
            reconnectMaxDelayMs: 1_000,
        );
        $reader = $this->startRecoveryInBackground($connection, $transport);
        // The only attempt was refused: the reconnect is backing off.
        delay(0.05);

        $connection->disconnect()->await();

        $start = hrtime(true);
        $reader->await();

        self::assertLessThan(0.2, $this->secondsSince($start));
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(1, $recorder->closedEvents());
    }

    /**
     * A drain that cannot wait for the reconnect closes the connection, and reports the messages already
     * received but not yet delivered alongside the buffered publishes - here there are none - instead of
     * dropping them silently.
     */
    public function testDrainThatCannotWaitReportsTheMessagesItDiscards(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, waitForReconnect: false, errorListener: $recorder->errorListener());
        $sid = $connection->subscribe('updates', static function (NatsMessage $message): void {
            if ($message->payload === 'boom') {
                throw new \RuntimeException('handler failed');
            }
        })->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('updates', $sid, 'boom')
            . ReconnectingTransport::msgFrame('updates', $sid, 'q1')
            . ReconnectingTransport::msgFrame('updates', $sid, 'q2'),
        );
        $this->expectHandlerFailure($connection);
        // Held mid-dial, the reconnect cannot deliver them itself before the drain closes the connection.
        $reader = $this->startRecoveryHeldMidDial($connection, $transport);

        try {
            $connection->drain()->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Cannot drain while reconnecting: the connection was closed instead', $e->getMessage());
        }

        self::assertContains('Connection closed: 2 parsed inbound message(s) were discarded undelivered', $recorder->errors);
        self::assertSame([], $recorder->errorsContaining('buffered publishes'), 'nothing was buffered');
        self::assertFalse($connection->isSubscriptionActive($sid), 'the subscriptions were released with the close');
        $transport->releaseDial();
        $reader->await();
    }

    /**
     * A publish while a drain that cannot wait closes the transport fails, instead of being accepted and
     * then dropped with the connection.
     */
    public function testPublishWhileADrainThatCannotWaitClosesTheTransportFails(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, waitForReconnect: false, errorListener: $recorder->errorListener());
        $reader = $this->startRecoveryHeldMidDial($connection, $transport);
        $connection->publish('events', 'early')->await();
        $transport->closeDelay = 0.1;

        $publisher = new class {
            public bool $published = false;
            public ?\Throwable $failure = null;
        };
        EventLoop::delay(0.05, static function () use ($connection, $publisher): void {
            try {
                $connection->publish('events', 'late')->await();
                $publisher->published = true;
            } catch (\Throwable $e) {
                $publisher->failure = $e;
            }
        });

        try {
            $connection->drain()->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException) {
            // Closed instead of drained.
        }

        self::assertFalse($publisher->published);
        self::assertInstanceOf(ConnectionException::class, $publisher->failure);
        self::assertSame(
            [sprintf('Drain could not wait for the reconnect: %d bytes of buffered publishes were discarded', strlen("PUB events 5\r\nearly\r\n"))],
            $recorder->errorsContaining('buffered publishes'),
        );
        $transport->closeDelay = 0.0;
        $transport->releaseDial();
        $reader->await();
    }

    /**
     * A drain waiting for a reconnect that the server refuses to authenticate ends Closed, and the
     * publishes buffered during the outage are reported as discarded.
     */
    public function testDrainWaitingForAReconnectThatFailsAuthenticationEndsClosedAndReportsTheDiscardedPublishes(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect(
            $transport,
            connectionListener: $recorder->connectionListener(),
            errorListener: $recorder->errorListener(),
        );
        $reader = $this->startRecoveryInBackground($connection, $transport);
        $connection->publish('events', 'buffered')->await();

        $transport->rejectAuthentication = true;
        $this->acceptDialsAfter($transport, 0.05);
        $connection->drain()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertContains(
            sprintf('Reconnect failed authentication: %d bytes of buffered publishes were discarded', strlen("PUB events 8\r\nbuffered\r\n")),
            $recorder->errors,
        );
        self::assertSame(1, $recorder->closedEvents());

        try {
            $reader->await();
            self::fail('the reader that ran the recovery gets its failure');
        } catch (AuthenticationException) {
            // The server refused the credentials.
        }
    }

    /**
     * A drain() from a Connected listener - the connect() that emitted it is still returning - while a
     * reconnect runs on another fiber waits for that reconnect like any other caller, then drains.
     */
    public function testDrainFromAConnectedListenerWaitsForAReconnectRunningElsewhere(): void
    {
        $transport = new ReconnectingTransport();
        $holder = new class {
            public ?NatsConnection $connection = null;
            public bool $drained = false;
            public ?\Throwable $failure = null;
            /** @var ?Future<int> */
            public ?Future $reader = null;
        };
        $listener = static function (ConnectionEvent $event) use ($holder, $transport): void {
            $connection = $holder->connection;
            if ($event !== ConnectionEvent::Connected || $connection === null) {
                return;
            }

            // The connection drops while this listener runs, and another fiber's read starts the reconnect.
            $transport->refuseDials();
            $transport->dropConnection();
            $holder->reader = async(static fn(): int => $connection->processIncoming()->await());
            delay(0.02);
            EventLoop::delay(0.05, static function () use ($transport): void {
                $transport->acceptDials();
            });

            try {
                $connection->drain()->await();
                $holder->drained = true;
            } catch (\Throwable $e) {
                $holder->failure = $e;
            }
        };
        $connection = new NatsConnection($this->options(true, 2_000, 1_000, 0, 2, $listener, 5, 20, null), $transport);
        $this->opened[] = $connection;
        $holder->connection = $connection;

        try {
            $connection->connect()->await();
            self::fail('the connection was drained before connect() returned');
        } catch (ConnectionException $e) {
            self::assertSame('Connect was aborted before the connection opened', $e->getMessage());
        }

        self::assertNull($holder->failure);
        self::assertTrue($holder->drained);
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(1, $connection->statistics()->reconnects);
        self::assertNotNull($holder->reader);
        $holder->reader->await();
    }

    /**
     * drainSubscription() unsubscribes, then flushes. A reconnect during that flush - here the heartbeat
     * gives up on a server that stopped answering - must not re-subscribe the sid: the server would keep
     * a subscription nothing handles any more, and for a queue group one that takes a share of the
     * group's messages.
     */
    public function testDrainSubscriptionIsNotReplayedByAReconnectDuringItsFlush(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, pingIntervalSeconds: 0.05, maxPingsOut: 1);
        $drained = $connection->subscribe('orders', static function (): void {}, 'workers')->await();
        $kept = $connection->subscribe('updates', static function (): void {})->await();
        // The flush waits for a PONG that never comes while the heartbeat reconnects.
        $transport->silence();

        $connection->drainSubscription($drained)->await();

        self::assertSame(1, $transport->epoch());
        self::assertSame(['SUB updates ' . $kept], $transport->controlLinesStartingWith('SUB', 1));
        self::assertNull($transport->sidFor('orders'));
        self::assertFalse($connection->isSubscriptionActive($drained));
        self::assertTrue($connection->isSubscriptionActive($kept));
    }

    /** The same when the flush's own read finds the connection gone and runs the reconnect itself. */
    public function testDrainSubscriptionIsNotReplayedWhenItsFlushLosesTheConnection(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $drained = $connection->subscribe('orders', static function (): void {}, 'workers')->await();
        $kept = $connection->subscribe('updates', static function (): void {})->await();
        // The connection dies once the flush's PING is out, before its PONG.
        $transport->answerPings = false;
        $transport->afterWrite = static function (string $bytes) use ($transport): void {
            if ($bytes === "PING\r\n") {
                $transport->afterWrite = null;
                $transport->dropConnection();
            }
        };

        $connection->drainSubscription($drained)->await();

        self::assertSame(1, $transport->epoch());
        self::assertSame(['SUB updates ' . $kept], $transport->controlLinesStartingWith('SUB', 1));
        self::assertNull($transport->sidFor('orders'));
        self::assertFalse($connection->isSubscriptionActive($drained));
    }

    /**
     * An UNSUB that fails because the connection just dropped still ends the drain: the messages already
     * received are delivered, the subscription is removed, and the reconnect does not bring it back.
     */
    public function testDrainSubscriptionWhoseUnsubscribeFailsStillDeliversAndIsNotReplayed(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $handled = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $drained = $connection->subscribe('orders', static function (NatsMessage $message) use ($handled): void {
            if ($message->payload === 'boom') {
                throw new \RuntimeException('handler failed');
            }

            $handled->payloads[] = $message->payload;
        }, 'workers')->await();
        $kept = $connection->subscribe('updates', static function (): void {})->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('orders', $drained, 'boom') . ReconnectingTransport::msgFrame('orders', $drained, 'q1'));
        $this->expectHandlerFailure($connection);
        // The UNSUB write finds the connection dead.
        $transport->failNextWriteContaining('UNSUB');

        $connection->drainSubscription($drained)->await();

        self::assertSame([], $transport->controlLinesStartingWith('UNSUB', 0), 'the UNSUB never went out');
        self::assertSame(['q1'], $handled->payloads);
        self::assertFalse($connection->isSubscriptionActive($drained));

        // A read notices the dropped connection and reconnects.
        $reader = async(static fn(): int => $connection->processIncoming()->await());
        $this->waitUntil(static fn(): bool => $transport->epoch() === 1 && $connection->state() === ConnectionState::Open);
        self::assertSame(['SUB updates ' . $kept], $transport->controlLinesStartingWith('SUB', 1));
        $reader->await();
    }

    /**
     * drainSubscription() while reconnecting waits for the reconnect, like flush(), and then drains the
     * subscription on the new connection: what already arrived is delivered, and the reconnect does not
     * bring the subscription back.
     */
    public function testDrainSubscriptionDuringAReconnectWaitsForItThenDeliversWhatAlreadyArrived(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, errorListener: $recorder->errorListener());
        $handled = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $drained = $connection->subscribe('orders', static function (NatsMessage $message) use ($handled): void {
            if ($message->payload === 'boom') {
                throw new \RuntimeException('handler failed');
            }

            $handled->payloads[] = $message->payload;
        })->await();
        $kept = $connection->subscribe('updates', static function (): void {})->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('orders', $drained, 'boom')
            . ReconnectingTransport::msgFrame('orders', $drained, 'q1')
            . ReconnectingTransport::msgFrame('orders', $drained, 'q2'),
        );
        $this->expectHandlerFailure($connection);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $drain = $connection->drainSubscription($drained);
        delay(0.05);
        self::assertFalse($drain->isComplete(), 'it waits for the reconnect');
        $transport->acceptDials();
        $drain->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(['q1', 'q2'], $handled->payloads);
        self::assertFalse($connection->isSubscriptionActive($drained));
        self::assertSame(['SUB updates ' . $kept], $transport->controlLinesStartingWith('SUB', 1), 'not re-subscribed');
        self::assertSame([], $recorder->errorsContaining('drainSubscription'), 'nothing was discarded');
        $reader->await();
    }

    /**
     * When its budget runs out before the reconnect finishes, drainSubscription() delivers what already
     * arrived and removes the subscription, which the reconnect then does not bring back.
     */
    public function testDrainSubscriptionThatRunsOutOfTimeWaitingForAReconnectDeliversWhatArrived(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 200);
        $handled = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $drained = $connection->subscribe('orders', static function (NatsMessage $message) use ($handled): void {
            if ($message->payload === 'boom') {
                throw new \RuntimeException('handler failed');
            }

            $handled->payloads[] = $message->payload;
        })->await();
        $kept = $connection->subscribe('updates', static function (): void {})->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('orders', $drained, 'boom') . ReconnectingTransport::msgFrame('orders', $drained, 'q1'));
        $this->expectHandlerFailure($connection);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $start = hrtime(true);
        $connection->drainSubscription($drained)->await();

        self::assertGreaterThanOrEqual(0.15, $this->secondsSince($start), 'it waited for the reconnect first');
        self::assertSame(ConnectionState::Connecting, $connection->state());
        self::assertSame(['q1'], $handled->payloads);
        self::assertFalse($connection->isSubscriptionActive($drained));

        $transport->acceptDials();
        $this->waitUntilOpen($connection);
        self::assertSame(['SUB updates ' . $kept], $transport->controlLinesStartingWith('SUB', 1));
        $reader->await();
    }

    /**
     * During a drain() the whole connection is already unsubscribed, and the drain's flush may still bring
     * messages for the subscription: drainSubscription() resolves at once and leaves the subscription to
     * the drain, which delivers what arrived - and what its flush still reads - before removing it.
     */
    public function testDrainSubscriptionDuringADrainLeavesTheSubscriptionToTheDrain(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 200);
        $handled = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $sid = $connection->subscribe('orders', static function (NatsMessage $message) use ($handled): void {
            if ($message->payload === 'boom') {
                throw new \RuntimeException('handler failed');
            }

            $handled->payloads[] = $message->payload;
        })->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('orders', $sid, 'boom') . ReconnectingTransport::msgFrame('orders', $sid, 'q1'));
        $this->expectHandlerFailure($connection);
        // drain() stays in its flush until the test answers its PING.
        $transport->answerPings = false;
        $drain = $connection->drain();
        $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Draining);

        $connection->drainSubscription($sid)->await();
        self::assertTrue($connection->isSubscriptionActive($sid), 'the drain removes it');

        // The server sent one more message before it processed the drain's UNSUB, then answers the PING.
        $transport->pushFrame(ReconnectingTransport::msgFrame('orders', $sid, 'in-flight') . "PONG\r\n");
        $drain->await();

        self::assertSame(['q1', 'in-flight'], $handled->payloads);
        self::assertFalse($connection->isSubscriptionActive($sid));
    }

    /**
     * drainSubscription() while a delivery for the sid is under way on another fiber - a handler still
     * busy with the previous message - hands the rest over to that delivery: the message queued behind it
     * is still delivered, and only then is the subscription removed.
     */
    public function testDrainSubscriptionHandsTheRestToADeliveryUnderWayOnAnotherFiber(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, errorListener: $recorder->errorListener());
        $handled = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        /** @var DeferredFuture<null> $busy */
        $busy = new DeferredFuture();
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use ($handled, $busy): void {
            $handled->payloads[] = $message->payload;
            if ($message->payload === 'm1') {
                // Still being handled when the subscription is drained.
                $busy->getFuture()->await();
            }
        })->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('updates', $sid, 'm1') . ReconnectingTransport::msgFrame('updates', $sid, 'm2'));
        $dispatcher = async(static fn(): int => $connection->processIncoming()->await());
        $this->waitUntil(static fn(): bool => $handled->payloads === ['m1']);

        $connection->drainSubscription($sid)->await();

        self::assertSame(['UNSUB ' . $sid], $transport->controlLinesStartingWith('UNSUB', 0));
        self::assertTrue($connection->isSubscriptionActive($sid), 'the delivery under way removes it once it is done');
        $busy->complete();
        $dispatcher->await();
        self::assertSame(['m1', 'm2'], $handled->payloads);
        self::assertFalse($connection->isSubscriptionActive($sid));
        self::assertSame([], $recorder->errorsContaining('drainSubscription'), 'nothing was discarded');
    }

    public function testSecondDrainSubscriptionForTheSameSidResolvesAtOnce(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 300);
        $sid = $connection->subscribe('updates', static function (): void {})->await();
        // The first call's flush waits out its budget.
        $transport->answerPings = false;
        $first = $connection->drainSubscription($sid);
        delay(0.02);

        $connection->drainSubscription($sid)->await();

        self::assertFalse($first->isComplete(), 'resolved without waiting for the first call');
        self::assertTrue($connection->isSubscriptionActive($sid), 'the first call is still draining it');
        $first->await();
        self::assertFalse($connection->isSubscriptionActive($sid));
        self::assertSame(['UNSUB ' . $sid], $transport->controlLinesStartingWith('UNSUB', 0));
    }

    /**
     * A subscription removed while a reconnect is between re-subscribing and going live - a removal the
     * client can then only make locally - is unsubscribed on the new connection before it opens.
     */
    public function testSubscriptionRemovedWhileAReconnectReplaysIsUnsubscribedBeforeTheConnectionOpens(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $removed = $connection->subscribe('orders', static function (): void {}, 'workers')->await();
        $kept = $connection->subscribe('updates', static function (): void {})->await();
        $reader = $this->startRecoveryInBackground($connection, $transport);
        $connection->publish('events', 'buffered')->await();
        // Holds the reconnect between its re-subscription and going live: flushing the buffered publish.
        $transport->stallNextWriteContaining('PUB events', 0.1);
        $transport->acceptDials();
        $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith('SUB', 1) !== []);
        self::assertSame(ConnectionState::Connecting, $connection->state());

        $connection->unsubscribe($removed)->await();

        $this->waitUntilOpen($connection);
        self::assertSame(['SUB orders workers ' . $removed, 'SUB updates ' . $kept], $transport->controlLinesStartingWith('SUB', 1));
        self::assertSame(['UNSUB ' . $removed], $transport->controlLinesStartingWith('UNSUB', 1));
        self::assertNull($transport->sidFor('orders'));
        self::assertSame($kept, $transport->sidFor('updates'));
        $reader->await();
    }

    /**
     * drainSubscription() while a reconnect is between re-subscribing and going live: the server can route
     * messages to the re-subscribed sid - a queue group's share - until the UNSUB reaches it. The drain
     * waits for the reconnect and flushes on the new connection, so those are delivered too.
     */
    public function testDrainSubscriptionWhileAReconnectReplaysDeliversWhatTheServerSendsBeforeItsUnsubscribe(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, errorListener: $recorder->errorListener());
        $handled = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $drained = $connection->subscribe('orders', static function (NatsMessage $message) use ($handled): void {
            $handled->payloads[] = $message->payload;
        }, 'workers')->await();
        $kept = $connection->subscribe('updates', static function (): void {})->await();
        $reader = $this->startRecoveryInBackground($connection, $transport);
        $connection->publish('events', 'buffered')->await();
        // Holds the reconnect between its re-subscription and going live: flushing the buffered publish.
        $transport->stallNextWriteContaining('PUB events', 0.1);
        $transport->acceptDials();
        $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith('SUB', 1) !== []);
        self::assertSame(ConnectionState::Connecting, $connection->state());

        $drain = $connection->drainSubscription($drained);
        delay(0.001);
        $transport->pushFrame(ReconnectingTransport::msgFrame('orders', $drained, 'routed'));
        $drain->await();

        self::assertSame(['routed'], $handled->payloads);
        self::assertFalse($connection->isSubscriptionActive($drained));
        self::assertNull($transport->sidFor('orders'));
        self::assertSame($kept, $transport->sidFor('updates'));
        self::assertSame([], $recorder->errorsContaining('drainSubscription'));
        $reader->await();
    }

    /** A subscription removed while the reconnect is sending such an UNSUB is unsubscribed too. */
    public function testSubscriptionRemovedWhileAReconnectUnsubscribesAnotherIsUnsubscribedToo(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $first = $connection->subscribe('first', static function (): void {})->await();
        $second = $connection->subscribe('second', static function (): void {})->await();
        $kept = $connection->subscribe('kept', static function (): void {})->await();
        $reader = $this->startRecoveryInBackground($connection, $transport);
        $connection->publish('events', 'buffered')->await();
        $transport->stallNextWriteContaining('PUB events', 0.05);
        $transport->stallNextWriteContaining('UNSUB ' . $first, 0.1);
        $transport->acceptDials();
        $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith('SUB', 1) !== []);
        $connection->unsubscribe($first)->await();

        // Once the buffered publish is out, the reconnect sends the first UNSUB - held up for a while.
        $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith('PUB events', 1) !== []);
        $connection->unsubscribe($second)->await();

        $this->waitUntilOpen($connection);
        self::assertSame(['UNSUB ' . $first, 'UNSUB ' . $second], $transport->controlLinesStartingWith('UNSUB', 1));
        self::assertNull($transport->sidFor('first'));
        self::assertNull($transport->sidFor('second'));
        self::assertSame($kept, $transport->sidFor('kept'));
        $reader->await();
    }

    /**
     * A handler that throws while drainSubscription()'s flush delivers is reported, like drain() reports
     * it, instead of being swallowed with the flush failure; the message queued behind it is still
     * delivered, and the subscription is removed and not brought back by a reconnect.
     */
    public function testDrainSubscriptionReportsAHandlerThatThrowsDuringItsFlushAndDeliversTheRest(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, errorListener: $recorder->errorListener());
        $handled = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $sid = $connection->subscribe('orders', static function (NatsMessage $message) use ($handled): void {
            if ($message->payload === 'boom' || $message->payload === 'bad') {
                throw new \RuntimeException('handler failed on ' . $message->payload);
            }

            $handled->payloads[] = $message->payload;
        }, 'workers')->await();
        $kept = $connection->subscribe('updates', static function (): void {})->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('orders', $sid, 'boom')
            . ReconnectingTransport::msgFrame('orders', $sid, 'bad')
            . ReconnectingTransport::msgFrame('orders', $sid, 'q1'),
        );
        $this->expectHandlerFailure($connection, 'handler failed on boom');

        $connection->drainSubscription($sid)->await();

        self::assertSame(['handler failed on bad'], $recorder->errorsContaining('handler failed'));
        self::assertSame(['q1'], $handled->payloads);
        self::assertSame([], $recorder->errorsContaining('drainSubscription'), 'nothing was discarded');
        self::assertFalse($connection->isSubscriptionActive($sid));

        $reader = $this->startRecoveryInBackground($connection, $transport);
        $transport->acceptDials();
        $this->waitUntilOpen($connection);
        self::assertSame(['SUB updates ' . $kept], $transport->controlLinesStartingWith('SUB', 1));
        $reader->await();
    }

    /** @return iterable<string, array{string}> */
    public static function drains(): iterable
    {
        yield 'drain()' => ['drain'];
        yield 'drainSubscription()' => ['drainSubscription'];
    }

    /**
     * A handler that throws while the flush of drain() or drainSubscription() delivers does not end that
     * flush: the failure is reported and the flush reads on to its PONG, so a message that arrives in a later
     * read is still delivered. The flush used to end with the failure, and that message was lost.
     */
    #[DataProvider('drains')]
    public function testDrainFlushReportsAThrowingHandlerAndReadsOnToTheMessagesStillInFlight(string $drain): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, errorListener: $recorder->errorListener());
        $handled = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $sid = $connection->subscribe('orders', static function (NatsMessage $message) use ($handled): void {
            $handled->payloads[] = $message->payload;
            if ($message->payload === 'first') {
                throw new \RuntimeException('handler failed on first');
            }
        })->await();
        // The server answers the flush's PING with one message, then another one and the PONG in a later read.
        $transport->answerPings = false;
        $transport->afterWrite = static function (string $bytes) use ($transport, $sid): void {
            if ($bytes === "PING\r\n") {
                $transport->afterWrite = null;
                $transport->pushFrame(ReconnectingTransport::msgFrame('orders', $sid, 'first'));
                $transport->pushFrame(ReconnectingTransport::msgFrame('orders', $sid, 'second') . "PONG\r\n");
            }
        };

        if ($drain === 'drain') {
            $connection->drain()->await();
        } else {
            $connection->drainSubscription($sid)->await();
        }

        self::assertSame(['first', 'second'], $handled->payloads);
        self::assertSame(['handler failed on first'], $recorder->errorsContaining('handler failed'));
    }

    /** @return iterable<string, array{string}> */
    public static function reportsDuringADrain(): iterable
    {
        yield 'a handler that throws' => ['handler'];
        yield 'a SubscriptionQueue whose buffer is full' => ['queue'];
    }

    /**
     * drain() keeps to its time budget when the error listener is slow - an error tracker making a
     * synchronous HTTP call, say - while the drain reports a handler failure, or a full SubscriptionQueue,
     * for every message it delivers: each report counts toward the budget, so the drain stops delivering
     * once it is spent. Reports used to be made only after the delivery pass, which then ran every message
     * past the deadline.
     */
    #[DataProvider('reportsDuringADrain')]
    public function testDrainKeepsToItsBudgetWhenTheErrorListenerIsSlow(string $report): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $listener = $recorder->errorListener();
        // A 0.3 s budget, room for 20 messages per subscription, and a listener taking 50 ms per report.
        $client = new NatsClient(
            new NatsOptions(
                connectTimeoutMs: 500,
                requestTimeoutMs: 300,
                pingIntervalSeconds: 0,
                errorListener: static function (\Throwable $error) use ($listener): void {
                    $listener($error);
                    usleep(50_000);
                },
                maxPendingMessagesPerSubscription: 20,
                slowConsumerPolicy: SlowConsumerPolicy::Error,
            ),
            $transport,
        );
        $client->connect()->await();
        if ($report === 'handler') {
            $sid = $client->subscribe('work', static function (): void {
                throw new \RuntimeException('handler failed');
            })->await();
        } else {
            $queue = $client->subscribeQueue('work')->await();
            $sid = $queue->sid;
            $backlog = '';
            for ($i = 1; $i <= 20; $i++) {
                $backlog .= ReconnectingTransport::msgFrame('work', $sid, 'b' . $i);
            }
            $transport->pushFrame($backlog);
            $client->processIncoming()->await();
        }
        // drain()'s flush brings twenty messages, each of which makes a report.
        $frames = '';
        for ($i = 1; $i <= 20; $i++) {
            $frames .= ReconnectingTransport::msgFrame('work', $sid, 'm' . $i);
        }
        $transport->answerPings = false;
        $transport->afterWrite = static function (string $bytes) use ($transport, $frames): void {
            if ($bytes === "PING\r\n") {
                $transport->afterWrite = null;
                $transport->pushFrame($frames . "PONG\r\n");
            }
        };

        $start = hrtime(true);
        $client->drain()->await();

        self::assertLessThan(0.7, $this->secondsSince($start));
        $reported = $report === 'handler' ? $recorder->errorsContaining('handler failed') : $recorder->errorsContaining('overflow');
        self::assertLessThan(20, count($reported), 'the drain stopped delivering once its budget was spent');
        self::assertCount(1, $recorder->errorsContaining('drain deadline exceeded'));
    }

    /**
     * The same when it cannot wait for a reconnect in flight (waiting disabled): it delivers what already
     * arrived at once, without a flush.
     */
    public function testDrainSubscriptionThatCannotWaitForAReconnectReportsAHandlerThatThrowsAndDeliversTheRest(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, waitForReconnect: false, errorListener: $recorder->errorListener());
        $handled = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $sid = $connection->subscribe('orders', static function (NatsMessage $message) use ($handled): void {
            if ($message->payload === 'boom' || str_starts_with($message->payload, 'bad')) {
                throw new \RuntimeException('handler failed on ' . $message->payload);
            }

            $handled->payloads[] = $message->payload;
        })->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('orders', $sid, 'boom')
            . ReconnectingTransport::msgFrame('orders', $sid, 'bad1')
            . ReconnectingTransport::msgFrame('orders', $sid, 'q1')
            . ReconnectingTransport::msgFrame('orders', $sid, 'bad2')
            . ReconnectingTransport::msgFrame('orders', $sid, 'q2'),
        );
        $this->expectHandlerFailure($connection, 'handler failed on boom');
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $connection->drainSubscription($sid)->await();

        self::assertSame(ConnectionState::Connecting, $connection->state(), 'it did not wait for the reconnect');
        self::assertSame(['handler failed on bad1', 'handler failed on bad2'], $recorder->errorsContaining('handler failed'));
        self::assertSame(['q1', 'q2'], $handled->payloads);
        self::assertFalse($connection->isSubscriptionActive($sid));
        $transport->acceptDials();
        $reader->await();
    }

    /**
     * A drain that ran out of time waiting for a reconnect winds down without a connection. A
     * drainSubscription() issued then, past that drain's deadline, delivers like the drain does: a
     * throwing handler ends its delivery, and the rest is reported as discarded instead of delivered past it.
     */
    public function testDrainSubscriptionDuringADrainWindingDownPastItsDeadlineStopsAfterAThrowingHandler(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, requestTimeoutMs: 100, errorListener: $recorder->errorListener());
        $slow = $connection->subscribe('slow', static function (NatsMessage $message): void {
            if ($message->payload === 'boom') {
                throw new \RuntimeException('handler failed on boom');
            }

            // Keeps the drain's backlog pass busy past its deadline.
            delay(0.25);
        })->await();
        $orders = $connection->subscribe('orders', static function (NatsMessage $message): void {
            throw new \RuntimeException('handler failed on ' . $message->payload);
        })->await();
        // One chunk: the first handler failure leaves everything after it queued.
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('slow', $slow, 'boom')
            . ReconnectingTransport::msgFrame('slow', $slow, 'busy')
            . ReconnectingTransport::msgFrame('orders', $orders, 'bad1')
            . ReconnectingTransport::msgFrame('orders', $orders, 'bad2')
            . ReconnectingTransport::msgFrame('orders', $orders, 'bad3'),
        );
        $this->expectHandlerFailure($connection, 'handler failed on boom');
        // A reconnect parked mid-dial: it neither reopens the connection nor, stopped, delivers anything.
        $reader = $this->startRecoveryHeldMidDial($connection, $transport);

        $drainSubscription = new class {
            /** @var ?Future<void> */
            public ?Future $future = null;
        };
        // The drain gives up on the reconnect at 0.1 s; its backlog pass then sits in the 'busy' handler.
        EventLoop::delay(0.15, static function () use ($connection, $orders, $drainSubscription): void {
            $drainSubscription->future = $connection->drainSubscription($orders);
        });
        $connection->drain()->await();

        self::assertNotNull($drainSubscription->future);
        $drainSubscription->future->await();
        self::assertSame(['handler failed on bad1'], $recorder->errorsContaining('handler failed'));
        self::assertSame(
            [sprintf('drainSubscription: 2 buffered message(s) for sid %d were discarded undelivered', $orders)],
            $recorder->errorsContaining('drainSubscription'),
        );
        self::assertSame(ConnectionState::Closed, $connection->state());
        $transport->releaseDial();
        $reader->await();
    }

    /**
     * drainSubscription()'s UNSUB held up by a peer that stopped reading cannot hold it past its budget,
     * like drain()'s writes: it reports the timeout and removes the subscription.
     */
    public function testDrainSubscriptionWhoseUnsubscribeIsHeldUpEndsWithinItsBudget(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, requestTimeoutMs: 200, errorListener: $recorder->errorListener());
        $sid = $connection->subscribe('orders', static function (): void {})->await();
        $transport->stallNextWriteContaining('UNSUB', 3.0);

        $start = hrtime(true);
        $connection->drainSubscription($sid)->await();

        self::assertLessThan(1.0, $this->secondsSince($start));
        self::assertSame(
            ['drainSubscription timed out writing UNSUB (transport backpressure)'],
            $recorder->errorsContaining('drainSubscription'),
        );
        self::assertFalse($connection->isSubscriptionActive($sid));
    }

    /**
     * A drain whose budget runs out while the reconnect it waits for, having given up, is still closing the
     * transport ends with that reconnect's Closed event only.
     */
    public function testDrainRunningOutOfTimeWhileAReconnectThatGaveUpClosesTheTransportEndsWithOneClosedEvent(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect(
            $transport,
            requestTimeoutMs: 100,
            maxReconnectAttempts: 3,
            connectionListener: $recorder->connectionListener(),
            reconnectDelayMs: 20,
        );
        $reader = $this->startRecoveryInBackground($connection, $transport);
        // The reconnect gives up well within the drain's budget, but its close outlasts that budget.
        $transport->beforeClose = static function () use ($transport, $connection): void {
            if ($connection->state() !== ConnectionState::Closed) {
                return;
            }

            $transport->beforeClose = null;
            $transport->closeDelay = 0.3;
            // Only that close is slow: the drain's own is quick.
            EventLoop::queue(static function () use ($transport): void {
                $transport->closeDelay = 0.0;
            });
        };

        $connection->drain()->await();
        try {
            $reader->await();
            self::fail('expected the reconnect to give up');
        } catch (ConnectionException $e) {
            self::assertSame('Reconnect attempts exhausted', $e->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Closed], $recorder->events);
        self::assertNotNull($recorder->closedErrors[0], 'announced by the reconnect that gave up');
    }

    /**
     * A connection reopened after a drain announces its next close too: each connect() starts the Closed
     * bookkeeping afresh.
     */
    public function testConnectionReopenedAfterADrainAnnouncesItsNextCloseToo(): void
    {
        $recorder = new LifecycleRecorder();
        $connection = $this->connect(new ReconnectingTransport(), connectionListener: $recorder->connectionListener());

        $connection->drain()->await();
        $connection->connect()->await();
        $connection->drain()->await();
        $connection->connect()->await();
        $connection->disconnect()->await();

        self::assertSame(
            [
                ConnectionEvent::Connected,
                ConnectionEvent::Closed,
                ConnectionEvent::Connected,
                ConnectionEvent::Closed,
                ConnectionEvent::Connected,
                ConnectionEvent::Closed,
            ],
            $recorder->events,
        );
    }

    /**
     * drain() then disconnect() - a common shutdown idiom - announces the close once: disconnect() of a
     * connection already closed and announced closes it again quietly.
     */
    public function testDisconnectAfterADrainDoesNotAnnounceTheCloseAgain(): void
    {
        $recorder = new LifecycleRecorder();
        $connection = $this->connect(new ReconnectingTransport(), connectionListener: $recorder->connectionListener());

        $connection->drain()->await();
        $connection->disconnect()->await();
        $connection->disconnect()->await();

        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Closed], $recorder->events);
    }

    /** @return iterable<string, array{ConnectionEvent}> */
    public static function recoveryListenerEvents(): iterable
    {
        yield 'Disconnected listener' => [ConnectionEvent::Disconnected];
        yield 'Reconnected listener' => [ConnectionEvent::Reconnected];
    }

    /**
     * A listener inside a reconnect awaits drain(), and a supervisor reconnects on the Closed that drain
     * announces: the supervisor's connect() fails at once instead of joining that reconnect - which waits
     * on the very listener awaiting the drain - and hanging for good.
     */
    #[DataProvider('recoveryListenerEvents')]
    public function testSupervisorReconnectingAfterADrainFromARecoveryListenerFailsInsteadOfHanging(ConnectionEvent $trigger): void
    {
        $transport = new ReconnectingTransport();
        $holder = new class {
            public ?NatsConnection $connection = null;
            public bool $drained = false;
            public bool $supervised = false;
            public ?\Throwable $supervisorFailure = null;
        };
        $listener = static function (ConnectionEvent $event) use ($holder, $trigger): void {
            $connection = $holder->connection;
            if ($connection === null) {
                return;
            }

            if ($event === $trigger && !$holder->drained) {
                $holder->drained = true;
                try {
                    $connection->drain()->await();
                } catch (ConnectionException) {
                    // A drain that cannot wait for the reconnect closes the connection instead.
                }
            }

            if ($event === ConnectionEvent::Closed && !$holder->supervised) {
                $holder->supervised = true;
                try {
                    $connection->connect()->await();
                } catch (\Throwable $e) {
                    $holder->supervisorFailure = $e;
                }
            }
        };
        $connection = $this->connect($transport, requestTimeoutMs: 200, connectionListener: $listener);
        $holder->connection = $connection;
        $connection->subscribe('updates', static function (): void {})->await();

        if ($trigger === ConnectionEvent::Reconnected) {
            $reader = $this->startRecoveryInBackground($connection, $transport);
            $transport->acceptDials();
        } else {
            // The Disconnected listener closes the connection at once: do not wait for Connecting.
            $transport->refuseDials();
            $transport->dropConnection();
            $reader = async(static fn(): int => $connection->processIncoming()->await());
        }

        $reader->await(new TimeoutCancellation(3));

        self::assertTrue($holder->supervised);
        self::assertInstanceOf(ConnectionException::class, $holder->supervisorFailure);
        self::assertSame('Recovery was aborted before the connection opened', $holder->supervisorFailure->getMessage());
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * A reconnect that gives up while drain() waits for it announces Closed before the drain is over. A
     * supervisor's connect() then is refused - the application is shutting the connection down - and one
     * issued once the drain has ended opens a new connection.
     */
    public function testConnectAnsweringAClosedThatComesWhileADrainRunsIsRefusedUntilTheDrainIsOver(): void
    {
        $transport = new ReconnectingTransport();
        $holder = new class {
            public ?NatsConnection $connection = null;
            public ?\Throwable $supervisorFailure = null;
        };
        $listener = static function (ConnectionEvent $event) use ($holder): void {
            $connection = $holder->connection;
            if ($event !== ConnectionEvent::Closed || $connection === null) {
                return;
            }

            EventLoop::queue(static function () use ($connection, $holder): void {
                try {
                    $connection->connect()->await();
                } catch (\Throwable $e) {
                    $holder->supervisorFailure = $e;
                }
            });
        };
        $connection = $this->connect($transport, maxReconnectAttempts: 2, connectionListener: $listener);
        $holder->connection = $connection;
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $connection->drain()->await();

        self::assertInstanceOf(ConnectionException::class, $holder->supervisorFailure);
        self::assertSame('Cannot connect: drain in progress', $holder->supervisorFailure->getMessage());
        self::assertSame(ConnectionState::Closed, $connection->state());

        try {
            $reader->await();
        } catch (ConnectionException) {
            // The reconnect gave up.
        }

        $transport->acceptDials();
        $connection->connect()->await();
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * disconnect() while a drain waits for a reconnect ends the drain. The publishes buffered during the
     * outage are then discarded by disconnect() - the documented lossy close - and not reported as if the
     * drain had run out of time, even when disconnect()'s close is still under way when the drain ends.
     */
    public function testDisconnectInterruptingAWaitingDrainIsNotReportedAsTheDrainRunningOutOfTime(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, errorListener: $recorder->errorListener());
        $reader = $this->startRecoveryInBackground($connection, $transport);
        $connection->publish('events', 'buffered')->await();
        $transport->closeDelay = 0.1;
        EventLoop::delay(0.05, static function () use ($connection): void {
            $connection->disconnect()->await();
        });

        $connection->drain()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([], $recorder->errorsContaining('Drain ran out of time'));
        $reader->await();
    }

    /**
     * The socket dying while drain() waits for its flush PONG ends the flush at once - that PONG died
     * with the socket - instead of reading a dead socket until the budget runs out.
     */
    public function testDrainEndsPromptlyWhenTheSocketDiesDuringItsFlush(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 2_000);
        $connection->subscribe('updates', static function (): void {})->await();
        $transport->answerPings = false;
        $transport->afterWrite = static function (string $bytes) use ($transport): void {
            if ($bytes === "PING\r\n") {
                $transport->afterWrite = null;
                $transport->dropConnection();
            }
        };

        $start = hrtime(true);
        $connection->drain()->await();

        self::assertLessThan(0.5, $this->secondsSince($start));
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(0, $connection->statistics()->reconnects);
    }

    /**
     * A server PING while drain() waits for its flush PONG is answered with a write bounded by the drain
     * budget, like drain()'s own writes: a PONG stuck behind a stalled socket cannot hold it past that.
     */
    public function testServerPingDuringADrainCannotHoldItPastItsBudget(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 300);
        $transport->answerPings = false;
        $transport->stallNextWriteContaining('PONG', 3.0);
        // The server PINGs once the drain's own PING is out.
        $transport->afterWrite = static function (string $bytes) use ($transport): void {
            if ($bytes === "PING\r\n") {
                $transport->afterWrite = null;
                $transport->pushFrame("PING\r\n");
            }
        };

        $start = hrtime(true);
        $connection->drain()->await();

        self::assertLessThan(1.0, $this->secondsSince($start));
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * drain() while disconnect() is still closing the transport during a reconnect finds nothing to drain:
     * the connection is not open, and the user is closing it. (A transport that cannot stop its dial keeps
     * the reconnect in flight through the close; a built-in one's reconnect would already have ended.)
     */
    public function testDrainWhileADisconnectIsClosingTheTransportFailsWithNothingToDrain(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect(new UncancellableDialTransport($transport), connectionListener: $recorder->connectionListener());
        $reader = $this->startRecoveryHeldMidDial($connection, $transport);
        $transport->closeDelay = 0.1;
        $disconnect = $connection->disconnect();
        delay(0.02);
        self::assertSame(ConnectionState::Connecting, $connection->state(), 'disconnect() is in its close');

        try {
            $connection->drain()->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }

        // disconnect() waits for the reconnect it stopped: the dial it could not stop ends first.
        $transport->closeDelay = 0.0;
        $transport->releaseDial();
        $disconnect->await();
        self::assertSame(1, $recorder->closedEvents());
        $reader->await();
    }

    /**
     * A logger that throws on every reconnect attempt no longer ends the reconnect - that left the
     * connection Connecting for good - so a drain waiting for it winds down when its budget runs out and
     * ends Closed, announced once, instead of failing with the logger's error.
     */
    public function testDrainWaitingForAReconnectWhoseLoggerThrowsOnEveryAttemptStillEndsClosed(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = new NatsConnection(
            new NatsOptions(
                connectTimeoutMs: 500,
                requestTimeoutMs: 150,
                reconnectEnabled: true,
                maxReconnectAttempts: 1_000,
                reconnectDelayMs: 5,
                reconnectMaxDelayMs: 20,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
                connectionListener: $recorder->connectionListener(),
                logger: new ThrowingLogger('reconnect attempt'),
            ),
            $transport,
        );
        $this->opened[] = $connection;
        $connection->connect()->await();
        $sid = $connection->subscribe('updates', static function (): void {})->await();
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $connection->drain()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(1, $recorder->closedEvents());
        self::assertFalse($connection->isSubscriptionActive($sid));
        $reader->await();
    }

    /**
     * A logger that throws while the Closed event is logged neither keeps that event from the listener
     * nor makes the drain fail.
     */
    public function testDrainStillAnnouncesTheCloseWhenTheLoggerThrowsOnIt(): void
    {
        $recorder = new LifecycleRecorder();
        $connection = new NatsConnection(
            new NatsOptions(
                pingIntervalSeconds: 0,
                connectionListener: $recorder->connectionListener(),
                logger: new ThrowingLogger('NATS connection Closed'),
            ),
            new ReconnectingTransport(),
        );
        $this->opened[] = $connection;
        $connection->connect()->await();

        $connection->drain()->await();

        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Closed], $recorder->events);
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * A handler that drains its own subscription and awaits it: the messages queued behind the one it is
     * handling are still delivered to it before the subscription is removed, instead of being dropped.
     */
    public function testHandlerDrainingItsOwnSubscriptionStillGetsTheMessagesQueuedBehindIt(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, errorListener: $recorder->errorListener());
        $holder = new class {
            public int $sid = 0;
            /** @var list<string> */
            public array $payloads = [];
        };
        $holder->sid = $connection->subscribe('updates', static function (NatsMessage $message) use ($connection, $holder): void {
            $holder->payloads[] = $message->payload;
            if ($message->payload === 'stop') {
                $connection->drainSubscription($holder->sid)->await();
            }
        })->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('updates', $holder->sid, 'stop')
            . ReconnectingTransport::msgFrame('updates', $holder->sid, 'after1')
            . ReconnectingTransport::msgFrame('updates', $holder->sid, 'after2'),
        );

        $connection->processIncoming()->await();

        self::assertSame(['stop', 'after1', 'after2'], $holder->payloads);
        self::assertFalse($connection->isSubscriptionActive($holder->sid));
        self::assertSame(['UNSUB ' . $holder->sid], $transport->controlLinesStartingWith('UNSUB', 0));
        self::assertSame([], $recorder->errorsContaining('drainSubscription'), 'nothing was discarded');
    }

    /**
     * drainSubscription() of a subscription that reaches its auto-unsubscribe max while it delivers stops
     * at the max, like any delivery, and reports nothing: the messages the max holds back are not
     * discarded by the drain.
     */
    public function testDrainSubscriptionStopsAtTheAutoUnsubscribeMaxWithoutReportingTheRest(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, errorListener: $recorder->errorListener());
        $handled = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use ($handled): void {
            if ($message->payload === 'boom') {
                throw new \RuntimeException('handler failed');
            }

            $handled->payloads[] = $message->payload;
        })->await();
        // 'boom' is delivery 1 of 3.
        $connection->unsubscribe($sid, 3)->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('updates', $sid, 'boom')
            . ReconnectingTransport::msgFrame('updates', $sid, 'm1')
            . ReconnectingTransport::msgFrame('updates', $sid, 'm2')
            . ReconnectingTransport::msgFrame('updates', $sid, 'm3'),
        );
        $this->expectHandlerFailure($connection);

        $connection->drainSubscription($sid)->await();

        self::assertSame(['m1', 'm2'], $handled->payloads);
        self::assertSame([], $recorder->errorsContaining('drainSubscription'));
        self::assertFalse($connection->isSubscriptionActive($sid));
    }

    /**
     * A removed subscription stays gone when the UNSUB that ends the replay window fails: that attempt
     * fails, and the next one does not re-subscribe it.
     */
    public function testRemovedSubscriptionStaysGoneWhenTheReplayWindowUnsubscribeFails(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $removed = $connection->subscribe('orders', static function (): void {}, 'workers')->await();
        $kept = $connection->subscribe('updates', static function (): void {})->await();
        $reader = $this->startRecoveryInBackground($connection, $transport);
        $connection->publish('events', 'buffered')->await();
        $transport->stallNextWriteContaining('PUB events', 0.05);
        $transport->acceptDials();
        $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith('SUB', 1) !== []);
        $connection->unsubscribe($removed)->await();
        $transport->failNextWriteContaining('UNSUB ' . $removed);

        $this->waitUntil(static fn(): bool => $transport->epoch() === 2 && $connection->state() === ConnectionState::Open);

        self::assertSame([], $transport->controlLinesStartingWith('UNSUB', 1), 'that UNSUB failed');
        self::assertSame(['SUB updates ' . $kept], $transport->controlLinesStartingWith('SUB', 2));
        self::assertNull($transport->sidFor('orders'));
        $reader->await();
    }

    /**
     * A publish made while the reconnect sends the UNSUB for a subscription removed during its replay
     * window goes out on the new connection before it opens, in publish order - instead of staying in the
     * reconnect buffer behind later, direct publishes.
     */
    public function testPublishDuringAReplayWindowUnsubscribeGoesOutBeforeTheConnectionOpens(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $removed = $connection->subscribe('orders', static function (): void {}, 'workers')->await();
        $connection->subscribe('updates', static function (): void {})->await();
        $reader = $this->startRecoveryInBackground($connection, $transport);
        $connection->publish('events', 'buffered')->await();
        $transport->stallNextWriteContaining('PUB events', 0.05);
        $transport->stallNextWriteContaining('UNSUB ' . $removed, 0.1);
        $transport->acceptDials();
        $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith('SUB', 1) !== []);
        $connection->unsubscribe($removed)->await();
        // Once the buffered publish is out, the reconnect writes that UNSUB - held up for a while.
        $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith('PUB events', 1) !== []);

        $connection->publish('events', 'late')->await();
        $this->waitUntilOpen($connection);
        $connection->publish('events', 'direct')->await();

        self::assertSame(['PUB events 8', 'PUB events 4', 'PUB events 6'], $transport->controlLinesStartingWith('PUB events', 1));
        $reader->await();
    }

    /**
     * An auto-unsubscribe armed while a reconnect is between re-subscribing and going live reaches the new
     * connection before it opens, with the max counted from the replay: the server stops at the max too,
     * instead of going on delivering to a sid nothing handles any more.
     */
    public function testAutoUnsubscribeArmedWhileAReconnectReplaysReachesTheServer(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $sid = $connection->subscribe('orders', static function (): void {}, 'workers')->await();
        $reader = $this->startRecoveryInBackground($connection, $transport);
        $connection->publish('events', 'buffered')->await();
        $transport->stallNextWriteContaining('PUB events', 0.1);
        $transport->acceptDials();
        $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith('SUB', 1) !== []);

        $connection->unsubscribe($sid, 2)->await();
        $this->waitUntilOpen($connection);

        self::assertSame(['UNSUB ' . $sid . ' 2'], $transport->controlLinesStartingWith('UNSUB', 1));
        self::assertTrue($connection->isSubscriptionActive($sid), 'it stays until the max is delivered');
        $reader->await();
    }

    /**
     * While drain() runs, operations that need an open connection are refused - they are not let through
     * onto a connection being closed.
     */
    public function testOperationsOtherThanReadsAreRefusedWhileADrainRuns(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 300);
        // The drain waits in its flush.
        $transport->answerPings = false;
        $drain = $connection->drain();
        $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Draining);

        $operations = [
            'subscribe' => static function () use ($connection): void {
                $connection->subscribe('late', static function (): void {})->await();
            },
            'flush' => static function () use ($connection): void {
                $connection->flush()->await();
            },
        ];
        foreach ($operations as $name => $operation) {
            try {
                $operation();
                self::fail($name . '() must be refused while the drain runs');
            } catch (ConnectionException $e) {
                self::assertSame('Connection is not open', $e->getMessage(), $name);
            }
        }

        $drain->await();
        self::assertSame([], $transport->controlLinesStartingWith('SUB late'));
    }

    /**
     * A reader waiting for a reconnect keeps reading when a drain waiting for the same reconnect resumes
     * first and starts draining: reads are what answer the drain's flush, so the reader must not fail
     * because the connection is no longer Open.
     */
    public function testReaderWaitingForAReconnectThatADrainThenDrainsKeepsReading(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 300);
        $connection->subscribe('updates', static function (): void {})->await();
        $reader = $this->startRecoveryInBackground($connection, $transport);
        // The drain waits for the reconnect first, the second reader after it.
        $drain = $connection->drain();
        delay(0.01);
        $secondReader = async(static fn(): int => $connection->processIncoming()->await());
        delay(0.01);

        $transport->acceptDials();
        $drain->await();

        $secondReader->await();
        self::assertSame(ConnectionState::Closed, $connection->state());
        $reader->await();
    }

    /**
     * Several changes made in one replay window - two subscriptions removed, an auto-unsubscribe armed on
     * a third - all reach the new connection before it opens.
     */
    public function testChangesToSeveralSubscriptionsInOneReplayWindowAllReachTheServer(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $first = $connection->subscribe('first', static function (): void {})->await();
        $second = $connection->subscribe('second', static function (): void {})->await();
        $armed = $connection->subscribe('armed', static function (): void {})->await();
        $reader = $this->startRecoveryInBackground($connection, $transport);
        $connection->publish('events', 'buffered')->await();
        $transport->stallNextWriteContaining('PUB events', 0.1);
        $transport->acceptDials();
        $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith('SUB', 1) !== []);

        $connection->unsubscribe($first)->await();
        $connection->unsubscribe($second)->await();
        $connection->unsubscribe($armed, 5)->await();
        $this->waitUntilOpen($connection);

        self::assertSame(
            ['UNSUB ' . $first, 'UNSUB ' . $second, 'UNSUB ' . $armed . ' 5'],
            $transport->controlLinesStartingWith('UNSUB', 1),
        );
        $reader->await();
    }

    /**
     * An auto-unsubscribe max armed during the replay window counts the messages received before the
     * outage: the replayed SUB counts from zero on the server, so the server is told only what is left.
     * A message the replayed SUB already delivered is not subtracted again: the server counts it against
     * that max itself.
     */
    public function testAutoUnsubscribeArmedWhileAReconnectReplaysCountsWhatWasReceivedBefore(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $sid = $connection->subscribe('orders', static function (): void {})->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('orders', $sid, 'm1') . ReconnectingTransport::msgFrame('orders', $sid, 'm2'));
        $connection->processIncoming()->await();
        $reader = $this->startRecoveryInBackground($connection, $transport);
        $connection->publish('events', 'buffered')->await();
        $transport->stallNextWriteContaining('PUB events', 0.1);
        // The replayed SUB delivers a message at once.
        $transport->afterWrite = static function (string $bytes) use ($transport, $sid): void {
            if (str_contains($bytes, 'SUB orders ')) {
                $transport->afterWrite = null;
                $transport->pushFrame(ReconnectingTransport::msgFrame('orders', $sid, 'live'));
            }
        };
        $transport->acceptDials();
        $this->waitUntil(static fn(): bool => $connection->statistics()->inMsgs === 3);
        self::assertSame(ConnectionState::Connecting, $connection->state());

        $connection->unsubscribe($sid, 5)->await();
        $this->waitUntilOpen($connection);

        self::assertSame(['UNSUB ' . $sid . ' 3'], $transport->controlLinesStartingWith('UNSUB', 1));
        $reader->await();
    }

    /**
     * A server PING while drain() waits for its flush PONG is answered, once, so the server does not take
     * the draining connection for dead.
     */
    public function testServerPingDuringADrainIsAnsweredOnce(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 300);
        $transport->answerPings = false;
        $transport->afterWrite = static function (string $bytes) use ($transport): void {
            if ($bytes === "PING\r\n") {
                $transport->afterWrite = null;
                $transport->pushFrame("PING\r\n");
            }
        };

        $connection->drain()->await();

        self::assertSame(['PONG'], $transport->controlLinesStartingWith('PONG', 0));
    }

    /**
     * When the delivery drainSubscription() handed the rest over to stops at a handler that throws, the
     * rest is not lost: the next delivery pass hands it over, and only then is the subscription removed.
     */
    public function testHandOverWhoseDeliveryHitsAThrowingHandlerStillDeliversTheRest(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $handled = new class {
            public bool $busy = true;
            /** @var list<string> */
            public array $payloads = [];
        };
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use ($handled): void {
            $handled->payloads[] = $message->payload;
            while ($message->payload === 'm1' && $handled->busy) {
                delay(0.01);
            }

            if ($message->payload === 'bad') {
                throw new \RuntimeException('handler failed');
            }
        })->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('updates', $sid, 'm1')
            . ReconnectingTransport::msgFrame('updates', $sid, 'bad')
            . ReconnectingTransport::msgFrame('updates', $sid, 'm2'),
        );
        $dispatcher = async(static fn(): int => $connection->processIncoming()->await());
        $this->waitUntil(static fn(): bool => $handled->payloads === ['m1']);

        $connection->drainSubscription($sid)->await();
        $handled->busy = false;
        try {
            $dispatcher->await();
            self::fail('expected the handler failure');
        } catch (\RuntimeException $e) {
            self::assertSame('handler failed', $e->getMessage());
        }

        self::assertTrue($connection->isSubscriptionActive($sid), 'm2 is still to be delivered');
        // The next read's delivery pass hands m2 over and removes the subscription.
        $transport->pushFrame("PING\r\n");
        $connection->processIncoming()->await();
        self::assertSame(['m1', 'bad', 'm2'], $handled->payloads);
        self::assertFalse($connection->isSubscriptionActive($sid));
    }

    /** Delivers the frames pushed so far, whose first handler fails: the rest stay queued behind it. */
    private function expectHandlerFailure(NatsConnection $connection, string $message = 'handler failed'): void
    {
        $failure = null;
        try {
            $connection->processIncoming()->await();
        } catch (\RuntimeException $e) {
            $failure = $e;
        }

        self::assertNotNull($failure, 'expected the handler failure');
        self::assertSame($message, $failure->getMessage());
    }
}
