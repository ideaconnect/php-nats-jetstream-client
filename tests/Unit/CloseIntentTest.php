<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\ByteStream\ClosedException;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\IncomingChunkResult;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\ThrowingLogger;
use IDCT\NATS\Tests\Support\UncancellableDialTransport;
use IDCT\NATS\Transport\TransportClosedException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;

/**
 * A close wins over what is still in flight on the connection: a connect() racing it, a reconnect
 * backing off, failing or writing its log line, and a read, write or heartbeat that fails on a
 * connection the application has since closed and reopened.
 */
final class CloseIntentTest extends TestCase
{
    use ReconnectScenarios;

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /**
     * An error listener that closes and reopens the connection when a read fails: the failed read's
     * recovery, which runs once the listener returns, leaves the new connection alone instead of tearing
     * it down and reconnecting.
     */
    public function testErrorListenerThatReopensTheConnectionIsNotUndoneByTheFailedRead(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $holder = new class {
            public ?NatsConnection $connection = null;
            public bool $reopened = false;
        };
        $connection = $this->connect(
            $transport,
            connectionListener: $recorder->connectionListener(),
            errorListener: static function () use ($holder): void {
                $connection = $holder->connection;
                if ($connection === null || $holder->reopened) {
                    return;
                }

                $holder->reopened = true;
                $connection->disconnect()->await();
                $connection->connect()->await();
            },
        );
        $holder->connection = $connection;

        $transport->dropConnection();
        $connection->processIncoming()->await(new TimeoutCancellation(2));

        self::assertTrue($holder->reopened);
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $transport->epoch(), 'no reconnect after the reopen');
        self::assertSame(0, $connection->statistics()->reconnects);
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Closed, ConnectionEvent::Connected], $recorder->events);
    }

    /**
     * A publish whose write was still in flight when the application closed and reopened the connection,
     * over a transport that fails such a write only later - after the reopen (TransportInterface allows
     * it; a plain socket fails it at the close, see the test below). That failure no longer tears down the
     * new connection with a reconnect: the frame goes out on the new one.
     */
    public function testWriteFailingOnAReplacedConnectionDoesNotTearDownTheNewOne(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $transport->stallNextWriteContaining('PUB events', 0.1, outlivesSession: true);
        $publish = $connection->publish('events', 'stalled');
        $this->waitUntil(static fn(): bool => $transport->writesStalled() === 1);

        $connection->disconnect()->await();
        $connection->connect()->await();
        self::assertFalse($publish->isComplete(), 'the write fails only after the reopen');
        $publish->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $transport->epoch(), 'no reconnect after the reopen');
        self::assertSame(0, $connection->statistics()->reconnects);
        self::assertSame(['PUB events 7'], $transport->controlLinesStartingWith('PUB events', 1));
    }

    /**
     * A heartbeat whose PING write was still in flight when the application closed and reopened the
     * connection, over a transport that fails such a write only after the reopen (see above). That failure
     * no longer reconnects the new connection or stops its own heartbeat.
     */
    public function testHeartbeatFailingOnAReplacedConnectionLeavesTheNewOneAlone(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, pingIntervalSeconds: 0.05);
        // The first heartbeat PING is held up well past the reopen below.
        $transport->stallNextWriteContaining('PING', 0.4, outlivesSession: true);
        $this->waitUntil(static fn(): bool => $transport->writesStalled() === 1);

        $connection->disconnect()->await();
        $connection->connect()->await();
        $stalledAfterTheReopen = $transport->writesStalled();
        self::assertSame(1, $stalledAfterTheReopen, 'the PING fails only after the reopen');
        $this->waitUntil(static fn(): bool => $transport->writesStalled() === 0);
        delay(0.2);

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $transport->epoch(), 'no reconnect after the reopen');
        self::assertSame(0, $connection->statistics()->reconnects);
        self::assertGreaterThan(2, count($transport->controlLinesStartingWith('PING', 1)), 'the new heartbeat keeps running');
    }

    /**
     * disconnect() while connect() is still dialling wins: the dial that completes afterwards is closed
     * again and connect() fails, leaving the connection Closed with the one Closed event disconnect()
     * announced - instead of going Open behind the close, with nothing able to recover it any more.
     */
    public function testDisconnectDuringTheInitialDialWins(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->unconnected($transport, $recorder);
        $transport->holdNextDial();
        $connect = $connection->connect();
        $this->waitUntil(static fn(): bool => $transport->connectCalls !== []);

        $connection->disconnect()->await();
        $transport->releaseDial();

        try {
            $connect->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Connect was aborted before the connection opened', $e->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertFalse($transport->sessionLive(), 'the socket the dial opened was closed again');
        self::assertSame([ConnectionEvent::Closed], $recorder->events);
        $connection->connect()->await();
        self::assertSame(ConnectionState::Open, $connection->state(), 'a later connect() starts afresh');
    }

    /** The same when the server then refuses the credentials: the close wins, announced once. */
    public function testDisconnectDuringTheInitialDialWinsOverAnAuthenticationFailure(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->unconnected($transport, $recorder);
        $transport->holdNextDial();
        $connect = $connection->connect();
        $this->waitUntil(static fn(): bool => $transport->connectCalls !== []);

        $connection->disconnect()->await();
        $transport->rejectAuthentication = true;
        $transport->releaseDial();

        try {
            $connect->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Connect was aborted before the connection opened', $e->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([ConnectionEvent::Closed], $recorder->events);
    }

    /**
     * disconnect() while connect() retries a failed first dial (retryOnFailedInitialConnect, reconnect
     * disabled) stops the retries at once - their backoff is cut short - and connect() fails.
     */
    public function testDisconnectDuringTheInitialConnectRetriesWinsAtOnce(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = new NatsConnection(
            new NatsOptions(
                connectTimeoutMs: 500,
                reconnectEnabled: false,
                maxReconnectAttempts: 10,
                reconnectDelayMs: 1_000,
                reconnectMaxDelayMs: 1_000,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
                connectionListener: $recorder->connectionListener(),
                retryOnFailedInitialConnect: true,
            ),
            $transport,
        );
        $this->opened[] = $connection;
        $transport->refuseDials();
        $connect = $connection->connect();
        // The first dial was refused; the next retry is a second away.
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 1);
        delay(0.02);

        $start = hrtime(true);
        $connection->disconnect()->await();

        try {
            $connect->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Connect was aborted before the connection opened', $e->getMessage());
        }

        self::assertLessThan(0.2, $this->secondsSince($start));
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertCount(1, $transport->connectCalls, 'no dial after the close');
        self::assertSame([ConnectionEvent::Closed], $recorder->events);
    }

    /**
     * disconnect() stops a reconnect that is dialling - the dial included - and returns once that reconnect
     * has ended, so a connect() issued after it dials afresh. The reconnect used to go on dialling after
     * disconnect() returned, and that connect() failed with "Recovery was aborted before the connection
     * opened". In a synchronous application the reconnect never got the event-loop time to end at all, and
     * every connect() failed that way.
     */
    public function testConnectAfterDisconnectDialsAfreshWhileTheReconnectItStoppedWasDialling(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $reader = $this->startRecoveryHeldMidDial($connection, $transport);

        $start = hrtime(true);
        $connection->disconnect()->await();

        self::assertLessThan(0.2, $this->secondsSince($start));
        self::assertSame(1, $transport->dialsCancelled, 'the dial was stopped');
        $connection->connect()->await(new TimeoutCancellation(1));
        self::assertSame(ConnectionState::Open, $connection->state());
        $reader->await(new TimeoutCancellation(1));
    }

    /** drain() waits for a dial it cannot stop too, so a connect() issued after it dials afresh. */
    public function testDrainWaitsForTheDialOfATransportThatCannotStopIt(): void
    {
        $inner = new ReconnectingTransport();
        $connection = $this->connect(new UncancellableDialTransport($inner), requestTimeoutMs: 100);
        $reader = $this->startRecoveryHeldMidDial($connection, $inner);
        // Released after the drain's budget has run out and it has closed the connection.
        EventLoop::delay(0.2, static function () use ($inner): void {
            $inner->releaseDial();
        });

        try {
            $connection->drain()->await(new TimeoutCancellation(2));
        } catch (ConnectionException) {
            // However the drain reports its budget running out, it closes the connection.
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        $connection->connect()->await(new TimeoutCancellation(1));
        self::assertSame(ConnectionState::Open, $connection->state());
        $reader->await(new TimeoutCancellation(1));
    }

    /**
     * A close that comes while a reconnect attempt is still closing the previous socket stops the attempt
     * before it dials: nothing dials after the close.
     */
    public function testReconnectDoesNotDialAfterACloseThatCameWhileItClosedThePreviousSocket(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $transport->closeDelay = 0.1;
        $transport->dropConnection();
        $reader = async(static fn(): int => $connection->processIncoming()->await());
        $reader->ignore();
        // The reconnect's first attempt is closing the dead socket now.
        $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Connecting);
        $dials = count($transport->connectCalls);

        $connection->disconnect()->await(new TimeoutCancellation(2));
        delay(0.2);

        $transport->closeDelay = 0.0;
        self::assertSame($dials, count($transport->connectCalls), 'no dial after the close');
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * disconnect() overtaking a connect() whose dial it cannot stop waits for that connect() to end, so the
     * next connect() dials afresh instead of joining it.
     */
    public function testDisconnectWaitsForAConnectWhoseDialItCannotStop(): void
    {
        $inner = new ReconnectingTransport();
        $connection = new NatsConnection($this->options(true, 2_000, 1_000, 0, 2, null, 5, 20, null), new UncancellableDialTransport($inner));
        $this->opened[] = $connection;
        $inner->holdNextDial();
        $first = $connection->connect();
        $first->ignore();
        $this->waitUntil(static fn(): bool => count($inner->connectCalls) === 1);
        EventLoop::delay(0.1, static function () use ($inner): void {
            $inner->releaseDial();
        });

        $start = hrtime(true);
        $connection->disconnect()->await(new TimeoutCancellation(2));

        self::assertGreaterThanOrEqual(0.08, $this->secondsSince($start), 'it waited for the connect()');
        $connection->connect()->await(new TimeoutCancellation(1));
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * disconnect() called from a fiber of the application's own - not from inside the reconnect - waits for
     * the reconnect it stopped, as it does from the main fiber.
     */
    public function testDisconnectFromAnotherFiberWaitsForTheReconnectItStopped(): void
    {
        $inner = new ReconnectingTransport();
        $connection = $this->connect(new UncancellableDialTransport($inner));
        $reader = $this->startRecoveryHeldMidDial($connection, $inner);
        EventLoop::delay(0.1, static function () use ($inner): void {
            $inner->releaseDial();
        });

        $start = hrtime(true);
        async(static fn() => $connection->disconnect()->await())->await(new TimeoutCancellation(2));

        self::assertGreaterThanOrEqual(0.08, $this->secondsSince($start), 'it waited for the reconnect');
        $connection->connect()->await(new TimeoutCancellation(1));
        self::assertSame(ConnectionState::Open, $connection->state());
        $reader->await(new TimeoutCancellation(1));
    }

    /**
     * disconnect() leaves no timer of its wait behind: a referenced timer would keep the event loop - and a
     * script that has just closed its connection - running until the connect timeout.
     */
    public function testDisconnectLeavesNoTimerOfItsWaitBehind(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $baseline = self::referencedWatchers();
        $reader = $this->startRecoveryHeldMidDial($connection, $transport);

        $connection->disconnect()->await();
        $reader->await(new TimeoutCancellation(1));

        self::assertSame($baseline, self::referencedWatchers());
    }

    /**
     * A reconnect that disconnect() stops does not deliver what is queued on its way out: disconnect()
     * discards it (nats.go Close() parity). The stopped reconnect used to deliver it, after disconnect() had
     * released the connection - and now that a close stops a reconnect at once, it would have every time.
     */
    public function testReconnectStoppedByDisconnectDoesNotDeliverWhatIsQueued(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $received = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $sid = $connection->subscribe('orders', static function (NatsMessage $message) use ($received): void {
            $received->payloads[] = $message->payload;
            if ($message->payload === 'o1') {
                throw new \RuntimeException('handler failed on o1');
            }
        })->await();
        // One chunk: the failure on o1 leaves o2 and o3 queued.
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('orders', $sid, 'o1')
            . ReconnectingTransport::msgFrame('orders', $sid, 'o2')
            . ReconnectingTransport::msgFrame('orders', $sid, 'o3'),
        );
        try {
            $connection->processIncoming()->await();
            self::fail('expected the handler failure');
        } catch (\RuntimeException $e) {
            self::assertSame('handler failed on o1', $e->getMessage());
        }

        // Backing off between refused dials: disconnect() cuts the backoff short, and the reconnect ends
        // while disconnect() closes the socket, before it discards what is queued.
        $reader = $this->startRecoveryInBackground($connection, $transport);
        $connection->disconnect()->await();
        $reader->await(new TimeoutCancellation(1));

        self::assertSame(['o1'], $received->payloads);
    }

    /**
     * A transport that cannot stop its dial: disconnect() waits for the dial to end, so a connect() issued
     * after it still dials afresh.
     */
    public function testDisconnectWaitsForTheDialOfATransportThatCannotStopIt(): void
    {
        $inner = new ReconnectingTransport();
        $connection = $this->connect(new UncancellableDialTransport($inner));
        $reader = $this->startRecoveryHeldMidDial($connection, $inner);
        EventLoop::delay(0.1, static function () use ($inner): void {
            $inner->releaseDial();
        });

        $start = hrtime(true);
        $connection->disconnect()->await(new TimeoutCancellation(2));

        self::assertGreaterThanOrEqual(0.08, $this->secondsSince($start), 'it waited for the dial');
        self::assertSame(0, $inner->dialsCancelled);
        $connection->connect()->await(new TimeoutCancellation(1));
        self::assertSame(ConnectionState::Open, $connection->state());
        $reader->await(new TimeoutCancellation(1));
    }

    /** The wait for a dial that cannot be stopped is bounded by the connect timeout (500 ms here). */
    public function testDisconnectStopsWaitingForADialThatCannotBeStoppedAtTheConnectTimeout(): void
    {
        $inner = new ReconnectingTransport();
        $connection = $this->connect(new UncancellableDialTransport($inner));
        $reader = $this->startRecoveryHeldMidDial($connection, $inner);

        $start = hrtime(true);
        $connection->disconnect()->await(new TimeoutCancellation(3));
        $elapsed = $this->secondsSince($start);

        self::assertGreaterThanOrEqual(0.45, $elapsed);
        self::assertLessThan(1.5, $elapsed);
        self::assertSame(ConnectionState::Closed, $connection->state());
        $inner->releaseDial();
        $reader->await(new TimeoutCancellation(1));
    }

    /**
     * A connect() racing a close still fails at once (#145): issued while disconnect() waits for the
     * reconnect it stopped, it cannot join that reconnect, which will never open the connection. Once
     * disconnect() has returned, connect() dials afresh.
     */
    public function testConnectRacingADisconnectThatWaitsForTheStoppedReconnectFailsAtOnce(): void
    {
        $inner = new ReconnectingTransport();
        $connection = $this->connect(new UncancellableDialTransport($inner));
        $reader = $this->startRecoveryHeldMidDial($connection, $inner);
        $disconnect = $connection->disconnect();
        delay(0.02);

        $start = hrtime(true);
        try {
            $connection->connect()->await(new TimeoutCancellation(0.5));
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Recovery was aborted before the connection opened', $e->getMessage());
        }

        self::assertLessThan(0.1, $this->secondsSince($start));
        $inner->releaseDial();
        $disconnect->await(new TimeoutCancellation(1));
        $reader->await(new TimeoutCancellation(1));
        $connection->connect()->await(new TimeoutCancellation(1));
        self::assertSame(ConnectionState::Open, $connection->state(), 'once disconnect() has returned, connect() dials afresh');
    }

    /**
     * disconnect() called from a listener that runs inside the reconnect does not wait for that reconnect,
     * which waits for the listener to return: it would wait out its whole bound.
     */
    public function testDisconnectFromAListenerInsideTheReconnectDoesNotWaitForIt(): void
    {
        $transport = new ReconnectingTransport();
        $holder = new class {
            public ?NatsConnection $connection = null;
            public ?float $disconnectSeconds = null;
        };
        $listener = static function (ConnectionEvent $event) use ($holder): void {
            $connection = $holder->connection;
            if ($event !== ConnectionEvent::Disconnected || $connection === null) {
                return;
            }

            $start = hrtime(true);
            $connection->disconnect()->await(new TimeoutCancellation(2));
            $holder->disconnectSeconds = (hrtime(true) - $start) / 1e9;
        };
        $connection = $this->connect($transport, connectionListener: $listener);
        $holder->connection = $connection;
        $transport->refuseDials();
        $transport->dropConnection();

        $connection->processIncoming()->await(new TimeoutCancellation(2));

        self::assertNotNull($holder->disconnectSeconds);
        self::assertLessThan(0.2, $holder->disconnectSeconds);
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * disconnect() overtaking a connect() that is still dialling stops that dial too, and returns once that
     * connect() has ended - with "Connect was aborted before the connection opened" - so the next connect()
     * dials afresh instead of joining it.
     */
    public function testConnectAfterADisconnectThatOvertookADiallingConnectDialsAfresh(): void
    {
        $transport = new ReconnectingTransport();
        $connection = new NatsConnection($this->options(true, 2_000, 1_000, 0, 2, null, 5, 20, null), $transport);
        $this->opened[] = $connection;
        $transport->holdNextDial();
        $first = $connection->connect();
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 1);

        $connection->disconnect()->await(new TimeoutCancellation(2));

        try {
            $first->await(new TimeoutCancellation(1));
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Connect was aborted before the connection opened', $e->getMessage());
        }

        self::assertSame(1, $transport->dialsCancelled);
        $connection->connect()->await(new TimeoutCancellation(1));
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * A drain() whose budget runs out while the reconnect is dialling stops that reconnect too, and a
     * connect() issued after it dials afresh.
     */
    public function testConnectAfterADrainThatStoppedADiallingReconnectDialsAfresh(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 200);
        $reader = $this->startRecoveryHeldMidDial($connection, $transport);

        try {
            $connection->drain()->await(new TimeoutCancellation(2));
        } catch (ConnectionException) {
            // However the drain reports its budget running out, it closes the connection.
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(1, $transport->dialsCancelled);
        $connection->connect()->await(new TimeoutCancellation(1));
        self::assertSame(ConnectionState::Open, $connection->state());
        $reader->await(new TimeoutCancellation(1));
    }

    /**
     * A reconnect that disconnect() stopped while it was authenticating ends quietly when the server then
     * refuses the credentials: no second Closed event, no authentication error for a close the user made.
     */
    public function testAuthenticationFailureOfAStoppedReconnectIsNotAnnouncedAgain(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect(
            $transport,
            connectionListener: $recorder->connectionListener(),
            errorListener: $recorder->errorListener(),
        );
        $reader = $this->startRecoveryHeldMidDial($connection, $transport);
        $connection->disconnect()->await();

        $transport->rejectAuthentication = true;
        $transport->releaseDial();
        // Resolves: the stopped reconnect ended, it did not fail.
        $reader->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertFalse($transport->sessionLive(), 'the socket that attempt opened was closed again');
        self::assertSame(1, $recorder->closedEvents());
        self::assertSame([], $recorder->errorsContaining('authentication'));
    }

    /**
     * disconnect() while the reconnect is writing its "attempt failed" log line - through a logger that
     * suspends, like an async stream under backpressure - still stops the reconnect at once, not when
     * its backoff delay ends.
     */
    public function testCloseWhileTheReconnectLogIsWrittenStillStopsTheReconnectAtOnce(): void
    {
        $transport = new ReconnectingTransport();
        $logger = new class extends AbstractLogger {
            public bool $suspend = true;

            /** @param array<mixed> $context */
            public function log($level, string|\Stringable $message, array $context = []): void
            {
                if ($this->suspend && str_contains((string) $message, 'reconnect attempt')) {
                    $this->suspend = false;
                    delay(0.1);
                }
            }
        };
        $connection = new NatsConnection(
            new NatsOptions(
                connectTimeoutMs: 500,
                reconnectEnabled: true,
                maxReconnectAttempts: 1_000,
                reconnectDelayMs: 1_000,
                reconnectMaxDelayMs: 1_000,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
                logger: $logger,
            ),
            $transport,
        );
        $this->opened[] = $connection;
        $connection->connect()->await();
        $reader = $this->startRecoveryInBackground($connection, $transport);
        // The first attempt was refused; its log line is being written.
        delay(0.02);

        $start = hrtime(true);
        $connection->disconnect()->await();
        $reader->await();

        self::assertLessThan(0.3, $this->secondsSince($start));
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * A logger that throws on every "reconnect attempt failed" line no longer ends the reconnect: it goes
     * on backing off and reopens the connection once the server is back.
     */
    public function testReconnectOutlivesALoggerThatThrowsOnEveryAttempt(): void
    {
        $transport = new ReconnectingTransport();
        $connection = new NatsConnection(
            new NatsOptions(
                connectTimeoutMs: 500,
                reconnectEnabled: true,
                maxReconnectAttempts: 1_000,
                reconnectDelayMs: 5,
                reconnectMaxDelayMs: 20,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
                logger: new ThrowingLogger('reconnect attempt'),
            ),
            $transport,
        );
        $this->opened[] = $connection;
        $connection->connect()->await();
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $this->acceptDialsAfter($transport, 0.05);
        $reader->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $connection->statistics()->reconnects);
    }

    /**
     * With reconnect disabled, a dial that disconnect() overtook and that then fails: connect() fails with
     * the abort - the close wins - instead of closing the connection, and announcing it, a second time.
     */
    public function testDisconnectDuringAFailingInitialDialWinsWithReconnectDisabled(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = new NatsConnection(
            new NatsOptions(connectTimeoutMs: 500, reconnectEnabled: false, pingIntervalSeconds: 0, connectionListener: $recorder->connectionListener()),
            $transport,
        );
        $this->opened[] = $connection;
        $transport->holdNextDial();
        $connect = $connection->connect();
        $this->waitUntil(static fn(): bool => $transport->connectCalls !== []);

        $connection->disconnect()->await();
        $transport->refuseDials();
        $transport->releaseDial();

        try {
            $connect->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Connect was aborted before the connection opened', $e->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([ConnectionEvent::Closed], $recorder->events);
    }

    /** Two disconnect() calls at once announce the close once. */
    public function testConcurrentDisconnectsAnnounceTheCloseOnce(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, connectionListener: $recorder->connectionListener());
        $transport->closeDelay = 0.05;

        $first = $connection->disconnect();
        $second = $connection->disconnect();
        $first->await();
        $second->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Closed], $recorder->events);
    }

    /** With retryOnFailedInitialConnect, connect() keeps retrying a refused first dial until one succeeds. */
    public function testInitialConnectRetriesUntilADialSucceeds(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->retryingInitialConnect($transport, new LifecycleRecorder(), 5);
        $transport->refuseDials();
        $this->acceptDialsAfter($transport, 0.05);

        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertGreaterThan(2, count($transport->connectCalls), 'refused more than once before it got through');
    }

    /**
     * disconnect() while a retry of the first dial is dialling, and that dial then succeeds: the close
     * wins - connect() fails and the socket the retry opened is closed again.
     */
    public function testDisconnectDuringAnInitialConnectRetryDialWins(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->retryingInitialConnect($transport, $recorder, 5);
        $connect = $this->connectWithARetryHeldMidDial($connection, $transport);

        $connection->disconnect()->await();
        $transport->releaseDial();

        try {
            $connect->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Connect was aborted before the connection opened', $e->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertFalse($transport->sessionLive(), 'the socket the retry opened was closed again');
        self::assertSame([ConnectionEvent::Closed], $recorder->events);
    }

    /** The same when the server then refuses the retry's credentials: the close wins, announced once. */
    public function testDisconnectDuringAnInitialConnectRetryDialWinsOverAnAuthenticationFailure(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->retryingInitialConnect($transport, $recorder, 5);
        $connect = $this->connectWithARetryHeldMidDial($connection, $transport);

        $connection->disconnect()->await();
        $transport->rejectAuthentication = true;
        $transport->releaseDial();

        try {
            $connect->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Connect was aborted before the connection opened', $e->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([ConnectionEvent::Closed], $recorder->events);
    }

    /**
     * Over a plain socket a write still pending when the application closes the connection fails at the
     * close, while the close is under way: the publish fails - nothing reconnects, and nothing re-sends
     * it on a connection opened afterwards.
     */
    public function testPublishInFlightWhenTheConnectionIsClosedFailsAndLeavesTheReopenedConnectionAlone(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $transport->stallNextWriteContaining('PUB events', 1.0);
        $publish = $connection->publish('events', 'stalled');
        $this->waitUntil(static fn(): bool => $transport->writesStalled() === 1);

        $connection->disconnect()->await();
        $connection->connect()->await();

        $this->waitUntil(static fn(): bool => $publish->isComplete());
        try {
            $publish->await();
            self::fail('expected the publish to fail');
        } catch (\Throwable $e) {
            self::assertStringContainsString('gone', $e->getMessage());
        }

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $transport->epoch(), 'no reconnect after the reopen');
        self::assertSame(0, $connection->statistics()->reconnects);
        self::assertSame([], $transport->controlLinesStartingWith('PUB events'));
    }

    /**
     * A publish whose failed write runs the reconnect, which disconnect() stops: the publish fails with an error
     * of this library even when its write failed with the raw stream error a built-in transport passes on - a
     * TransportClosedException "Transport is not connected" carrying that error, as when the retry went into
     * the closed transport. The raw error, thrown as it was, would escape a catch (NatsThrowable).
     */
    public function testPublishWhoseReconnectADisconnectStopsFailsWithAnErrorOfThisLibrary(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $writeError = new ClosedException('The stream is not writable');
        $transport->refuseDials();
        $transport->failNextWriteContaining('PUB orders', $writeError);

        $publish = $connection->publish('orders', 'payload');
        $publish->ignore();
        $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Connecting);
        $connection->disconnect()->await();

        $failure = null;
        try {
            $publish->await(new TimeoutCancellation(2));
        } catch (\Throwable $e) {
            $failure = $e;
        }

        self::assertInstanceOf(TransportClosedException::class, $failure);
        self::assertSame('Transport is not connected', $failure->getMessage());
        self::assertSame($writeError, $failure->getPrevious());
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([], $transport->controlLinesStartingWith('PUB'));
    }

    /** @return iterable<string, array{string, bool}> The path, and whether its Closed event carries an error. */
    public static function pathsThatGiveUpOnTheConnection(): iterable
    {
        yield 'reconnect attempts exhausted' => ['reconnect exhausted', true];
        yield 'reconnect refused authentication' => ['reconnect auth', true];
        // With the error that ended the connection, the dropped socket's EOF (#172); 2.10.2 and earlier announced
        // this Closed without one.
        yield 'connection lost with reconnect disabled' => ['reconnect disabled', true];
        yield 'first connect failed' => ['connect failed', true];
        yield 'first connect refused authentication' => ['connect auth', true];
        yield 'initial connect retries ran out' => ['retries exhausted', true];
        yield 'initial connect retry refused authentication' => ['retry auth', true];
    }

    /**
     * A disconnect() issued while a connect or a reconnect that gave up on the connection is still closing
     * the transport - a TLS or WebSocket close takes a while - closes quietly: the close is announced
     * once, by the path that gave up, with its error.
     */
    #[DataProvider('pathsThatGiveUpOnTheConnection')]
    public function testDisconnectWhileAPathThatGaveUpClosesTheTransportAnnouncesTheCloseOnce(string $path, bool $closedWithError): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = new NatsConnection(
            new NatsOptions(
                connectTimeoutMs: 500,
                reconnectEnabled: $path === 'reconnect exhausted' || $path === 'reconnect auth',
                maxReconnectAttempts: 2,
                reconnectDelayMs: 5,
                reconnectMaxDelayMs: 5,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
                connectionListener: $recorder->connectionListener(),
                retryOnFailedInitialConnect: $path === 'retries exhausted' || $path === 'retry auth',
            ),
            $transport,
        );
        $this->opened[] = $connection;
        $holder = new class {
            /** @var ?Future<void> */
            public ?Future $disconnect = null;
        };
        // The path that gave up marks the connection Closed before it closes the transport; the closes of
        // the attempts before it find it still connecting.
        $transport->beforeClose = static function () use ($transport, $connection, $holder): void {
            if ($connection->state() !== ConnectionState::Closed) {
                return;
            }

            $transport->beforeClose = null;
            $transport->closeDelay = 0.05;
            $holder->disconnect = $connection->disconnect();
        };

        $failure = null;
        try {
            if (str_starts_with($path, 'reconnect')) {
                $connection->connect()->await();
                $transport->rejectAuthentication = $path === 'reconnect auth';
                if ($path === 'reconnect exhausted') {
                    $transport->refuseDials();
                }
                $transport->dropConnection();
                $connection->processIncoming()->await();
            } elseif ($path === 'retry auth') {
                $transport->refuseDials();
                $connect = $connection->connect();
                $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 1);
                $transport->rejectAuthentication = true;
                $transport->acceptDials();
                $connect->await();
            } else {
                $transport->rejectAuthentication = $path === 'connect auth';
                if ($path !== 'connect auth') {
                    $transport->refuseDials();
                }
                $connection->connect()->await();
            }
        } catch (\Throwable $e) {
            $failure = $e;
        }

        self::assertNotNull($failure, 'the path gave up');
        $this->waitUntil(static fn(): bool => $holder->disconnect?->isComplete() ?? false);
        self::assertNotNull($holder->disconnect);
        $holder->disconnect->await();
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(1, $recorder->closedEvents(), 'announced once');
        self::assertSame($closedWithError, ($recorder->closedErrors[0] ?? null) !== null, 'by the path that gave up');
    }

    /**
     * disconnect() while the last retry of a failed first dial is dialling, and that dial then fails too:
     * the close wins - connect() fails as stopped rather than as failed, and the close is announced once.
     */
    public function testDisconnectDuringTheLastInitialConnectRetryWinsWhenThatDialFails(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->retryingInitialConnect($transport, $recorder, 5, maxAttempts: 1);
        $connect = $this->connectWithARetryHeldMidDial($connection, $transport);

        $connection->disconnect()->await();
        $transport->refuseDials();
        $transport->releaseDial();

        try {
            $connect->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Connect was aborted before the connection opened', $e->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([ConnectionEvent::Closed], $recorder->events);
        self::assertSame([null], $recorder->closedErrors, 'announced by the disconnect()');
    }

    /**
     * disconnect() while the retries of a failed first dial, all of them failed, close the last one's
     * socket: the close wins as well.
     */
    public function testDisconnectWhileTheInitialConnectRetriesCloseTheirLastSocketWins(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->retryingInitialConnect($transport, $recorder, 5, maxAttempts: 1);
        // The retry closes the socket before its dial, and after it once the retries ran out.
        $transport->beforeClose = static function () use ($transport, $connection): void {
            if (count($transport->connectCalls) < 2) {
                return;
            }

            $transport->beforeClose = null;
            $transport->closeDelay = 0.05;
            $connection->disconnect()->ignore();
        };
        $transport->refuseDials();

        try {
            $connection->connect()->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Connect was aborted before the connection opened', $e->getMessage());
        }

        delay(0.1);
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertCount(2, $transport->connectCalls);
        self::assertSame([ConnectionEvent::Closed], $recorder->events);
        self::assertSame([null], $recorder->closedErrors, 'announced by the disconnect()');
    }

    /**
     * An error listener that closes and reopens the connection when the stream turns out corrupt: the
     * failed parse's recovery, which runs once the listener returns, leaves the new connection alone.
     */
    public function testErrorListenerThatReopensTheConnectionOnACorruptStreamIsNotUndone(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $holder = new class {
            public ?NatsConnection $connection = null;
            public bool $reopened = false;
        };
        $connection = $this->connect(
            $transport,
            connectionListener: $recorder->connectionListener(),
            errorListener: static function () use ($holder): void {
                $connection = $holder->connection;
                if ($connection === null || $holder->reopened) {
                    return;
                }

                $holder->reopened = true;
                $connection->disconnect()->await();
                $connection->connect()->await();
            },
        );
        $holder->connection = $connection;

        $transport->pushFrame("BOGUS\r\n");
        $connection->processIncoming()->await(new TimeoutCancellation(2));

        self::assertTrue($holder->reopened);
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $transport->epoch(), 'no reconnect after the reopen');
        self::assertSame(0, $connection->statistics()->reconnects);
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Closed, ConnectionEvent::Connected], $recorder->events);
    }

    /** @return iterable<string, array{bool}> */
    public static function leftoversDuringADisconnect(): iterable
    {
        yield 'left queued before the serving read starts' => [false];
        yield 'left queued by the read it waits for' => [true];
    }

    /**
     * A serving loop's read - the read a service's run() makes - while a disconnect() is closing the connection
     * leaves what an earlier read left queued to the close, which discards it (#134): what a read that stopped at a
     * throwing handler left behind, before the serving read started or while it waited for that read. With a close
     * that takes a while, a TLS or WebSocket one, the serving read used to deliver it to its handler after
     * disconnect() had been called.
     */
    #[DataProvider('leftoversDuringADisconnect')]
    public function testServingReadDuringADisconnectLeavesWhatAnEarlierReadLeftQueuedToTheClose(bool $whileItWaits): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $seen = [];
        $sidA = $connection->subscribe('a', static function (): void {
            throw new \RuntimeException('handler a');
        })->await();
        $sidB = $connection->subscribe('b', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = $message->payload;
        })->await();
        $messages = ReconnectingTransport::msgFrame('a', $sidA, 'x') . ReconnectingTransport::msgFrame('b', $sidB, 'y');
        $holder = new class {
            /** @var Future<IncomingChunkResult>|null */
            public ?Future $serving = null;
        };
        $transport->closeDelay = 0.2;

        // A plain read stops at a's throwing handler and leaves b's message queued.
        $plainRead = $connection->processIncoming(new TimeoutCancellation(2));
        if ($whileItWaits) {
            // The serving read waits for the plain read, which reads the messages once the close is under way.
            delay(0.01);
            $holder->serving = $connection->readIncomingForOperation(new TimeoutCancellation(2), alwaysReport: true);
            delay(0.01);
            $transport->beforeClose = static function () use ($transport, $messages): void {
                $transport->beforeClose = null;
                $transport->pushFrame($messages);
            };
        } else {
            // The plain read is over when the close starts, and the serving read starts with it.
            $transport->pushFrame($messages);
            $this->assertFailsWithTheHandlersException($plainRead);
            $transport->beforeClose = static function () use ($transport, $connection, $holder): void {
                $transport->beforeClose = null;
                $holder->serving = $connection->readIncomingForOperation(new TimeoutCancellation(2), alwaysReport: true);
            };
        }

        $connection->disconnect()->await(new TimeoutCancellation(3));
        $this->assertFailsWithTheHandlersException($plainRead);
        self::assertInstanceOf(Future::class, $holder->serving);
        try {
            $holder->serving->await(new TimeoutCancellation(3));
        } catch (\Throwable) {
            // The close ended its read of the socket.
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([], $seen, 'discarded by disconnect()');
    }

    /** @param Future<int> $read */
    private function assertFailsWithTheHandlersException(Future $read): void
    {
        try {
            $read->await(new TimeoutCancellation(3));
            self::fail('expected the handler\'s exception');
        } catch (\RuntimeException $e) {
            self::assertSame('handler a', $e->getMessage());
        }
    }

    /** A connection that retries a failed first dial (retryOnFailedInitialConnect, reconnect disabled). */
    private function retryingInitialConnect(ReconnectingTransport $transport, LifecycleRecorder $recorder, int $retryDelayMs, int $maxAttempts = 10): NatsConnection
    {
        $connection = new NatsConnection(
            new NatsOptions(
                connectTimeoutMs: 500,
                reconnectEnabled: false,
                maxReconnectAttempts: $maxAttempts,
                reconnectDelayMs: $retryDelayMs,
                reconnectMaxDelayMs: 4 * $retryDelayMs,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
                connectionListener: $recorder->connectionListener(),
                retryOnFailedInitialConnect: true,
            ),
            $transport,
        );
        $this->opened[] = $connection;

        return $connection;
    }

    /**
     * Starts connect() with its first dial refused and its first retry parked mid-dial.
     *
     * @return Future<void>
     */
    private function connectWithARetryHeldMidDial(NatsConnection $connection, ReconnectingTransport $transport): Future
    {
        $transport->refuseDials();
        $connect = $connection->connect();
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 1);
        $transport->acceptDials();
        $transport->holdNextDial();
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);

        return $connect;
    }

    /** A connection that has not connected yet, with its lifecycle recorded. */
    private function unconnected(ReconnectingTransport $transport, LifecycleRecorder $recorder): NatsConnection
    {
        $connection = new NatsConnection(
            $this->options(true, 2_000, 1_000, 0, 2, $recorder->connectionListener(), 5, 20, null),
            $transport,
        );
        $this->opened[] = $connection;

        return $connection;
    }

    /** The enabled watchers that keep the event loop running. */
    private static function referencedWatchers(): int
    {
        return count(array_filter(
            EventLoop::getIdentifiers(),
            static fn(string $id): bool => EventLoop::isEnabled($id) && EventLoop::isReferenced($id),
        ));
    }
}
