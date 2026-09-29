<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\ThrowingLogger;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;

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
     * connect() while a reconnect that disconnect() stopped is still finishing an attempt fails at once:
     * that reconnect can never open the connection, so there is nothing to wait for.
     */
    public function testConnectWhileAStoppedReconnectWindsDownFailsAtOnce(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $reader = $this->startRecoveryHeldMidDial($connection, $transport);
        $connection->disconnect()->await();

        $start = hrtime(true);
        try {
            $connection->connect()->await(new TimeoutCancellation(0.5));
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Recovery was aborted before the connection opened', $e->getMessage());
        }

        self::assertLessThan(0.1, $this->secondsSince($start));
        $transport->releaseDial();
        $reader->await();
        $connection->connect()->await();
        self::assertSame(ConnectionState::Open, $connection->state(), 'once it has ended, connect() dials afresh');
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

    /** @return iterable<string, array{string, bool}> The path, and whether its Closed event carries an error. */
    public static function pathsThatGiveUpOnTheConnection(): iterable
    {
        yield 'reconnect attempts exhausted' => ['reconnect exhausted', true];
        yield 'reconnect refused authentication' => ['reconnect auth', true];
        yield 'connection lost with reconnect disabled' => ['reconnect disabled', false];
        yield 'first connect failed' => ['connect failed', true];
        yield 'first connect refused authentication' => ['connect auth', true];
        yield 'initial connect retries ran out' => ['retries exhausted', true];
        yield 'initial connect retry refused authentication' => ['retry auth', true];
    }

    /**
     * A disconnect() issued while a connect or a reconnect that gave up on the connection is still closing
     * the transport - a TLS or WebSocket close takes a while - closes quietly: the close is announced
     * once, by the path that gave up (with its error, where it has one).
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
}
