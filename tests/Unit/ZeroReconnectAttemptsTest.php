<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Transport\TransportClosedException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\delay;

/**
 * `maxReconnectAttempts: 0` disables reconnect retries, as NatsOptions documents (#268): a lost connection makes no
 * reconnect attempt and ends as it does with reconnect off - Closed, the Closed event carrying the cause, and
 * "Reconnect is disabled" with the cause as its previous for the operation that waits for it - and a lame duck notice
 * naming another server starts no failover. The initial connect already read 0 that way; the recovery after a lost
 * connection made max(1, 0) = 1 attempt: one more backoff and dial after every drop, the connection carrying on with
 * its subscriptions replayed when that dial succeeded. The option is global, for the whole server pool: at 0 no server
 * of the pool is dialled, the others included.
 *
 * Each case runs with reconnect off and with reconnect on at 0 attempts, and expects the same outcome. The scripted
 * server accepts every dial, so an attempt made anyway would reopen the connection. The heartbeat cases also hold
 * the connection to one Closed event when a PONG read or a PING write of the heartbeat outlives the close.
 */
final class ZeroReconnectAttemptsTest extends TestCase
{
    use ReconnectScenarios;

    private const LAME_DUCK_INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","max_payload":1048576,"headers":true,"ldm":true,"connect_urls":["10.0.0.2:4222"]}' . "\r\n";

    private const STALE = "-ERR 'Stale Connection'\r\n";

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /**
     * @return iterable<string, array{bool, int}>
     */
    public static function noReconnect(): iterable
    {
        yield 'reconnect off' => [false, 10];
        yield 'reconnect on, 0 attempts' => [true, 0];
    }

    #[DataProvider('noReconnect')]
    public function testAConnectionLostOnAFailedReadMakesNoDialAndClosesWithTheCause(bool $reconnectEnabled, int $maxReconnectAttempts): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->open($transport, $recorder, $reconnectEnabled, $maxReconnectAttempts);
        $transport->dropConnection();

        $error = $this->reconnectDisabledError(static fn() => $connection->processIncoming()->await());

        self::assertInstanceOf(TransportClosedException::class, $error->getPrevious());
        self::assertSame('Socket closed by peer (EOF)', $error->getPrevious()->getMessage());
        $this->assertClosedWithoutADial($connection, $transport, $recorder, 'Socket closed by peer (EOF)');
    }

    /**
     * A plain publish whose write fails does not hand its frame to a reconnect buffer that nothing would flush: it
     * fails, as with reconnect off. At 0 attempts it used to resolve, and the frame went out on the connection the one
     * attempt opened.
     */
    #[DataProvider('noReconnect')]
    public function testAPublishWhoseWriteFailsMakesNoDialAndFailsWithTheCause(bool $reconnectEnabled, int $maxReconnectAttempts): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->open($transport, $recorder, $reconnectEnabled, $maxReconnectAttempts);
        $transport->failNextWriteContaining('PUB orders');

        $error = $this->reconnectDisabledError(static fn() => $connection->publish('orders', 'o1')->await());

        self::assertInstanceOf(TransportClosedException::class, $error->getPrevious());
        $this->assertClosedWithoutADial($connection, $transport, $recorder, $error->getPrevious()->getMessage());
        self::assertSame([], $transport->controlLinesStartingWith('PUB orders'), 'the frame reached no server');
    }

    /** A control write - a SUB - that fails does the same. */
    #[DataProvider('noReconnect')]
    public function testASubscribeWhoseWriteFailsMakesNoDialAndFailsWithTheCause(bool $reconnectEnabled, int $maxReconnectAttempts): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->open($transport, $recorder, $reconnectEnabled, $maxReconnectAttempts);
        $transport->failNextWriteContaining('SUB updates');

        $error = $this->reconnectDisabledError(static fn() => $connection->subscribe('updates', static function (): void {})->await());

        self::assertInstanceOf(TransportClosedException::class, $error->getPrevious());
        $this->assertClosedWithoutADial($connection, $transport, $recorder, $error->getPrevious()->getMessage());
    }

    /**
     * The heartbeat that gives up on a server which stopped answering its PINGs ends the connection, with its reason,
     * and announces that once. The previous tick's bounded PONG read is still waiting on the socket when the tick that
     * gives up closes it: that read then fails with the EOF of the client's own close, on a connection already ended
     * and announced, and must neither announce a second Closed (with that EOF as its cause) nor start a recovery. Up
     * to 2.31.0 it announced that second Closed with reconnect off, and at 0 attempts too once those ended alike.
     */
    #[DataProvider('noReconnect')]
    public function testMissedPingsMakeNoDialAndCloseOnceWithTheCause(bool $reconnectEnabled, int $maxReconnectAttempts): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->open($transport, $recorder, $reconnectEnabled, $maxReconnectAttempts, maxPingsOut: 1);
        $readsPendingAtClose = $this->recordReadsPendingAtClose($connection, $transport);
        $transport->silence();

        // The first tick sends its PING and waits for the PONG the silent server never sends.
        $firstTick = async(fn() => $this->tick($connection));
        $this->waitUntil(fn(): bool => $this->readInProgress($connection));

        // The next tick finds that PING unanswered and gives up on the connection while the read still waits.
        $this->tick($connection);
        // The first tick's read has failed on the closed socket meanwhile, and whatever it started is over.
        $firstTick->await();

        self::assertSame([true], $readsPendingAtClose->getArrayCopy(), 'the PONG read was pending when the socket was closed');
        self::assertFalse($this->readInProgress($connection));
        $this->assertClosedWithoutADial($connection, $transport, $recorder, 'The server did not answer the last PING');
    }

    /**
     * The same with the heartbeat's own timer, as an application runs it: every tick reads for its PONG for up to
     * 50 ms, so the tick that gives up 20 ms after the last PING closes the socket under that tick's read.
     */
    #[DataProvider('noReconnect')]
    public function testAHeartbeatTimerThatGivesUpMakesNoDialAndClosesOnceWithTheCause(bool $reconnectEnabled, int $maxReconnectAttempts): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->open($transport, $recorder, $reconnectEnabled, $maxReconnectAttempts, maxPingsOut: 1, pingIntervalSeconds: 0.02);
        $readsPendingAtClose = $this->recordReadsPendingAtClose($connection, $transport);
        $transport->silence();

        $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Closed);
        $this->waitUntil(fn(): bool => !$this->readInProgress($connection));
        // Past the longest the read could still wait (50 ms), and a few timer intervals on.
        delay(0.15);

        self::assertSame(true, $readsPendingAtClose[0] ?? null, 'the PONG read was pending when the socket was closed');
        $this->assertClosedWithoutADial($connection, $transport, $recorder, 'The server did not answer the last PING');
    }

    /**
     * The same for a tick whose PING write is still suspended when another fiber's read finds the connection lost and
     * ends it: the write then fails on the ended connection, which the heartbeat leaves alone.
     */
    #[DataProvider('noReconnect')]
    public function testAPingWriteThatFailsAfterTheConnectionEndedMakesNoSecondClose(bool $reconnectEnabled, int $maxReconnectAttempts): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->open($transport, $recorder, $reconnectEnabled, $maxReconnectAttempts, maxPingsOut: 1);
        // Held past the end of its session, and failed only then: a transport whose close leaves a pending write to
        // fail later, as TransportInterface allows.
        $transport->stallNextWriteContaining('PING', 5.0, outlivesSession: true);
        $tick = async(fn() => $this->tick($connection));
        $this->waitUntil(static fn(): bool => $transport->writesStalled() === 1);

        $transport->dropConnection();
        $this->reconnectDisabledError(static fn() => $connection->processIncoming()->await());
        $transport->releaseStalledWrites();
        $tick->await();

        $this->assertClosedWithoutADial($connection, $transport, $recorder, 'Socket closed by peer (EOF)');
    }

    /**
     * A heartbeat read that fails is ignored only for the connection that ended while it waited: the connection a
     * later connect() opens is still ended by a heartbeat read that finds it lost, and the Closed event says why.
     */
    #[DataProvider('noReconnect')]
    public function testAHeartbeatReadThatFindsTheNextConnectionLostStillEndsIt(bool $reconnectEnabled, int $maxReconnectAttempts): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->open($transport, $recorder, $reconnectEnabled, $maxReconnectAttempts, maxPingsOut: 1);
        $transport->silence();
        $firstTick = async(fn() => $this->tick($connection));
        $this->waitUntil(fn(): bool => $this->readInProgress($connection));
        $this->tick($connection);
        $firstTick->await();

        $connection->connect()->await();
        $transport->silence();
        $tick = async(fn() => $this->tick($connection));
        $this->waitUntil(fn(): bool => $this->readInProgress($connection));
        $transport->dropConnection();
        $tick->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Closed, ConnectionEvent::Connected, ConnectionEvent::Closed], $recorder->events);
        self::assertSame(['The server did not answer the last PING', 'Socket closed by peer (EOF)'], $recorder->closedErrors);
        self::assertCount(2, $transport->connectCalls, 'the second connect(), and no reconnect dial');
    }

    /** And by a PING write that finds it lost. */
    #[DataProvider('noReconnect')]
    public function testAPingWriteThatFindsTheNextConnectionLostStillEndsIt(bool $reconnectEnabled, int $maxReconnectAttempts): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->open($transport, $recorder, $reconnectEnabled, $maxReconnectAttempts, maxPingsOut: 1);
        $transport->dropConnection();
        $this->reconnectDisabledError(static fn() => $connection->processIncoming()->await());

        $connection->connect()->await();
        $transport->failNextWriteContaining('PING');
        $this->tick($connection);

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Closed, ConnectionEvent::Connected, ConnectionEvent::Closed], $recorder->events);
        self::assertSame(['Socket closed by peer (EOF)', 'The connection is gone'], $recorder->closedErrors);
        self::assertCount(2, $transport->connectCalls, 'the second connect(), and no reconnect dial');
    }

    /**
     * A lame duck notice naming another server starts no failover: the connection stays on the leaving server until
     * that server closes it, and then ends.
     */
    #[DataProvider('noReconnect')]
    public function testALameDuckNoticeNamingAnotherServerStartsNoFailover(bool $reconnectEnabled, int $maxReconnectAttempts): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->open($transport, $recorder, $reconnectEnabled, $maxReconnectAttempts);
        $transport->pushFrame(self::LAME_DUCK_INFO);

        $connection->processIncoming()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::DiscoveredServers, ConnectionEvent::LameDuck], $recorder->events);
        self::assertCount(1, $transport->connectCalls, 'no failover dial');
        self::assertTrue($transport->sessionLive(), 'still on the leaving server');

        $transport->dropConnection();
        $this->reconnectDisabledError(static fn() => $connection->processIncoming()->await());

        $this->assertClosedWithoutADial($connection, $transport, $recorder, 'Socket closed by peer (EOF)', [
            ConnectionEvent::Connected,
            ConnectionEvent::DiscoveredServers,
            ConnectionEvent::LameDuck,
            ConnectionEvent::Closed,
        ]);
    }

    /**
     * One of the library's operations whose own read finds the connection lost waits for the recovery that closes
     * it even with waiting for a reconnect disabled, since there is no reconnect to wait for: it fails with
     * "Reconnect is disabled" once the connection is Closed and the listener has heard so. The transport's close
     * takes a while, so an operation that did not wait would fail before the Closed event.
     */
    #[DataProvider('noReconnect')]
    public function testARequestWhoseReadFindsTheConnectionLostWithWaitingDisabledFailsOnceItIsClosed(bool $reconnectEnabled, int $maxReconnectAttempts): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->open($transport, $recorder, $reconnectEnabled, $maxReconnectAttempts, waitForReconnect: false);
        $transport->responder = static function () use ($transport): array {
            $transport->closeDelay = 0.2;
            $transport->dropConnection();

            return [];
        };

        $error = $this->reconnectDisabledError(static fn() => $connection->request('svc', 'one')->await());

        self::assertInstanceOf(TransportClosedException::class, $error->getPrevious());
        $this->assertClosedWithoutADial($connection, $transport, $recorder, 'Socket closed by peer (EOF)');
    }

    /**
     * The same for a fatal -ERR that the request's read meets: the request fails with the server's error once the
     * connection is Closed.
     */
    #[DataProvider('noReconnect')]
    public function testARequestWhoseReadMeetsAFatalErrWithWaitingDisabledFailsOnceItIsClosed(bool $reconnectEnabled, int $maxReconnectAttempts): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->open($transport, $recorder, $reconnectEnabled, $maxReconnectAttempts, waitForReconnect: false);
        $transport->responder = static function () use ($transport): array {
            $transport->closeDelay = 0.2;
            $transport->silence();

            return [self::STALE];
        };

        try {
            $connection->request('svc', 'one')->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame("Server sent error frame: 'Stale Connection'", $e->getMessage());
        }

        $this->assertClosedWithoutADial($connection, $transport, $recorder, "Server sent error frame: 'Stale Connection'");
    }

    /** One attempt allowed is one attempt made: the lost connection is dialled once more, and reopens. */
    public function testWithOneAttemptALostConnectionIsDialledOnceAndReopens(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->open($transport, $recorder, true, 1);
        $transport->dropConnection();

        $connection->processIncoming()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->connectCalls, 'one reconnect dial');
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected], $recorder->events);
    }

    private function open(
        ReconnectingTransport $transport,
        LifecycleRecorder $recorder,
        bool $reconnectEnabled,
        int $maxReconnectAttempts,
        int $maxPingsOut = 2,
        bool $waitForReconnect = true,
        int|float $pingIntervalSeconds = 0,
    ): NatsConnection {
        $connection = $this->own(new NatsConnection(
            new NatsOptions(
                connectTimeoutMs: 500,
                requestTimeoutMs: 2_000,
                reconnectEnabled: $reconnectEnabled,
                maxReconnectAttempts: $maxReconnectAttempts,
                reconnectDelayMs: 5,
                reconnectMaxDelayMs: 20,
                reconnectJitterMs: 0,
                pingIntervalSeconds: $pingIntervalSeconds,
                maxPingsOut: $maxPingsOut,
                connectionListener: $recorder->connectionListener(),
                errorListener: $recorder->errorListener(),
                waitForReconnect: $waitForReconnect,
            ),
            $transport,
        ));
        $this->opened[] = $connection;
        $connection->connect()->await();

        return $connection;
    }

    /**
     * Closed, announced once with the cause, and no dial after the first.
     *
     * @param list<ConnectionEvent> $events
     */
    private function assertClosedWithoutADial(
        NatsConnection $connection,
        ReconnectingTransport $transport,
        LifecycleRecorder $recorder,
        string $cause,
        array $events = [ConnectionEvent::Connected, ConnectionEvent::Closed],
    ): void {
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame($events, $recorder->events, 'no Disconnected, no Reconnected');
        self::assertSame([$cause], $recorder->closedErrors, 'the Closed event carries the cause');
        self::assertCount(1, $transport->connectCalls, 'no reconnect dial');
    }

    /** Runs the operation and returns the "Reconnect is disabled" error it fails with. */
    private function reconnectDisabledError(\Closure $operation): ConnectionException
    {
        try {
            $operation();
        } catch (ConnectionException $e) {
            self::assertSame('Reconnect is disabled', $e->getMessage());

            return $e;
        }

        self::fail('expected ConnectionException: Reconnect is disabled');
    }

    /** One heartbeat tick, as the heartbeat's timer runs it. */
    private function tick(NatsConnection $connection): void
    {
        (new \ReflectionMethod($connection, 'pingTimerTick'))->invoke($connection);
    }

    private function readInProgress(NatsConnection $connection): bool
    {
        return (bool) (new \ReflectionProperty(NatsConnection::class, 'readInProgress'))->getValue($connection);
    }

    /**
     * Whether a read was waiting on the socket each time the transport was asked to close it.
     *
     * @return \ArrayObject<int, bool>
     */
    private function recordReadsPendingAtClose(NatsConnection $connection, ReconnectingTransport $transport): \ArrayObject
    {
        /** @var \ArrayObject<int, bool> $pending */
        $pending = new \ArrayObject();
        $transport->beforeClose = fn() => $pending->append($this->readInProgress($connection));

        return $pending;
    }
}
