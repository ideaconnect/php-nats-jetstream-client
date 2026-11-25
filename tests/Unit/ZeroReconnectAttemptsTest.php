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

/**
 * `maxReconnectAttempts: 0` disables reconnect retries, as NatsOptions documents (#268): a lost connection makes no
 * reconnect attempt and ends as it does with reconnect off - Closed, the Closed event carrying the cause, and
 * "Reconnect is disabled" with the cause as its previous for the operation that waits for it - and a lame duck notice
 * naming another server starts no failover. The initial connect already read 0 that way; the recovery after a lost
 * connection made max(1, 0) = 1 attempt: one more backoff and dial after every drop, the connection carrying on with
 * its subscriptions replayed when that dial succeeded. nats.go reads its MaxReconnect of 0 as no reconnect attempt too.
 *
 * Each case runs with reconnect off and with reconnect on at 0 attempts, and expects the same outcome. The scripted
 * server accepts every dial, so an attempt made anyway would reopen the connection.
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
     * The heartbeat that gives up on a server which stopped answering its PINGs ends the connection, with its reason.
     * The tick is run directly, as in {@see ReconnectDisabledCauseTest}: with the timer, the next tick's bounded PONG
     * read can still be pending when the recovery closes the socket, and with reconnect off that read's EOF announces
     * a second Closed on main as well, a separate matter.
     */
    #[DataProvider('noReconnect')]
    public function testMissedPingsMakeNoDialAndCloseWithTheCause(bool $reconnectEnabled, int $maxReconnectAttempts): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->open($transport, $recorder, $reconnectEnabled, $maxReconnectAttempts, maxPingsOut: 1);
        $transport->silence();
        (new \ReflectionProperty(NatsConnection::class, 'outstandingPings'))->setValue($connection, 1);

        (new \ReflectionMethod($connection, 'pingTimerTick'))->invoke($connection);

        $this->assertClosedWithoutADial($connection, $transport, $recorder, 'The server did not answer the last PING');
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

    /** One attempt allowed is one failover dial for a lame duck notice naming another server. */
    public function testWithOneAttemptALameDuckNoticeFailsOverWithOneDial(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->open($transport, $recorder, true, 1);
        $transport->pushFrame(self::LAME_DUCK_INFO);

        $connection->processIncoming()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->connectCalls, 'one failover dial');
        self::assertSame(1, $transport->epoch(), 'on the new connection');
        self::assertContains(ConnectionEvent::Reconnected, $recorder->events);
    }

    private function open(
        ReconnectingTransport $transport,
        LifecycleRecorder $recorder,
        bool $reconnectEnabled,
        int $maxReconnectAttempts,
        int $maxPingsOut = 2,
        bool $waitForReconnect = true,
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
                pingIntervalSeconds: 0,
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
}
