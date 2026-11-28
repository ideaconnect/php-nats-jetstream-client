<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\KeepsEventLoopAlive;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ListenerAction;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The rest of the chunk a lame-duck INFO came in (#182). A lame-duck notice moves nothing (#265), so the rest of the
 * chunk is the connection's own and is handled as any other chunk: its PING is answered, and an INFO behind the notice
 * applies. Only a LameDuck listener that closes the connection itself ends it while the chunk is handled: the rest of
 * the chunk is then the closed connection's, its PING not answered on the closed transport and its messages gone with
 * the connection. Up to 2.35.0 the notice failed the connection over (#47), and this class pinned what became of the
 * remainder on the server the connection moved to; those tests went with the failover.
 *
 * The guards run over `tests/Support/FakeTransport.php`; the close over the scripted server in
 * `tests/Support/ReconnectingTransport.php`, with the lame-duck INFO naming a second server in `connect_urls`.
 */
final class LameDuckChunkRemainderTest extends TestCase
{
    use KeepsEventLoopAlive;
    use ReconnectScenarios;

    /** The first server's lame-duck INFO, naming the second server: the pool grows to two. */
    private const LAME_DUCK = 'INFO {"server_id":"S1","version":"2.12.0","max_payload":1048576,"ldm":true,"connect_urls":["10.0.0.2:4222"]}' . "\r\n";

    /** A lame-duck INFO naming no other server: the pool stays at one. */
    private const LAME_DUCK_ALONE = 'INFO {"server_id":"S1","version":"2.12.0","max_payload":1048576,"ldm":true}' . "\r\n";

    /** An INFO of the first server behind its lame-duck INFO: no lame-duck flag, and a server id of its own. */
    private const STALE_INFO = 'INFO {"server_id":"S1-leaving","version":"2.12.0","max_payload":1048576}' . "\r\n";

    private const PING = "PING\r\n";

    private const PONG = "PONG\r\n";

    protected function setUp(): void
    {
        $this->keepEventLoopAlive();
    }

    protected function tearDown(): void
    {
        $this->letEventLoopEnd();
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{bool, string}> */
    public static function connectionsThatDoNotFailOver(): iterable
    {
        yield 'reconnect disabled' => [false, self::LAME_DUCK];
        yield 'a pool of one' => [true, self::LAME_DUCK_ALONE];
        yield 'reconnect enabled, the INFO naming another server (#265)' => [true, self::LAME_DUCK];
    }

    /**
     * No failover - reconnect disabled, a pool of one, or reconnect enabled with the INFO naming another server, which
     * failed over up to 2.35.0 - and [INFO ldm | PING | INFO stale] is handled as any other chunk: LameDuck is emitted,
     * the PING is answered on the only connection, and the stale INFO applies.
     */
    #[DataProvider('connectionsThatDoNotFailOver')]
    public function testWithoutAFailoverTheRestOfTheChunkIsHandledAsBefore(bool $reconnectEnabled, string $lameDuck): void
    {
        $transport = new FakeTransport([ReconnectingTransport::INFO, self::PONG, $lameDuck . self::PING . self::STALE_INFO]);
        $recorder = new LifecycleRecorder();
        $connection = $this->own(new NatsConnection(
            new NatsOptions(
                reconnectEnabled: $reconnectEnabled,
                maxReconnectAttempts: 1,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
                connectionListener: $recorder->connectionListener(),
            ),
            $transport,
        ));
        $connection->connect()->await();

        $frames = $connection->processIncoming()->await();

        self::assertSame(3, $frames);
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(1, $transport->connectCalls, 'no failover');
        self::assertContains(self::PONG, $transport->writes, 'the PING was answered');
        self::assertSame('S1-leaving', $connection->serverInfo()?->serverId, 'the stale INFO applied');
        self::assertContains(ConnectionEvent::LameDuck, $recorder->events);
    }

    /**
     * The LameDuck listener closes the connection itself, a disconnect() awaited from the listener: the close count
     * moves, so [INFO ldm | PING | MSG on a user subscription] is the closed connection's. The PING is not answered on
     * the closed transport, the message goes with the connection, as what a disconnect() discards does, the read
     * returns what it read, and the connection is Closed with one Closed event. The PONG written on the closed transport
     * used to fail the read with "The connection is gone". It exercises the close-count half of the dispatch's check.
     */
    public function testWhenTheLameDuckListenerClosesTheConnectionAPingBehindTheInfoIsNotAnsweredOnTheClosedTransport(): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection): ConnectionState {
            $connection->disconnect()->await();

            return $connection->state();
        }, ConnectionEvent::LameDuck);
        $connection = $this->connect($transport, connectionListener: $action->listener());
        $action->connection = $connection;
        $received = [];
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        $transport->pushFrame(self::LAME_DUCK . self::PING . ReconnectingTransport::msgFrame('updates', $sid, 'm1'));
        $frames = $connection->processIncoming()->await(new TimeoutCancellation(3));

        self::assertSame(3, $frames);
        self::assertNull($action->failure);
        self::assertSame(ConnectionState::Closed, $action->result, 'the disconnect() from the listener closed the connection');
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertFalse($transport->sessionLive());
        self::assertCount(1, $transport->connectCalls, 'no failover');
        self::assertSame([], $transport->controlLinesStartingWith('PONG'), 'the PING was not answered');
        self::assertSame([], $received, 'the message went with the closed connection');
        self::assertSame([
            ConnectionEvent::Connected,
            ConnectionEvent::DiscoveredServers,
            ConnectionEvent::LameDuck,
            ConnectionEvent::Closed,
        ], $action->recorder->events);
    }
}
