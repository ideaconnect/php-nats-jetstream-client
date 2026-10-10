<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\ByteStream\ClosedException;
use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\KeepsEventLoopAlive;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ListenerAction;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\SlowLogger;
use IDCT\NATS\Transport\TransportClosedException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

use function Amp\async;
use function Amp\delay;

/**
 * The listener a reconnect announces the new connection to - Reconnected, or Connected for a failed initial
 * connect that the reconnect completed - and the operations it calls, over the scripted server in
 * `tests/Support/ReconnectingTransport.php`.
 *
 * The announcement used to be made inside the reconnect, which counted as in flight until the listener had
 * returned. An operation the listener called that found the new connection gone already - a read meeting EOF
 * or a fatal -ERR, a failed write - joined that reconnect instead of starting one: it waited out its timeout,
 * or for good, and the connection stayed Open on the dead socket. The connection is now announced once the
 * reconnect is over. Such an operation reconnects again, operations waiting for the reconnect resume without
 * waiting for the listener, and an open announced while a Connected or Reconnected listener call runs is
 * delivered from the event loop, so that the calls do not nest under a server that drops every connection.
 *
 * A referenced timer keeps the event loop running ({@see KeepsEventLoopAlive}), so that the old deadlocks fail
 * at their timeouts.
 */
final class ReconnectedListenerTest extends TestCase
{
    use KeepsEventLoopAlive;
    use ReconnectScenarios;

    private const STALE = "-ERR 'Stale Connection'\r\n";

    private const LAME_DUCK = 'INFO {"server_id":"S1","version":"2.12.0","max_payload":1048576,"ldm":true,"connect_urls":["10.0.0.2:4222"]}' . "\r\n";

    private const TWO_RECONNECTS = [
        ConnectionEvent::Connected,
        ConnectionEvent::Disconnected,
        ConnectionEvent::Reconnected,
        ConnectionEvent::Disconnected,
        ConnectionEvent::Reconnected,
    ];

    protected function setUp(): void
    {
        $this->keepEventLoopAlive();
    }

    protected function tearDown(): void
    {
        $this->letEventLoopEnd();
        $this->closeOpenedConnections();
    }

    /**
     * The listener drops the new connection and reads: the read meets EOF, reconnects again at once and returns
     * without error. It used to join the reconnect that was waiting for the listener, wait out its budget (5 s
     * here) and fail with CancelledException, and the connection stayed Open on the dead socket.
     */
    public function testReadInAReconnectedListenerThatMeetsEofReconnectsAgain(): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection) use ($transport): int {
            $transport->dropConnection();

            return $connection->processIncoming(new TimeoutCancellation(5))->await();
        });
        $connection = $this->connect($transport, connectionListener: $action->listener());
        $action->connection = $connection;

        $this->loseConnectionAndReadThroughTheReconnect($connection, $transport);

        self::assertNull($action->failure);
        self::assertLessThan(2.0, self::secondsTaken($action), 'the read did not wait out its budget');
        $this->assertReconnectedAgain($connection, $transport);
        self::assertSame(self::TWO_RECONNECTS, $action->recorder->events);
    }

    /** The same read without a cancellation used to never return. */
    public function testUnboundedReadInAReconnectedListenerThatMeetsEofDoesNotHang(): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection) use ($transport): int {
            $transport->dropConnection();

            return $connection->processIncoming()->await();
        });
        $connection = $this->connect($transport, connectionListener: $action->listener());
        $action->connection = $connection;

        $this->loseConnectionAndReadThroughTheReconnect($connection, $transport);

        self::assertNull($action->failure);
        $this->assertReconnectedAgain($connection, $transport);
    }

    /**
     * The server ends the new connection with a fatal -ERR, which the listener reads: the read reconnects again,
     * then fails with the server's error at once. It used to wait out its budget (5 s here) first, and the
     * connection stayed on the connection the server had ended.
     */
    public function testFatalErrReadInAReconnectedListenerReconnectsAgain(): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection) use ($transport): int {
            $transport->silence();
            $transport->pushFrame(self::STALE);

            return $connection->processIncoming(new TimeoutCancellation(5))->await();
        });
        $connection = $this->connect($transport, connectionListener: $action->listener());
        $action->connection = $connection;

        $this->loseConnectionAndReadThroughTheReconnect($connection, $transport);

        self::assertInstanceOf(ConnectionException::class, $action->failure);
        self::assertSame("Server sent error frame: 'Stale Connection'", $action->failure->getMessage());
        self::assertLessThan(2.0, self::secondsTaken($action), 'the read did not wait out its budget');
        $this->assertReconnectedAgain($connection, $transport);
    }

    /**
     * With waiting disabled the read fails with the -ERR at once and the reconnect it starts goes on without it.
     * That reconnect used to join the one waiting for the listener and end with it, reopening nothing.
     */
    public function testFatalErrReadInAReconnectedListenerWithWaitingOffReconnectsAgain(): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection) use ($transport): int {
            $transport->silence();
            $transport->pushFrame(self::STALE);

            return $connection->processIncoming(new TimeoutCancellation(0.5))->await();
        });
        $connection = $this->connect($transport, waitForReconnect: false, connectionListener: $action->listener());
        $action->connection = $connection;

        $this->loseConnectionAndReadThroughTheReconnect($connection, $transport);
        $this->waitUntil(static fn(): bool => $transport->epoch() === 2 && $connection->state() === ConnectionState::Open, 1.0);

        self::assertInstanceOf(ConnectionException::class, $action->failure);
        self::assertSame("Server sent error frame: 'Stale Connection'", $action->failure->getMessage());
        self::assertTrue($transport->sessionLive());
    }

    /**
     * A publish the listener makes on the dead new connection reconnects and is sent on the next connection. It
     * used to never return.
     */
    public function testPublishInAReconnectedListenerOnADeadNewConnectionIsSentOnTheNextOne(): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection) use ($transport): void {
            $transport->dropConnection();
            $connection->publish('orders', 'from-the-listener')->await();
        });
        $connection = $this->connect($transport, connectionListener: $action->listener());
        $action->connection = $connection;

        $this->loseConnectionAndReadThroughTheReconnect($connection, $transport);

        self::assertNull($action->failure);
        self::assertSame(['PUB orders 17'], $transport->controlLinesStartingWith('PUB', 2), 'sent on the next connection');
        $this->assertReconnectedAgain($connection, $transport);
    }

    /**
     * A request the listener makes on the dead new connection gets its reply. It used to hang past its own
     * 0.5 s timeout: its publish joined the reconnect with nothing to bound the wait.
     */
    public function testRequestInAReconnectedListenerOnADeadNewConnectionGetsItsReply(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $replyTo !== null
            ? $transport->replyFrame($replyTo, 're: ' . $payload)
            : [];
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection) use ($transport): string {
            $transport->dropConnection();

            return $connection->request('svc', 'from-the-listener', 500)->await()->payload;
        });
        $connection = $this->connect($transport, connectionListener: $action->listener());
        $action->connection = $connection;
        $connection->request('svc', 'warm-up')->await();

        $this->loseConnectionAndReadThroughTheReconnect($connection, $transport);

        self::assertNull($action->failure);
        self::assertSame('re: from-the-listener', $action->result);
        $this->assertReconnectedAgain($connection, $transport);
    }

    /**
     * A flush the listener makes on the dead new connection reconnects and fails at once with "Connection lost
     * before the server answered the PING", as anywhere else. It used to time out after its budget (5 s here),
     * leaving the connection Open on the dead socket.
     */
    public function testFlushInAReconnectedListenerOnADeadNewConnectionReconnects(): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection) use ($transport): void {
            $transport->dropConnection();
            $connection->flush()->await();
        });
        $connection = $this->connect($transport, requestTimeoutMs: 5_000, connectionListener: $action->listener());
        $action->connection = $connection;

        $this->loseConnectionAndReadThroughTheReconnect($connection, $transport);

        self::assertInstanceOf(ConnectionException::class, $action->failure);
        self::assertSame('Connection lost before the server answered the PING', $action->failure->getMessage());
        self::assertLessThan(2.0, self::secondsTaken($action), 'the flush did not wait out its budget');
        $this->assertReconnectedAgain($connection, $transport);
    }

    /**
     * A subscribe the listener makes on the dead new connection reconnects and subscribes on the next
     * connection. It used to time out after its budget (5 s here), and the subscription was dropped.
     */
    public function testSubscribeInAReconnectedListenerOnADeadNewConnectionSubscribesOnTheNextOne(): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection) use ($transport): int {
            $transport->dropConnection();

            return $connection->subscribe('orders', static function (): void {})->await();
        });
        $connection = $this->connect($transport, requestTimeoutMs: 5_000, connectionListener: $action->listener());
        $action->connection = $connection;

        $this->loseConnectionAndReadThroughTheReconnect($connection, $transport);

        self::assertNull($action->failure);
        self::assertIsInt($action->result);
        self::assertSame(['SUB orders ' . $action->result], $transport->controlLinesStartingWith('SUB orders', 2));
        self::assertLessThan(2.0, self::secondsTaken($action), 'the subscribe did not wait out its budget');
        $this->assertReconnectedAgain($connection, $transport);
    }

    /**
     * The listener reads a lame-duck INFO naming another server: the connection fails over to it. The read used
     * to hang, past its own 0.5 s, in a failover that joined the reconnect with nothing to bound the wait.
     */
    public function testLameDuckInfoReadInAReconnectedListenerFailsOver(): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection) use ($transport): int {
            $transport->pushFrame(self::LAME_DUCK);

            return $connection->processIncoming(new TimeoutCancellation(0.5))->await();
        });
        $connection = $this->connect($transport, connectionListener: $action->listener());
        $action->connection = $connection;

        $this->loseConnectionAndReadThroughTheReconnect($connection, $transport);

        self::assertNull($action->failure);
        $this->assertReconnectedAgain($connection, $transport);
    }

    /**
     * The listener reads a lame-duck INFO with a PING behind it in the same chunk (#182): the connection fails over,
     * and the PING, the leaving server's, is not answered on the connection the failover opened. The dispatch used to
     * go on with the chunk after the failover and write the PONG there.
     */
    public function testPingBehindALameDuckInfoReadInAReconnectedListenerIsNotAnsweredOnTheNewConnection(): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection) use ($transport): int {
            $transport->pushFrame(self::LAME_DUCK . "PING\r\n");

            return $connection->processIncoming(new TimeoutCancellation(0.5))->await();
        });
        $connection = $this->connect($transport, connectionListener: $action->listener());
        $action->connection = $connection;

        $this->loseConnectionAndReadThroughTheReconnect($connection, $transport);

        self::assertNull($action->failure);
        self::assertSame(2, $action->result, 'the read returned both frames');
        $this->assertReconnectedAgain($connection, $transport);
        self::assertSame([], $transport->controlLinesStartingWith('PONG', 2), 'no PONG on the connection the failover opened');
        self::assertSame([], $transport->controlLinesStartingWith('PONG'), 'the leaving server\'s PING was not answered at all');
    }

    /**
     * A failed initial connect that the reconnect completes is announced as Connected, once the reconnect is
     * over, like a Reconnected: a read in its listener that meets EOF reconnects at once. It used to wait out its
     * budget (5 s here), and the connection stayed Open on the dead socket.
     */
    public function testReadInAConnectedListenerOfARecoveredInitialConnectThatMeetsEofReconnects(): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection) use ($transport): int {
            $transport->dropConnection();

            return $connection->processIncoming(new TimeoutCancellation(5))->await();
        }, ConnectionEvent::Connected);
        $connection = $this->unconnected($transport, $action->listener());
        $action->connection = $connection;
        $transport->refuseDials();
        $this->acceptDialsAfter($transport, 0.05);

        $connection->connect()->await(new TimeoutCancellation(10));

        self::assertNull($action->failure);
        self::assertLessThan(2.0, self::secondsTaken($action), 'the read did not wait out its budget');
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertTrue($transport->sessionLive(), 'not left Open on the dead socket');
        self::assertSame(1, $transport->epoch());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected], $action->recorder->events);
    }

    /**
     * The reconnect a read in the listener starts gives up: the read fails with "Reconnect attempts exhausted"
     * and the close is announced once, by that reconnect. The read used to fail with CancelledException after its
     * 0.5 s, and nothing was announced.
     */
    public function testReconnectStartedFromAReconnectedListenerThatGivesUpClosesOnce(): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection) use ($transport): int {
            $transport->refuseDials();
            $transport->dropConnection();

            return $connection->processIncoming(new TimeoutCancellation(0.5))->await();
        });
        $connection = $this->connect($transport, maxReconnectAttempts: 2, connectionListener: $action->listener());
        $action->connection = $connection;

        $this->loseConnectionAndReadThroughTheReconnect($connection, $transport);

        self::assertInstanceOf(ConnectionException::class, $action->failure);
        self::assertSame('Reconnect attempts exhausted', $action->failure->getMessage());
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(1, $action->recorder->closedEvents(), 'announced once');
        self::assertSame(
            [ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected, ConnectionEvent::Disconnected, ConnectionEvent::Closed],
            $action->recorder->events,
        );
    }

    /**
     * disconnect() from the listener closes the connection once, and nothing reopens it. It does not wait for
     * the reconnect that called the listener: that wait would last until the ten-second connect timeout, or the
     * listener's own five-second bound on the call.
     */
    public function testDisconnectFromAReconnectedListenerClosesOnce(): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection): void {
            $connection->disconnect()->await(new TimeoutCancellation(5));
        });
        $connection = $this->connect($transport, connectionListener: $action->listener(), connectTimeoutMs: 10_000);
        $action->connection = $connection;

        $this->loseConnectionAndReadThroughTheReconnect($connection, $transport);

        self::assertNull($action->failure);
        self::assertLessThan(2.0, self::secondsTaken($action));
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected, ConnectionEvent::Closed], $action->recorder->events);
        self::assertSame(1, $transport->epoch());
    }

    /**
     * Another fiber's read meets EOF while the listener is still busy: the connection reconnects again. That
     * read used to join the reconnect waiting for the listener and end with it, and the connection stayed Open on
     * the dead socket.
     */
    public function testConnectionLostWhileItsReconnectedListenerRunsIsRecovered(): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection) use ($transport): Future {
            $reader = async(static fn(): int => $connection->processIncoming(new TimeoutCancellation(1))->await());
            delay(0.05);
            $transport->dropConnection();
            // Still busy: an HTTP call, a JetStream request...
            delay(0.1);

            return $reader;
        });
        $connection = $this->connect($transport, connectionListener: $action->listener());
        $action->connection = $connection;

        $this->loseConnectionAndReadThroughTheReconnect($connection, $transport);
        self::assertInstanceOf(Future::class, $action->result);
        $action->result->await(new TimeoutCancellation(2));

        $this->assertReconnectedAgain($connection, $transport);
    }

    /**
     * The heartbeat finds the new connection dead while the listener is still busy: it reconnects. It used to
     * join the reconnect waiting for the listener, after cancelling its own timer, so nothing noticed any more.
     */
    public function testHeartbeatThatFindsTheNewConnectionDeadWhileTheReconnectedListenerRunsRecoversIt(): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), static function () use ($transport): void {
            $transport->dropConnection();
            delay(0.2);
        });
        $connection = $this->connect($transport, pingIntervalSeconds: 0.05, connectionListener: $action->listener());
        $action->connection = $connection;

        $this->loseConnectionAndReadThroughTheReconnect($connection, $transport);
        $this->waitUntil(static fn(): bool => $transport->epoch() === 2 && $connection->state() === ConnectionState::Open, 1.0);

        self::assertTrue($transport->sessionLive());
    }

    /**
     * A connect() that joined a failing first dial is settled before the Connected listener of the reconnect
     * that completed the connect runs, as on a direct connect: a listener that waits for it does not wait for
     * itself. It used to wait out its whole bound, five seconds here.
     */
    public function testConnectedListenerOfARecoveredInitialConnectCanAwaitAConnectThatJoinedTheDial(): void
    {
        $transport = new ReconnectingTransport();
        $joining = new class {
            /** @var ?Future<void> */
            public ?Future $connect = null;
        };
        $action = new ListenerAction(new LifecycleRecorder(), static function () use ($joining): void {
            $joining->connect?->await(new TimeoutCancellation(5));
        }, ConnectionEvent::Connected);
        $connection = $this->unconnected($transport, $action->listener());
        $action->connection = $connection;
        $transport->holdNextDial();

        $owner = $connection->connect();
        delay(0.01);
        // Joins while the first dial is under way: it waits on the connect, not on a reconnect.
        $joining->connect = $connection->connect();
        delay(0.01);
        // The held dial fails, and the connect hands off to a reconnect, which succeeds once dials are let through.
        $transport->refuseDials();
        $transport->releaseDial();
        $this->acceptDialsAfter($transport, 0.05);

        $owner->await(new TimeoutCancellation(10));

        self::assertNull($action->failure);
        self::assertLessThan(2.0, self::secondsTaken($action));
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /** @return iterable<string, array{bool}> */
    public static function connectPaths(): iterable
    {
        yield 'first dial succeeds' => [false];
        yield 'first dial fails and a reconnect completes the connect' => [true];
    }

    /**
     * The Connected listener lets the new connection die and returns with the next reconnect in flight. The
     * connect() joiners were settled with success before the listener ran; the owner's connect() checks the
     * connection after the listener and fails with "Connect was aborted before the connection opened", though
     * the connection comes back moments later. Pinned as it is: the direct path has always split this way, and
     * a connect that a reconnect completes now does the same. It used to report success to both, with the
     * connection Open on the dead socket.
     */
    #[DataProvider('connectPaths')]
    public function testConnectOwnerFailsAndItsJoinerSucceedsWhenTheConnectedListenerLeavesAReconnectInFlight(bool $firstDialFails): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection) use ($transport): void {
            $transport->refuseDials();
            $transport->dropConnection();
            async(static fn(): int => $connection->processIncoming(new TimeoutCancellation(3))->await())->ignore();
            delay(0.03);
        }, ConnectionEvent::Connected);
        $connection = $this->unconnected($transport, $action->listener());
        $action->connection = $connection;
        $transport->holdNextDial();

        $owner = $connection->connect();
        $owner->ignore();
        delay(0.01);
        $joiner = $connection->connect();
        $joiner->ignore();
        delay(0.01);
        if ($firstDialFails) {
            $transport->refuseDials();
            $this->acceptDialsAfter($transport, 0.03);
        }
        $transport->releaseDial();

        $ownerFailure = null;
        try {
            $owner->await(new TimeoutCancellation(3));
        } catch (ConnectionException $e) {
            $ownerFailure = $e;
        }
        $joiner->await(new TimeoutCancellation(3));

        self::assertNotNull($ownerFailure, 'the owner was told the connect was aborted');
        self::assertSame('Connect was aborted before the connection opened', $ownerFailure->getMessage());
        self::assertSame(ConnectionState::Connecting, $connection->state(), 'the next reconnect is under way');
        $transport->acceptDials();
        $this->waitUntilOpen($connection);
        self::assertTrue($transport->sessionLive());
    }

    /**
     * A request and a flush issued during the outage complete while the Reconnected listener is still busy: they
     * resume once the reconnect is over. They used to wait for the listener to return.
     */
    public function testOperationsWaitingForTheReconnectCompleteWhileItsListenerRuns(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $replyTo !== null
            ? $transport->replyFrame($replyTo, 're: ' . $payload)
            : [];
        $waiting = new class {
            /** @var ?Future<NatsMessage> */
            public ?Future $request = null;
            /** @var ?Future<void> */
            public ?Future $flush = null;
        };
        $action = new ListenerAction(new LifecycleRecorder(), static function () use ($waiting): bool {
            delay(0.3);

            return $waiting->request?->isComplete() === true && $waiting->flush?->isComplete() === true;
        });
        $connection = $this->connect($transport, connectionListener: $action->listener());
        $action->connection = $connection;
        $connection->request('svc', 'warm-up')->await();
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $waiting->request = $connection->request('svc', 'during-the-outage');
        $waiting->flush = $connection->flush();
        delay(0.02);
        $transport->acceptDials();
        $reader->await(new TimeoutCancellation(3));

        self::assertTrue($action->result, 'both completed before the listener returned');
        self::assertSame('re: during-the-outage', $waiting->request->await()->payload);
        $waiting->flush->await();
    }

    /**
     * A publish parked on the reconnect's sealed flush (#165) is written before the Reconnected listener
     * returns: the flush gate opens once the reconnect is over. It used to stay parked until the listener had
     * returned.
     */
    public function testPublishParkedOnTheSealedReconnectFlushIsWrittenWhileTheListenerRuns(): void
    {
        $transport = new ReconnectingTransport();
        $publishing = new class {
            /** @var ?Future<void> */
            public ?Future $publisher = null;
            public int $published = 0;
            public bool $sealed = false;
        };
        $action = new ListenerAction(new LifecycleRecorder(), static function () use ($publishing): bool {
            delay(0.05);

            return $publishing->publisher?->isComplete() === true;
        });
        // Waiting disabled: a publish buffered during the reconnect returns without yielding, so the publisher
        // below fills the buffer again during every write of the flush, until the flush seals it.
        $connection = $this->connect($transport, waitForReconnect: false, connectionListener: $action->listener());
        $action->connection = $connection;
        $gate = new \ReflectionProperty(NatsConnection::class, 'reconnectFlushGate');
        $transport->afterWrite = static function () use ($connection, $gate, $publishing): void {
            $publishing->sealed = $publishing->sealed || $gate->getValue($connection) !== null;
        };
        $reader = $this->startRecoveryHeldMidDial($connection, $transport);
        $connection->publish('buffered', 'first')->await();

        $publishing->publisher = async(static function () use ($connection, $publishing): void {
            while ($publishing->published < 2_000 && $connection->state() !== ConnectionState::Open) {
                $connection->publish('live', 'x')->await();
                $publishing->published++;
            }
        });
        $transport->releaseDial();
        $publishing->publisher->await(new TimeoutCancellation(3));
        $reader->await(new TimeoutCancellation(3));

        self::assertTrue($publishing->sealed, 'the publisher filled the buffer until the flush sealed it');
        self::assertLessThan(2_000, $publishing->published);
        self::assertTrue($action->result, 'the parked publish was written before the listener returned');
    }

    /**
     * disconnect() while the Reconnected listener is busy returns without waiting for it and announces one
     * Closed; the listener's next operation fails with "Connection is not open", and the message the reconnect
     * received is not delivered after the close. disconnect() used to wait for the listener, the reconnect still
     * counting as in flight, until the connect timeout (ten seconds here) bounded its wait.
     */
    public function testDisconnectWhileTheReconnectedListenerRunsDoesNotWaitForIt(): void
    {
        $transport = new ReconnectingTransport();
        /** @var DeferredFuture<null> $listenerDone */
        $listenerDone = new DeferredFuture();
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection) use ($listenerDone): void {
            // Busy until the test has measured the disconnect().
            $listenerDone->getFuture()->await();
            $connection->flush()->await();
        });
        $connection = $this->connect($transport, connectionListener: $action->listener(), connectTimeoutMs: 10_000);
        $action->connection = $connection;
        $delivered = [];
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use (&$delivered): void {
            $delivered[] = $message->payload;
        })->await();
        // The server sends a message right after the reconnect re-subscribes: the reconnect reads it, to deliver
        // once it is over.
        $transport->afterWrite = static function (string $bytes) use ($transport, $sid): void {
            if ($transport->epoch() === 1 && str_contains($bytes, 'SUB updates')) {
                $transport->pushFrame(ReconnectingTransport::msgFrame('updates', $sid, 'm1'));
            }
        };
        $reader = $this->startRecoveryInBackground($connection, $transport);
        $transport->acceptDials();
        $this->waitUntil(static fn(): bool => $action->recorder->events === [ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected]);

        $start = hrtime(true);
        // A disconnect() that waited for the listener would wait for good: the listener goes on only below.
        $connection->disconnect()->await(new TimeoutCancellation(5));
        $seconds = $this->secondsSince($start);
        $listenerStillBusy = $action->seconds === null;
        $listenerDone->complete();
        $reader->await(new TimeoutCancellation(3));

        self::assertLessThan(2.0, $seconds, 'did not wait for the listener');
        self::assertTrue($listenerStillBusy);
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected, ConnectionEvent::Closed], $action->recorder->events);
        self::assertInstanceOf(ConnectionException::class, $action->failure);
        self::assertSame('Connection is not open', $action->failure->getMessage());
        self::assertSame(1, $connection->statistics()->inMsgs, 'the reconnect read the message');
        self::assertSame([], $delivered, 'nothing delivered after the close');
    }

    /**
     * The listener calls disconnect() and returns while the close is still under way - a TLS or WebSocket close
     * takes a while - so the connection is still Open: the message the reconnect read is the close's to discard,
     * and the reconnect does not deliver it once the listener has returned. It used to deliver it after
     * disconnect() had been called, checking only that the connection was still Open.
     */
    public function testMessageTheReconnectReadIsNotDeliveredWhileTheListenersDisconnectIsUnderWay(): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection) use ($transport): Future {
            $transport->closeDelay = 0.2;
            $close = $connection->disconnect();
            delay(0.05);

            return $close;
        });
        $connection = $this->connect($transport, connectionListener: $action->listener());
        $action->connection = $connection;
        $delivered = [];
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use (&$delivered): void {
            $delivered[] = $message->payload;
        })->await();
        // The server sends a message right after the reconnect re-subscribes: the reconnect reads it, to deliver
        // once it is over.
        $transport->afterWrite = static function (string $bytes) use ($transport, $sid): void {
            if ($transport->epoch() === 1 && str_contains($bytes, 'SUB updates')) {
                $transport->pushFrame(ReconnectingTransport::msgFrame('updates', $sid, 'm1'));
            }
        };

        $reader = $this->startRecoveryInBackground($connection, $transport);
        $transport->acceptDials();
        $reader->await(new TimeoutCancellation(3));
        $deliveredWhenTheReconnectEnded = $delivered;
        self::assertInstanceOf(Future::class, $action->result);
        self::assertFalse($action->result->isComplete(), 'the close was still under way when the reconnect went on');
        $action->result->await(new TimeoutCancellation(3));

        self::assertSame(1, $connection->statistics()->inMsgs, 'the reconnect read the message');
        self::assertSame([], $deliveredWhenTheReconnectEnded);
        self::assertSame([], $delivered, 'discarded by the close');
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected, ConnectionEvent::Closed], $action->recorder->events);
    }

    /**
     * As above, with a service's run() serving the connection: its loop's read runs the reconnect, and reads
     * again while the listener's close is still under way. That read used to deliver the message the reconnect
     * read, as one an earlier read left queued, after disconnect() had been called; it leaves it to the close.
     */
    public function testMessageTheReconnectReadIsNotDeliveredByAServiceLoopWhileTheListenersDisconnectIsUnderWay(): void
    {
        $transport = new ReconnectingTransport();
        $holder = new class {
            public ?NatsClient $client = null;
            /** @var Future<void>|null */
            public ?Future $close = null;
        };
        $recorder = new LifecycleRecorder();
        $record = $recorder->connectionListener();
        $listener = static function (ConnectionEvent $event, ?\Throwable $error = null) use ($record, $transport, $holder): void {
            $record($event, $error);
            if ($event === ConnectionEvent::Reconnected && $holder->client !== null && $holder->close === null) {
                $transport->closeDelay = 0.2;
                $holder->close = $holder->client->disconnect();
                delay(0.05);
            }
        };
        $client = $this->own(new NatsClient($this->options(true, 2_000, 1_000, 0, 2, $listener, 5, 20, null), $transport));
        $this->opened[] = $client;
        $client->connect()->await();
        $holder->client = $client;
        $delivered = [];
        $sid = $client->subscribe('updates', static function (NatsMessage $message) use (&$delivered): void {
            $delivered[] = $message->payload;
        })->await();
        // The server sends a message right after the reconnect re-subscribes: the reconnect reads it.
        $transport->afterWrite = static function (string $bytes) use ($transport, $sid): void {
            if ($transport->epoch() === 1 && str_contains($bytes, 'SUB updates')) {
                $transport->pushFrame(ReconnectingTransport::msgFrame('updates', $sid, 'm1'));
            }
        };
        $service = $client->service('work', '1.0.0')
            ->addEndpoint('work', 'svc.work', static fn(NatsMessage $message): string => 'done');
        $running = $service->run(3.0);
        // The loop's read is parked on the socket when it drops.
        delay(0.05);

        $transport->refuseDials();
        $transport->dropConnection();
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Connecting);
        $transport->acceptDials();
        $running->await(new TimeoutCancellation(5));
        self::assertInstanceOf(Future::class, $holder->close);
        $holder->close->await(new TimeoutCancellation(3));

        self::assertSame(1, $client->statistics()->inMsgs, 'the reconnect read the message');
        self::assertSame([], $delivered, 'discarded by the close');
        self::assertSame(ConnectionState::Closed, $client->state());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected, ConnectionEvent::Closed], $recorder->events);
    }

    /**
     * As above, with the shipped transport's instant close and a listener that awaits its disconnect(): a request's
     * read runs the reconnect, and a service's run() waits for that reconnect to read again. The loop resumes once
     * the disconnect() has set out to close the connection, before the close is done, and its read delivered the
     * message the reconnect read.
     */
    public function testMessageTheReconnectReadIsNotDeliveredByAServiceLoopThatWaitedForTheReconnectWhenTheListenerAwaitsItsDisconnect(): void
    {
        $transport = new ReconnectingTransport();
        $holder = new class {
            public ?NatsClient $client = null;
            /** @var Future<void>|null */
            public ?Future $close = null;
        };
        $recorder = new LifecycleRecorder();
        $record = $recorder->connectionListener();
        $listener = static function (ConnectionEvent $event, ?\Throwable $error = null) use ($record, $holder): void {
            $record($event, $error);
            if ($event === ConnectionEvent::Reconnected && $holder->client !== null && $holder->close === null) {
                $holder->close = $holder->client->disconnect();
                $holder->close->await();
            }
        };
        $client = $this->own(new NatsClient($this->options(true, 2_000, 1_000, 0, 2, $listener, 5, 20, null), $transport));
        $this->opened[] = $client;
        $client->connect()->await();
        $holder->client = $client;
        $delivered = [];
        $sid = $client->subscribe('updates', static function (NatsMessage $message) use (&$delivered): void {
            $delivered[] = $message->payload;
        })->await();
        $transport->afterWrite = static function (string $bytes) use ($transport, $sid): void {
            if ($transport->epoch() === 1 && str_contains($bytes, 'SUB updates')) {
                $transport->pushFrame(ReconnectingTransport::msgFrame('updates', $sid, 'm1'));
            }
        };
        // A request nobody answers holds the socket read when the connection drops.
        $request = $client->request('svc.nobody', 'q', 2_500);
        $request->ignore();
        delay(0.05);
        $service = $client->service('work', '1.0.0')
            ->addEndpoint('work', 'svc.work', static fn(NatsMessage $message): string => 'done');
        $running = $service->run(3.0);
        delay(0.05);

        $transport->refuseDials();
        $transport->dropConnection();
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Connecting);
        $transport->acceptDials();
        $running->await(new TimeoutCancellation(5));
        self::assertInstanceOf(Future::class, $holder->close);
        $holder->close->await(new TimeoutCancellation(3));

        self::assertSame(1, $client->statistics()->inMsgs, 'the reconnect read the message');
        self::assertSame([], $delivered, 'discarded by the close');
        self::assertSame(ConnectionState::Closed, $client->state());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected, ConnectionEvent::Closed], $recorder->events);
    }

    /**
     * A publish whose failed write runs the reconnect, whose listener returns while a lame-duck failover it let
     * start holds its dial: the publish is buffered behind the failover and sent on the connection the failover
     * opens. The failover used to join the reconnect, which was waiting for its listener, and never happen: the
     * publish went to the connection the server was about to close.
     */
    public function testPublishWhoseListenerLeavesAFailoverInFlightIsSentOnTheConnectionItOpens(): void
    {
        $transport = new ReconnectingTransport();
        [$connection, $action] = $this->connectWithReconnectedAction($transport, $this->startFailover(...));

        $transport->failNextWriteContaining('PUB orders');
        $connection->publish('orders', 'payload')->await(new TimeoutCancellation(3));
        $transport->releaseDial();
        $this->waitUntilOpen($connection);

        self::assertListenerLeftAReconnectInFlight($action);
        self::assertSame(2, $transport->epoch(), 'failed over');
        self::assertSame(['PUB orders 7'], $transport->controlLinesStartingWith('PUB', 2));
        self::assertSame([], $transport->controlLinesStartingWith('PUB', 1));
    }

    /**
     * The same while the next reconnect backs off between refused dials: the publish is buffered and sent on
     * the next connection. It used to fail with "The connection is gone", the frame lost.
     */
    public function testPublishWhoseListenerLeavesAReconnectBackingOffIsSentOnTheNextConnection(): void
    {
        $transport = new ReconnectingTransport();
        [$connection, $action] = $this->connectWithReconnectedAction($transport, $this->startReconnectBetweenRefusedDials(...));

        $transport->failNextWriteContaining('PUB orders');
        $connection->publish('orders', 'payload')->await(new TimeoutCancellation(3));
        $transport->acceptDials();
        $this->waitUntilOpen($connection);

        self::assertListenerLeftAReconnectInFlight($action);
        self::assertSame(2, $transport->epoch());
        self::assertSame(['PUB orders 7'], $transport->controlLinesStartingWith('PUB', 2));
    }

    /**
     * The same while the next reconnect has dialled and waits for the server's greeting: nothing is written on
     * the new connection ahead of its CONNECT, which a retry written as soon as the reconnect returned would be.
     * The publish used to fail with "The connection is gone", the next reconnect joining the first.
     */
    public function testPublishRetryIsNotWrittenAheadOfTheNextConnectionsConnect(): void
    {
        $transport = new ReconnectingTransport();
        [$connection, $action] = $this->connectWithReconnectedAction($transport, $this->startReconnectAwaitingTheGreeting(...));

        $transport->failNextWriteContaining('PUB orders');
        $connection->publish('orders', 'payload')->await(new TimeoutCancellation(3));
        $transport->releaseGreeting();
        $this->waitUntilOpen($connection);

        self::assertListenerLeftAReconnectInFlight($action);
        $lines = $transport->controlLines(2);
        self::assertNotSame([], $lines);
        self::assertStringStartsWith('CONNECT ', $lines[0]);
        self::assertSame(['PUB orders 7'], $transport->controlLinesStartingWith('PUB', 2));
    }

    /**
     * Against a server that requires authentication, which refuses a connection whose first message is not
     * CONNECT: the connection is not closed for good, as it would be by a retry written ahead of CONNECT - the
     * reconnect refused as unauthorized gives up at once. The publish used to fail with "The connection is
     * gone".
     */
    public function testPublishRetryDoesNotGetTheConnectionClosedByAServerRequiringAuthentication(): void
    {
        $transport = new ReconnectingTransport();
        $transport->requireConnectFirst = true;
        $recorder = new LifecycleRecorder();
        [$connection, $action] = $this->connectWithReconnectedAction($transport, $this->startReconnectAwaitingTheGreeting(...), $recorder);

        $transport->failNextWriteContaining('PUB orders');
        $connection->publish('orders', 'payload')->await(new TimeoutCancellation(3));
        $transport->releaseGreeting();
        $this->waitUntilOpen($connection);

        self::assertListenerLeftAReconnectInFlight($action);
        self::assertSame(0, $recorder->closedEvents());
        self::assertSame(['PUB orders 7'], $transport->controlLinesStartingWith('PUB', 2));
    }

    /**
     * A request whose publish runs the reconnect, whose listener returns while a lame-duck failover holds its
     * dial, gets its reply on the connection the failover opens. The failover used to join the reconnect and
     * never happen.
     */
    public function testRequestWhoseListenerLeavesAFailoverInFlightGetsItsReply(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $replyTo !== null
            ? $transport->replyFrame($replyTo, 're: ' . $payload)
            : [];
        [$connection, $action] = $this->connectWithReconnectedAction($transport, $this->startFailover(...));
        $connection->request('svc', 'warm-up')->await();

        $transport->failNextWriteContaining('PUB svc');
        $request = $connection->request('svc', 'during-the-failover');
        $this->waitUntil(static fn(): bool => $action->seconds !== null);
        $transport->releaseDial();

        self::assertSame('re: during-the-failover', $request->await(new TimeoutCancellation(3))->payload);
        self::assertListenerLeftAReconnectInFlight($action);
        self::assertSame(2, $transport->epoch());
    }

    /**
     * A subscribe whose failed SUB write starts the reconnect, whose listener returns with the next reconnect in
     * flight: the subscribe waits for that one too, within its budget, and the SUB reaches the next connection.
     * Without that wait it would fail with "Connection is not open" at once and drop the subscription.
     */
    public function testSubscribeWhoseListenerLeavesAReconnectInFlightWaitsForIt(): void
    {
        $transport = new ReconnectingTransport();
        [$connection, $action] = $this->connectWithReconnectedAction(
            $transport,
            function (NatsConnection $connection, ReconnectingTransport $transport): void {
                $this->startReconnectBetweenRefusedDials($connection, $transport);
                $this->acceptDialsAfter($transport, 0.05);
            },
        );

        $transport->failNextWriteContaining('SUB orders');
        $sid = $connection->subscribe('orders', static function (): void {})->await(new TimeoutCancellation(3));

        self::assertListenerLeftAReconnectInFlight($action);
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(2, $transport->epoch());
        self::assertSame(['SUB orders ' . $sid], $transport->controlLinesStartingWith('SUB orders', 2));
    }

    /** @return iterable<string, array{?\Throwable}> What the failed write throws; null for the double's own error. */
    public static function writeErrors(): iterable
    {
        yield 'an error of this library' => [null];
        // AmpSocketTransport and WebSocketTransport pass on what their socket's write throws.
        yield 'a raw stream error, as from a built-in transport' => [new ClosedException('The stream is not writable')];
    }

    /**
     * A publish whose failed write runs the reconnect, whose listener closes the connection: the publish fails
     * with the error of its write, and nothing is sent after the close. A raw stream error is wrapped in a
     * TransportClosedException "Transport is not connected", so that the publish still fails with an error of
     * this library, as when its retry went into the closed transport; thrown as it was, it would escape a
     * catch (NatsThrowable).
     */
    #[DataProvider('writeErrors')]
    public function testPublishWhoseListenerClosesTheConnectionFailsWithTheErrorOfItsWrite(?\Throwable $writeError): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection): void {
            $connection->disconnect()->await();
        });
        $connection = $this->connect($transport, connectionListener: $action->listener());
        $action->connection = $connection;

        $transport->failNextWriteContaining('PUB orders', $writeError);
        $failure = $this->publishFailure($connection->publish('orders', 'payload'));

        self::assertInstanceOf(TransportClosedException::class, $failure);
        if ($writeError === null) {
            self::assertSame('The connection is gone', $failure->getMessage());
        } else {
            self::assertSame('Transport is not connected', $failure->getMessage());
            self::assertSame($writeError, $failure->getPrevious());
        }
        self::assertNull($action->failure);
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([], $transport->controlLinesStartingWith('PUB'));
    }

    /**
     * A publish whose failed write runs the reconnect, while a drain() waits for that reconnect: the drain goes on
     * once the reconnect is over, while the Reconnected listener is still busy, and is still flushing when the
     * listener returns - its PING goes unanswered. The publish's retry goes to the connection being drained, like
     * any publish made during a drain, and the publish succeeds. Failed with the error of its write instead, it
     * would lose a frame the open connection could still take.
     */
    public function testPublishWhoseReconnectADrainWaitsForIsWrittenWhileTheDrainFlushes(): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), static function () use ($transport): bool {
            delay(0.05);

            return $transport->controlLinesStartingWith('UNSUB', 1) !== [];
        });
        $connection = $this->connect($transport, requestTimeoutMs: 500, connectionListener: $action->listener());
        $action->connection = $connection;
        $connection->subscribe('updates', static function (): void {})->await();
        $transport->answerPings = false;

        [$publish, $drain] = $this->publishWhoseReconnectADrainWaitsFor($connection, $transport);
        $publish->await(new TimeoutCancellation(3));
        $drain->await(new TimeoutCancellation(3));

        self::assertTrue($action->result, 'the drain was under way while the listener ran');
        self::assertSame(['PUB orders 7'], $transport->controlLinesStartingWith('PUB', 1), 'written to the connection being drained');
        self::assertSame(['UNSUB 1'], $transport->controlLinesStartingWith('UNSUB', 1));
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /** @return iterable<string, array{bool}> */
    public static function slowAnnouncements(): iterable
    {
        yield 'the Reconnected listener is busy' => [true];
        // An application with no connection listener, whose logger writes to a slow stream.
        yield 'the logger is slow, and no listener is set' => [false];
    }

    /**
     * The same, but the drain is over before the announcement of the reconnect is - the Reconnected listener, or
     * the logger recording the Reconnected, takes its time: the publish fails with the error of its write, and its
     * frame is not sent. The drain waits for the reconnect, not for its announcement, and closed the connection
     * before the publish could retry. Pinned as it is: when the reconnect was announced from inside it, the retry
     * came first, because the drain could not go on until the listener and the logger had returned.
     */
    #[DataProvider('slowAnnouncements')]
    public function testPublishWhoseReconnectADrainWaitsForFailsWhenTheDrainEndsFirst(bool $slowListener): void
    {
        $transport = new ReconnectingTransport();
        if ($slowListener) {
            $action = new ListenerAction(new LifecycleRecorder(), static function (): void {
                delay(0.1);
            });
            $connection = $this->connect($transport, connectionListener: $action->listener());
            $action->connection = $connection;
            $announcementUnderWay = static fn(): bool => $action->seconds === null;
        } else {
            $logger = new SlowLogger('NATS connection Reconnected', 0.1);
            $connection = $this->connectWithLogger($transport, null, $logger);
            $announcementUnderWay = static fn(): bool => $logger->suspended;
        }
        $connection->subscribe('updates', static function (): void {})->await();

        [$publish, $drain] = $this->publishWhoseReconnectADrainWaitsFor($connection, $transport);
        $drain->await(new TimeoutCancellation(3));
        $drainEndedFirst = $announcementUnderWay();
        $failure = $this->publishFailure($publish);

        self::assertTrue($drainEndedFirst, 'the drain was over before the announcement');
        self::assertInstanceOf(TransportClosedException::class, $failure);
        self::assertSame('The connection is gone', $failure->getMessage());
        self::assertSame([], $transport->controlLinesStartingWith('PUB'));
        self::assertSame(['UNSUB 1'], $transport->controlLinesStartingWith('UNSUB', 1), 'the drain drained the reopened connection');
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /** @return iterable<string, array{bool}> */
    public static function listenersLeavingTheConnectionNotOpen(): iterable
    {
        yield 'the listener closes the connection' => [false];
        yield 'the next reconnect gives up' => [true];
    }

    /**
     * A subscribe whose failed SUB write starts the reconnect, whose listener leaves the connection not to come
     * back: the subscribe fails with "Connection is not open", the error of its write as the previous one, and
     * the subscription is dropped.
     */
    #[DataProvider('listenersLeavingTheConnectionNotOpen')]
    public function testSubscribeWhoseListenerLeavesTheConnectionNotOpenFails(bool $nextReconnectGivesUp): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), function (NatsConnection $connection) use ($transport, $nextReconnectGivesUp): void {
            if ($nextReconnectGivesUp) {
                $this->startReconnectBetweenRefusedDials($connection, $transport);
            } else {
                $connection->disconnect()->await();
            }
        });
        $connection = $this->connect($transport, maxReconnectAttempts: 3, connectionListener: $action->listener());
        $action->connection = $connection;

        $transport->failNextWriteContaining('SUB orders');
        try {
            $connection->subscribe('orders', static function (): void {})->await(new TimeoutCancellation(3));
            self::fail('expected the subscribe to fail');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
            self::assertInstanceOf(TransportClosedException::class, $e->getPrevious());
        }

        self::assertNull($action->failure);
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertFalse($connection->isSubscriptionActive(1));
    }

    /**
     * A server that drops every new connection while the listener reads on each Reconnected: 200 times. The
     * listener calls do not nest - each open is announced from the event loop while another call runs - so no
     * more than two calls are ever under way, the read that ran the first reconnect returns while the server is
     * still flapping, and every call finds the connection Open. Announced from the operation that reconnected,
     * the calls would nest one level deeper with every connection, a suspended fiber each. The old code stopped
     * at the first flap: the listener's read waited out its budget, and the dead connection stayed Open.
     */
    public function testListenerCallsDoNotNestUnderAFlappingServer(): void
    {
        $transport = new ReconnectingTransport();
        $flapping = new class {
            public ?NatsConnection $connection = null;
            public int $flapsLeft = 200;
            public int $callsUnderWay = 0;
            public int $mostCallsUnderWay = 0;
            public int $callsOnAConnectionNotOpen = 0;
        };
        $recorder = new LifecycleRecorder();
        $record = $recorder->connectionListener();
        $listener = static function (ConnectionEvent $event, ?\Throwable $error = null) use ($record, $flapping, $transport): void {
            $record($event, $error);
            $connection = $flapping->connection;
            if ($event !== ConnectionEvent::Reconnected || $connection === null) {
                return;
            }

            if ($connection->state() !== ConnectionState::Open) {
                $flapping->callsOnAConnectionNotOpen++;
            }

            if ($flapping->flapsLeft === 0) {
                return;
            }

            $flapping->flapsLeft--;
            $flapping->callsUnderWay++;
            $flapping->mostCallsUnderWay = max($flapping->mostCallsUnderWay, $flapping->callsUnderWay);
            try {
                $transport->dropConnection();
                $connection->processIncoming(new TimeoutCancellation(0.3))->await();
            } finally {
                $flapping->callsUnderWay--;
            }
        };
        $connection = $this->connect($transport, connectionListener: $listener);
        $flapping->connection = $connection;

        $this->loseConnectionAndReadThroughTheReconnect($connection, $transport);
        $flapsLeftWhenTheReadReturned = $flapping->flapsLeft;
        $this->waitUntil(static fn(): bool => $flapping->flapsLeft === 0 && $flapping->callsUnderWay === 0 && $connection->state() === ConnectionState::Open);

        self::assertGreaterThan(0, $flapsLeftWhenTheReadReturned, 'the first read returned while the server was still flapping');
        self::assertLessThanOrEqual(2, $flapping->mostCallsUnderWay, 'the calls do not pile up');
        self::assertSame(0, $flapping->callsOnAConnectionNotOpen);
        self::assertSame(201, $transport->epoch());
        self::assertTrue($transport->sessionLive());
        self::assertSame(
            array_merge([ConnectionEvent::Connected], ...array_fill(0, 201, [ConnectionEvent::Disconnected, ConnectionEvent::Reconnected])),
            $recorder->events,
        );
    }

    /**
     * The listener's read finds the new connection gone and reconnects again, and the next server sends a message
     * right after the client re-subscribes: that reconnect's Reconnected is announced from the event loop, as a
     * Reconnected listener call is running, and the message it read reaches its handler first - the read goes on
     * at once, its delivery included. Pinned as it is; announced from the read, the Reconnected would come first.
     */
    public function testMessageANestedReconnectReadReachesItsHandlerBeforeItsReconnected(): void
    {
        $transport = new ReconnectingTransport();
        $order = new class {
            /** @var list<string> */
            public array $entries = [];
            public ?NatsConnection $connection = null;
            public bool $armed = true;
        };
        $listener = static function (ConnectionEvent $event) use ($order, $transport): void {
            $order->entries[] = $event->name . ' on ' . $transport->epoch();
            $connection = $order->connection;
            if ($event !== ConnectionEvent::Reconnected || !$order->armed || $connection === null) {
                return;
            }

            $order->armed = false;
            $transport->dropConnection();
            $connection->processIncoming(new TimeoutCancellation(0.5))->await();
            $order->entries[] = 'the listener read returned';
        };
        $connection = $this->connect($transport, connectionListener: $listener);
        $order->connection = $connection;
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use ($order, $transport): void {
            $order->entries[] = $message->payload . ' on ' . $transport->epoch();
        })->await();
        $transport->afterWrite = static function (string $bytes) use ($transport, $sid): void {
            if ($transport->epoch() === 2 && str_contains($bytes, 'SUB updates')) {
                $transport->pushFrame(ReconnectingTransport::msgFrame('updates', $sid, 'm'));
            }
        };

        $this->loseConnectionAndReadThroughTheReconnect($connection, $transport);
        $this->waitUntil(static fn(): bool => count($order->entries) >= 7);

        self::assertSame(
            ['Connected on 0', 'Disconnected on 0', 'Reconnected on 1', 'Disconnected on 1', 'm on 2', 'Reconnected on 2', 'the listener read returned'],
            $order->entries,
        );
    }

    /** @return iterable<string, array{string}> */
    public static function closesBeforeTheListenerHearsOfTheOpen(): iterable
    {
        yield 'closed' => ['closed'];
        // A TLS or WebSocket close takes a while: the connection is still Open, but being closed.
        yield 'being closed' => ['being closed'];
        // A supervisor reconnecting on Closed: the connection is Open again, but it is another one.
        yield 'closed and reopened' => ['closed and reopened'];
    }

    /**
     * The connection is closed while the logger records the Reconnected - a logger writing to a slow stream: the
     * listener is not told of that open, whether the connection is Closed by the time the logger returns, still
     * being closed, or reopened since by a supervisor. The Closed is the last it hears of that connection. It
     * used to be called with the Reconnected all the same, for a connection no longer open: after the Closed, or
     * just before it while the close was still under way.
     */
    #[DataProvider('closesBeforeTheListenerHearsOfTheOpen')]
    public function testReconnectedIsNotAnnouncedForAConnectionClosedBeforeTheListenerHearsOfIt(string $close): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $closing = new class {
            public ?NatsConnection $connection = null;
            /** @var ?Future<void> */
            public ?Future $close = null;
        };
        $logger = new SlowLogger('NATS connection Reconnected', 0.05, static function () use ($closing, $transport, $close): void {
            $connection = $closing->connection;
            if ($connection === null) {
                return;
            }

            if ($close === 'being closed') {
                $transport->closeDelay = 0.2;
            }

            $closing->close = async(static function () use ($connection, $close): void {
                $connection->disconnect()->await();
                if ($close === 'closed and reopened') {
                    $connection->connect()->await();
                }
            });
        });
        $connection = $this->connectWithLogger($transport, $recorder, $logger);
        $closing->connection = $connection;

        $reader = $this->startRecoveryInBackground($connection, $transport);
        $transport->acceptDials();
        $reader->await(new TimeoutCancellation(3));
        self::assertNotNull($closing->close);
        $closing->close->await(new TimeoutCancellation(3));

        $closed = [ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Closed];
        self::assertSame($close === 'closed and reopened' ? [...$closed, ConnectionEvent::Connected] : $closed, $recorder->events);
        self::assertContains(ConnectionEvent::Reconnected, $logger->lifecycleEvents(), 'the log records the open');
    }

    /**
     * The new connection is lost while the logger records its Reconnected, and an operation that was waiting for
     * the reconnect reconnects again: the listener hears of neither that connection nor its loss, only of the
     * next connection - one Disconnected, one Reconnected. It used to hear Reconnected for the dead connection,
     * then Disconnected and Reconnected again.
     */
    public function testConnectionLostBeforeTheListenerHearsOfItIsNotAnnounced(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $logger = new SlowLogger('NATS connection Reconnected', 0.05, static function () use ($transport): void {
            $transport->dropConnection();
        });
        $connection = $this->connectWithLogger($transport, $recorder, $logger);

        $reader = $this->startRecoveryInBackground($connection, $transport);
        $waiter = async(static fn(): int => $connection->processIncoming(new TimeoutCancellation(2))->await());
        delay(0.01);
        $transport->acceptDials();
        $waiter->await(new TimeoutCancellation(3));
        $reader->await(new TimeoutCancellation(3));

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(2, $transport->epoch());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected], $recorder->events);
        self::assertSame(self::TWO_RECONNECTS, $logger->lifecycleEvents(), 'the log records every transition');
    }

    /**
     * The same for a failed initial connect that a reconnect completes: the connection is lost while the logger
     * records its Connected, and the listener hears Connected once, for the next connection. It used to hear
     * Connected for the dead connection, then Disconnected and Reconnected.
     */
    public function testInitialConnectLostBeforeTheListenerHearsOfItIsAnnouncedAsConnectedOnce(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $logger = new SlowLogger('NATS connection Connected', 0.05, static function () use ($transport): void {
            $transport->dropConnection();
        });
        $connection = $this->connectWithLogger($transport, $recorder, $logger, connect: false);
        $transport->refuseDials();

        $connect = $connection->connect();
        delay(0.02);
        $waiter = async(static fn(): int => $connection->processIncoming(new TimeoutCancellation(2))->await());
        delay(0.01);
        $transport->acceptDials();
        $connect->await(new TimeoutCancellation(3));
        $waiter->await(new TimeoutCancellation(3));

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $transport->epoch());
        self::assertSame([ConnectionEvent::Connected], $recorder->events);
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Connected], $logger->lifecycleEvents());
    }

    /**
     * With no connection listener, the new connection is lost while a slow logger records its Reconnected, and
     * nothing reads any more: the heartbeat finds the connection dead and reconnects. The open is logged once the
     * reconnect is over, like the listener is told of it. Logged from inside the reconnect, as it used to be, it
     * kept the reconnect in flight: the heartbeat cancelled its own timer and joined it, and the connection stayed
     * Open on the dead socket with nothing left to notice.
     */
    public function testHeartbeatRecoversAConnectionLostWhileTheLoggerRecordsItsReconnected(): void
    {
        $transport = new ReconnectingTransport();
        $logger = new SlowLogger('NATS connection Reconnected', 0.2, static function () use ($transport): void {
            $transport->dropConnection();
        });
        $connection = $this->connectWithLogger($transport, null, $logger, pingIntervalSeconds: 0.05, maxPingsOut: 1);

        $this->loseConnectionAndReadThroughTheReconnect($connection, $transport);
        $this->waitUntil(static fn(): bool => $transport->epoch() === 2 && $connection->state() === ConnectionState::Open, 1.5);

        self::assertTrue($transport->sessionLive());
    }

    /**
     * Lets the server drop the live connection and reads: the read meets EOF and runs the reconnect, whose
     * listener runs the action under test. Returns once that read has returned, which it used to do for some
     * of the cases here only after their timeouts, or never.
     */
    private function loseConnectionAndReadThroughTheReconnect(NatsConnection $connection, ReconnectingTransport $transport): void
    {
        $transport->dropConnection();
        try {
            $connection->processIncoming()->await(new TimeoutCancellation(2));
        } catch (CancelledException) {
            self::fail('the read that ran the reconnect did not return: the listener is stuck');
        }
    }

    private function assertReconnectedAgain(NatsConnection $connection, ReconnectingTransport $transport): void
    {
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertTrue($transport->sessionLive(), 'not left Open on the dead socket');
        self::assertSame(2, $transport->epoch(), 'reconnected again');
    }

    /**
     * Fails the next publish's write and holds the dial of the reconnect the publish then runs; calls drain() while
     * that reconnect is in flight, and lets the dial through. Returns the publish and the drain.
     *
     * @return array{Future<void>, Future<void>}
     */
    private function publishWhoseReconnectADrainWaitsFor(NatsConnection $connection, ReconnectingTransport $transport): array
    {
        $transport->failNextWriteContaining('PUB orders');
        $transport->holdNextDial();
        $publish = $connection->publish('orders', 'payload');
        $publish->ignore();
        $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Connecting);
        $drain = $connection->drain();
        $drain->ignore();
        delay(0.01);
        $transport->releaseDial();

        return [$publish, $drain];
    }

    /**
     * What the publish failed with; fails the test when it succeeds.
     *
     * @param Future<void> $publish
     */
    private function publishFailure(Future $publish): \Throwable
    {
        try {
            $publish->await(new TimeoutCancellation(3));
        } catch (\Throwable $e) {
            return $e;
        }

        self::fail('expected the publish to fail');
    }

    /** How long the listener's action took; fails when it never returned. */
    private static function secondsTaken(ListenerAction $action): float
    {
        self::assertNotNull($action->seconds, 'the listener action returned');

        return $action->seconds;
    }

    /** @param \Closure(ConnectionEvent, ?\Throwable): void $listener */
    private function unconnected(ReconnectingTransport $transport, \Closure $listener): NatsConnection
    {
        $connection = $this->own(new NatsConnection($this->options(true, 2_000, 1_000, 0, 2, $listener, 5, 20, null), $transport));
        $this->opened[] = $connection;

        return $connection;
    }

    /** Connects with $logger, and with $recorder as the connection listener - none when it is null. */
    private function connectWithLogger(
        ReconnectingTransport $transport,
        ?LifecycleRecorder $recorder,
        LoggerInterface $logger,
        bool $connect = true,
        int|float $pingIntervalSeconds = 0,
        int $maxPingsOut = 2,
    ): NatsConnection {
        $connection = $this->own(new NatsConnection(
            new NatsOptions(
                connectTimeoutMs: 500,
                requestTimeoutMs: 2_000,
                reconnectEnabled: true,
                maxReconnectAttempts: 1_000,
                reconnectDelayMs: 5,
                reconnectMaxDelayMs: 20,
                reconnectJitterMs: 0,
                pingIntervalSeconds: $pingIntervalSeconds,
                maxPingsOut: $maxPingsOut,
                connectionListener: $recorder?->connectionListener(),
                logger: $logger,
            ),
            $transport,
        ));
        $this->opened[] = $connection;
        if ($connect) {
            $connection->connect()->await();
        }

        return $connection;
    }

    /**
     * Connects with a listener that, on the first Reconnected, lets $leaveInFlight start the next reconnect and
     * returns with it in flight. The action's result is the state the listener left the connection in.
     *
     * @param \Closure(NatsConnection, ReconnectingTransport): void $leaveInFlight
     * @return array{NatsConnection, ListenerAction}
     */
    private function connectWithReconnectedAction(
        ReconnectingTransport $transport,
        \Closure $leaveInFlight,
        ?LifecycleRecorder $recorder = null,
    ): array {
        $action = new ListenerAction($recorder ?? new LifecycleRecorder(), static function (NatsConnection $connection) use ($transport, $leaveInFlight): ConnectionState {
            $leaveInFlight($connection, $transport);

            return $connection->state();
        });
        $connection = $this->connect($transport, connectionListener: $action->listener());
        $action->connection = $connection;

        return [$connection, $action];
    }

    private static function assertListenerLeftAReconnectInFlight(ListenerAction $action): void
    {
        self::assertNull($action->failure);
        self::assertSame(ConnectionState::Connecting, $action->result, 'the listener returned with the next reconnect in flight');
    }

    /** A lame-duck INFO naming another server starts a failover, whose dial is held. */
    private function startFailover(NatsConnection $connection, ReconnectingTransport $transport): void
    {
        $transport->holdNextDial();
        $transport->pushFrame(self::LAME_DUCK);
        async(static fn(): int => $connection->processIncoming(new TimeoutCancellation(3))->await())->ignore();
        $this->waitUntil(static fn(): bool => $transport->epoch() === 1 && !$transport->sessionLive());
    }

    /** The new connection dies with dials refused: a reader starts the next reconnect, which backs off. */
    private function startReconnectBetweenRefusedDials(NatsConnection $connection, ReconnectingTransport $transport): void
    {
        $transport->refuseDials();
        $transport->dropConnection();
        async(static fn(): int => $connection->processIncoming(new TimeoutCancellation(3))->await())->ignore();
        $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Connecting);
    }

    /** The new connection dies: a reader starts the next reconnect, which dials and waits for the greeting. */
    private function startReconnectAwaitingTheGreeting(NatsConnection $connection, ReconnectingTransport $transport): void
    {
        $transport->holdNextGreeting();
        $transport->dropConnection();
        async(static fn(): int => $connection->processIncoming(new TimeoutCancellation(3))->await())->ignore();
        $this->waitUntil(static fn(): bool => $transport->greetingHeld());
    }
}
