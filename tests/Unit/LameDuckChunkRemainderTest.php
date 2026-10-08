<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\KeepsEventLoopAlive;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ListenerAction;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\async;

/**
 * The rest of the chunk a lame-duck INFO came in (#182). The INFO's dispatch fails the connection over (#47), inline
 * for your own read, which most tests here read with: the dispatching fiber comes back from it with the connection on
 * the second server, or ended when none could be reached, and the chunk's remaining frames are the first server's.
 * Those frames used to be handled as if nothing had happened, on the connection that replaced the one they were read
 * on: a PING answered with a PONG on the second connection, a stale INFO overwriting the second server's info, a
 * second lame-duck INFO starting a second failover, a PONG confirming the replayed reply inbox before the second
 * server had answered anything, a late reply on that inbox confirming it the same way, an -ERR dropping that inbox or
 * firing a replayed subscription's rejection handler and failing the read although the failover had succeeded. The
 * dispatch now handles the remainder for the connection it was read on: its messages are delivered, without a late
 * reply counting as the new server's confirmation of the replayed inbox, its PING, PONG and INFO are dropped, and its
 * -ERR is reported through the error listener and not applied. The same holds when the connection ended instead: the
 * failover reached no server, or the LameDuck listener closed the connection itself. The read of one of the library's
 * operations, a request's or a pull consumer's here, starts the failover in a fiber of its own instead (#191, see
 * OperationReadLameDuckFailoverTest): the remaining frames are the leaving server's from that moment, a PONG among
 * them still answering the leaving connection's PING until the failover replaces it, with the same outcome here once
 * the failover is over.
 *
 * Over the scripted server in `tests/Support/ReconnectingTransport.php`, whose every dial opens a new session
 * (epoch 0, then 1 after the failover), with the lame-duck INFO naming the second server in `connect_urls`, so that
 * the pool grows to two and the failover dials it; the chunk is pushed as one read. The guards without a failover
 * run over `tests/Support/FakeTransport.php`. Each regression test fails on the code before the fix (how, in its
 * docblock); the guards pass on both.
 */
final class LameDuckChunkRemainderTest extends TestCase
{
    use KeepsEventLoopAlive;
    use ReconnectScenarios;

    /** The first server's lame-duck INFO, naming the second server: the pool grows to two and the connection fails over. */
    private const LAME_DUCK = 'INFO {"server_id":"S1","version":"2.12.0","max_payload":1048576,"ldm":true,"connect_urls":["10.0.0.2:4222"]}' . "\r\n";

    /** A lame-duck INFO naming no other server: the pool stays at one, and nothing fails over. */
    private const LAME_DUCK_ALONE = 'INFO {"server_id":"S1","version":"2.12.0","max_payload":1048576,"ldm":true}' . "\r\n";

    /** An INFO of the first server behind its lame-duck INFO: no lame-duck flag, and a server id of its own. */
    private const STALE_INFO = 'INFO {"server_id":"S1-leaving","version":"2.12.0","max_payload":1048576}' . "\r\n";

    private const PING = "PING\r\n";

    private const PONG = "PONG\r\n";

    private const LIMIT_ERR = "-ERR 'maximum subscriptions exceeded'\r\n";

    /** The second server, as the client dials the one the lame-duck INFO names. */
    private const SECOND_SERVER = 'tcp://10.0.0.2:4222';

    private const PULL = 'PUB $JS.API.CONSUMER.MSG.NEXT.';

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
     * [INFO ldm | PING] read by processIncoming(): the connection is on the second server afterwards, and no PONG was
     * written on it. The dispatch used to answer the PING, the first server's, with a PONG on the second connection.
     */
    public function testAPingBehindTheLameDuckInfoIsNotAnsweredOnTheNewConnection(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);

        $transport->pushFrame(self::LAME_DUCK . self::PING);
        $frames = $connection->processIncoming()->await(new TimeoutCancellation(3));

        self::assertSame(2, $frames);
        $this->assertFailedOver($connection, $transport);
        self::assertSame([], $transport->controlLinesStartingWith('PONG', 1), 'no PONG on the second connection');
        self::assertSame([], $transport->controlLinesStartingWith('PONG'), 'the first server\'s PING was not answered at all');
    }

    /** [PING | INFO ldm]: the PING is answered on the first connection, where it came in, and nothing on the second (guard). */
    public function testAPingAheadOfTheLameDuckInfoIsAnsweredOnTheConnectionItCameIn(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);

        $transport->pushFrame(self::PING . self::LAME_DUCK);
        $frames = $connection->processIncoming()->await(new TimeoutCancellation(3));

        self::assertSame(2, $frames);
        $this->assertFailedOver($connection, $transport);
        self::assertSame(['PONG'], $transport->controlLinesStartingWith('PONG', 0));
        self::assertSame([], $transport->controlLinesStartingWith('PONG', 1));
    }

    /**
     * [INFO ldm | INFO of the first server]: serverInfo() is the second server's afterwards. The stale INFO used to
     * overwrite it with the first server's data (server id "S1-leaving").
     */
    public function testAStaleInfoBehindTheLameDuckInfoDoesNotReplaceTheNewServersInfo(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);

        $transport->pushFrame(self::LAME_DUCK . self::STALE_INFO);
        $connection->processIncoming()->await(new TimeoutCancellation(3));

        $this->assertFailedOver($connection, $transport);
        self::assertSame('S1', $connection->serverInfo()?->serverId, 'the second server\'s INFO stands');
    }

    /**
     * [INFO ldm | INFO ldm]: exactly one failover - one dial of the second server, the connection stays there, one
     * LameDuck event. The second lame-duck INFO used to start a second failover, since the lame-duck flag had been
     * reset for the new connection: a third dial, back to the first server, and a second LameDuck event.
     */
    public function testASecondLameDuckInfoBehindTheFirstStartsNoSecondFailover(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, connectionListener: $recorder->connectionListener());

        $transport->pushFrame(self::LAME_DUCK . self::LAME_DUCK);
        $connection->processIncoming()->await(new TimeoutCancellation(3));

        $this->assertFailedOver($connection, $transport);
        self::assertSame([
            ConnectionEvent::Connected,
            ConnectionEvent::DiscoveredServers,
            ConnectionEvent::LameDuck,
            ConnectionEvent::Disconnected,
            ConnectionEvent::Reconnected,
        ], $recorder->events);
    }

    /**
     * [INFO ldm | PONG] with a reply inbox in place, the second server yet to answer the PING behind the replayed inbox
     * SUB: the replayed inbox is not confirmed by the stale PONG. Shown by the second server answering, in wire order
     * and in one chunk, with a 'maximum subscriptions exceeded' for the SUB and then the PONG for the PING: the -ERR
     * fails the read that brings it and drops the replayed inbox, so that the next request subscribes a new one (two
     * inbox SUBs on the second connection), which the second server confirms, and gets its reply. The stale PONG used
     * to complete the fence slot behind the replayed SUB and confirm the inbox, so the -ERR only failed the read and
     * the inbox stayed: one inbox SUB on the second connection.
     */
    public function testAStalePongBehindTheLameDuckInfoDoesNotConfirmTheReplayedReplyInbox(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = self::echoResponder($transport);
        $connection = $this->connect($transport);
        self::assertSame('ok', $connection->request('svc.echo', 'x', 1_000)->await()->payload);
        // The second server answers the PING behind the replayed SUB only in the chunk pushed below, after its -ERR.
        $transport->answerPings = false;

        $transport->pushFrame(self::LAME_DUCK . self::PONG);
        $frames = $connection->processIncoming()->await(new TimeoutCancellation(3));

        self::assertSame(2, $frames);
        $this->assertFailedOver($connection, $transport);

        // The second server rejects the replayed SUB and then answers the PING behind it: the inbox, unconfirmed, is dropped.
        $transport->pushFrame(self::LIMIT_ERR . self::PONG);
        try {
            $connection->processIncoming()->await(new TimeoutCancellation(3));
            self::fail('expected the read to fail with the second server\'s -ERR');
        } catch (ConnectionException $e) {
            self::assertStringContainsString('maximum subscriptions exceeded', $e->getMessage());
        }

        // The second server confirms the new inbox's SUB by answering the PING behind it.
        $transport->answerPings = true;
        self::assertSame('ok', $connection->request('svc.echo', 'y', 2_000)->await()->payload);
        self::assertCount(2, $transport->controlLinesStartingWith('SUB _INBOX.', 1), 'the dropped inbox was replaced by a new one');
    }

    /**
     * [INFO ldm | MSG on the reply inbox] with a reply inbox in place, the second server yet to answer the PING behind
     * the replayed inbox SUB: a late reply of the first server, for a request no longer waiting for it (the inbox's
     * dispatcher drops it, as it drops any late reply), does not confirm the replayed inbox. Shown as for the stale
     * PONG: the second server rejects the replayed SUB with 'maximum subscriptions exceeded' ahead of its PONG, the
     * -ERR fails the read that brings it and drops the replayed inbox, and the next request subscribes a new one (two
     * inbox SUBs on the second connection). The stale reply used to count as the new server's confirmation, as any
     * delivery on the inbox does, so the -ERR only failed the read and the rejected inbox stayed, every request on it
     * timing out: one inbox SUB on the second connection.
     */
    public function testAStaleReplyOnTheInboxBehindTheLameDuckInfoDoesNotConfirmTheReplayedReplyInbox(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = self::echoResponder($transport);
        $connection = $this->connect($transport);
        self::assertSame('ok', $connection->request('svc.echo', 'x', 1_000)->await()->payload);
        [$inboxSub] = $transport->controlLinesStartingWith('SUB _INBOX.', 0);
        [, $inbox, $inboxSid] = explode(' ', $inboxSub);
        // The second server answers the PING behind the replayed SUB only in the chunk pushed below, after its -ERR.
        $transport->answerPings = false;

        // A late reply on the inbox "<base>.*", with a token no request waits for.
        $transport->pushFrame(self::LAME_DUCK . ReconnectingTransport::msgFrame(substr($inbox, 0, -1) . 'late', (int) $inboxSid, 'late'));
        $frames = $connection->processIncoming()->await(new TimeoutCancellation(3));

        self::assertSame(2, $frames);
        $this->assertFailedOver($connection, $transport);

        // The second server rejects the replayed SUB and then answers the PING behind it: the inbox, unconfirmed, is dropped.
        $transport->pushFrame(self::LIMIT_ERR . self::PONG);
        try {
            $connection->processIncoming()->await(new TimeoutCancellation(3));
            self::fail('expected the read to fail with the second server\'s -ERR');
        } catch (ConnectionException $e) {
            self::assertStringContainsString('maximum subscriptions exceeded', $e->getMessage());
        }

        // The second server confirms the new inbox's SUB by answering the PING behind it.
        $transport->answerPings = true;
        self::assertSame('ok', $connection->request('svc.echo', 'y', 2_000)->await()->payload);
        self::assertCount(2, $transport->controlLinesStartingWith('SUB _INBOX.', 1), 'the dropped inbox was replaced by a new one');
    }

    /**
     * [INFO ldm | -ERR 'maximum subscriptions exceeded'] with a reply inbox in place, the second server yet to answer
     * the PING behind the replayed inbox SUB: the -ERR reaches the error listener as the leaving server's, the read
     * that brought the chunk does not fail, and the replayed inbox is not dropped - once the second server's PONG has
     * confirmed it, a request is sent on it and answered, and no new inbox is subscribed. The stale -ERR used to drop
     * the replayed, still unconfirmed inbox and fail the read with "Server sent error frame" although the failover
     * had succeeded.
     */
    public function testAStaleSubscriptionLimitErrBehindTheLameDuckInfoIsReportedAndLeavesTheReplayedReplyInboxAlone(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = self::echoResponder($transport);
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, errorListener: $recorder->errorListener());
        self::assertSame('ok', $connection->request('svc.echo', 'x', 1_000)->await()->payload);
        [$inboxSub] = $transport->controlLinesStartingWith('SUB _INBOX.', 0);
        // The second server answers the PING behind the replayed SUB only when pushed below: the replayed inbox is
        // unconfirmed when the chunk's remainder is handled.
        $transport->answerPings = false;

        $transport->pushFrame(self::LAME_DUCK . self::LIMIT_ERR);
        $frames = $connection->processIncoming()->await(new TimeoutCancellation(3));

        self::assertSame(2, $frames, 'the read did not fail');
        $this->assertFailedOver($connection, $transport);
        self::assertSame(
            ["The server the connection left sent an error frame: 'maximum subscriptions exceeded'"],
            $recorder->errorsContaining('maximum subscriptions exceeded'),
        );

        // The second server's PONG confirms the replayed inbox: the next request is sent on it.
        $transport->pushFrame(self::PONG);
        self::assertSame(1, $connection->processIncoming()->await(new TimeoutCancellation(3)));
        self::assertSame('ok', $connection->request('svc.echo', 'y', 2_000)->await()->payload);
        self::assertSame([$inboxSub], $transport->controlLinesStartingWith('SUB _INBOX.', 1), 'the replayed inbox was kept');
    }

    /**
     * [INFO ldm | -ERR 'Permissions Violation for Subscription to "<pull inbox>"'] read by the read of a pull consumer
     * run in flight, which starts the failover in a fiber of its own (#191), the -ERR the leaving server's from that
     * moment: the engine does not fail from it, but re-issues its pull on the second server, the -ERR reported, and
     * fails only from the second server's own -ERR when that one rejects the replayed SUB, with the clear permissions
     * error. The stale -ERR used to fire the replayed inbox's rejection handler, and the run failed before the second
     * server had answered anything.
     */
    public function testAStalePermissionsErrBehindTheLameDuckInfoDoesNotFailARunningPullConsumer(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->connectClientReporting($transport, $recorder);
        // Pulls get no answer: the run keeps one in flight, its read parked on the socket until the pull expires.
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(2_000);
        $run = async(static fn(): int => $iterator->handle(static function (): void {})->await());
        $run->ignore();
        $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith(self::PULL, 0) !== []);
        $inbox = self::pullInboxOf($transport);

        $transport->pushFrame(self::LAME_DUCK . self::permissionsErr($inbox));
        // The run's read brings the chunk and fails the connection over; the engine then re-issues its pull there.
        $this->waitUntil(static fn(): bool => $run->isComplete() || $transport->controlLinesStartingWith(self::PULL, 1) !== []);

        self::assertFalse($run->isComplete(), 'the run did not fail from the -ERR of the server the connection left');
        $this->assertFailedOver($client, $transport);
        self::assertSame(
            ['The server the connection left sent an error frame: ' . self::permissionsErrText($inbox)],
            $recorder->errorsContaining('Permissions Violation'),
        );

        // The second server rejects the replayed SUB itself: the run fails from that, with the clear error.
        $transport->pushFrame(self::permissionsErr($inbox));
        try {
            $run->await(new TimeoutCancellation(3));
            self::fail('expected the run to fail from the second server\'s rejection');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('"' . $inbox . '" was rejected by server permissions: ' . self::permissionsErrText($inbox), $e->getMessage());
        }
    }

    /** [INFO ldm | MSG on a user subscription]: the message is delivered after the failover (guard). */
    public function testAMessageBehindTheLameDuckInfoIsDeliveredAfterTheFailover(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $received = [];
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        $transport->pushFrame(self::LAME_DUCK . ReconnectingTransport::msgFrame('updates', $sid, 'm1'));
        $frames = $connection->processIncoming()->await(new TimeoutCancellation(3));

        self::assertSame(2, $frames);
        $this->assertFailedOver($connection, $transport);
        self::assertSame(['m1'], $received);
        self::assertSame(['SUB updates ' . $sid], $transport->controlLinesStartingWith('SUB updates', 1), 'the subscription was replayed');
    }

    /** @return iterable<string, array{bool, string}> */
    public static function connectionsThatDoNotFailOver(): iterable
    {
        yield 'reconnect disabled' => [false, self::LAME_DUCK];
        yield 'a pool of one' => [true, self::LAME_DUCK_ALONE];
    }

    /**
     * No failover - reconnect disabled, or a pool of one - and [INFO ldm | PING | INFO stale] is handled as before:
     * LameDuck is emitted, the PING is answered on the only connection, and the stale INFO applies (guard).
     */
    #[DataProvider('connectionsThatDoNotFailOver')]
    public function testWithoutAFailoverTheRestOfTheChunkIsHandledAsBefore(bool $reconnectEnabled, string $lameDuck): void
    {
        $transport = new FakeTransport([ReconnectingTransport::INFO, self::PONG, $lameDuck . self::PING . self::STALE_INFO]);
        $recorder = new LifecycleRecorder();
        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: $reconnectEnabled,
                maxReconnectAttempts: 1,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
                connectionListener: $recorder->connectionListener(),
            ),
            $transport,
        );
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
     * The failover fails - the second server refuses the dial, the one attempt is used up, the connection ends:
     * the chunk's PING is not answered, so nothing is written on the closed transport, the read returns what it read
     * (the failed failover is reported through the error listener and the Closed event, as before), the state is
     * Closed, and the next read fails with the closed connection. The read used to fail with the error of the PONG
     * written on the closed transport.
     */
    public function testWhenTheFailoverFailsAPingBehindTheLameDuckInfoIsNotAnsweredOnTheClosedConnection(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect(
            $transport,
            maxReconnectAttempts: 1,
            connectionListener: $recorder->connectionListener(),
            errorListener: $recorder->errorListener(),
        );
        $transport->refuseDials();

        $transport->pushFrame(self::LAME_DUCK . self::PING);
        $frames = $connection->processIncoming()->await(new TimeoutCancellation(3));

        self::assertSame(2, $frames);
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertFalse($transport->sessionLive());
        self::assertSame([], $transport->controlLinesStartingWith('PONG'), 'the PING was not answered');
        self::assertCount(2, $transport->connectCalls, 'one dial of the second server, refused');
        self::assertStringStartsWith(self::SECOND_SERVER, $transport->connectCalls[1]);
        self::assertSame(1, $recorder->closedEvents());
        self::assertCount(1, $recorder->errorsContaining('Reconnect attempts exhausted'));
        try {
            $connection->processIncoming()->await(new TimeoutCancellation(3));
            self::fail('expected the next read to fail with the closed connection');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }
    }

    /**
     * The LameDuck listener closes the connection itself, a disconnect() awaited from the listener: no failover runs,
     * and the close count alone moves, so [INFO ldm | PING | MSG on a user subscription] is the closed connection's.
     * The PING is not answered on the closed transport, the message goes with the connection, as what a disconnect()
     * discards does, the read returns what it read, and the connection is Closed with one Closed event. The PONG
     * written on the closed transport used to fail the read with "The connection is gone". The one staging that
     * exercises the close-count half of the dispatch's check: every failover, a refused one included, moves the
     * connection generation as well.
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

    /**
     * The same [INFO ldm | PING] chunk read by a request's own read, which starts the failover in a fiber of its own
     * (#191), the PING the leaving server's from that moment: no PONG on either connection. The request, whose reply
     * never comes on either server, times out on its own, the connection on the second server by then (it times out
     * while the failover still dials in OperationReadLameDuckFailoverTest). The request's read used to answer the PING
     * on the second connection.
     */
    public function testAPingBehindTheLameDuckInfoReadByARequestIsNotAnsweredOnTheNewConnection(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = self::echoResponder($transport);
        $connection = $this->connect($transport);
        self::assertSame('ok', $connection->request('svc.echo', 'x', 1_000)->await()->payload);
        // No answer to the next request: its own read is the one on the socket, and the chunk is what it reads.
        $transport->responder = null;

        $transport->pushFrame(self::LAME_DUCK . self::PING);
        try {
            $connection->request('svc.echo', 'y', 500)->await();
            self::fail('expected the request to time out: its reply comes on neither server');
        } catch (TimeoutException) {
            // Its own timeout, as expected.
        }

        $this->assertFailedOver($connection, $transport);
        self::assertSame([], $transport->controlLinesStartingWith('PONG', 1), 'no PONG on the second connection');
        self::assertSame([], $transport->controlLinesStartingWith('PONG'));
    }

    /**
     * The same chunk read by the heartbeat tick's read of its PONG, nothing else reading: no PONG on the second
     * connection. The heartbeat's read used to answer the PING there.
     */
    public function testAPingBehindTheLameDuckInfoReadByTheHeartbeatIsNotAnsweredOnTheNewConnection(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, pingIntervalSeconds: 0.05, maxPingsOut: 2);

        // Pushed between two ticks: the next tick's read of its PONG brings the chunk ahead of the PONG.
        $transport->pushFrame(self::LAME_DUCK . self::PING);
        $this->waitUntil(static fn(): bool => $transport->epoch() === 1 && $connection->state() === ConnectionState::Open);

        $this->assertFailedOver($connection, $transport);
        self::assertNotSame([], $transport->controlLinesStartingWith('PING', 0), 'the heartbeat pinged the first server');
        self::assertSame([], $transport->controlLinesStartingWith('PONG', 1), 'no PONG on the second connection');
        self::assertSame([], $transport->controlLinesStartingWith('PONG'));
    }

    /** The connection failed over once, to the second server, and is open there. */
    private function assertFailedOver(NatsConnection|NatsClient $connection, ReconnectingTransport $transport): void
    {
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertTrue($transport->sessionLive());
        self::assertSame(1, $transport->epoch(), 'one failover');
        self::assertCount(2, $transport->connectCalls, 'one dial, of the second server');
        self::assertStringStartsWith(self::SECOND_SERVER, $transport->connectCalls[1]);
    }

    /** Connects a client that reports its errors to $recorder. */
    private function connectClientReporting(ReconnectingTransport $transport, LifecycleRecorder $recorder): NatsClient
    {
        $client = new NatsClient($this->options(true, 2_000, 1_000, 0, 2, null, 5, 20, $recorder->errorListener()), $transport);
        $this->opened[] = $client;
        $client->connect()->await();

        return $client;
    }

    /**
     * Answers every request with "ok" on the reply inbox the client subscribed on the live session.
     *
     * @return \Closure(string, ?string, string): list<string>
     */
    private static function echoResponder(ReconnectingTransport $transport): \Closure
    {
        return static fn(string $subject, ?string $replyTo, string $payload): array => $replyTo === null ? [] : $transport->replyFrame($replyTo, 'ok');
    }

    /** The subject of the pull consumer's reply inbox SUB, "_INBOX.JS.PULL.<base>.*". */
    private static function pullInboxOf(ReconnectingTransport $transport): string
    {
        [$sub] = $transport->controlLinesStartingWith('SUB _INBOX.JS.PULL.', 0);

        return explode(' ', $sub)[1];
    }

    private static function permissionsErrText(string $subject): string
    {
        return "'Permissions Violation for Subscription to \"" . $subject . "\"'";
    }

    private static function permissionsErr(string $subject): string
    {
        return '-ERR ' . self::permissionsErrText($subject) . "\r\n";
    }
}
