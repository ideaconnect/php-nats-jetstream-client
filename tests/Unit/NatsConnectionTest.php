<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use IDCT\NATS\Connection\ConnectionStats;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use Amp\Socket\ClientTlsContext;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\NatsException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\FixedNonceSigner;
use IDCT\NATS\Tests\Support\FlakyTransport;
use IDCT\NATS\Transport\TransportClosedException;
use IDCT\NATS\Transport\TransportInterface;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\delay;

final class NatsConnectionTest extends TestCase
{
    /**
     * Verifies a successful handshake transitions state to open and sends CONNECT/PING.
     */
    public function testConnectTransitionsToOpenAndSendsConnectAndPing(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            options: new NatsOptions(servers: ['nats://127.0.0.1:4222'], name: 'unit-test-client'),
            transport: $transport,
        );

        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        $serverInfo = $connection->serverInfo();
        self::assertNotNull($serverInfo);
        self::assertSame('S1', $serverInfo->serverId);
        self::assertSame('tcp://127.0.0.1:4222|5000', $transport->connectCalls[0]);
        self::assertCount(2, $transport->writes);
        self::assertStringStartsWith('CONNECT ', $transport->writes[0]);
        self::assertSame("PING\r\n", $transport->writes[1]);
    }

    /**
     * Verifies handshake accepts +OK and responds to server PING before final PONG.
     */
    public function testConnectHandlesOkAndPingBeforePong(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "+OK\r\n",
            "PING\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame("PONG\r\n", $transport->writes[2]);
    }

    /**
     * Verifies the handshake reassembles an INFO frame fragmented across TCP segments (regression for
     * issue #2). A long INFO (carrying an `xkey`, NATS 2.10+) can be split mid-frame over a real
     * network; the tail segment, if parsed on its own, looks like a bogus control line (e.g.
     * `...VDGU3DPK"}`). The parser must buffer the partial frame and only act on the reassembled whole,
     * rather than parsing the raw tail and throwing "Unsupported control frame".
     */
    public function testConnectReassemblesFragmentedInfoFrame(): void
    {
        $info = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,'
            . '"max_payload":1048576,"headers":true,"xkey":"XKEYVDGU3DPKVDGU3DPKVDGU3DPKVDGU3DPK"}' . "\r\n";
        $split = intdiv(strlen($info), 2);

        $transport = new FakeTransport([
            substr($info, 0, $split),  // first segment: incomplete INFO - the parser must buffer it
            substr($info, $split),     // tail segment completes the frame
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        $serverInfo = $connection->serverInfo();
        self::assertNotNull($serverInfo);
        self::assertSame('S1', $serverInfo->serverId);
    }

    /**
     * Verifies that an unknown control-line op during handshake raises a ConnectionException
     * wrapping the parser's ProtocolException.
     *
     * When the parser receives a CRLF-terminated line it does not recognise, it throws a
     * ProtocolException which the connection layer wraps in a ConnectionException.
     */
    public function testConnectFailsOnUnknownControlLineDuringHandshake(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "UNKNOWN\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Unsupported control frame: UNKNOWN');

        try {
            $connection->connect()->await();
        } finally {
            self::assertSame(ConnectionState::Closed, $connection->state());
        }
    }

    /**
     * Verifies that an incomplete chunk without CRLF exhausts the poll budget and times out.
     *
     * When the transport delivers a partial line that never terminates, the parser buffers it
     * indefinitely and the handshake loop runs out of polls, resulting in a ConnectionException.
     */
    public function testConnectFailsWhenNoPongAndMovesToClosed(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            'INCOMPLETE_NO_CRLF',
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Expected PONG after CONNECT');

        try {
            $connection->connect()->await();
        } finally {
            self::assertSame(ConnectionState::Closed, $connection->state());
        }
    }

    /**
     * Verifies handshake fails fast when server emits an error line.
     */
    public function testConnectFailsOnServerErrLine(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "-ERR Maximum Connections Exceeded\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Server error during connect');

        $connection->connect()->await();
    }

    /**
     * Verifies an authorization error during connect raises AuthenticationException and is NOT retried (#46).
     */
    public function testConnectAuthErrorThrowsAuthenticationExceptionWithoutRetry(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "-ERR Authorization Violation\r\n",
        ]);

        // Reconnect is ENABLED, yet an auth error must fail fast (a single connect attempt).
        $connection = new NatsConnection(
            new NatsOptions(reconnectEnabled: true, maxReconnectAttempts: 5),
            $transport,
        );

        try {
            $connection->connect()->await();
            self::fail('Expected AuthenticationException');
        } catch (\IDCT\NATS\Exception\AuthenticationException $e) {
            self::assertStringContainsString('authentication', strtolower($e->getMessage()));
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        // Exactly one connect attempt was made (CONNECT written once), not maxReconnectAttempts.
        self::assertSame(1, count(array_filter($transport->writes, static fn(string $w): bool => str_starts_with($w, 'CONNECT '))));
    }

    /**
     * Verifies JWT auth signs the server nonce from INFO in CONNECT payload.
     */
    public function testConnectIncludesJwtSignatureFromInfoNonce(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true,"nonce":"n-123"}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(jwt: 'jwt-token', nkey: 'UABC123', nonceSigner: new FixedNonceSigner('sig:')),
            $transport,
        );

        $connection->connect()->await();

        self::assertStringContainsString('"jwt":"jwt-token"', $transport->writes[0]);
        self::assertStringContainsString('"sig":"sig:n-123"', $transport->writes[0]);
        self::assertStringContainsString('"nkey":"UABC123"', $transport->writes[0]);
    }

    /**
     * Verifies publishing is rejected when connection is not open.
     */
    public function testPublishRequiresOpenConnection(): void
    {
        $transport = new FakeTransport();
        $connection = new NatsConnection(new NatsOptions(), $transport);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Connection is not open');

        $connection->publish('a.b', 'payload')->await();
    }

    /**
     * Verifies disconnect closes transport and updates state.
     */
    public function testDisconnectClosesTransportAndState(): void
    {
        $transport = new FakeTransport();
        $connection = new NatsConnection(new NatsOptions(), $transport);

        $connection->disconnect()->await();

        self::assertTrue($transport->closed);
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * Verifies subscribe/unsubscribe commands are emitted with expected SID.
     */
    public function testSubscribeAndUnsubscribeSendProtocolCommands(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $sid = $connection->subscribe('orders.created', static function (NatsMessage $message): void {})->await();

        $connection->unsubscribe($sid)->await();

        self::assertSame(1, $sid);
        self::assertSame("SUB orders.created 1\r\n", $transport->writes[2]);
        self::assertSame("UNSUB 1\r\n", $transport->writes[3]);
    }

    public function testUnsubscribeWithMaxDeliversRemainingMessagesThenRemoves(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 2\r\nm1\r\n",
            "MSG updates 1 2\r\nm2\r\n",
            "MSG updates 1 2\r\nm3\r\n",
            "MSG updates 1 2\r\nm4\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $received = [];
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        $connection->processIncoming()->await();
        self::assertSame(['m1'], $received);

        // Auto-unsubscribe at 3 TOTAL messages: m1 already counts, so m2 and m3 must still reach the
        // handler instead of being silently discarded; m4 is past the max (#112).
        $connection->unsubscribe($sid, 3)->await();
        self::assertContains("UNSUB 1 3\r\n", $transport->writes);

        for ($i = 0; $i < 3; $i++) {
            $connection->processIncoming()->await();
        }

        self::assertSame(['m1', 'm2', 'm3'], $received);
    }

    public function testUnsubscribeWithMaxAlreadyReachedRemovesImmediately(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 2\r\nm1\r\n",
            "MSG updates 1 2\r\nm2\r\n",
            "MSG updates 1 2\r\nm3\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $received = [];
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        $connection->processIncoming()->await();
        $connection->processIncoming()->await();
        self::assertSame(['m1', 'm2'], $received);

        // Two messages were already delivered, satisfying max=2: the subscription is removed at once
        // (the UNSUB still carries the max for the server's own accounting) and later frames for the
        // sid are discarded.
        $connection->unsubscribe($sid, 2)->await();
        self::assertContains("UNSUB 1 2\r\n", $transport->writes);

        $connection->processIncoming()->await();
        self::assertSame(['m1', 'm2'], $received);
    }

    public function testReconnectReplaysRemainingAutoUnsubscribeAllowance(): void
    {
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    "MSG updates 1 2\r\nm1\r\n",
                    '__THROW__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    "MSG updates 1 2\r\nm2\r\n",
                    "MSG updates 1 2\r\nm3\r\n",
                    "MSG updates 1 2\r\nm4\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $options = new NatsOptions(
            reconnectEnabled: true,
            maxReconnectAttempts: 3,
            reconnectDelayMs: 1,
            reconnectJitterMs: 0,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $received = [];
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        $connection->processIncoming()->await();
        self::assertSame(['m1'], $received);

        $connection->unsubscribe($sid, 3)->await();
        self::assertContains("UNSUB 1 3\r\n", $transport->writes);

        // The read failure triggers recovery; the replayed SUB resets the server's per-sid count, so
        // auto-unsubscribe must be re-armed with the REMAINING allowance (3 total - 1 delivered = 2),
        // mirroring nats.go resendSubscriptions (#112). The replay is coalesced into a single
        // transport write (#137), so the SUB and its UNSUB re-arm arrive as one buffer entry.
        $connection->processIncoming()->await();
        self::assertContains("SUB updates 1\r\nUNSUB 1 2\r\n", $transport->writes);

        for ($i = 0; $i < 3; $i++) {
            $connection->processIncoming()->await();
        }

        self::assertSame(['m1', 'm2', 'm3'], $received);
    }

    public function testReconnectCoalescesResubscribeReplayIntoSingleWrite(): void
    {
        // The resubscribe replay must be O(1) transport writes regardless of subscription count:
        // the per-sid awaited write + ~5ms drain poll made reconnect latency scale at ~5ms x N
        // inside the reconnect critical section (#137). Three subscriptions, one with a #112
        // remaining allowance armed, must arrive as ONE write containing every SUB (+UNSUB
        // re-arm) frame in registration order - and the single post-replay drain must still
        // detect a prompt fatal -ERR (attempt 1 fails, attempt 2 recovers).
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    "MSG updates 2 2\r\nm1\r\n",
                    "MSG updates 2 2\r\nm2\r\n",
                    '__THROW__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    // Delivered by the drain right after the coalesced replay write: fatal, so
                    // this reconnect attempt must fail and the loop must retry on S3.
                    "-ERR 'Authorization Violation'\r\n",
                ],
                [
                    'INFO {"server_id":"S3","server_name":"n3","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $options = new NatsOptions(
            reconnectEnabled: true,
            maxReconnectAttempts: 3,
            reconnectDelayMs: 1,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $received = [];
        $handler = static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        };
        $connection->subscribe('orders', $handler)->await();   // sid 1
        $sid = $connection->subscribe('updates', $handler)->await(); // sid 2
        $connection->subscribe('metrics', $handler)->await();  // sid 3

        // Arm auto-unsubscribe on sid 2 with max 5; two deliveries below leave a remaining
        // allowance of 3 for the replay to re-arm (#112).
        $connection->unsubscribe($sid, 5)->await();

        $connection->processIncoming()->await();
        $connection->processIncoming()->await();
        self::assertSame(['m1', 'm2'], $received);

        // Read failure -> recovery: attempt 1 (S2) replays, drain hits the fatal -ERR, attempt 2
        // (S3) replays again and succeeds.
        $connection->processIncoming()->await();
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(3, $transport->connectCalls);

        $expectedReplay = "SUB orders 1\r\nSUB updates 2\r\nUNSUB 2 3\r\nSUB metrics 3\r\n";

        // Exactly 12 writes total pins each replay to ONE transport write:
        // conn 1: CONNECT, PING, SUB x3, UNSUB 2 5 (0-5)
        // conn 2: CONNECT, PING, coalesced replay (6-8)
        // conn 3: CONNECT, PING, coalesced replay (9-11)
        self::assertCount(12, $transport->writes);
        self::assertSame($expectedReplay, $transport->writes[8]);
        self::assertSame($expectedReplay, $transport->writes[11]);
    }

    public function testSubscribeRollsBackStateWhenSubWriteFails(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 2\r\nxx\r\n",
            "MSG other 2 2\r\nyy\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $firstReceived = [];
        $transport->throwOnWriteContaining = 'SUB updates';

        try {
            $connection->subscribe('updates', static function (NatsMessage $message) use (&$firstReceived): void {
                $firstReceived[] = $message->payload;
            })->await();
            self::fail('subscribe() must rethrow the transport write failure');
        } catch (TransportClosedException) {
            // Expected: the SUB write failed.
        }

        $transport->throwOnWriteContaining = null;

        $otherReceived = [];
        $sid = $connection->subscribe('other', static function (NatsMessage $message) use (&$otherReceived): void {
            $otherReceived[] = $message->payload;
        })->await();

        // The failed attempt consumed sid 1 but rolled its registry entry back (#116): a frame for
        // sid 1 is discarded rather than dispatched to the never-established subscription.
        self::assertSame(2, $sid);

        $connection->processIncoming()->await();
        $connection->processIncoming()->await();

        self::assertSame([], $firstReceived);
        self::assertSame(['yy'], $otherReceived);
    }

    public function testUnsubscribeDropsLocalStateWhenUnsubWriteFails(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 2\r\nm1\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $received = [];
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        $transport->throwOnWriteContaining = 'UNSUB';

        try {
            $connection->unsubscribe($sid)->await();
            self::fail('unsubscribe() must rethrow the transport write failure');
        } catch (TransportClosedException) {
            // Expected: the UNSUB write failed.
        }

        // Local state must be gone regardless (#116): the entry would otherwise leak forever and
        // resubscribeAll() would revive it as a ghost subscription after the next recovery.
        $connection->processIncoming()->await();
        self::assertSame([], $received);
    }

    public function testAutoUnsubscribeCompletesAndCleansUpEvenWhenSlowConsumerDropsMessages(): void
    {
        // A burst of 4 frames for one sid arrives in a SINGLE chunk while the pending cap is 2 with the
        // default DropOldest policy, so 2 are dropped before the handler ever runs. The server sent all
        // 4 (its UNSUB max), so auto-unsub must still complete and drop the subscription even though the
        // handler only saw 2 - otherwise the sid leaks forever and a reconnect would re-arm a ghost
        // (#112, counting at receive not at delivery).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 2\r\nm1\r\nMSG updates 1 2\r\nm2\r\nMSG updates 1 2\r\nm3\r\nMSG updates 1 2\r\nm4\r\n",
        ]);

        $options = new NatsOptions(
            maxPendingMessagesPerSubscription: 2,
            slowConsumerPolicy: SlowConsumerPolicy::DropOldest,
        );
        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $received = [];
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        // Arm auto-unsub for the full burst of 4 before any message is pumped.
        $connection->unsubscribe($sid, 4)->await();

        $connection->processIncoming()->await();

        // DropOldest kept the two newest; the handler saw fewer than the max, but the subscription is
        // fully torn down because all 4 were RECEIVED (received >= max, backlog drained).
        self::assertSame(['m3', 'm4'], $received);

        $meta = (new \ReflectionProperty($connection, 'subscriptionMeta'))->getValue($connection);
        self::assertArrayNotHasKey($sid, $meta, 'auto-unsub sid must be cleaned up despite slow-consumer drops');
    }

    public function testAutoUnsubscribeCapsHandlerDeliveryAtMaxEvenIfServerOverSends(): void
    {
        // Defense in depth: even if more frames than the max arrive on the sid (a server race, or a
        // replayed read), the handler must never be called more than max times (#112).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 2\r\nm1\r\nMSG updates 1 2\r\nm2\r\nMSG updates 1 2\r\nm3\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $received = [];
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        $connection->unsubscribe($sid, 2)->await();
        $connection->processIncoming()->await();

        self::assertSame(['m1', 'm2'], $received);

        $meta = (new \ReflectionProperty($connection, 'subscriptionMeta'))->getValue($connection);
        self::assertArrayNotHasKey($sid, $meta);
    }

    public function testAutoUnsubscribeOnNonOpenConnectionDefersArmingInsteadOfDestroyingSubscription(): void
    {
        // Regression: the arm form of unsubscribe($sid, $max) must be handled BEFORE the not-open
        // guard, so arming during a reconnect window defers to recovery (resubscribeAll re-arms) rather
        // than destroying the subscription and silently dropping the remaining armed deliveries with the
        // caller believing they will still arrive (#112).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 2\r\nm1\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $received = [];
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();
        $connection->processIncoming()->await();
        self::assertSame(['m1'], $received);

        // Simulate a reconnect in flight: state is not Open when the app arms auto-unsub.
        $stateProp = new \ReflectionProperty($connection, 'state');
        $stateProp->setValue($connection, ConnectionState::Connecting);

        // Must not throw and must not destroy the subscription.
        $connection->unsubscribe($sid, 3)->await();

        $meta = (new \ReflectionProperty($connection, 'subscriptionMeta'))->getValue($connection);
        self::assertArrayHasKey($sid, $meta, 'arming while not open must NOT destroy the subscription');
        $armed = (new \ReflectionProperty($connection, 'autoUnsubMax'))->getValue($connection);
        self::assertSame(3, $armed[$sid] ?? null, 'the auto-unsub max must be armed for recovery to replay');
    }

    public function testAutoUnsubscribeArmWriteFailureKeepsSubscriptionArmedForRecovery(): void
    {
        // If the arming UNSUB write fails, the error propagates but the arm state is kept so the next
        // recovery re-arms it - the subscription must not be silently torn down (#112/#116).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $sid = $connection->subscribe('updates', static function (NatsMessage $message): void {})->await();

        $transport->throwOnWriteContaining = 'UNSUB';

        try {
            $connection->unsubscribe($sid, 3)->await();
            self::fail('the failing UNSUB write must propagate');
        } catch (TransportClosedException) {
            // Expected.
        }

        $meta = (new \ReflectionProperty($connection, 'subscriptionMeta'))->getValue($connection);
        self::assertArrayHasKey($sid, $meta, 'a failed arm write must not tear down the subscription');
        $armed = (new \ReflectionProperty($connection, 'autoUnsubMax'))->getValue($connection);
        self::assertSame(3, $armed[$sid] ?? null);
    }

    public function testPlainUnsubscribeOnClosedConnectionDropsStateWithoutThrowing(): void
    {
        // The #116 not-open drop branch for a KNOWN sid (plain unsubscribe): release state silently.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $sid = $connection->subscribe('updates', static function (NatsMessage $message): void {})->await();

        (new \ReflectionProperty($connection, 'state'))->setValue($connection, ConnectionState::Closed);

        // Must not throw.
        $connection->unsubscribe($sid)->await();

        $meta = (new \ReflectionProperty($connection, 'subscriptionMeta'))->getValue($connection);
        self::assertArrayNotHasKey($sid, $meta, 'plain unsubscribe on a closed connection must drop local state');
    }

    public function testAutoUnsubscribeCleansUpEvenWhenHandlerThrowsOnMaxDelivery(): void
    {
        // A handler that throws on its final (max-th) delivery must still leave the subscription torn
        // down - the terminal cleanup runs in a finally, not after the handler returns (#112).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 2\r\nm1\r\n",
            "MSG updates 1 2\r\nm2\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $count = 0;
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use (&$count): void {
            $count++;
            if ($count === 2) {
                throw new \RuntimeException('handler failed on the max-th message');
            }
        })->await();

        $connection->unsubscribe($sid, 2)->await();

        $connection->processIncoming()->await();
        try {
            $connection->processIncoming()->await();
        } catch (\Throwable) {
            // The handler exception may surface; what matters is the end state below.
        }

        $meta = (new \ReflectionProperty($connection, 'subscriptionMeta'))->getValue($connection);
        self::assertArrayNotHasKey($sid, $meta, 'a throwing max-th handler must not strand the subscription');
    }

    /**
     * Verifies the callback subscribe() API with a queue group emits a `SUB <subject> <queue> <sid>`
     * frame and that a matching MSG is dispatched to the registered handler. This is the exact API the
     * README "Queue Group Subscribe" example uses: subscribe($subject, $handler, queue: 'workers').
     */
    public function testSubscribeWithQueueGroupSendsSubFrameAndDeliversToHandler(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG tasks.process 1 4\r\nwork\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $received = null;
        $sid = $connection->subscribe('tasks.process', static function (NatsMessage $message) use (&$received): void {
            $received = $message;
        }, 'workers')->await();

        self::assertSame(1, $sid);
        self::assertSame("SUB tasks.process workers 1\r\n", $transport->writes[2]);

        $frames = $connection->processIncoming()->await();

        self::assertSame(1, $frames);
        self::assertInstanceOf(NatsMessage::class, $received);
        /** @var NatsMessage $receivedMessage */
        $receivedMessage = $received;
        self::assertSame('tasks.process', $receivedMessage->subject);
        self::assertSame('work', $receivedMessage->payload);
    }

    public function testMalformedAsyncInfoDoesNotTearDownTheReadLoop(): void
    {
        // A malformed async INFO frame must not throw out of processIncoming() and abort delivery of the
        // MSG frames parsed from the same chunk (mirrors the #97 dispatch-containment principle). The bad
        // INFO is skipped; the co-chunked MSG is still delivered and the connection stays open.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // One read carrying a malformed async INFO followed by a valid MSG on a subscribed subject.
            "INFO {not-json\r\nMSG updates 1 5\r\nhello\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $received = null;
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received = $message;
        })->await();

        // Must not throw despite the malformed INFO.
        $connection->processIncoming()->await();

        self::assertInstanceOf(NatsMessage::class, $received);
        /** @var NatsMessage $receivedMessage */
        $receivedMessage = $received;
        self::assertSame('hello', $receivedMessage->payload);
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    public function testLargeInboundMessageReceivedWhenServerAdvertisesLargeMaxPayload(): void
    {
        // #94: the server advertises a 16 MiB max_payload, so a 9 MiB inbound message (larger than the
        // historical fixed 8 MiB inbound bound) must be delivered intact rather than rejected as an
        // oversized frame - which previously threw a ProtocolException and forced a reconnect.
        $payload = str_repeat('A', 9 * 1024 * 1024);
        $frame = sprintf("MSG big.subject 1 %d\r\n%s\r\n", strlen($payload), $payload);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":16777216,"headers":true}' . "\r\n",
            "PONG\r\n",
            $frame,
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $received = null;
        $connection->subscribe('big.subject', static function (NatsMessage $message) use (&$received): void {
            $received = $message;
        })->await();

        $frames = $connection->processIncoming()->await();

        self::assertSame(1, $frames);
        self::assertInstanceOf(NatsMessage::class, $received);
        /** @var NatsMessage $receivedMessage */
        $receivedMessage = $received;
        self::assertSame(9 * 1024 * 1024, strlen($receivedMessage->payload));
        // The connection stayed up: the large frame was accepted, not turned into a reconnect.
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * Verifies MSG frames are dispatched to matching subscription handlers.
     */
    public function testProcessIncomingDispatchesMsgToSubscriber(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 5\r\nhello\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $received = null;
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received = $message;
        })->await();

        $frames = $connection->processIncoming()->await();

        self::assertSame(1, $frames);
        self::assertInstanceOf(NatsMessage::class, $received);
        /** @var NatsMessage $receivedMessage */
        $receivedMessage = $received;
        self::assertSame('updates', $receivedMessage->subject);
        self::assertSame('hello', $receivedMessage->payload);
    }

    /**
     * Verifies a delivered message can reply to its own reply subject via respond() (#17).
     */
    public function testDeliveredMessageCanRespondToReplySubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.echo 1 _INBOX.reply 4\r\nping\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $connection->subscribe('svc.echo', static function (NatsMessage $message): void {
            self::assertTrue($message->isReplyable());
            $message->respond('pong')->await();
        })->await();

        $connection->processIncoming()->await();

        $replies = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_starts_with($w, 'PUB _INBOX.reply '),
        ));
        self::assertCount(1, $replies);
        self::assertSame("PUB _INBOX.reply 4\r\npong\r\n", $replies[0]);
    }

    /**
     * Verifies respond() with headers emits an HPUB to the reply subject (#17).
     */
    public function testDeliveredMessageCanRespondWithHeaders(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.echo 1 _INBOX.reply 4\r\nping\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $connection->subscribe('svc.echo', static function (NatsMessage $message): void {
            $message->respondWithHeaders('pong', ['X-Trace' => 'abc'])->await();
        })->await();

        $connection->processIncoming()->await();

        $replies = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_starts_with($w, 'HPUB _INBOX.reply '),
        ));
        self::assertCount(1, $replies);
        self::assertStringContainsString('X-Trace:abc', $replies[0]);
    }

    /**
     * Verifies respond() throws when the message carries no reply subject (#17).
     */
    public function testRespondThrowsWithoutReplySubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 5\r\nhello\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $caught = null;
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$caught): void {
            self::assertFalse($message->isReplyable());
            try {
                $message->respond('nope')->await();
            } catch (\LogicException $e) {
                $caught = $e;
            }
        })->await();

        $connection->processIncoming()->await();

        self::assertInstanceOf(\LogicException::class, $caught);
        self::assertStringContainsString('no reply subject', $caught->getMessage());
    }

    /**
     * Verifies a message constructed outside the delivery path cannot respond (#17).
     */
    public function testRespondThrowsWhenNotBoundToConnection(): void
    {
        $message = new NatsMessage('svc.echo', 1, '_INBOX.reply', 'ping');

        self::assertFalse($message->isReplyable());
        $this->expectException(\LogicException::class);
        $this->expectExceptionMessage('not bound to a live connection');
        $message->respond('pong')->await();
    }

    /**
     * Verifies the connection listener receives Connected then Closed across the lifecycle (#22).
     */
    public function testConnectionListenerReceivesConnectedAndClosed(): void
    {
        $events = [];
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(connectionListener: static function (ConnectionEvent $e, ?\Throwable $err) use (&$events): void {
                $events[] = $e;
            }),
            $transport,
        );

        $connection->connect()->await();
        $connection->disconnect()->await();

        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Closed], $events);
    }

    /**
     * Verifies an async INFO update emits LameDuck and DiscoveredServers events (#22).
     */
    public function testConnectionListenerReceivesLameDuckAndDiscoveredServers(): void
    {
        $events = [];
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","max_payload":1048576,"headers":true,"ldm":true,"connect_urls":["10.0.0.2:4222"]}' . "\r\n",
        ]);

        $connection = new NatsConnection(
            // reconnectEnabled: false isolates event emission from the lame-duck auto-failover (#47).
            new NatsOptions(reconnectEnabled: false, connectionListener: static function (ConnectionEvent $e) use (&$events): void {
                $events[] = $e;
            }),
            $transport,
        );

        $connection->connect()->await();
        $connection->processIncoming()->await();

        // Discovery is applied before lame-duck (so a failover can use freshly-advertised peers).
        self::assertSame([
            ConnectionEvent::Connected,
            ConnectionEvent::DiscoveredServers,
            ConnectionEvent::LameDuck,
        ], $events);
    }

    /**
     * Verifies the error listener is notified of slow-consumer drops (#23).
     */
    public function testErrorListenerReceivesSlowConsumerDrop(): void
    {
        $errors = [];
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 1\r\nA\r\nMSG updates 1 1\r\nB\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(
                maxPendingMessagesPerSubscription: 1,
                slowConsumerPolicy: SlowConsumerPolicy::DropOldest,
                errorListener: static function (\Throwable $err) use (&$errors): void {
                    $errors[] = $err->getMessage();
                },
            ),
            $transport,
        );
        $connection->connect()->await();
        $connection->subscribe('updates', static function (NatsMessage $message): void {})->await();

        $connection->processIncoming()->await();

        self::assertCount(1, $errors);
        self::assertStringContainsString('Slow consumer', $errors[0]);
    }

    /**
     * Verifies the error listener is notified of recoverable server -ERR frames (#23).
     */
    public function testErrorListenerReceivesRecoverableServerError(): void
    {
        $errors = [];
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "-ERR 'Permissions Violation for Subscription to foo'\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(errorListener: static function (\Throwable $err) use (&$errors): void {
                $errors[] = $err->getMessage();
            }),
            $transport,
        );
        $connection->connect()->await();

        $connection->processIncoming()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(1, $errors);
        self::assertStringContainsString('recoverable error frame', $errors[0]);
    }

    /**
     * Verifies drainSubscription() UNSUBs, flushes, delivers the in-flight message, then drops the sub (#43).
     */
    public function testDrainSubscriptionDeliversInFlightThenRemoves(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // The drain flush reads this chunk: an in-flight message for sid 1, then the flush PONG.
            "MSG updates 1 4\r\nlast\r\nPONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $delivered = [];
        $sid = $connection->subscribe('updates', static function (NatsMessage $m) use (&$delivered): void {
            $delivered[] = $m->payload;
        })->await();

        $connection->drainSubscription($sid)->await();

        self::assertSame(['last'], $delivered);
        $writes = implode('', $transport->writes);
        self::assertStringContainsString('UNSUB ' . $sid . "\r\n", $writes);
        self::assertStringContainsString("PING\r\n", $writes);

        // The subscription is gone: a further frame for that sid is ignored.
        self::assertSame(0, $connection->processIncoming()->await());
    }

    /**
     * Verifies connection accessors + traffic statistics (#52).
     */
    public function testConnectionAccessorsAndStatistics(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG x 1 5\r\nhello\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(servers: ['nats://127.0.0.1:4222']), $transport);
        $connection->connect()->await();

        self::assertSame('nats://127.0.0.1:4222', $connection->connectedUrl());
        self::assertSame(1048576, $connection->maxPayload());

        $connection->subscribe('x', static function (NatsMessage $m): void {})->await();
        $connection->publish('a.b', 'hi')->await();
        $connection->processIncoming()->await();

        $stats = $connection->statistics();
        self::assertInstanceOf(ConnectionStats::class, $stats);
        self::assertSame(1, $stats->outMsgs);
        self::assertSame(2, $stats->outBytes);
        self::assertSame(1, $stats->inMsgs);
        self::assertSame(5, $stats->inBytes);

        $connection->disconnect()->await();
        self::assertNull($connection->connectedUrl());
    }

    /**
     * Verifies rtt() measures a PING/PONG round trip (#52).
     */
    public function testRttMeasuresPingPong(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $rtt = $connection->rtt()->await();

        self::assertGreaterThanOrEqual(0.0, $rtt);
        self::assertLessThan(5.0, $rtt);
    }

    /**
     * Verifies discoveredServers() reflects connect_urls from an async INFO (#47).
     */
    public function testDiscoveredServersFromAsyncInfo(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            'INFO {"server_id":"S1","version":"2.12.0","max_payload":1048576,"connect_urls":["10.0.0.2:4222","10.0.0.3:4222"]}' . "\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        self::assertSame([], $connection->discoveredServers());

        $connection->processIncoming()->await();

        self::assertSame(['10.0.0.2:4222', '10.0.0.3:4222'], $connection->discoveredServers());
    }

    /**
     * Verifies publishes during an in-flight reconnect are buffered and flushed on reconnect (#49).
     */
    public function testPublishBuffersDuringReconnectAndFlushesOnReconnect(): void
    {
        $info = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";
        $release = new DeferredFuture();

        $transport = new class ($info, $release) implements TransportInterface {
            /** @var list<string> */
            public array $writes = [];
            private int $connects = 0;
            /** @var list<list<string>> */
            private array $reads;

            /** @param DeferredFuture<void> $release */
            public function __construct(string $info, private DeferredFuture $release)
            {
                $this->reads = [
                    [$info, "PONG\r\n", '__EOF__'], // connection 0: handshake, then EOF -> reconnect
                    [$info, "PONG\r\n"],            // connection 1: reconnect handshake
                ];
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function (): void {
                    // Hold the reconnect (second connect) open so a publish lands mid-reconnect.
                    if ($this->connects === 1) {
                        $this->release->getFuture()->await();
                    }
                    $this->connects++;
                });
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                });
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $conn = max(0, $this->connects - 1);
                    $next = array_shift($this->reads[$conn]) ?? '';
                    if ($next === '__EOF__') {
                        throw new TransportClosedException('eof');
                    }

                    return $next;
                });
            }

            public function close(): Future
            {
                return async(static fn(): null => null);
            }
        };

        $connection = new NatsConnection(new NatsOptions(reconnectDelayMs: 1, reconnectJitterMs: 0), $transport);
        $connection->connect()->await();

        // Drive the read loop in the background: it reads EOF, starts reconnect, and blocks in connect().
        $pump = async(static fn(): int => $connection->processIncoming()->await());
        delay(0.05); // let the reconnect begin and suspend on the held connect()

        self::assertSame(ConnectionState::Connecting, $connection->state());
        // This publish lands during the reconnect window: it must buffer (not throw).
        $connection->publish('a.b', 'buffered')->await();

        $release->complete();   // let the reconnect finish
        $pump->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertContains("PUB a.b 8\r\nbuffered\r\n", $transport->writes);
        self::assertSame(1, $connection->statistics()->reconnects);
    }

    public function testReconnectBufferFlushesMultiplePublishesInOrderBeforeLivePublishes(): void
    {
        // Hardening (3b): several publishes buffered during a reconnect must be replayed in their exact
        // publish order, as a single ordered block, and BEFORE any publish issued after the reconnect
        // completes - so a reconnect never reorders or interleaves the outbound stream.
        $info = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";
        $release = new DeferredFuture();

        $transport = new class ($info, $release) implements TransportInterface {
            /** @var list<string> */
            public array $writes = [];
            private int $connects = 0;
            /** @var list<list<string>> */
            private array $reads;

            /** @param DeferredFuture<void> $release */
            public function __construct(string $info, private DeferredFuture $release)
            {
                $this->reads = [
                    [$info, "PONG\r\n", '__EOF__'],
                    [$info, "PONG\r\n"],
                ];
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function (): void {
                    if ($this->connects === 1) {
                        $this->release->getFuture()->await();
                    }
                    $this->connects++;
                });
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                });
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $conn = max(0, $this->connects - 1);
                    $next = array_shift($this->reads[$conn]) ?? '';
                    if ($next === '__EOF__') {
                        throw new TransportClosedException('eof');
                    }

                    return $next;
                });
            }

            public function close(): Future
            {
                return async(static fn(): null => null);
            }
        };

        $connection = new NatsConnection(new NatsOptions(reconnectDelayMs: 1, reconnectJitterMs: 0), $transport);
        $connection->connect()->await();

        $pump = async(static fn(): int => $connection->processIncoming()->await());
        delay(0.05); // reconnect begins and suspends on the held connect()
        self::assertSame(ConnectionState::Connecting, $connection->state());

        // Three publishes land during the reconnect window, in order.
        $connection->publish('a.1', 'p1')->await();
        $connection->publish('a.2', 'p2')->await();
        $connection->publish('a.3', 'p3')->await();

        $release->complete();
        $pump->await();
        self::assertSame(ConnectionState::Open, $connection->state());

        // A publish issued after the reconnect completes.
        $connection->publish('a.4', 'p4')->await();

        // The buffered frames are flushed as one ordered block.
        $flushed = "PUB a.1 2\r\np1\r\nPUB a.2 2\r\np2\r\nPUB a.3 2\r\np3\r\n";
        $flushIndex = array_search($flushed, $transport->writes, true);
        self::assertNotFalse($flushIndex, 'buffered publishes were not flushed as one in-order block');

        // ...and that block precedes the post-reconnect live publish.
        $liveIndex = array_search("PUB a.4 2\r\np4\r\n", $transport->writes, true);
        self::assertNotFalse($liveIndex, 'post-reconnect live publish was not written');
        self::assertLessThan($liveIndex, $flushIndex, 'buffered block must flush before later live publishes');
    }

    /**
     * A flush failure on one reconnect attempt must not lose the buffered publishes: the frames
     * were already acknowledged to their publishers, so the next attempt has to replay them (#123).
     */
    public function testReconnectFlushFailureRetainsBufferedPublishesForNextAttempt(): void
    {
        $info = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";
        $release = new DeferredFuture();

        $transport = new class ($info, $release) implements TransportInterface {
            /** @var list<string> */
            public array $writes = [];
            private int $connects = 0;
            private bool $flushFailedOnce = false;
            /** @var list<list<string>> */
            private array $reads;

            /** @param DeferredFuture<void> $release */
            public function __construct(string $info, private DeferredFuture $release)
            {
                $this->reads = [
                    [$info, "PONG\r\n", '__EOF__'], // connection 0: handshake, then EOF -> reconnect
                    [$info, "PONG\r\n"],            // connection 1: attempt 1 handshake (its flush write fails)
                    [$info, "PONG\r\n"],            // connection 2: attempt 2 handshake (flush succeeds)
                ];
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function (): void {
                    // Hold the first reconnect attempt open so a publish lands mid-reconnect.
                    if ($this->connects === 1) {
                        $this->release->getFuture()->await();
                    }
                    $this->connects++;
                });
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(function () use ($bytes): void {
                    if (!$this->flushFailedOnce && str_contains($bytes, 'PUB a.b')) {
                        $this->flushFailedOnce = true;
                        throw new TransportClosedException('socket died during reconnect flush');
                    }
                    $this->writes[] = $bytes;
                });
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $conn = max(0, $this->connects - 1);
                    $next = array_shift($this->reads[$conn]) ?? '';
                    if ($next === '__EOF__') {
                        throw new TransportClosedException('eof');
                    }

                    return $next;
                });
            }

            public function close(): Future
            {
                return async(static fn(): null => null);
            }
        };

        $connection = new NatsConnection(new NatsOptions(reconnectDelayMs: 1, reconnectJitterMs: 0), $transport);
        $connection->connect()->await();

        $pump = async(static fn(): int => $connection->processIncoming()->await());
        delay(0.05); // let the reconnect begin and suspend on the held connect()

        self::assertSame(ConnectionState::Connecting, $connection->state());
        $connection->publish('a.b', 'buffered')->await();

        $release->complete();   // attempt 1 completes its handshake, then its flush write fails
        $pump->await();

        // Attempt 2 must have replayed the buffered frame even though attempt 1's flush failed.
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertContains("PUB a.b 8\r\nbuffered\r\n", $transport->writes);
    }

    /**
     * A publish that lands while recovery is closing the dead socket must take the reconnect
     * buffer, not race a socket that is about to disappear: the state flips off Open before
     * recovery's first await, so the frame is replayed only after the new handshake (#124).
     */
    public function testPublishDuringRecoverySocketCloseBuffersInsteadOfRacingDeadSocket(): void
    {
        $info = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";
        $release = new DeferredFuture();

        $transport = new class ($info, $release) implements TransportInterface {
            /** @var list<string> */
            public array $writes = [];
            private int $connects = 0;
            private int $closes = 0;
            /** @var list<list<string>> */
            private array $reads;

            /** @param DeferredFuture<void> $release */
            public function __construct(string $info, private DeferredFuture $release)
            {
                $this->reads = [
                    [$info, "PONG\r\n", '__EOF__'], // connection 0: handshake, then EOF -> reconnect
                    [$info, "PONG\r\n"],            // connection 1: reconnect handshake
                ];
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function (): void {
                    $this->connects++;
                });
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                });
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $conn = max(0, $this->connects - 1);
                    $next = array_shift($this->reads[$conn]) ?? '';
                    if ($next === '__EOF__') {
                        throw new TransportClosedException('eof');
                    }

                    return $next;
                });
            }

            public function close(): Future
            {
                return async(function (): void {
                    // Hold recovery's dead-socket close open so a publish lands in that window.
                    if ($this->closes === 0) {
                        $this->closes++;
                        $this->release->getFuture()->await();

                        return;
                    }
                    $this->closes++;
                });
            }
        };

        $connection = new NatsConnection(new NatsOptions(reconnectDelayMs: 1, reconnectJitterMs: 0), $transport);
        $connection->connect()->await();

        $pump = async(static fn(): int => $connection->processIncoming()->await());
        delay(0.05); // recovery has started and is suspended closing the dead socket

        // The recovery entry must already have routed the connection off Open.
        self::assertSame(ConnectionState::Connecting, $connection->state());

        $connection->publish('a.b', 'buffered')->await();
        // The frame must NOT have been written to the dying socket in this window.
        self::assertNotContains("PUB a.b 8\r\nbuffered\r\n", $transport->writes);

        $release->complete();
        $pump->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        // The frame reaches the wire only via the post-handshake flush.
        $pubIndex = array_search("PUB a.b 8\r\nbuffered\r\n", $transport->writes, true);
        self::assertNotFalse($pubIndex, 'buffered publish must be flushed after reconnect');
        $reconnectHandshakeIndex = null;
        foreach ($transport->writes as $i => $frame) {
            if ($i > 0 && str_starts_with($frame, 'CONNECT')) {
                $reconnectHandshakeIndex = $i;
            }
        }
        self::assertNotNull($reconnectHandshakeIndex, 'reconnect handshake CONNECT must be on the wire');
        self::assertGreaterThan($reconnectHandshakeIndex, $pubIndex, 'flush must follow the reconnect handshake');
    }

    /**
     * When reconnect attempts are exhausted, buffered publishes cannot be delivered anymore; the
     * abandonment must surface through the error listener and the buffer must not leak into a
     * later manual connect() where a future recovery would replay frames from a dead epoch (#123).
     */
    public function testReconnectExhaustionReportsAbandonedBufferedPublishes(): void
    {
        $info = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";
        $release = new DeferredFuture();

        $transport = new class ($info, $release) implements TransportInterface {
            /** @var list<string> */
            public array $writes = [];
            private int $connects = 0;
            /** @var list<string> */
            private array $reads;

            /** @param DeferredFuture<void> $release */
            public function __construct(string $info, private DeferredFuture $release)
            {
                $this->reads = [$info, "PONG\r\n", '__EOF__'];
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function (): void {
                    if ($this->connects === 0) {
                        $this->connects++;

                        return;
                    }
                    if ($this->connects === 1) {
                        // Hold the first reconnect attempt so a publish can land mid-reconnect.
                        $this->release->getFuture()->await();
                    }
                    $this->connects++;
                    throw new TransportClosedException('dial failed');
                });
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                });
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $next = array_shift($this->reads) ?? '';
                    if ($next === '__EOF__') {
                        throw new TransportClosedException('eof');
                    }

                    return $next;
                });
            }

            public function close(): Future
            {
                return async(static fn(): null => null);
            }
        };

        $errors = [];
        $connection = new NatsConnection(
            new NatsOptions(
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                maxReconnectAttempts: 2,
                errorListener: static function (\Throwable $err) use (&$errors): void {
                    $errors[] = $err;
                },
            ),
            $transport,
        );
        $connection->connect()->await();

        $pump = async(static fn(): int => $connection->processIncoming()->await());
        delay(0.05); // let the reconnect begin and suspend on the held connect()

        self::assertSame(ConnectionState::Connecting, $connection->state());
        $connection->publish('a.b', 'doomed')->await();

        $release->complete();   // every reconnect attempt now fails -> exhaustion

        try {
            $pump->await();
            self::fail('expected ConnectionException after reconnect exhaustion');
        } catch (ConnectionException $e) {
            self::assertSame('Reconnect attempts exhausted', $e->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        $abandonment = array_filter(
            $errors,
            static fn(\Throwable $err): bool => str_contains($err->getMessage(), 'buffered publishes were discarded'),
        );
        self::assertCount(1, $abandonment, 'abandoning the reconnect buffer must surface via the error listener');
        self::assertSame(
            '',
            (new \ReflectionProperty(NatsConnection::class, 'reconnectBuffer'))->getValue($connection),
            'the dead-epoch buffer must not survive into a later manual connect()',
        );
    }

    /**
     * Verifies HMSG frames preserve raw headers and payload separation.
     */
    public function testProcessIncomingDispatchesHmsgWithRawHeaders(): void
    {
        $headerPayload = "NATS/1.0\r\n\r\n";
        $bodyPayload = 'hello';
        $merged = $headerPayload . $bodyPayload;

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "HMSG updates 1 12 17\r\n{$merged}\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $received = null;
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received = $message;
        })->await();

        $connection->processIncoming()->await();

        self::assertInstanceOf(NatsMessage::class, $received);
        /** @var NatsMessage $receivedMessage */
        $receivedMessage = $received;
        self::assertSame($headerPayload, $receivedMessage->rawHeaders);
        self::assertSame($bodyPayload, $receivedMessage->payload);
    }

    /**
     * Verifies server PING frames are answered with protocol PONG.
     */
    public function testProcessIncomingRespondsToServerPing(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "PING\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();
        $frames = $connection->processIncoming()->await();

        self::assertSame(1, $frames);
        self::assertSame("PONG\r\n", $transport->writes[2]);
    }

    /**
     * Verifies overflow with drop-oldest policy keeps newest buffered message.
     */
    public function testSlowConsumerDropOldestPolicy(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 5\r\nfirst\r\nMSG updates 1 6\r\nsecond\r\n",
        ]);

        $options = new NatsOptions(
            maxPendingMessagesPerSubscription: 1,
            slowConsumerPolicy: SlowConsumerPolicy::DropOldest,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $delivered = [];
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$delivered): void {
            $delivered[] = $message->payload;
        })->await();

        $connection->processIncoming()->await();

        self::assertSame(['second'], $delivered);
    }

    /**
     * Verifies overflow with drop-newest policy keeps the earliest buffered message.
     */
    public function testSlowConsumerDropNewestPolicy(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 5\r\nfirst\r\nMSG updates 1 6\r\nsecond\r\n",
        ]);

        $options = new NatsOptions(
            maxPendingMessagesPerSubscription: 1,
            slowConsumerPolicy: SlowConsumerPolicy::DropNewest,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $delivered = [];
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$delivered): void {
            $delivered[] = $message->payload;
        })->await();

        $connection->processIncoming()->await();

        self::assertSame(['first'], $delivered);
    }

    /**
     * Verifies overflow with error policy raises a connection exception.
     */
    public function testSlowConsumerErrorPolicyThrows(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 5\r\nfirst\r\nMSG updates 1 6\r\nsecond\r\n",
        ]);

        $options = new NatsOptions(
            maxPendingMessagesPerSubscription: 1,
            slowConsumerPolicy: SlowConsumerPolicy::Error,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();
        $connection->subscribe('updates', static function (NatsMessage $message): void {})->await();

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Subscription queue overflow');

        $connection->processIncoming()->await();
    }

    /**
     * One sid overflowing under SlowConsumerPolicy::Error must not discard frames for OTHER
     * healthy subscriptions parsed from the same chunk: the parser already consumed the bytes and
     * core NATS never resends them (#128). The overflow still surfaces after full dispatch.
     */
    public function testSlowConsumerErrorOnOneSidDoesNotDiscardSiblingFrames(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // sid 1 gets two messages (second overflows with maxPending=1); sid 2's message follows.
            "MSG a 1 2\r\nm1\r\nMSG a 1 2\r\nm2\r\nMSG b 2 2\r\nm3\r\n",
        ]);

        $options = new NatsOptions(
            maxPendingMessagesPerSubscription: 1,
            slowConsumerPolicy: SlowConsumerPolicy::Error,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $receivedA = [];
        $receivedB = [];
        $connection->subscribe('a', static function (NatsMessage $message) use (&$receivedA): void {
            $receivedA[] = $message->payload;
        })->await();
        $connection->subscribe('b', static function (NatsMessage $message) use (&$receivedB): void {
            $receivedB[] = $message->payload;
        })->await();

        try {
            $connection->processIncoming()->await();
            self::fail('expected the overflow to surface after dispatch');
        } catch (ConnectionException $e) {
            self::assertStringContainsString('Subscription queue overflow for sid 1', $e->getMessage());
        }

        self::assertSame(['m1'], $receivedA, 'frames before the overflow must be delivered');
        self::assertSame(['m3'], $receivedB, 'a healthy sibling subscription must not lose its frame');
    }

    /**
     * A failing PONG reply to a server PING must not discard the MSG frames parsed from the same
     * chunk: dispatch completes (messages enqueued and drained) before the failure surfaces (#128).
     */
    public function testServerPingWithFailingPongWriteStillDeliversCoChunkedMessages(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "PING\r\nMSG updates 1 5\r\nhello\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        $received = [];
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        // Fail the PONG reply write (only the reply contains "PONG"; the handshake writes CONNECT+PING).
        $transport->throwOnWriteContaining = 'PONG';

        try {
            $connection->processIncoming()->await();
            self::fail('expected the PONG write failure to surface after dispatch');
        } catch (TransportClosedException $e) {
            self::assertSame('Simulated write failure', $e->getMessage());
        }

        self::assertSame(['hello'], $received, 'the co-chunked MSG must be delivered despite the PONG failure');
    }

    /**
     * A fatal -ERR observed during the heartbeat self-read was previously swallowed whole,
     * discarding co-chunked messages with it. The messages must be delivered and the fatal error
     * surfaced through the error listener (escalation stays with the next user read) (#128).
     */
    public function testHeartbeatReadSurfacesFatalErrAndDeliversCoChunkedMessages(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "-ERR 'Unknown Protocol Operation'\r\nMSG updates 1 5\r\nhello\r\n",
        ]);

        $errors = [];
        $options = new NatsOptions(
            pingIntervalSeconds: 0.05,
            maxPingsOut: 3,
            reconnectEnabled: false,
            errorListener: static function (\Throwable $err) use (&$errors): void {
                $errors[] = $err;
            },
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $received = [];
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        // Let the ping timer fire; its heartbeat self-read consumes the -ERR + MSG chunk.
        delay(0.1);

        self::assertSame(['hello'], $received, 'the co-chunked MSG must be delivered despite the fatal -ERR');
        $fatal = array_filter(
            $errors,
            static fn(\Throwable $err): bool => str_contains($err->getMessage(), 'Server sent error frame'),
        );
        self::assertNotSame([], $fatal, 'the fatal -ERR must surface via the error listener, not vanish');

        $connection->disconnect()->await();
    }

    /**
     * Verifies request/reply returns the first received response message.
     */
    public function testRequestReturnsFirstReplyMessage(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG _INBOX.any 1 5\r\nhello\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $response = $connection->request('svc.echo', '{"x":1}', 50)->await();

        self::assertSame('hello', $response->payload);
        self::assertStringStartsWith('SUB _INBOX.', $transport->writes[2]);
        self::assertStringStartsWith('PUB svc.echo _INBOX.', $transport->writes[3]);
        self::assertSame("UNSUB 1\r\n", $transport->writes[4]);
    }

    public function testRequestReturnsReplyDeliveredOnSameTickAsTimeout(): void
    {
        // A reply delivered in the same processIncoming() call the deadline fires in must be returned,
        // not discarded as a spurious timeout. The transport holds the reply chunk for 50ms (past the
        // 10ms request deadline, ignoring the cancellation) to reproduce the completion-vs-timeout race.
        $transport = new FakeTransport(
            [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                "PONG\r\n",
                "MSG _INBOX.any 1 5\r\nhello\r\n",
            ],
            holdChunkContaining: 'hello',
            holdSeconds: 0.05,
        );

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $response = $connection->request('svc.echo', '{"x":1}', 10)->await();

        self::assertSame('hello', $response->payload);
    }

    /**
     * Verifies request/reply raises timeout when no response arrives before deadline.
     */
    public function testRequestTimesOutWithoutReply(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $this->expectException(TimeoutException::class);
        $this->expectExceptionMessage('Request timed out');

        try {
            $connection->request('svc.echo', '{"x":1}', 5)->await();
        } finally {
            self::assertSame("UNSUB 1\r\n", $transport->writes[4]);
        }
    }

    /**
     * Verifies request-many collects multiple replies and stops at maxResponses (#21).
     */
    public function testRequestManyCollectsUpToMaxResponses(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG _INBOX.any 1 1\r\nA\r\nMSG _INBOX.any 1 1\r\nB\r\nMSG _INBOX.any 1 1\r\nC\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $replies = $connection->requestMany('svc.scan', 'q', null, 3, 1000)->await();

        $payloads = array_map(static fn(NatsMessage $m): string => $m->payload, $replies);
        self::assertSame(['A', 'B', 'C'], $payloads);
        self::assertStringStartsWith('PUB svc.scan _INBOX.', $transport->writes[3]);
        self::assertSame("UNSUB 1\r\n", $transport->writes[4]);
    }

    /**
     * Verifies request-many stops once the per-message stall interval elapses (#21).
     */
    public function testRequestManyStopsOnStallInterval(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG _INBOX.any 1 1\r\nA\r\nMSG _INBOX.any 1 1\r\nB\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        // No maxResponses: collection ends because no further reply arrives within the 20ms stall.
        $replies = $connection->requestMany('svc.scan', 'q', null, null, 5000, 20)->await();

        self::assertCount(2, $replies);
    }

    /**
     * Verifies request-many returns an empty set on a no-responders sentinel (#21).
     */
    public function testRequestManyReturnsEmptyOnNoResponders(): void
    {
        $status = "NATS/1.0 503\r\n\r\n";
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            'HMSG _INBOX.any 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $replies = $connection->requestMany('svc.scan', 'q', null, null, 1000)->await();

        self::assertSame([], $replies);
    }

    /**
     * Verifies draining stops cleanly if a handler unsubscribes itself mid-delivery.
     */
    public function testDrainStopsWhenHandlerUnsubscribesItself(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 5\r\nfirst\r\nMSG updates 1 6\r\nsecond\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $sid = 0;
        $delivered = [];
        $sid = $connection->subscribe('updates', function (NatsMessage $message) use (&$delivered, &$connection, &$sid): void {
            $delivered[] = $message->payload;
            if ($message->payload === 'first') {
                $connection->unsubscribe($sid)->await();
            }
        })->await();

        $connection->processIncoming()->await();

        self::assertSame(['first'], $delivered);
    }

    /**
     * Verifies randomizeServers=false dials the configured pool in order (#55).
     */
    public function testServerPoolPreservesOrderWithoutRandomize(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(servers: ['nats://127.0.0.1:4001', 'nats://127.0.0.1:4002', 'nats://127.0.0.1:4003']),
            $transport,
        );
        $connection->connect()->await();

        self::assertSame('tcp://127.0.0.1:4001|5000', $transport->connectCalls[0]);
    }

    /**
     * Verifies randomizeServers=true still dials a member of the configured pool (#55).
     */
    public function testRandomizeServersDialsFromPool(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(
                servers: ['nats://127.0.0.1:4001', 'nats://127.0.0.1:4002', 'nats://127.0.0.1:4003'],
                randomizeServers: true,
            ),
            $transport,
        );
        $connection->connect()->await();

        self::assertContains($transport->connectCalls[0], [
            'tcp://127.0.0.1:4001|5000',
            'tcp://127.0.0.1:4002|5000',
            'tcp://127.0.0.1:4003|5000',
        ]);
    }

    /**
     * Verifies retryOnFailedInitialConnect retries the first connect (reconnect disabled) until it succeeds (#56).
     */
    public function testRetryOnFailedInitialConnectSucceedsAfterRetry(): void
    {
        $transport = new FlakyTransport([
            ['INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n", "PONG\r\n"],
        ], connectFailures: 1);

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: false,
                retryOnFailedInitialConnect: true,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
            ),
            $transport,
        );

        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertGreaterThanOrEqual(2, count($transport->connectCalls));
    }

    /**
     * Verifies a failed first connect throws when retryOnFailedInitialConnect is off and reconnect is off (#56).
     */
    public function testFailedInitialConnectThrowsWithoutRetryOption(): void
    {
        $transport = new FlakyTransport([
            ['INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n", "PONG\r\n"],
        ], connectFailures: 1);

        $connection = new NatsConnection(
            new NatsOptions(reconnectEnabled: false, retryOnFailedInitialConnect: false),
            $transport,
        );

        $this->expectException(ConnectionException::class);
        $connection->connect()->await();
    }

    /**
     * A terminal initial-connect failure (reconnect and initial-retry disabled) must close the
     * transport socket best-effort: connectOnce() dials BEFORE the handshake can fail, so without
     * the close the failed client pins an open fd until the object itself is GC'd (#133).
     */
    public function testTerminalInitialConnectFailureClosesTransportSocket(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "-ERR Maximum Connections Exceeded\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(reconnectEnabled: false, retryOnFailedInitialConnect: false),
            $transport,
        );

        try {
            $connection->connect()->await();
            self::fail('expected ConnectionException for the failed handshake');
        } catch (ConnectionException) {
            // Terminal by construction: no reconnect, no initial-connect retry.
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertTrue($transport->closed, 'a terminal connect failure must not leave the socket open');
    }

    /**
     * Reconnect exhaustion must close the LAST attempt's socket: performRecovery() closes only at
     * the START of each attempt, so the final failed dial's socket survived the terminal exit and
     * stayed pinned by the transport until the client object was GC'd (#133). With every attempt
     * preceded by one close, the terminal exit must add exactly one more (attempts + 1 in total).
     */
    public function testReconnectExhaustionClosesLastAttemptSocket(): void
    {
        $transport = new class implements TransportInterface {
            public int $closeCalls = 0;
            private int $connects = 0;
            /** @var list<string> */
            private array $reads = [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                "PONG\r\n",
            ];

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function (): void {
                    // The first dial succeeds; every reconnect attempt fails.
                    if (++$this->connects > 1) {
                        throw new TransportClosedException('dial failed');
                    }
                });
            }

            public function upgradeTls(): Future
            {
                return async(static function (): void {});
            }

            public function write(string $bytes): Future
            {
                return async(static function (): void {});
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $next = array_shift($this->reads);
                    if ($next === null) {
                        // Epoch 1 dies: the EOF triggers automatic recovery.
                        throw new TransportClosedException('Socket closed by peer (EOF)');
                    }

                    return $next;
                });
            }

            public function close(): Future
            {
                return async(function (): void {
                    $this->closeCalls++;
                });
            }
        };

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 2,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );
        $connection->connect()->await();

        try {
            $connection->processIncoming()->await();
            self::fail('expected ConnectionException after reconnect exhaustion');
        } catch (ConnectionException $e) {
            self::assertSame('Reconnect attempts exhausted', $e->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        // One close before each of the 2 attempts + the terminal close of the last attempt's socket.
        self::assertSame(3, $transport->closeCalls, 'the exhausted recovery must close the last attempt\'s socket');
    }

    /**
     * Verifies credentials embedded in the server URL are applied to CONNECT and stripped from the dial (#37).
     */
    public function testConnectExtractsUrlCredentials(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(servers: ['nats://alice:s3cret@127.0.0.1:4222']),
            $transport,
        );
        $connection->connect()->await();

        // The userinfo is stripped from the dialed DSN...
        self::assertSame('tcp://127.0.0.1:4222|5000', $transport->connectCalls[0]);
        // ...and applied to the CONNECT payload.
        $connect = $transport->writes[0];
        self::assertStringStartsWith('CONNECT ', $connect);
        self::assertStringContainsString('"user":"alice"', $connect);
        self::assertStringContainsString('"pass":"s3cret"', $connect);
    }

    /**
     * Verifies request uses configured inbox prefix for subscription and publish reply subject.
     */
    public function testRequestUsesConfiguredInboxPrefix(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG TMPBOX.any 1 5\r\nhello\r\n",
        ]);

        $options = new NatsOptions(inboxPrefix: 'TMPBOX');
        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $connection->request('svc.echo', '{"x":1}', 50)->await();

        self::assertStringStartsWith('SUB TMPBOX.', $transport->writes[2]);
        self::assertStringStartsWith('PUB svc.echo TMPBOX.', $transport->writes[3]);
    }

    /**
     * Verifies request rejects non-positive timeout values.
     */
    public function testRequestRejectsNonPositiveTimeout(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $this->expectException(TimeoutException::class);
        $this->expectExceptionMessage('Request timeout must be greater than zero');

        $connection->request('svc.echo', '{"x":1}', 0)->await();
    }

    /**
     * Verifies request propagates external cancellation and still unsubscribes inbox listener.
     */
    public function testRequestCanBeCancelledAndCleansUpSubscription(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $deferredCancellation = new DeferredCancellation();
        $deferredCancellation->cancel();

        $this->expectException(CancelledException::class);

        try {
            $connection->request('svc.echo', '{"x":1}', 1_000, $deferredCancellation->getCancellation())->await();
        } finally {
            self::assertSame("UNSUB 1\r\n", $transport->writes[4]);
        }
    }

    /**
     * Two concurrent requests share one socket: when the fiber owning the read completes, a
     * parked waiter must take over the read pump and receive its own reply from a later chunk.
     * Guards the #135 park-instead-of-poll rework against lost wakeups (a broken handoff would
     * leave request B waiting for its full timeout and fail this test).
     */
    public function testConcurrentRequestWaiterTakesOverReadPumpAfterFirstReplyArrives(): void
    {
        $info = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

        $transport = new class ($info) implements TransportInterface {
            /** @var list<string> */
            public array $writes = [];
            /** @var list<string> */
            private array $chunks;
            /** @var ?DeferredFuture<null> */
            private ?DeferredFuture $waiting = null;

            public function __construct(string $info)
            {
                $this->chunks = [$info, "PONG\r\n"];
            }

            public function feed(string $chunk): void
            {
                $this->chunks[] = $chunk;
                $waiting = $this->waiting;
                $this->waiting = null;
                $waiting?->complete();
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(static fn(): null => null);
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                });
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function () use ($cancellation): string {
                    while ($this->chunks === []) {
                        // Model a live idle socket: park until data is fed or the read is cancelled.
                        $this->waiting = new DeferredFuture();
                        $this->waiting->getFuture()->await($cancellation);
                    }

                    return array_shift($this->chunks);
                });
            }

            public function close(): Future
            {
                return async(static fn(): null => null);
            }
        };

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $a = async(static fn(): NatsMessage => $connection->request('svc.a', 'x', 2_000)->await());
        delay(0.01); // request A owns the read pump and is parked on the idle socket
        $b = async(static fn(): NatsMessage => $connection->request('svc.b', 'x', 2_000)->await());
        delay(0.01); // request B sees the read slot taken and parks

        // Extract each request's reply inbox from its PUB frame ("PUB <subject> <inbox> <len>").
        $inboxOf = static function (array $writes, string $subject): string {
            foreach ($writes as $frame) {
                if (str_starts_with($frame, 'PUB ' . $subject . ' ')) {
                    return explode(' ', $frame)[2];
                }
            }

            return '';
        };
        $inboxA = $inboxOf($transport->writes, 'svc.a');
        $inboxB = $inboxOf($transport->writes, 'svc.b');
        self::assertNotSame('', $inboxA);
        self::assertNotSame('', $inboxB);

        // A's reply arrives first; B's follows in a separate chunk that only a handed-over
        // pump can read (request inbox sids are 1 and 2).
        $transport->feed("MSG {$inboxA} 1 2\r\nra\r\n");
        $transport->feed("MSG {$inboxB} 2 2\r\nrb\r\n");

        self::assertSame('ra', $a->await()->payload);
        self::assertSame('rb', $b->await()->payload);
    }

    /**
     * A request waiter parked behind another fiber's read still honors its own deadline and an
     * external cancellation while parked (the awaitFirst park must not outlive them) (#135).
     */
    public function testParkedRequestWaiterStillHonorsDeadlineAndCancellation(): void
    {
        $transport = new FakeTransport(
            [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                "PONG\r\n",
            ],
            blockWhenEmpty: true,
        );

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        // Request A owns the read pump (parked on the idle blocking socket until its deadline).
        $a = async(static fn(): NatsMessage => $connection->request('svc.a', 'x', 400)->await());
        delay(0.01);

        // Request B parks behind A's read; its shorter deadline must still fire while parked.
        try {
            $connection->request('svc.b', 'x', 100)->await();
            self::fail('expected request B to time out while parked behind the read owner');
        } catch (TimeoutException $e) {
            self::assertStringContainsString('svc.b', $e->getMessage());
        }

        // Request C parks behind A's read; an external cancellation must still fire while parked.
        $deferredCancellation = new DeferredCancellation();
        $c = async(static fn(): NatsMessage => $connection->request('svc.c', 'x', 2_000, $deferredCancellation->getCancellation())->await());
        delay(0.01);
        $deferredCancellation->cancel();

        try {
            $c->await();
            self::fail('expected request C to observe its external cancellation while parked');
        } catch (CancelledException) {
            // Expected.
        }

        try {
            $a->await();
            self::fail('expected request A to time out on the idle socket');
        } catch (TimeoutException) {
            // Expected.
        }
    }

    /**
     * Verifies processIncoming reconnects after read failure and replays subscriptions.
     */
    public function testProcessIncomingReconnectsAndResubscribesAfterReadFailure(): void
    {
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    '__THROW__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    "MSG updates 1 5\r\nhello\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $options = new NatsOptions(
            reconnectEnabled: true,
            maxReconnectAttempts: 3,
            reconnectDelayMs: 1,
            reconnectJitterMs: 0,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $received = [];
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        self::assertSame(0, $connection->processIncoming()->await());
        self::assertSame(ConnectionState::Open, $connection->state());

        $connection->processIncoming()->await();

        self::assertSame(['hello'], $received);
        self::assertCount(2, $transport->connectCalls);
        self::assertSame("SUB updates 1\r\n", $transport->writes[2]);
        // writes[5] is the whole coalesced replay buffer (#137); with a single subscription it
        // is byte-identical to the lone SUB frame.
        self::assertSame("SUB updates 1\r\n", $transport->writes[5]);
    }

    /**
     * Verifies max outstanding pings triggers reconnect path and preserves open state.
     */
    public function testMaxPingsOutTriggersReconnect(): void
    {
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $client = new NatsClient(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 1,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0.05,
                maxPingsOut: 0,
            ),
            $transport,
        );
        $client->connect()->await();

        // Let the ping timer fire once; with maxPingsOut=0 this must trigger reconnect. The window
        // must end before the restarted timer's next tick, or a second reconnect would fire.
        delay(0.08);

        self::assertCount(2, $transport->connectCalls);
        self::assertNotNull($client->serverInfo());
        self::assertSame('S2', $client->serverInfo()->serverId);

        $client->disconnect()->await();
    }

    /**
     * Verifies reconnect attempts exhausted path surfaces connection failure after transport loss.
     */
    public function testReconnectAttemptsExhaustedReturnsClosed(): void
    {
        $transport = new class implements \IDCT\NATS\Transport\TransportInterface {
            public int $connectAttempts = 0;

            /** @var list<string> */
            private array $readQueue = [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                "PONG\r\n",
            ];

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function (): void {
                    $this->connectAttempts++;
                    if ($this->connectAttempts > 1) {
                        throw new \RuntimeException('connect failed');
                    }
                });
            }

            public function write(string $bytes): Future
            {
                return async(static function (): void {});
            }

            public function upgradeTls(): Future
            {
                return async(static function (): void {});
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    if ($this->readQueue !== []) {
                        return array_shift($this->readQueue);
                    }

                    throw new \RuntimeException('read failed');
                });
            }

            public function close(): Future
            {
                return async(static function (): void {});
            }
        };

        $client = new NatsClient(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 2,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );
        $client->connect()->await();

        $caught = false;
        try {
            $client->processIncoming()->await();
        } catch (ConnectionException $e) {
            $caught = true;
            self::assertStringContainsString('Reconnect attempts exhausted', $e->getMessage());
        }

        self::assertTrue($caught);

        try {
            $client->publish('updates', 'hello')->await();
            self::fail('Expected closed connection after reconnect exhaustion.');
        } catch (ConnectionException $e) {
            self::assertStringContainsString('Connection is not open', $e->getMessage());
        }
    }

    /**
     * Verifies reconnect backoff delay progression contributes measurable wait before recovery.
     */
    public function testReconnectBackoffDelayProgression(): void
    {
        $transport = new class implements \IDCT\NATS\Transport\TransportInterface {
            public int $connectAttempts = 0;

            /** @var array<int, list<string>> */
            private array $readQueues = [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    '__THROW__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ];

            private int $successfulConnects = 0;

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function (): void {
                    $this->connectAttempts++;

                    // Initial connect succeeds. Reconnect attempts 2 and 3 fail, then attempt 4 succeeds.
                    if ($this->connectAttempts === 2 || $this->connectAttempts === 3) {
                        throw new \RuntimeException('connect failed');
                    }

                    $this->successfulConnects++;
                });
            }

            public function write(string $bytes): Future
            {
                return async(static function (): void {});
            }

            public function upgradeTls(): Future
            {
                return async(static function (): void {});
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $index = max(0, $this->successfulConnects - 1);
                    if (!isset($this->readQueues[$index])) {
                        return '';
                    }

                    $next = array_shift($this->readQueues[$index]);
                    if ($next === null) {
                        return '';
                    }

                    if ($next === '__THROW__') {
                        throw new \RuntimeException('read failed');
                    }

                    return $next;
                });
            }

            public function close(): Future
            {
                return async(static function (): void {});
            }
        };

        $client = new NatsClient(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 5,
                reconnectDelayMs: 20,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );
        $client->connect()->await();

        self::assertSame(0, $client->processIncoming()->await());

        // The read failure drives reconnect: attempts 2 and 3 fail, attempt 4 succeeds and lands on the
        // second server. These outcomes are deterministic. The backoff-delay *magnitude* is asserted by
        // the unit test testBackoffDelayIsExponential; we do NOT assert wall-clock elapsed here because
        // the host clock is not guaranteed monotonic (microtime() can jump forward or backward), which
        // made an elapsed-time bound flaky (#70).
        self::assertSame(4, $transport->connectAttempts);
        self::assertSame('S2', $client->serverInfo()?->serverId);

        $client->disconnect()->await();
    }

    /**
     * A connection that dies mid-MSG-payload leaves the parser expecting the rest of the frame.
     * The reconnect handshake must start from a clean parser: with the stale one, the new server's
     * INFO is swallowed as phantom payload bytes and every attempt fails against a healthy server
     * until the reconnect budget exhausts (#125).
     */
    public function testReconnectAfterMidFramePayloadDropSucceedsOnFirstAttempt(): void
    {
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    // The server dies while streaming a large message: the control line promises
                    // 500000 payload bytes but only a fragment arrives before the read fails.
                    "MSG updates 1 500000\r\npartial-payload-then-the-server-died",
                    '__THROW__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    "MSG updates 1 5\r\nhello\r\n",
                ],
            ],
        );

        $options = new NatsOptions(
            reconnectEnabled: true,
            maxReconnectAttempts: 2,
            reconnectDelayMs: 1,
            reconnectJitterMs: 0,
            connectTimeoutMs: 250,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $received = [];
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        // Reads the partial frame + read failure, reconnects, and replays the subscription.
        $connection->processIncoming()->await();
        self::assertSame(ConnectionState::Open, $connection->state());

        $connection->processIncoming()->await();

        self::assertSame(['hello'], $received, 'post-reconnect delivery must work after a mid-frame drop');
        // Exactly one reconnect dial: the stale-parser bug burns extra attempts eating INFO as payload.
        self::assertCount(2, $transport->connectCalls);
        self::assertSame(1, $connection->statistics()->reconnects);
    }

    /**
     * A terminally closed connection (exhausted reconnect) must not strand its subscription
     * state: a later manual connect() starts clean, and a future recovery must not resurrect the
     * dead epoch's sids as ghost subscriptions delivering to stale handlers (#127).
     */
    public function testExhaustedReconnectReleasesStateSoManualReconnectStartsClean(): void
    {
        $info = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

        $transport = new class ($info) implements TransportInterface {
            /** @var list<string> */
            public array $writes = [];
            private int $connectAttempts = 0;
            private int $successfulConnects = 0;
            /** @var list<list<string>> */
            private array $reads;

            public function __construct(string $info)
            {
                $this->reads = [
                    [$info, "PONG\r\n", '__EOF__'],                      // epoch 0: sub made here, then EOF
                    [$info, "PONG\r\n", '__EOF__'],                      // epoch 1: manual reconnect, then EOF
                    [$info, "PONG\r\n", "MSG updates 1 5\r\nghost\r\n"], // epoch 2: recovery; stray MSG for dead sid
                ];
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function (): void {
                    $attempt = $this->connectAttempts++;
                    // Attempts 1 and 2 are the automatic recovery after epoch 0: fail them both so
                    // the reconnect budget (maxReconnectAttempts: 2) exhausts terminally.
                    if ($attempt === 1 || $attempt === 2) {
                        throw new TransportClosedException('dial failed');
                    }
                    $this->successfulConnects++;
                });
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                });
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $conn = max(0, $this->successfulConnects - 1);
                    $next = array_shift($this->reads[$conn]) ?? '';
                    if ($next === '__EOF__') {
                        throw new TransportClosedException('eof');
                    }

                    return $next;
                });
            }

            public function close(): Future
            {
                return async(static fn(): null => null);
            }
        };

        $connection = new NatsConnection(
            new NatsOptions(reconnectDelayMs: 1, reconnectJitterMs: 0, maxReconnectAttempts: 2),
            $transport,
        );
        $connection->connect()->await();

        $received = [];
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        // Epoch 0 dies; both recovery attempts fail; the connection closes terminally.
        try {
            $connection->processIncoming()->await();
            self::fail('expected ConnectionException after reconnect exhaustion');
        } catch (ConnectionException $e) {
            self::assertSame('Reconnect attempts exhausted', $e->getMessage());
        }
        self::assertSame(ConnectionState::Closed, $connection->state());

        // Manual reconnect starts a clean epoch; its EOF then forces an automatic recovery.
        $connection->connect()->await();
        self::assertSame(ConnectionState::Open, $connection->state());
        $writesBeforeRecovery = count($transport->writes);
        $connection->processIncoming()->await(); // EOF -> recovery -> epoch 2 + resubscribeAll()

        // The dead epoch's sid must not be resurrected: no SUB replay, no delivery to old handler.
        $replayed = array_filter(
            array_slice($transport->writes, $writesBeforeRecovery),
            static fn(string $frame): bool => str_starts_with($frame, 'SUB '),
        );
        self::assertSame([], array_values($replayed), 'recovery must not replay sids from a terminally closed epoch');

        $connection->processIncoming()->await(); // the stray MSG for the dead sid arrives here
        self::assertSame([], $received, 'a handler from a dead epoch must never fire again');
    }

    /**
     * Verifies a recovery that races a user disconnect() does NOT re-open the connection: once
     * close-intent is set, recoverConnection() is a no-op even though a healthy server is reachable
     * (#84). Simulates the race by invoking recovery (as an in-flight heartbeat/read path would) after
     * disconnect() has signalled close-intent.
     */
    public function testDisconnectIsNotReversedByAnInFlightRecovery(): void
    {
        $events = [];
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
                [
                    // A healthy server a reconnect WOULD latch onto if recovery were allowed to run.
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
                connectionListener: static function (ConnectionEvent $e) use (&$events): void {
                    $events[] = $e;
                },
            ),
            $transport,
        );
        $connection->connect()->await();
        $connection->disconnect()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());

        // The race outcome: a recovery is triggered around disconnect time. It must be a no-op.
        (new \ReflectionMethod(NatsConnection::class, 'recoverConnection'))->invoke($connection);

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertNotContains(ConnectionEvent::Reconnected, $events);
        // The second server was never latched onto, and no extra connect happened.
        self::assertSame('S1', $connection->serverInfo()?->serverId);
        self::assertCount(1, $transport->connectCalls);
    }

    /**
     * Verifies performRecovery() itself bails when close-intent is set (defense in depth for the case
     * where recovery had already started before disconnect()/drain()), without reopening (#84).
     */
    public function testPerformRecoveryBailsWhenClosing(): void
    {
        $events = [];
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
                connectionListener: static function (ConnectionEvent $e) use (&$events): void {
                    $events[] = $e;
                },
            ),
            $transport,
        );
        $connection->connect()->await();

        // Simulate close-intent already latched (as disconnect()/drain() would set it).
        (new \ReflectionProperty(NatsConnection::class, 'closing'))->setValue($connection, true);

        (new \ReflectionMethod(NatsConnection::class, 'performRecovery'))->invoke($connection);

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertNotContains(ConnectionEvent::Reconnected, $events);
        self::assertSame('S1', $connection->serverInfo()?->serverId);
        self::assertCount(1, $transport->connectCalls);
    }

    /**
     * Verifies disconnect() releases per-connection state (subscriptions, buffered messages, parser
     * buffer, reconnect buffer) so a reused/pooled client does not retain it until GC (#85).
     */
    public function testDisconnectReleasesSubscriptionAndBufferState(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();
        $connection->subscribe('updates', static function (NatsMessage $message): void {})->await();

        $metaProp = new \ReflectionProperty(NatsConnection::class, 'subscriptionMeta');
        $parserProp = new \ReflectionProperty(NatsConnection::class, 'parser');

        self::assertNotEmpty($metaProp->getValue($connection));
        $parserBefore = $parserProp->getValue($connection);

        $connection->disconnect()->await();

        self::assertSame([], (new \ReflectionProperty(NatsConnection::class, 'subscriptions'))->getValue($connection));
        self::assertSame([], $metaProp->getValue($connection));
        self::assertSame([], (new \ReflectionProperty(NatsConnection::class, 'pendingMessages'))->getValue($connection));
        self::assertSame('', (new \ReflectionProperty(NatsConnection::class, 'reconnectBuffer'))->getValue($connection));
        // The parser is reset to a fresh instance (no residual partial-frame bytes retained).
        self::assertNotSame($parserBefore, $parserProp->getValue($connection));
    }

    public function testCallbackMayPublishDuringPostReconnectDeliveryWithoutDeadlock(): void
    {
        // A message buffered during reconnect replay is now delivered AFTER recovery exits its critical
        // section (not inside it). So a handler that publishes when invoked completes normally instead of
        // re-entering recoverConnection() and deadlocking on the in-progress reconnect.
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    '__THROW__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    "MSG updates 1 5\r\nhello\r\n", // buffered during replay
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $options = new NatsOptions(reconnectEnabled: true, maxReconnectAttempts: 3, reconnectDelayMs: 1, reconnectJitterMs: 0, pingIntervalSeconds: 0);
        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $received = [];
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$received, $connection): void {
            $received[] = $message->payload;
            // Publishing from within the post-reconnect delivery must not deadlock.
            $connection->publish('ack.' . $message->payload, 'ok')->await();
        })->await();

        $connection->processIncoming()->await();

        self::assertSame(['hello'], $received);
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertStringContainsString('PUB ack.hello 2', implode('', $transport->writes));
    }

    public function testProcessIncomingRecoversOnPeerEof(): void
    {
        // A graceful peer close (EOF), not a thrown read error, must still trigger reconnect + replay.
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    '__EOF__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    "MSG updates 1 5\r\nhello\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $options = new NatsOptions(reconnectEnabled: true, maxReconnectAttempts: 3, reconnectDelayMs: 1, reconnectJitterMs: 0);
        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $received = [];
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        self::assertSame(0, $connection->processIncoming()->await()); // hits EOF -> recovers
        self::assertSame(ConnectionState::Open, $connection->state());

        $connection->processIncoming()->await();

        self::assertSame(['hello'], $received);
        self::assertCount(2, $transport->connectCalls);
        self::assertSame("SUB updates 1\r\n", $transport->writes[5]);
    }

    public function testProcessIncomingRecoversOnPeerEofWithPingsDisabled(): void
    {
        // With pings disabled the read path is the ONLY way to detect loss, so EOF recovery is the
        // only thing standing between a peer close and a permanent silent outage.
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    '__EOF__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $options = new NatsOptions(reconnectEnabled: true, maxReconnectAttempts: 3, reconnectDelayMs: 1, reconnectJitterMs: 0, pingIntervalSeconds: 0);
        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        self::assertSame(0, $connection->processIncoming()->await());

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->connectCalls);
    }

    public function testProcessIncomingMovesToClosedOnPeerEofWhenReconnectDisabled(): void
    {
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    '__EOF__',
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        try {
            $connection->processIncoming()->await();
        } catch (\Throwable) {
            // recoverConnection() throws 'Reconnect is disabled'; the connection is left Closed.
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertCount(1, $transport->connectCalls);
    }

    public function testConsumeHeartbeatResponseRecoversOnPeerEof(): void
    {
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    '__EOF__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: true, maxReconnectAttempts: 3, reconnectDelayMs: 1, reconnectJitterMs: 0), $transport);
        $connection->connect()->await();

        // The heartbeat self-read hits EOF: it must recover (after clearing readInProgress), not swallow.
        (new \ReflectionMethod($connection, 'consumeHeartbeatResponse'))->invoke($connection);

        self::assertCount(2, $transport->connectCalls);
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    public function testConsumeHeartbeatResponseDoesNotRecoverWithoutEof(): void
    {
        // An empty/no-PONG-yet read (not EOF) must be swallowed, never trigger a reconnect.
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: true, maxReconnectAttempts: 3, reconnectDelayMs: 1, reconnectJitterMs: 0), $transport);
        $connection->connect()->await();

        (new \ReflectionMethod($connection, 'consumeHeartbeatResponse'))->invoke($connection);

        self::assertCount(1, $transport->connectCalls);
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * Verifies connect retries across rotated servers when earlier attempts fail.
     */
    public function testConnectRotatesServersOnReconnectAttempts(): void
    {
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S3","server_name":"n3","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ],
            connectFailures: 1,
            readFailures: 0,
        );

        $options = new NatsOptions(
            servers: ['nats://127.0.0.1:4222', 'nats://127.0.0.2:4222'],
            reconnectEnabled: true,
            maxReconnectAttempts: 3,
            reconnectDelayMs: 1,
            reconnectJitterMs: 0,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->connectCalls);
        self::assertStringStartsWith('tcp://127.0.0.1:4222', $transport->connectCalls[0]);
        self::assertStringStartsWith('tcp://127.0.0.2:4222', $transport->connectCalls[1]);
    }

    /**
     * Verifies that receiving a server PONG frame does not throw and resets ping tracking.
     */
    public function testProcessIncomingHandlesServerPongSilently(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $frames = $connection->processIncoming()->await();

        self::assertSame(1, $frames);
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * Verifies async INFO frames refresh server capabilities during an open connection.
     */
    public function testProcessIncomingUpdatesServerInfoFromAsyncInfoFrame(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":64,"headers":true}' . "\r\n",
            "PONG\r\n",
            "INFO {\"server_id\":\"S1\",\"server_name\":\"n1\",\"version\":\"2.12.1\",\"jetstream\":true,\"max_payload\":128,\"headers\":true}\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $serverInfo = $connection->serverInfo();
        self::assertNotNull($serverInfo);
        self::assertSame(64, $serverInfo->maxPayload);

        $frames = $connection->processIncoming()->await();

        $updatedServerInfo = $connection->serverInfo();
        self::assertNotNull($updatedServerInfo);
        self::assertSame(1, $frames);
        self::assertSame(128, $updatedServerInfo->maxPayload);
        self::assertSame('2.12.1', $updatedServerInfo->version);
    }

    /**
     * Verifies recoverable server permission errors do not close the connection.
     */
    public function testProcessIncomingIgnoresRecoverableServerErrFrame(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "-ERR 'Permissions Violation for Publish to updates'\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $frames = $connection->processIncoming()->await();

        self::assertSame(1, $frames);
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * Verifies the ping timer sends PING frames at the configured interval.
     */
    public function testPingTimerSendsPingAtInterval(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        // Use a very short fractional interval (50 ms) and let the event loop tick.
        $options = new NatsOptions(
            pingIntervalSeconds: 0.05,
            maxPingsOut: 3,
            reconnectEnabled: false,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $writesBeforePing = count($transport->writes);

        // Let the event loop run long enough for the timer to fire.
        delay(0.1);

        self::assertGreaterThan($writesBeforePing, count($transport->writes));
        self::assertSame("PING\r\n", $transport->writes[$writesBeforePing]);

        $connection->disconnect()->await();
    }

    /**
     * A connection abandoned without disconnect()/drain() must be collectable: the ping timer's
     * repeat closure holds $this only weakly, so dropping the last application reference frees
     * the whole graph (previously the event loop rooted it forever - open socket, handler
     * closures, buffers - while the timer kept PINGing) and the destructor closes the socket (#126).
     */
    public function testAbandonedOpenConnectionIsCollectedAndClosesItsSocket(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();
        // Register a handler whose closure captures state, as real applications do.
        $connection->subscribe('updates', static function (NatsMessage $message): void {
        })->await();
        self::assertSame(ConnectionState::Open, $connection->state());

        $weak = \WeakReference::create($connection);
        unset($connection);
        gc_collect_cycles();

        self::assertNull($weak->get(), 'an abandoned Open connection must be freed, not rooted by its ping timer');

        // The destructor defers the socket close to the event loop; let it run one tick.
        delay(0);
        self::assertTrue($transport->closed, 'the abandoned connection\'s socket must be closed best-effort');
    }

    /**
     * Verifies ping timer is not started when pingIntervalSeconds is zero.
     */
    public function testPingTimerDisabledWhenIntervalIsZero(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $options = new NatsOptions(
            pingIntervalSeconds: 0,
            reconnectEnabled: false,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $writesAfterConnect = count($transport->writes);

        delay(0.1);

        // No additional writes (no PING sent).
        self::assertCount($writesAfterConnect, $transport->writes);

        $connection->disconnect()->await();
    }

    /**
     * Verifies disconnect cancels the ping timer cleanly.
     */
    public function testDisconnectCancelsPingTimer(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $options = new NatsOptions(
            pingIntervalSeconds: 0.05,
            maxPingsOut: 2,
            reconnectEnabled: false,
        );

        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();
        $connection->disconnect()->await();

        $writesAfterDisconnect = count($transport->writes);

        // Let event loop tick past when timer would have fired.
        delay(0.12);

        // No PING sent after disconnect.
        self::assertCount($writesAfterDisconnect, $transport->writes);
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * Verifies ping timer marks connection closed when max outstanding pings is exceeded and reconnect fails.
     */
    public function testPingTimerClosesWhenMaxPingsExceededAndReconnectFails(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(
                pingIntervalSeconds: 0.05,
                maxPingsOut: 0,
                reconnectEnabled: false,
            ),
            $transport,
        );
        $connection->connect()->await();

        delay(0.1);

        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * Verifies publish throws when payload exceeds server max_payload.
     */
    public function testPublishRejectsOversizedPayload(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":64,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Payload size 65 exceeds server max_payload of 64');

        $connection->publish('test.subject', str_repeat('x', 65))->await();
    }

    /**
     * Verifies publish succeeds when payload exactly matches max_payload.
     */
    public function testPublishAcceptsPayloadAtExactLimit(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":64,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $connection->publish('test.subject', str_repeat('x', 64))->await();

        self::assertStringContainsString('PUB test.subject 64', $transport->writes[2]);
    }

    /**
     * Verifies publishWithHeaders checks total (headers + payload) against max_payload.
     */
    public function testPublishWithHeadersRejectsOversizedTotal(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":32,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('exceeds server max_payload of 32');

        $connection->publishWithHeaders('test.subject', str_repeat('x', 32), ['Key' => 'Val'])->await();
    }

    /**
     * Verifies publishWithHeaders fails client-side when INFO advertises "headers":false, before
     * any HPUB reaches the wire (nats.go ErrHeadersNotSupported parity, #132).
     */
    public function testPublishWithHeadersThrowsWhenServerLacksHeadersSupport(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":false}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Server does not advertise headers support');

        try {
            $connection->publishWithHeaders('test.subject', 'data', ['Key' => 'Val'])->await();
        } finally {
            self::assertStringNotContainsString('HPUB', implode('', $transport->writes));
        }
    }

    /**
     * Verifies requestWithHeaders shares the HPUB capability guard: no HPUB is written against a
     * headers-disabled server (#132).
     */
    public function testRequestWithHeadersThrowsWhenServerLacksHeadersSupport(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":false}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Server does not advertise headers support');

        try {
            $connection->requestWithHeaders('svc.echo', 'data', ['Key' => 'Val'])->await();
        } finally {
            self::assertStringNotContainsString('HPUB', implode('', $transport->writes));
        }
    }

    /**
     * Verifies CONNECT payload includes no_responders flag.
     */
    public function testConnectPayloadIncludesNoResponders(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        self::assertStringContainsString('"no_responders":true', $transport->writes[0]);
    }

    /**
     * Verifies request throws NatsException when server returns 503 No Responders.
     */
    public function testRequestThrowsOnNoRespondersStatus(): void
    {
        $noRespondersHeader = "NATS/1.0 503 No Responders\r\n\r\n";
        $headerBytes = strlen($noRespondersHeader);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "HMSG _INBOX.any 1 {$headerBytes} {$headerBytes}\r\n{$noRespondersHeader}\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(NatsException::class);
        $this->expectExceptionMessage('No responders for subject svc.missing');

        $connection->request('svc.missing', 'hello', 500)->await();
    }

    // ─── Subject Validation ─────────────────────────────────────────────

    public function testPublishRejectsEmptySubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Subject must not be empty');
        $connection->publish('', 'data')->await();
    }

    public function testPublishRejectsSubjectWithWhitespace(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Subject must not contain whitespace');
        $connection->publish('foo bar', 'data')->await();
    }

    public function testPublishRejectsWildcardSubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Wildcards are not allowed in publish subjects');
        $connection->publish('foo.*', 'data')->await();
    }

    public function testPublishRejectsEmptyTokenInSubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Subject must not contain empty tokens');
        $connection->publish('foo..bar', 'data')->await();
    }

    /**
     * Verifies the validated-subject memo (#136) is keyed per subject: caching a valid subject
     * (including a repeat publish that hits the memo) must not let a NEW invalid subject bypass
     * validation.
     */
    public function testPublishSubjectCacheDoesNotBypassValidationForNewInvalidSubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        // Prime the memo, then publish again through the memo-hit path.
        $connection->publish('cache.valid', 'data')->await();
        $connection->publish('cache.valid', 'data')->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Subject must not contain empty tokens');
        $connection->publish('cache..invalid', 'data')->await();
    }

    public function testPublishRejectsFullWildcardToken(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Wildcards are not allowed in publish subjects');
        $connection->publish('foo.>', 'data')->await();
    }

    public function testSubscribeAcceptsWildcardSubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $sid = $connection->subscribe('foo.*', function (): void {})->await();
        self::assertSame(1, $sid);

        $sid2 = $connection->subscribe('bar.>', function (): void {})->await();
        self::assertSame(2, $sid2);
    }

    public function testSubscribeRejectsGreaterThanNotInLastToken(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Wildcard ">" must be the last token');
        $connection->subscribe('>.foo', function (): void {})->await();
    }

    public function testSubscribeRejectsPartialWildcardToken(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Wildcards must occupy an entire token');
        $connection->subscribe('foo.ba*', function (): void {})->await();
    }

    // ─── Drain ──────────────────────────────────────────────────────────

    public function testDrainUnsubscribesAllAndCloses(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $connection->subscribe('foo', function (): void {})->await();
        $connection->subscribe('bar', function (): void {})->await();

        $connection->drain()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertTrue($transport->closed);

        $writes = implode('', $transport->writes);
        self::assertStringContainsString("UNSUB 1\r\n", $writes);
        self::assertStringContainsString("UNSUB 2\r\n", $writes);
        self::assertStringContainsString("PING\r\n", $writes);
    }

    public function testSubscriptionDispatchIsNotReentrantWhenHandlerAwaits(): void
    {
        // First message's handler awaits a request() on the same connection. That request self-pumps
        // processIncoming(), which reads the SECOND message for the same sid and tries to drain it.
        // Without the per-sid re-entrancy guard the second handler would fire on top of the suspended
        // first one (log: start:A, start:B, end:B, end:A); with it, B waits for A to finish.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG events 1 1\r\nA\r\n",       // first delivery (sid 1)
            "MSG events 1 1\r\nB\r\n",       // second delivery (sid 1), read during the awaited request
            "MSG _INBOX.r 2 1\r\nR\r\n",     // the awaited request's reply (inbox sid 2)
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $log = [];
        $first = true;
        $connection->subscribe('events', function (NatsMessage $message) use (&$log, &$first, $connection): void {
            $tag = $message->payload;
            $log[] = 'start:' . $tag;
            if ($first) {
                $first = false;
                $connection->request('svc', 'x', 1000)->await();
            }
            $log[] = 'end:' . $tag;
        })->await();

        $connection->processIncoming()->await();

        self::assertSame(['start:A', 'end:A', 'start:B', 'end:B'], $log);
    }

    /**
     * Verifies an injected PSR-3 logger records the full connection lifecycle - connect/close,
     * server discovery + lame-duck (from an async INFO), and a real reconnect (disconnect,
     * per-attempt backoff warning, reconnect) - with the exact message strings and levels the
     * connection emits (#69).
     */
    public function testLoggerCapturesLifecycleEvents(): void
    {
        $logger = new class extends \Psr\Log\AbstractLogger {
            /** @var list<array{level:string,message:string}> */
            public array $records = [];

            public function log($level, string|\Stringable $message, array $context = []): void
            {
                $this->records[] = ['level' => (string) $level, 'message' => (string) $message];
            }
        };

        // 1) connect() -> Connected (info); disconnect() -> Closed (info).
        $connectClose = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $lifecycle = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0, logger: $logger), $connectClose);
        $lifecycle->connect()->await();
        $lifecycle->disconnect()->await();

        // 2) An async INFO advertising a new peer + lame-duck mode -> DiscoveredServers then LameDuck
        //    (info). reconnectEnabled: false isolates the events from auto-failover.
        $discovery = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","max_payload":1048576,"headers":true,"ldm":true,"connect_urls":["10.0.0.2:4222"]}' . "\r\n",
        ]);
        $discoveryConnection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0, reconnectEnabled: false, logger: $logger),
            $discovery,
        );
        $discoveryConnection->connect()->await();
        $discoveryConnection->processIncoming()->await();

        // 3) A real recovery: the first reconnect attempt fails its handshake read (per-attempt
        //    backoff warning), the next succeeds -> Disconnected, backoff warning, Reconnected.
        $flaky = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    '__THROW__',
                ],
                [
                    '__THROW__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );
        $reconnecting = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
                logger: $logger,
            ),
            $flaky,
        );
        $reconnecting->connect()->await();
        self::assertSame(0, $reconnecting->processIncoming()->await());
        self::assertSame(ConnectionState::Open, $reconnecting->state());

        // Every advertised lifecycle event is logged with its exact message string and level.
        self::assertContains(['level' => 'info', 'message' => 'NATS connection Connected'], $logger->records);
        self::assertContains(['level' => 'info', 'message' => 'NATS connection Closed'], $logger->records);
        self::assertContains(['level' => 'info', 'message' => 'NATS connection DiscoveredServers'], $logger->records);
        self::assertContains(['level' => 'info', 'message' => 'NATS connection LameDuck'], $logger->records);
        self::assertContains(['level' => 'info', 'message' => 'NATS connection Disconnected'], $logger->records);
        self::assertContains(['level' => 'info', 'message' => 'NATS connection Reconnected'], $logger->records);

        // The failed first recovery attempt logged a per-attempt backoff warning at warning level.
        $backoffWarnings = array_filter(
            $logger->records,
            static fn (array $record): bool =>
                $record['level'] === 'warning'
                && str_starts_with($record['message'], 'NATS reconnect attempt ')
                && str_contains($record['message'], 'failed; retrying in'),
        );
        self::assertNotEmpty($backoffWarnings);
    }

    public function testFlushSendsPingAndResolvesOnPong(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",   // connect handshake PONG
            "PONG\r\n",   // the flush() PONG
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $connection->flush()->await();

        // flush() wrote a PING and resolved once the server's PONG round-tripped.
        self::assertContains("PING\r\n", $transport->writes);
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    public function testDrainedSubscriptionQueueIsKeptEmptyForReuseAndDroppedWithTheSubscription(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG events 1 5\r\nhello\r\n",
            "MSG events 1 5\r\nworld\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();
        $sid = $connection->subscribe('events', function (): void {})->await();

        $pendingProp = new \ReflectionProperty(NatsConnection::class, 'pendingMessages');

        $connection->processIncoming()->await();

        // Once delivered, the per-SID queue is kept (empty) rather than freed: unset-on-empty cost a
        // SplQueue alloc/free per delivered message in the promptly-drained common case (#139).
        $pending = $pendingProp->getValue($connection);
        self::assertArrayHasKey($sid, $pending);
        self::assertTrue($pending[$sid]->isEmpty());
        $queueAfterFirstDrain = $pending[$sid];

        // The next delivery reuses the SAME queue instance - no reallocation per message.
        $connection->processIncoming()->await();
        $pending = $pendingProp->getValue($connection);
        self::assertSame($queueAfterFirstDrain, $pending[$sid]);
        self::assertTrue($pending[$sid]->isEmpty());

        // The retained queue still dies with the subscription, so sid cleanup does not leak it.
        $connection->unsubscribe($sid)->await();
        self::assertSame([], $pendingProp->getValue($connection));
    }

    public function testDrainDoesNotResurrectConnectionOnReadFailure(): void
    {
        // Connect, then the flush read FAILS (peer close / EOF) instead of returning the drain PONG.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            FakeTransport::EOF,
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0, requestTimeoutMs: 200, reconnectEnabled: true),
            $transport,
        );
        $connection->connect()->await();
        $connection->subscribe('events', function (): void {})->await();

        $connection->drain()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());

        // A resurrection would reconnect (a second CONNECT) and re-SUBscribe the just-UNSUBbed sid.
        $writes = implode('', $transport->writes);
        self::assertSame(1, substr_count($writes, 'CONNECT '), 'drain must not reconnect on a read failure while draining');
        self::assertSame(1, substr_count($writes, 'SUB events '), 'drain must not re-subscribe while draining');
        self::assertStringContainsString("UNSUB 1\r\n", $writes);
    }

    public function testDrainTerminatesViaDeadlineWhenNoFlushPongArrives(): void
    {
        // The queue empties with NO drain PONG: readLine returns '' synchronously. drain() must end
        // via its flush deadline rather than busy-spin (a synchronous 0-frame loop would starve the
        // event loop so the TimeoutCancellation could never fire). The fix yields between empty reads.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0, requestTimeoutMs: 150),
            $transport,
        );
        $connection->connect()->await();
        $connection->subscribe('events', function (): void {})->await();

        $connection->drain()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    public function testDrainRequiresOpenConnection(): void
    {
        $transport = new FakeTransport();
        $connection = new NatsConnection(new NatsOptions(), $transport);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Connection is not open');
        $connection->drain()->await();
    }

    public function testDrainDeliversBufferedMessagesBeforeClosing(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG events 1 5\r\nhello\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        $received = [];
        $connection->subscribe('events', static function (NatsMessage $msg) use (&$received): void {
            $received[] = $msg->payload;
        })->await();

        // Drain should flush the in-flight delivery, then close.
        $connection->drain()->await();

        self::assertSame(['hello'], $received);
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    public function testRequestTimeoutPreservesOriginalExceptionDuringCleanup(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        try {
            $connection->request('svc.echo', '{"x":1}', 5)->await();
            self::fail('Expected timeout');
        } catch (TimeoutException $e) {
            self::assertStringContainsString('Request timed out', $e->getMessage());
        }

        self::assertSame("UNSUB 1\r\n", $transport->writes[4]);
    }

    public function testMalformedHmsgTriggersRecoveryInsteadOfEscaping(): void
    {
        // headerBytes (20) > totalBytes (10): a corrupt frame. The parser rejects it, and
        // processIncoming treats an unparseable stream as a transport failure (recovery) rather
        // than letting the ProtocolException escape the caller's loop. With reconnect disabled that
        // surfaces as a ConnectionException and closes the connection.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "HMSG updates 1 20 10\r\n1234567890\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0, reconnectEnabled: false),
            $transport,
        );
        $connection->connect()->await();
        $connection->subscribe('updates', static function (NatsMessage $message): void {})->await();

        try {
            $connection->processIncoming()->await();
            self::fail('Expected a ConnectionException from the corrupt-stream recovery path');
        } catch (ConnectionException) {
            // Expected: corrupt stream -> recoverConnection() -> reconnect disabled -> Closed.
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    // ─── Exponential Backoff ────────────────────────────────────────────

    public function testBackoffDelayIsExponential(): void
    {
        // Use reflection to test private backoffDelayMs method.
        $transport = new FakeTransport();
        $connection = new NatsConnection(
            new NatsOptions(reconnectDelayMs: 100, reconnectMaxDelayMs: 5000, reconnectJitterMs: 0),
            $transport,
        );

        $method = new \ReflectionMethod($connection, 'backoffDelayMs');

        self::assertSame(100, $method->invoke($connection, 1));   // 100 * 2^0 = 100
        self::assertSame(200, $method->invoke($connection, 2));   // 100 * 2^1 = 200
        self::assertSame(400, $method->invoke($connection, 3));   // 100 * 2^2 = 400
        self::assertSame(800, $method->invoke($connection, 4));   // 100 * 2^3 = 800
        self::assertSame(1600, $method->invoke($connection, 5));  // 100 * 2^4 = 1600
        self::assertSame(3200, $method->invoke($connection, 6));  // 100 * 2^5 = 3200
        self::assertSame(5000, $method->invoke($connection, 7));  // 100 * 2^6 = 6400 -> capped at 5000
        self::assertSame(5000, $method->invoke($connection, 10)); // capped
    }

    /**
     * Verifies requestWithHeaders uses HPUB and returns first reply message.
     */
    public function testRequestWithHeadersReturnsReply(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG _INBOX.any 1 2\r\nok\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $reply = $connection->requestWithHeaders('svc.echo', 'hi', ['X-Test' => '1'], 100)->await();

        self::assertSame('ok', $reply->payload);
        self::assertStringStartsWith('HPUB svc.echo _INBOX.', $transport->writes[3]);
        self::assertStringContainsString('X-Test:1', $transport->writes[3]);
    }

    public function testProcessIncomingRequiresOpenConnection(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Connection is not open');
        $connection->processIncoming()->await();
    }

    public function testUnsubscribeOnUnopenedConnectionIsSilentNoOp(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        // Must not throw: finally-based inbox cleanup runs on broken connections, and an exception
        // here would both leak the subscription entry and mask the caller's original error (#116).
        $connection->unsubscribe(1)->await();

        self::assertSame(ConnectionState::Idle, $connection->state());
    }

    public function testPublishWithHeadersRequiresOpenConnection(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Connection is not open');
        $connection->publishWithHeaders('orders.created', '{}', ['X' => '1'])->await();
    }

    public function testProcessIncomingThrowsOnErrFrame(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "-ERR 'Permissions Violation'\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Server sent error frame');
        $connection->processIncoming()->await();
    }

    public function testConnectUsesDefaultServerWhenListEmpty(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(servers: []), $transport);
        $connection->connect()->await();

        self::assertSame('tcp://127.0.0.1:4222|5000', $transport->connectCalls[0]);
    }

    public function testSubscribeRejectsEmbeddedWildcardToken(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Wildcards must occupy an entire token');
        $connection->subscribe('orders.a*', static function (NatsMessage $message): void {})->await();
    }

    public function testPublishRecoversAndRetriesAfterWriteFailure(): void
    {
        $transport = new class implements TransportInterface {
            /** @var list<string> */
            public array $connectCalls = [];
            /** @var list<string> */
            public array $writes = [];

            private int $connected = 0;
            private bool $failNextPub = true;
            /** @var array<int, list<string>> */
            private array $queues = [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ];

            public function connect(string $dsn, int $timeoutMs): \Amp\Future
            {
                return async(function () use ($dsn, $timeoutMs): void {
                    $this->connected++;
                    $this->connectCalls[] = $dsn . '|' . $timeoutMs;
                });
            }

            public function write(string $bytes): \Amp\Future
            {
                return async(function () use ($bytes): void {
                    if ($this->failNextPub && str_starts_with($bytes, 'PUB orders.created ')) {
                        $this->failNextPub = false;
                        throw new \RuntimeException('write failed');
                    }

                    $this->writes[] = $bytes;
                });
            }

            public function upgradeTls(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): \Amp\Future
            {
                return async(function (): string {
                    $index = max(0, $this->connected - 1);

                    return array_shift($this->queues[$index]) ?? '';
                });
            }

            public function close(): \Amp\Future
            {
                return async(static function (): void {});
            }
        };

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 1,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );
        $connection->connect()->await();

        $connection->publish('orders.created', '{"id":1}')->await();

        self::assertCount(2, $transport->connectCalls);
        self::assertStringContainsString('PUB orders.created ', implode('', $transport->writes));
    }

    public function testPublishWithHeadersRecoversAndRetriesAfterWriteFailure(): void
    {
        $transport = new class implements TransportInterface {
            /** @var list<string> */
            public array $connectCalls = [];
            /** @var list<string> */
            public array $writes = [];

            private int $connected = 0;
            private bool $failNextHpub = true;
            /** @var array<int, list<string>> */
            private array $queues = [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ];

            public function connect(string $dsn, int $timeoutMs): \Amp\Future
            {
                return async(function () use ($dsn, $timeoutMs): void {
                    $this->connected++;
                    $this->connectCalls[] = $dsn . '|' . $timeoutMs;
                });
            }

            public function write(string $bytes): \Amp\Future
            {
                return async(function () use ($bytes): void {
                    if ($this->failNextHpub && str_starts_with($bytes, 'HPUB orders.created ')) {
                        $this->failNextHpub = false;
                        throw new \RuntimeException('write failed');
                    }

                    $this->writes[] = $bytes;
                });
            }

            public function upgradeTls(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): \Amp\Future
            {
                return async(function (): string {
                    $index = max(0, $this->connected - 1);

                    return array_shift($this->queues[$index]) ?? '';
                });
            }

            public function close(): \Amp\Future
            {
                return async(static function (): void {});
            }
        };

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 1,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );
        $connection->connect()->await();

        $connection->publishWithHeaders('orders.created', '{"id":1}', ['X-Test' => '1'])->await();

        self::assertCount(2, $transport->connectCalls);
        self::assertStringContainsString('HPUB orders.created ', implode('', $transport->writes));
    }

    public function testPingTimerReconnectsWhenMaxOutstandingPingsExceeded(): void
    {
        $transport = new class implements TransportInterface {
            /** @var list<string> */
            public array $connectCalls = [];
            /** @var list<string> */
            public array $writes = [];

            private int $connected = 0;
            /** @var array<int, list<string>> */
            private array $queues = [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ];

            public function connect(string $dsn, int $timeoutMs): \Amp\Future
            {
                return async(function () use ($dsn, $timeoutMs): void {
                    $this->connected++;
                    $this->connectCalls[] = $dsn . '|' . $timeoutMs;
                });
            }

            public function write(string $bytes): \Amp\Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                });
            }

            public function upgradeTls(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): \Amp\Future
            {
                return async(function (): string {
                    $index = max(0, $this->connected - 1);

                    return array_shift($this->queues[$index]) ?? '';
                });
            }

            public function close(): \Amp\Future
            {
                return async(static function (): void {});
            }
        };

        $connection = new NatsConnection(
            new NatsOptions(
                pingIntervalSeconds: 0.05,
                maxPingsOut: 0,
                reconnectEnabled: true,
                maxReconnectAttempts: 1,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
            ),
            $transport,
        );
        $connection->connect()->await();

        // One tick + reconnect; must end before the restarted timer's next tick (maxPingsOut=0
        // reconnects on every tick).
        delay(0.08);

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->connectCalls);

        $connection->disconnect()->await();
    }

    public function testReconnectRetriesWhenResubscribeGetsFatalServerError(): void
    {
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    '__THROW__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    "-ERR 'Authorization Violation'\r\n",
                ],
                [
                    'INFO {"server_id":"S3","server_name":"n3","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    "MSG updates 1 5\r\nhello\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );
        $connection->connect()->await();

        $received = [];
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        self::assertSame(0, $connection->processIncoming()->await());
        self::assertSame(ConnectionState::Open, $connection->state());

        $connection->processIncoming()->await();

        self::assertSame(['hello'], $received);
        self::assertCount(3, $transport->connectCalls);

        $subWrites = array_values(array_filter(
            $transport->writes,
            static fn(string $write): bool => str_starts_with($write, 'SUB updates 1'),
        ));
        self::assertCount(3, $subWrites);
        self::assertSame('S3', $connection->serverInfo()?->serverId);
    }

    public function testPingTimerWriteFailureReconnectsWhenEnabled(): void
    {
        $transport = new class implements TransportInterface {
            /** @var list<string> */
            public array $connectCalls = [];

            private int $connected = 0;
            private bool $failFirstPing = true;
            /** @var array<int, list<string>> */
            private array $queues = [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                ],
            ];

            public function connect(string $dsn, int $timeoutMs): \Amp\Future
            {
                return async(function () use ($dsn, $timeoutMs): void {
                    $this->connected++;
                    $this->connectCalls[] = $dsn . '|' . $timeoutMs;
                });
            }

            public function write(string $bytes): \Amp\Future
            {
                return async(function () use ($bytes): void {
                    if ($this->failFirstPing && $bytes === "PING\r\n") {
                        $this->failFirstPing = false;
                        throw new \RuntimeException('ping write failed');
                    }
                });
            }

            public function upgradeTls(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): \Amp\Future
            {
                return async(function (): string {
                    $index = max(0, $this->connected - 1);

                    return array_shift($this->queues[$index]) ?? '';
                });
            }

            public function close(): \Amp\Future
            {
                return async(static function (): void {});
            }
        };

        $connection = new NatsConnection(
            new NatsOptions(
                pingIntervalSeconds: 0.05,
                maxPingsOut: 3,
                reconnectEnabled: true,
                maxReconnectAttempts: 1,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
            ),
            $transport,
        );
        $connection->connect()->await();

        delay(0.15);

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->connectCalls);

        $connection->disconnect()->await();
    }

    /**
     * Verifies handshake succeeds when the INFO frame arrives in multiple TCP segments.
     *
     * Simulates a NATS 2.10+ server whose INFO frame (containing the xkey field) is large
     * enough to be split by TCP into two reads. The first read delivers a partial JSON payload
     * with no CRLF terminator; the second read delivers the remainder with the terminator.
     * The ProtocolParser must buffer the first chunk and complete the frame on the second read.
     */
    public function testConnectHandlesFragmentedInfoFrame(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.0","jetstream":true,"max_payload":1048576,"headers":true,"xkey":"XNEYS5JGCB',
            "ID6OKSWDBK6DRYIBYQX3NWJQQSXMVDGU3DPK\"}\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(reconnectEnabled: false),
            $transport,
        );

        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * Verifies handshake succeeds when a re-INFO frame during the PONG phase is complete.
     *
     * This is the non-fragmented counterpart to the fragmented re-INFO regression test and
     * ensures awaitInitialPong() still handles a normal server INFO update before the final
     * PONG arrives.
     */
    public function testConnectHandlesNonFragmentedReInfoDuringPongPhase(): void
    {
        $transport = new FakeTransport([
            "INFO {\"server_id\":\"S1\",\"server_name\":\"n1\",\"version\":\"2.10.0\",\"jetstream\":true,\"max_payload\":1048576,\"headers\":true}\r\n",
            "INFO {\"server_id\":\"S1\",\"server_name\":\"n1\",\"version\":\"2.10.1\",\"jetstream\":true,\"max_payload\":2097152,\"headers\":true,\"xkey\":\"XNEYS5JGCBID6OKSWDBK6DRYIBYQX3NWJQQSXMVDGU3DPK\"}\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(reconnectEnabled: false),
            $transport,
        );

        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(2097152, $connection->serverInfo()?->maxPayload);
    }

    /**
     * Verifies handshake succeeds when a re-INFO frame during the PONG phase is fragmented.
     *
     * After the initial INFO exchange the server may send a second INFO (e.g. after TLS upgrade)
     * during the PONG phase. If that re-INFO contains the xkey field it can also be split across
     * TCP segments. The partial chunk must be buffered by the ProtocolParser and not parsed
     * directly via the raw-chunk fallback, which would receive truncated JSON and fail.
     */
    public function testConnectHandlesFragmentedReInfoDuringPongPhase(): void
    {
        $transport = new FakeTransport([
            "INFO {\"server_id\":\"S1\",\"server_name\":\"n1\",\"version\":\"2.10.0\",\"jetstream\":true,\"max_payload\":1048576,\"headers\":true}\r\n",
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.0","jetstream":true,"max_payload":1048576,"headers":true,"xkey":"XNEYS5JGCB',
            "ID6OKSWDBK6DRYIBYQX3NWJQQSXMVDGU3DPK\"}\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(reconnectEnabled: false),
            $transport,
        );

        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
    }

    // ─── Reply / queue-group injection guards (P0-3) ────────────────────

    public function testPublishRejectsReplyToWithCrlfInjection(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Subject must not contain whitespace');
        $connection->publish('orders.created', 'data', "reply\r\nPUB hack 0\r\n")->await();
    }

    public function testPublishWithHeadersRejectsReplyToWithCrlfInjection(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Subject must not contain whitespace');
        $connection->publishWithHeaders('orders.created', 'data', ['X' => '1'], "reply\r\nPUB hack 0\r\n")->await();
    }

    public function testPublishAcceptsValidReplyTo(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $connection->publish('orders.created', 'data', '_INBOX.reply.1')->await();

        self::assertSame("PUB orders.created _INBOX.reply.1 4\r\ndata\r\n", $transport->writes[2]);
    }

    public function testSubscribeRejectsQueueGroupWithWhitespace(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Queue group must not contain whitespace');
        $connection->subscribe('tasks.process', static function (NatsMessage $message): void {}, "workers\r\nSUB hack 99")->await();
    }

    public function testSubscribeRejectsEmptyQueueGroup(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Queue group must not be empty');
        $connection->subscribe('tasks.process', static function (NatsMessage $message): void {}, '')->await();
    }

    // ─── Cancellable reads / no orphaned read on timeout (P0-1) ─────────

    public function testRequestTimeoutCancelsReadAndAllowsSubsequentRequest(): void
    {
        $transport = new class implements TransportInterface {
            /** @var list<string> */
            public array $writes = [];
            public int $maxConcurrentReads = 0;
            public int $cancelledReads = 0;
            private int $activeReads = 0;
            /** @var list<string> */
            public array $queue = [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                "PONG\r\n",
            ];

            public function connect(string $dsn, int $timeoutMs): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function write(string $bytes): \Amp\Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                });
            }

            public function upgradeTls(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): \Amp\Future
            {
                return async(function () use ($cancellation): string {
                    if ($this->queue !== []) {
                        return (string) array_shift($this->queue);
                    }

                    // No data: behave like a real, cancellable long-blocking socket read.
                    $this->activeReads++;
                    $this->maxConcurrentReads = max($this->maxConcurrentReads, $this->activeReads);

                    try {
                        delay(30, cancellation: $cancellation ?? new \Amp\NullCancellation());

                        return '';
                    } catch (\Amp\CancelledException $e) {
                        $this->cancelledReads++;

                        throw $e;
                    } finally {
                        $this->activeReads--;
                    }
                });
            }

            public function close(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function pushReply(string $chunk): void
            {
                $this->queue[] = $chunk;
            }
        };

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        // First request times out: the underlying read must be cancelled, not orphaned.
        try {
            $connection->request('svc.echo', 'x', 20)->await();
            self::fail('Expected timeout');
        } catch (TimeoutException) {
            // expected
        }

        self::assertGreaterThanOrEqual(1, $transport->cancelledReads);

        // A subsequent request must succeed with no reconnect and no overlapping read.
        $transport->pushReply("MSG _INBOX.any 2 2\r\nok\r\n");

        $reply = $connection->request('svc.echo', 'x', 500)->await();

        self::assertSame('ok', $reply->payload);
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertLessThanOrEqual(1, $transport->maxConcurrentReads);
    }

    // ─── Idle heartbeat does not self-disconnect (P0-2) ─────────────────

    public function testIdleConnectionStaysOpenViaHeartbeatSelfRead(): void
    {
        // Live-server fake: every PING write produces a PONG on the next read. The application
        // never calls processIncoming(), so without the heartbeat self-read the outstanding-ping
        // counter would exceed maxPingsOut and (with reconnect disabled) close the connection.
        $transport = new class implements TransportInterface {
            /** @var list<string> */
            public array $writes = [];
            public int $pings = 0;
            /** @var list<string> */
            private array $queue = [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            ];

            public function connect(string $dsn, int $timeoutMs): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function write(string $bytes): \Amp\Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                    if ($bytes === "PING\r\n") {
                        $this->pings++;
                        $this->queue[] = "PONG\r\n";
                    }
                });
            }

            public function upgradeTls(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): \Amp\Future
            {
                return async(function () use ($cancellation): string {
                    if ($this->queue !== []) {
                        return (string) array_shift($this->queue);
                    }

                    delay(30, cancellation: $cancellation ?? new \Amp\NullCancellation());

                    return '';
                });
            }

            public function close(): \Amp\Future
            {
                return async(static function (): void {});
            }
        };

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0.05, maxPingsOut: 1, reconnectEnabled: false),
            $transport,
        );
        $connection->connect()->await();

        // Let several heartbeat ticks elapse without any application processIncoming() calls.
        delay(0.2);

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertGreaterThanOrEqual(2, $transport->pings);

        $connection->disconnect()->await();
    }

    /**
     * Verifies a message captured during the heartbeat self-read is delivered to its subscription
     * immediately (via drainAllPending) rather than left buffered until the next processIncoming().
     */
    public function testHeartbeatResponseDeliversBufferedMessageImmediately(): void
    {
        $transport = new class implements TransportInterface {
            /** @var list<string> */
            public array $writes = [];
            /** @var list<string> */
            private array $queue = [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            ];

            public function connect(string $dsn, int $timeoutMs): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function write(string $bytes): \Amp\Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                    if ($bytes === "PING\r\n") {
                        $this->queue[] = "PONG\r\n";
                    }
                });
            }

            public function upgradeTls(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): \Amp\Future
            {
                return async(function () use ($cancellation): string {
                    if ($this->queue !== []) {
                        return (string) array_shift($this->queue);
                    }

                    delay(30, cancellation: $cancellation ?? new \Amp\NullCancellation());

                    return '';
                });
            }

            public function close(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function enqueue(string $chunk): void
            {
                $this->queue[] = $chunk;
            }
        };

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 30, maxPingsOut: 5, reconnectEnabled: false),
            $transport,
        );
        $connection->connect()->await();

        $received = null;
        $connection->subscribe('events', static function (NatsMessage $message) use (&$received): void {
            $received = $message;
        })->await();

        // Stage a delivery that the heartbeat self-read will pick up, then run that read directly.
        $transport->enqueue("MSG events 1 7\r\nupdated\r\n");
        (new \ReflectionMethod($connection, 'consumeHeartbeatResponse'))->invoke($connection);

        // Without drainAllPending() in consumeHeartbeatResponse() this would still be null.
        self::assertInstanceOf(NatsMessage::class, $received);
        self::assertSame('events', $received->subject);
        self::assertSame('updated', $received->payload);

        $connection->disconnect()->await();
    }

    public function testProcessIncomingSkipsWhenAnotherReadIsInProgress(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        // Simulate a read already owning the socket (e.g. the heartbeat self-read).
        (new \ReflectionProperty($connection, 'readInProgress'))->setValue($connection, true);

        // processIncoming() must not start a second overlapping read; it reports zero frames.
        self::assertSame(0, $connection->processIncoming()->await());
    }

    public function testHeartbeatReadSkippedWhenAnotherReadIsInProgress(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        (new \ReflectionProperty($connection, 'readInProgress'))->setValue($connection, true);

        // The heartbeat self-read must yield to the in-flight read rather than colliding with it.
        (new \ReflectionMethod($connection, 'consumeHeartbeatResponse'))->invoke($connection);

        self::assertSame(ConnectionState::Open, $connection->state());
    }

    public function testHeartbeatReadHandlesEmptyErrorAndFatalFrames(): void
    {
        $transport = new class implements TransportInterface {
            /** @var list<string> */
            public array $writes = [];
            public string $mode = 'empty';
            /** @var list<string> */
            private array $queue = [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            ];

            public function connect(string $dsn, int $timeoutMs): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function write(string $bytes): \Amp\Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                    if ($bytes === "PING\r\n") {
                        $this->queue[] = "PONG\r\n";
                    }
                });
            }

            public function upgradeTls(): \Amp\Future
            {
                return async(static function (): void {});
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): \Amp\Future
            {
                return async(function (): string {
                    if ($this->queue !== []) {
                        return (string) array_shift($this->queue);
                    }

                    return match ($this->mode) {
                        'throw' => throw new \RuntimeException('transient read error'),
                        'fatal' => "-ERR 'fatal boom'\r\n",
                        default => '',
                    };
                });
            }

            public function close(): \Amp\Future
            {
                return async(static function (): void {});
            }
        };

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        $invoke = new \ReflectionMethod($connection, 'consumeHeartbeatResponse');

        $transport->mode = 'empty';
        $invoke->invoke($connection); // empty read returns early

        $transport->mode = 'throw';
        $invoke->invoke($connection); // a transient read error is swallowed

        $transport->mode = 'fatal';
        $invoke->invoke($connection); // a fatal -ERR frame is swallowed rather than thrown out of the timer

        // None of these escalate out of the heartbeat read or close the connection.
        self::assertSame(ConnectionState::Open, $connection->state());

        $connection->disconnect()->await();
    }

    public function testProcessIncomingResetsPingCounterOnlyOnPong(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 5\r\nhello\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();
        $connection->subscribe('updates', static function (NatsMessage $message): void {})->await();

        $pings = new \ReflectionProperty($connection, 'outstandingPings');
        $pings->setValue($connection, 2);

        // A data frame is inbound traffic but NOT a PONG: it must not reset the watchdog counter
        // (otherwise a server that stops answering PINGs but still sends data is never detected).
        $connection->processIncoming()->await();
        self::assertSame(2, $pings->getValue($connection));

        // Only an actual PONG resets it.
        $connection->processIncoming()->await();
        self::assertSame(0, $pings->getValue($connection));
    }

    public function testDrainContinuesPastTransientEmptyReadUntilPong(): void
    {
        $received = [];
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            '', // a transient 0-frame read during the flush must NOT end it early ...
            "MSG events 1 3\r\nabc\r\n", // ... so this server-flushed delivery is still delivered ...
            "PONG\r\n", // ... and the flush ends only on the PONG.
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();
        $connection->subscribe('events', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        $connection->drain()->await();

        self::assertContains('abc', $received);
    }

    // ─── TLS upgrade ordering (P1-4) ────────────────────────────────────

    public function testStandardTlsUpgradeRunsAfterInfoWhenNotHandshakeFirst(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true,"tls_required":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // The transport has TLS materials, so upgradeTls() actually establishes TLS.
        $transport->canUpgrade = true;

        $connection = new NatsConnection(
            new NatsOptions(tlsRequired: true, tlsHandshakeFirst: false, pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        // Post-INFO upgrade path performs exactly one explicit TLS upgrade and TLS is now active.
        self::assertSame(1, $transport->upgradeTlsCalls);
        self::assertTrue($transport->tlsActive());
    }

    public function testServerRequiresTlsButNoMaterialsFailsBeforeWritingConnect(): void
    {
        // Server advertises tls_required; the client did NOT configure TLS (tlsRequired:false). The
        // client must fail fast and must NOT write CONNECT/PING (credentials) over a plaintext socket.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true,"tls_required":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $transport->canUpgrade = false; // no TLS materials -> upgradeTls cannot establish TLS

        $connection = new NatsConnection(
            new NatsOptions(tlsRequired: false, tlsHandshakeFirst: false, reconnectEnabled: false, pingIntervalSeconds: 0),
            $transport,
        );

        try {
            $connection->connect()->await();
            self::fail('Expected a TLS ConnectionException');
        } catch (ConnectionException $e) {
            self::assertStringContainsStringIgnoringCase('tls', $e->getMessage());
        }

        $writes = implode('', $transport->writes);
        self::assertStringNotContainsString('CONNECT ', $writes);
        self::assertStringNotContainsString('PING', $writes);
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    public function testServerRequiresTlsUpgradesThenSendsConnect(): void
    {
        // Server advertises tls_required and the transport has TLS materials: it upgrades, then the
        // CONNECT is written only after TLS is active.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true,"tls_required":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $transport->canUpgrade = true;

        $connection = new NatsConnection(
            new NatsOptions(tlsRequired: false, tlsHandshakeFirst: false, reconnectEnabled: false, pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        self::assertSame(1, $transport->upgradeTlsCalls);
        self::assertTrue($transport->tlsActive());
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertStringContainsString('CONNECT ', implode('', $transport->writes));
    }

    public function testHandshakeFirstDoesNotCallExplicitUpgrade(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $transport->tlsActiveOnConnect = true; // handshake-first established TLS during connect()

        $connection = new NatsConnection(
            new NatsOptions(tlsRequired: true, tlsHandshakeFirst: true, pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        // Handshake-first negotiates TLS during connect(), so no explicit post-INFO upgrade.
        self::assertSame(0, $transport->upgradeTlsCalls);
        self::assertTrue($transport->tlsActive());
    }

    public function testHandshakeFirstWithoutEstablishedTlsFailsBeforeWritingConnect(): void
    {
        // Misconfiguration: tlsHandshakeFirst=true but no TLS materials/scheme (so the transport stays
        // plaintext), while the server's INFO advertises tls_required. The credentials fail-safe must
        // still fire on the handshake-first path and must NOT write CONNECT over the plaintext socket.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true,"tls_required":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $transport->tlsActiveOnConnect = false; // handshake-first did NOT establish TLS

        $connection = new NatsConnection(
            new NatsOptions(tlsRequired: false, tlsHandshakeFirst: true, reconnectEnabled: false, pingIntervalSeconds: 0),
            $transport,
        );

        try {
            $connection->connect()->await();
            self::fail('Expected a TLS ConnectionException');
        } catch (ConnectionException $e) {
            self::assertStringContainsStringIgnoringCase('tls', $e->getMessage());
        }

        $writes = implode('', $transport->writes);
        self::assertStringNotContainsString('CONNECT ', $writes);
        self::assertStringNotContainsString('PING', $writes);
        // Handshake-first never calls the explicit post-INFO upgrade.
        self::assertSame(0, $transport->upgradeTlsCalls);
    }

    public function testPlainConnectionDoesNotUpgradeTls(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(0, $transport->upgradeTlsCalls);
    }

    public function testTlsContextForcesUpgradeEvenWhenServerDoesNotAdvertiseTlsRequired(): void
    {
        // #95: a configured tlsContext is documented to imply TLS-required. The server's INFO does NOT
        // advertise tls_required and the DSN is plaintext (nats://), yet the client must still upgrade
        // to TLS before writing CONNECT (which carries credentials) - otherwise credentials leak in
        // cleartext despite the user having configured a TLS context.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $transport->canUpgrade = true;

        $connection = new NatsConnection(
            new NatsOptions(
                tlsRequired: false,
                tlsHandshakeFirst: false,
                tlsContext: new ClientTlsContext(),
                reconnectEnabled: false,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );
        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $transport->upgradeTlsCalls);
        self::assertTrue($transport->tlsActive());
        self::assertStringContainsString('CONNECT ', implode('', $transport->writes));
    }

    public function testTlsContextWithoutEstablishedTlsFailsBeforeWritingConnect(): void
    {
        // #95: when a tlsContext is configured but the handshake cannot establish TLS (canUpgrade=false),
        // the credentials fail-safe must fire and CONNECT/PING must NOT be written over the plaintext
        // socket - even though neither tlsRequired nor the server's INFO requested TLS.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $transport->canUpgrade = false;

        $connection = new NatsConnection(
            new NatsOptions(
                tlsRequired: false,
                tlsHandshakeFirst: false,
                tlsContext: new ClientTlsContext(),
                reconnectEnabled: false,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );

        try {
            $connection->connect()->await();
            self::fail('Expected a TLS ConnectionException');
        } catch (ConnectionException $e) {
            self::assertStringContainsStringIgnoringCase('tls', $e->getMessage());
        }

        $writes = implode('', $transport->writes);
        self::assertStringNotContainsString('CONNECT ', $writes);
        self::assertStringNotContainsString('PING', $writes);
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(1, $transport->upgradeTlsCalls);
    }

    // ─── New coverage additions ──────────────────────────────────────────

    /**
     * rtt() throws when connection is not open.
     */
    public function testRttThrowsWhenConnectionNotOpen(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Connection is not open');
        $connection->rtt()->await();
    }

    /**
     * drain() swallows a fatal frame error mid-flush and still closes.
     */
    public function testDrainSwallowsFatalFrameErrorMidFlush(): void
    {
        // The drain flush read returns a fatal -ERR that would normally throw; drain() must swallow it
        // and still close cleanly.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "-ERR 'Maximum Connections Exceeded'\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0, requestTimeoutMs: 200),
            $transport,
        );
        $connection->connect()->await();
        $connection->subscribe('events', function (): void {})->await();

        // drain() should not throw even though a fatal frame arrives mid-flush.
        $connection->drain()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * publishWithHeaders() buffers frame when reconnecting and records outbound stats.
     */
    public function testPublishWithHeadersBuffersDuringReconnectAndRecordsOutbound(): void
    {
        $info = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";
        $release = new DeferredFuture();

        $transport = new class ($info, $release) implements TransportInterface {
            /** @var list<string> */
            public array $writes = [];
            private int $connects = 0;
            /** @var list<list<string>> */
            private array $reads;

            /** @param DeferredFuture<void> $release */
            public function __construct(string $info, private DeferredFuture $release)
            {
                $this->reads = [
                    [$info, "PONG\r\n", '__EOF__'],
                    [$info, "PONG\r\n"],
                ];
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function (): void {
                    if ($this->connects === 1) {
                        $this->release->getFuture()->await();
                    }
                    $this->connects++;
                });
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                });
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $conn = max(0, $this->connects - 1);
                    $next = array_shift($this->reads[$conn]) ?? '';
                    if ($next === '__EOF__') {
                        throw new TransportClosedException('eof');
                    }
                    return $next;
                });
            }

            public function close(): Future
            {
                return async(static fn(): null => null);
            }
        };

        $connection = new NatsConnection(new NatsOptions(reconnectDelayMs: 1, reconnectJitterMs: 0), $transport);
        $connection->connect()->await();

        // Drive the read loop in the background: hits EOF, starts reconnect, blocks in connect().
        $pump = async(static fn(): int => $connection->processIncoming()->await());
        delay(0.05);

        self::assertSame(ConnectionState::Connecting, $connection->state());

        // publishWithHeaders during reconnect: must buffer (not throw) and record outbound stats.
        $connection->publishWithHeaders('a.b', 'data', ['X-H' => 'v'])->await();

        $statsBeforeRelease = $connection->statistics();
        self::assertSame(1, $statsBeforeRelease->outMsgs);

        $release->complete();
        $pump->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        // The buffered HPUB was flushed on reconnect.
        self::assertStringContainsString('HPUB a.b ', implode('', $transport->writes));
    }

    /**
     * bufferFrame() returns false when the buffer would overflow.
     */
    public function testPublishBufferOverflowThrowsDuringReconnect(): void
    {
        $info = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";
        $release = new DeferredFuture();

        $transport = new class ($info, $release) implements TransportInterface {
            /** @var list<string> */
            public array $writes = [];
            private int $connects = 0;
            /** @var list<list<string>> */
            private array $reads;

            /** @param DeferredFuture<void> $release */
            public function __construct(string $info, private DeferredFuture $release)
            {
                $this->reads = [
                    [$info, "PONG\r\n", '__EOF__'],
                    [$info, "PONG\r\n"],
                ];
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function (): void {
                    if ($this->connects === 1) {
                        $this->release->getFuture()->await();
                    }
                    $this->connects++;
                });
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                });
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $conn = max(0, $this->connects - 1);
                    $next = array_shift($this->reads[$conn]) ?? '';
                    if ($next === '__EOF__') {
                        throw new TransportClosedException('eof');
                    }
                    return $next;
                });
            }

            public function close(): Future
            {
                return async(static fn(): null => null);
            }
        };

        // reconnectBufferSize is tiny (1 byte) so any real publish frame overflows.
        $connection = new NatsConnection(
            new NatsOptions(reconnectDelayMs: 1, reconnectJitterMs: 0, reconnectBufferSize: 1),
            $transport,
        );
        $connection->connect()->await();

        $pump = async(static fn(): int => $connection->processIncoming()->await());
        delay(0.05);

        self::assertSame(ConnectionState::Connecting, $connection->state());

        // The buffer is only 1 byte; a publish frame won't fit -> bufferFrame() returns false -> throw.
        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Connection is not open');

        try {
            $connection->publish('a.b', 'payload')->await();
        } finally {
            $release->complete();
            $pump->await();
        }
    }

    /**
     * subscribe() throws when connection is not open.
     */
    public function testSubscribeThrowsWhenConnectionNotOpen(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Connection is not open');
        $connection->subscribe('events', static function (NatsMessage $message): void {})->await();
    }

    /**
     * drainSubscription() on a closed connection drops local state and returns cleanly.
     */
    public function testDrainSubscriptionOnClosedConnectionDropsStateAndReturns(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        // Force state to Closed (as if disconnect() was called).
        $connection->disconnect()->await();
        self::assertSame(ConnectionState::Closed, $connection->state());

        // drainSubscription() with a non-existent sid on a closed connection must not throw.
        $connection->drainSubscription(999)->await();
        // No assertion beyond "did not throw".
    }

    /**
     * drainSubscription() returns early when the SID is not registered.
     */
    public function testDrainSubscriptionOnUnknownSidReturnsEarly(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        // SID 999 was never subscribed; drainSubscription() should return without sending UNSUB/PING.
        $writesBefore = count($transport->writes);
        $connection->drainSubscription(999)->await();

        // No extra wire commands were emitted.
        self::assertSame($writesBefore, count($transport->writes));
    }

    /**
     * drainSubscription() swallows a flush failure and still drops the subscription.
     */
    public function testDrainSubscriptionSwallowsFlushFailureAndDropsSub(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // No PONG response for flush -> flush times out, but drainSubscription() must still succeed.
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0, requestTimeoutMs: 100),
            $transport,
        );
        $connection->connect()->await();

        $sid = $connection->subscribe('events', static function (NatsMessage $message): void {})->await();

        // drainSubscription() sends UNSUB then flush(); flush times out (no PONG in queue) -> swallowed.
        $connection->drainSubscription($sid)->await();

        // Subscription state must be gone after the (failed) flush.
        $meta = (new \ReflectionProperty(NatsConnection::class, 'subscriptionMeta'))->getValue($connection);
        self::assertArrayNotHasKey($sid, $meta);
    }

    /**
     * flush() throws when connection is not open.
     */
    public function testFlushThrowsWhenConnectionNotOpen(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Connection is not open');
        $connection->flush()->await();
    }

    /**
     * flush() throws TimeoutException when server PONG never arrives.
     */
    public function testFlushTimesOutWhenNoPongArrives(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // No PONG for the flush PING -> TimeoutException.
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0, requestTimeoutMs: 100),
            $transport,
        );
        $connection->connect()->await();

        $this->expectException(TimeoutException::class);
        $this->expectExceptionMessage('Flush timed out');
        $connection->flush()->await();
    }

    /**
     * requestInternal() throws when connection is not open (via request()).
     */
    public function testRequestThrowsWhenConnectionNotOpen(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Connection is not open');
        $connection->request('svc.echo', 'hello', 100)->await();
    }

    /**
     * requestMany() throws when maxResponses < 1.
     */
    public function testRequestManyThrowsWhenMaxResponsesLessThanOne(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('maxResponses must be at least 1');
        $connection->requestMany('svc.scan', 'q', null, 0)->await();
    }

    /**
     * requestMany() throws when stallMs <= 0.
     */
    public function testRequestManyThrowsWhenStallMsNotPositive(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('stallMs must be greater than zero');
        $connection->requestMany('svc.scan', 'q', null, null, null, 0)->await();
    }

    /**
     * requestManyInternal() throws ConnectionException when connection is not open.
     */
    public function testRequestManyThrowsWhenConnectionNotOpen(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Connection is not open');
        $connection->requestMany('svc.scan', 'q')->await();
    }

    /**
     * requestManyInternal() throws TimeoutException when totalTimeoutMs <= 0.
     * This is reached only via a requestTimeoutMs of 0, but NatsOptions disallows that,
     * so we pass totalTimeoutMs explicitly.
     */
    public function testRequestManyThrowsWhenTotalTimeoutIsZero(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $this->expectException(TimeoutException::class);
        $this->expectExceptionMessage('Request timeout must be greater than zero');
        // Pass totalTimeoutMs=0 which directly hits the non-positive-timeout guard.
        $connection->requestMany('svc.scan', 'q', null, null, 0)->await();
    }

    /**
     * requestManyInternal() uses HPUB when headers are supplied.
     */
    public function testRequestManyWithHeadersUsesHpub(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG _INBOX.any 1 2\r\nok\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $replies = $connection->requestMany('svc.scan', 'q', ['X-H' => '1'], 1, 1000)->await();

        self::assertCount(1, $replies);
        // The publish frame must be HPUB (not PUB) because headers were supplied.
        self::assertStringContainsString('HPUB svc.scan _INBOX.', implode('', $transport->writes));
    }

    /**
     * connectOnce() seeds knownConnectUrls from the initial INFO connect_urls.
     */
    public function testConnectSeedsKnownConnectUrlsFromInitialInfo(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true,"connect_urls":["10.0.0.2:4222","10.0.0.3:4222"]}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0, reconnectEnabled: false), $transport);
        $connection->connect()->await();

        // discoveredServers() should be pre-seeded from the initial INFO connect_urls.
        self::assertSame(['10.0.0.2:4222', '10.0.0.3:4222'], $connection->discoveredServers());
    }

    /**
     * serverPool() deduplicates and normalizes bare host:port discovered URLs.
     * Also indirectly verifies the discovered server is dialable after seeding.
     */
    public function testServerPoolNormalizesAndDeduplicatesDiscoveredUrls(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true,"connect_urls":["10.0.0.2:4222","10.0.0.2:4222","nats://10.0.0.3:4222"]}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(pingIntervalSeconds: 0, reconnectEnabled: false, servers: ['nats://10.0.0.1:4222']),
            $transport,
        );
        $connection->connect()->await();

        $pool = (new \ReflectionMethod($connection, 'serverPool'))->invoke($connection);

        // Configured server + 2 unique discovered peers (bare host normalized to nats://, schema-prefixed left as-is).
        self::assertContains('nats://10.0.0.1:4222', $pool);
        self::assertContains('nats://10.0.0.2:4222', $pool);
        self::assertContains('nats://10.0.0.3:4222', $pool);
        // Duplicate 10.0.0.2:4222 must be collapsed to one entry.
        self::assertSame(1, count(array_filter($pool, static fn(string $u): bool => $u === 'nats://10.0.0.2:4222')));
    }

    /**
     * retryInitialConnect() delays between attempts.
     * retryInitialConnect() fails fast on AuthenticationException.
     */
    public function testRetryInitialConnectFailsFastOnAuthError(): void
    {
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "-ERR Authorization Violation\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: false,
                retryOnFailedInitialConnect: true,
                maxReconnectAttempts: 5,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
            ),
            $transport,
        );

        try {
            $connection->connect()->await();
            self::fail('Expected AuthenticationException');
        } catch (\IDCT\NATS\Exception\AuthenticationException $e) {
            self::assertStringContainsString('authentication', strtolower($e->getMessage()));
        }

        // Auth error: must not exhaust all 5 retry attempts.
        self::assertSame(ConnectionState::Closed, $connection->state());
        // Only one successful connect attempt was made (the retry loop started but the auth error aborted it).
        self::assertSame(1, count($transport->connectCalls));
    }

    /**
     * retryInitialConnect() returns false after exhausting all attempts.
     */
    public function testRetryInitialConnectReturnsFalseWhenExhausted(): void
    {
        // All connect attempts fail -> retryInitialConnect() returns false -> connect() closes and throws.
        $transport = new FlakyTransport(
            readQueuesByConnection: [],
            connectFailures: 10, // more than maxReconnectAttempts
            readFailures: 0,
        );

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: false,
                retryOnFailedInitialConnect: true,
                maxReconnectAttempts: 2,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
            ),
            $transport,
        );

        $this->expectException(ConnectionException::class);
        $connection->connect()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * performRecovery() fails fast on AuthenticationException during reconnect.
     */
    public function testReconnectFailsFastOnAuthDuringReconnect(): void
    {
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    '__THROW__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "-ERR Authorization Violation\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $events = [];
        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 5,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
                connectionListener: static function (ConnectionEvent $e, ?\Throwable $err) use (&$events): void {
                    $events[] = $e;
                },
            ),
            $transport,
        );
        $connection->connect()->await();

        try {
            $connection->processIncoming()->await();
        } catch (\Throwable) {
            // recoverConnection() rethrows the AuthenticationException.
        }

        // Auth errors stop the reconnect loop and close the connection.
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertContains(ConnectionEvent::Closed, $events);
        // Only 2 connect calls: initial + one failed reconnect attempt.
        self::assertSame(2, count($transport->connectCalls));
    }

    /**
     * drainImmediateServerFrames() returns on CancelledException (poll timeout).
     * drainImmediateServerFrames() skips +OK frames.
     * This is exercised indirectly during resubscribeAll() on reconnect.
     */
    public function testDrainImmediateServerFramesHandlesOkAndTimeout(): void
    {
        // After reconnect, server sends +OK response to the replayed SUB (then times out on next poll).
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    '__THROW__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    "+OK\r\n",
                    // Queue exhausted -> subsequent poll reads return '' (treated as CancelledException poll timeout).
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $options = new NatsOptions(reconnectEnabled: true, maxReconnectAttempts: 3, reconnectDelayMs: 1, reconnectJitterMs: 0, pingIntervalSeconds: 0);
        $connection = new NatsConnection($options, $transport);
        $connection->connect()->await();

        $connection->subscribe('updates', static function (NatsMessage $message): void {})->await();

        // processIncoming() triggers reconnect; resubscribeAll() calls drainImmediateServerFrames()
        // which reads +OK (skips it) and then times out on the next poll (CancelledException) and returns.
        self::assertSame(0, $connection->processIncoming()->await());

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->connectCalls);
    }

    /**
     * awaitServerInfo() throws when an -ERR arrives instead of INFO.
     */
    public function testConnectFailsWhenErrArrivesInsteadOfInfo(): void
    {
        // Transport sends -ERR as the very first frame (no INFO at all).
        $transport = new FakeTransport([
            "-ERR 'Maximum Connections Exceeded'\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false, pingIntervalSeconds: 0), $transport);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Server error during connect');
        $connection->connect()->await();
    }

    /**
     * readHandshakeChunk() returns null when remaining deadline is <= 0.
     */
    public function testConnectHandlesExpiredDeadlineDuringHandshakeRead(): void
    {
        // The transport returns '' on each read (empty, non-blocking), so the handshake poll loop
        // exhausts its budget and eventually times out with "Expected PONG after CONNECT".
        // This exercises readHandshakeChunk() returning null when remainingMs <= 0.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            // All reads return '' (never delivers PONG), exhausting the budget.
        ]);

        $connection = new NatsConnection(
            new NatsOptions(reconnectEnabled: false, connectTimeoutMs: 10, pingIntervalSeconds: 0),
            $transport,
        );

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Expected PONG after CONNECT');
        $connection->connect()->await();
    }

    /**
     * isRecoverableServerError() returns true for "invalid subject".
     */
    public function testProcessIncomingTreatsInvalidSubjectErrAsRecoverable(): void
    {
        $errors = [];
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "-ERR 'Invalid Subject'\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(
                pingIntervalSeconds: 0,
                errorListener: static function (\Throwable $err) use (&$errors): void {
                    $errors[] = $err->getMessage();
                },
            ),
            $transport,
        );
        $connection->connect()->await();

        // "Invalid Subject" must be treated as recoverable: connection stays open, error listener notified.
        $connection->processIncoming()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(1, $errors);
        self::assertStringContainsString('invalid subject', strtolower($errors[0]));
    }

    /**
     * consumeHeartbeatResponse() catches recoverConnection() exception and marks closed.
     */
    public function testConsumeHeartbeatResponseMarksClosedWhenRecoveryFails(): void
    {
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                    "PONG\r\n",
                    '__EOF__',
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $connection = new NatsConnection(
            new NatsOptions(reconnectEnabled: false, pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        // consumeHeartbeatResponse() reads EOF -> calls recoverConnection() -> reconnect disabled -> throws.
        // The catch block must absorb the exception and mark state as Closed.
        (new \ReflectionMethod($connection, 'consumeHeartbeatResponse'))->invoke($connection);

        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * emitEvent() swallows exceptions thrown by the connection listener.
     */
    public function testConnectionListenerExceptionIsSwallowed(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(
                pingIntervalSeconds: 0,
                connectionListener: static function (ConnectionEvent $e, ?\Throwable $err): void {
                    throw new \RuntimeException('listener exploded');
                },
            ),
            $transport,
        );

        // connect() calls emitEvent(Connected); the throwing listener must be swallowed.
        $connection->connect()->await();
        // disconnect() calls emitEvent(Closed); same expectation.
        $connection->disconnect()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * emitError() swallows exceptions thrown by the error listener.
     */
    public function testErrorListenerExceptionIsSwallowed(): void
    {
        $errors = [];
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "-ERR 'Permissions Violation for Subscription to foo'\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(
                pingIntervalSeconds: 0,
                errorListener: static function (\Throwable $err) use (&$errors): void {
                    $errors[] = $err->getMessage();
                    throw new \RuntimeException('error listener exploded');
                },
            ),
            $transport,
        );
        $connection->connect()->await();

        // A recoverable -ERR triggers emitError(); the throwing error listener must be swallowed.
        $connection->processIncoming()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(1, $errors); // listener was called exactly once
    }

    /**
     * handleServerInfoUpdate() returns early when serverInfo is null.
     * This is a defensive guard; we verify it does not throw by calling it directly via reflection.
     */
    public function testHandleServerInfoUpdateNoopsWhenServerInfoIsNull(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        // Null out serverInfo to hit the early-return guard.
        (new \ReflectionProperty(NatsConnection::class, 'serverInfo'))->setValue($connection, null);

        // Must not throw.
        (new \ReflectionMethod($connection, 'handleServerInfoUpdate'))->invoke($connection);

        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * handleServerInfoUpdate() emits LameDuck, attempts failover, and emits error when
     * failover fails (reconnect enabled but no spare server).
     */
    public function testLameDuckWithFailoverEmitsErrorWhenRecoveryFails(): void
    {
        $events = [];
        $errors = [];
        // Single server + lame-duck INFO: pool has only one server so failover threshold (> 1) is not met.
        // Result: LameDuck is emitted but recoverConnection() is NOT called (not enough pool members).
        // To force the recoverConnection() code path we need pool > 1, which means we need a discovered
        // peer. We seed one via a prior async INFO so the pool grows to 2 before the lame-duck arrives.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // First async INFO: discover a peer (pool grows to 2).
            'INFO {"server_id":"S1","version":"2.12.0","max_payload":1048576,"connect_urls":["10.0.0.2:4222"]}' . "\r\n",
            // Second async INFO: lame-duck with reconnect enabled + pool of 2 -> recoverConnection() fires.
            // With no second server actually available, recovery will fail -> emitError() is called.
            'INFO {"server_id":"S1","version":"2.12.0","max_payload":1048576,"ldm":true,"connect_urls":["10.0.0.2:4222"]}' . "\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 1,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
                connectionListener: static function (ConnectionEvent $e, ?\Throwable $err) use (&$events): void {
                    $events[] = $e;
                },
                errorListener: static function (\Throwable $err) use (&$errors): void {
                    $errors[] = $err->getMessage();
                },
            ),
            $transport,
        );
        $connection->connect()->await();

        // Read the discovery INFO (pool grows to 2).
        $connection->processIncoming()->await();
        // Read the lame-duck INFO (triggers failover attempt -> fails -> emitError).
        $connection->processIncoming()->await();

        self::assertContains(ConnectionEvent::LameDuck, $events);
        // Recovery failed (no real second server), so emitError() was invoked.
        self::assertNotEmpty($errors);
    }

    /**
     * drainPendingForSid() unsets pendingMessages when subscription is gone.
     */
    public function testDrainPendingForSidRemovesPendingWhenSubscriptionIsGone(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $sid = $connection->subscribe('events', static function (NatsMessage $message): void {})->await();

        // Inject a pending queue for the sid but remove the subscription handler to simulate
        // an unsubscribe that left a residual pending entry.
        $pendingProp = new \ReflectionProperty(NatsConnection::class, 'pendingMessages');
        $subsProp = new \ReflectionProperty(NatsConnection::class, 'subscriptions');

        // Verify pending queue exists.
        $pending = $pendingProp->getValue($connection);
        self::assertArrayHasKey($sid, $pending);

        // Remove the subscription handler (but keep the pending queue).
        $subs = $subsProp->getValue($connection);
        unset($subs[$sid]);
        $subsProp->setValue($connection, $subs);

        // Call drainPendingForSid() which should detect the missing handler and unset the pending queue.
        (new \ReflectionMethod($connection, 'drainPendingForSid'))->invoke($connection, $sid);

        $pendingAfter = $pendingProp->getValue($connection);
        self::assertArrayNotHasKey($sid, $pendingAfter);
    }

    /**
     * requestManyInternal() rethrows CancelledException from external cancellation token.
     */
    public function testRequestManyRethrowsExternalCancellation(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $deferredCancellation = new DeferredCancellation();
        $deferredCancellation->cancel();

        $this->expectException(CancelledException::class);
        $connection->requestMany('svc.scan', 'q', null, null, 5000, null, $deferredCancellation->getCancellation())->await();
    }

    /**
     * Statistics accessors: outMsgs/outBytes are incremented on publish.
     * (Covers the statistics() snapshot path, complementing the existing test.)
     */
    public function testStatisticsTracksOutboundCountsForHeaderPublish(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $connection->publishWithHeaders('events', 'hello', ['X-T' => '1'])->await();

        $stats = $connection->statistics();
        self::assertSame(1, $stats->outMsgs);
        self::assertSame(5, $stats->outBytes); // strlen('hello')
    }

    // ─── New coverage additions ──────────────────────────────────────────

    /**
     * requestManyInternal() continues when the internal slice timeout fires
     * but no external cancellation was requested (covers the `continue` branch in the
     * CancelledException catch block).
     *
     * Strategy: supply a small stallMs and a reply that arrives before the stall expires.
     * After the stall window elapses with no further reply the stall break fires, but along
     * the way the slice TimeoutCancellation fires at least once inside processIncoming(),
     * triggering the catch block. Because no external cancellation is set, `continue` is taken.
     */
    public function testRequestManyInternalContinuesOnSliceTimeout(): void
    {
        // One reply arrives, then nothing.  With stallMs=50 the loop re-evaluates and eventually
        // hits the stall break; the internal slice timeouts (CancelledException) along the way
        // exercise the `continue` branch.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG _INBOX.any 1 1\r\nA\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        // totalTimeoutMs=2000ms, stallMs=50ms: after receiving A, wait for stall to expire.
        $replies = $connection->requestMany('svc.scan', 'q', null, null, 2000, 50)->await();

        $payloads = array_map(static fn(NatsMessage $m): string => $m->payload, $replies);
        self::assertSame(['A'], $payloads);
    }

    /**
     * requestManyInternal() rethrows CancelledException when the external
     * cancellation fires WHILE processIncoming() is blocking inside the requestMany loop.
     *
     * Strategy: use a blocking transport (blockWhenEmpty=true) so processIncoming() suspends
     * waiting for data. Cancel externally after a short delay; the CancelledException propagates
     * out of processIncoming(), the catch block detects that the external cancellation
     * is requested and rethrows.
     */
    public function testRequestManyRethrowsExternalCancellationFromProcessIncoming(): void
    {
        $transport = new FakeTransport(
            readQueue: [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                "PONG\r\n",
                // Nothing else: processIncoming() will block waiting for a frame.
            ],
            blockWhenEmpty: true,
        );

        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $deferredCancellation = new DeferredCancellation();

        // Fire the external cancellation after a short delay (while processIncoming() is suspended).
        $cancel = async(static function () use ($deferredCancellation): void {
            delay(0.03);
            $deferredCancellation->cancel();
        });

        $this->expectException(CancelledException::class);
        try {
            $connection->requestMany(
                'svc.scan',
                'q',
                null,
                null,
                5000,
                null,
                $deferredCancellation->getCancellation(),
            )->await();
        } finally {
            $cancel->await();
        }
    }

    /**
     * recoverConnection() coalesces concurrent callers: a second caller
     * that arrives while a reconnect is already in progress awaits the same future rather than
     * starting a second recovery attempt.
     *
     * Strategy: hold the second connect() attempt open with a DeferredFuture so both the
     * primary and a secondary recoverConnection() call are in flight simultaneously. Release
     * the latch and verify only two total connect calls were made (initial + one reconnect).
     */
    public function testRecoverConnectionCoalescesConcurrentCallers(): void
    {
        $info = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";
        $release = new DeferredFuture();
        $connectCount = 0;

        $transport = new class ($info, $release, $connectCount) implements TransportInterface {
            /** @var list<string> */
            public array $connectCalls = [];
            /** @var list<string> */
            public array $writes = [];
            /** @var list<list<string>> */
            private array $reads;
            private int $connects = 0;

            /** @param DeferredFuture<void> $release */
            public function __construct(
                string $info,
                private DeferredFuture $release,
                private int &$connectCount,
            ) {
                $this->reads = [
                    [$info, "PONG\r\n"],   // initial
                    [$info, "PONG\r\n"],   // reconnect
                ];
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function () use ($dsn, $timeoutMs): void {
                    $this->connectCalls[] = $dsn . '|' . $timeoutMs;
                    // Block the reconnect attempt until released.
                    if ($this->connects === 1) {
                        $this->release->getFuture()->await();
                    }
                    $this->connects++;
                    $this->connectCount++;
                });
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                });
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $conn = max(0, $this->connects - 1);
                    $next = array_shift($this->reads[$conn]) ?? '';
                    if ($next === '__EOF__') {
                        throw new TransportClosedException('eof');
                    }

                    return $next;
                });
            }

            public function close(): Future
            {
                return async(static fn(): null => null);
            }
        };

        $connection = new NatsConnection(
            new NatsOptions(reconnectDelayMs: 1, reconnectJitterMs: 0, pingIntervalSeconds: 0),
            $transport,
        );
        $connection->connect()->await();

        // Trigger a reconnect by invoking recoverConnection() twice concurrently via reflection.
        $recover = new \ReflectionMethod($connection, 'recoverConnection');

        // Force state to Connecting so recoverConnection() does not skip early.
        (new \ReflectionProperty($connection, 'state'))->setValue($connection, ConnectionState::Connecting);

        // First caller starts the reconnect and suspends in connect().
        $first = async(static function () use ($recover, $connection): void {
            $recover->invoke($connection);
        });

        delay(0.02); // let first caller enter and suspend

        // Second caller must coalesce: it awaits the same in-progress future.
        $second = async(static function () use ($recover, $connection): void {
            $recover->invoke($connection);
        });

        delay(0.01);

        $release->complete();  // release the blocked connect()

        $first->await();
        $second->await();

        // Only one reconnect attempt ran: total connects = initial(1) + reconnect(1) = 2.
        self::assertCount(2, $transport->connectCalls);
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * retryInitialConnect() ignores transport.close() failures between attempts.
     *
     * When a close() call throws during the retry loop, the exception is swallowed and the
     * next connect attempt still proceeds normally.
     */
    public function testRetryInitialConnectIgnoresCloseFailureBetweenAttempts(): void
    {
        $info = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

        $transport = new class ($info) implements TransportInterface {
            /** @var list<string> */
            public array $connectCalls = [];
            /** @var list<string> */
            public array $writes = [];

            private int $connects = 0;
            /** @var list<list<string>> */
            private array $reads;

            public function __construct(string $info)
            {
                $this->reads = [
                    // First attempt fails (no PONG for connectOnce to succeed).
                    [],
                    // Second attempt succeeds.
                    [$info, "PONG\r\n"],
                ];
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function () use ($dsn, $timeoutMs): void {
                    $this->connectCalls[] = $dsn . '|' . $timeoutMs;
                    $this->connects++;
                    // First connect succeeds (initial attempt), second connect also succeeds (retry).
                });
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                });
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $conn = max(0, $this->connects - 1);

                    return array_shift($this->reads[$conn]) ?? '';
                });
            }

            public function close(): Future
            {
                return async(static function (): void {
                    // Always throw: retryInitialConnect() must swallow this.
                    throw new \RuntimeException('close failed intentionally');
                });
            }
        };

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: false,
                retryOnFailedInitialConnect: true,
                maxReconnectAttempts: 2,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                connectTimeoutMs: 50,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );

        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        // At least 2 connect calls: initial (failed, exhausted poll budget) + retry that succeeded.
        self::assertGreaterThanOrEqual(2, count($transport->connectCalls));
    }

    /**
     * performRecovery() ignores transport.close() failures during reconnect loop.
     *
     * A close() that throws between reconnect attempts must be swallowed so subsequent
     * connect attempts still proceed and recovery can succeed.
     */
    public function testPerformRecoveryIgnoresCloseFailureDuringReconnect(): void
    {
        $info = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

        $transport = new class ($info) implements TransportInterface {
            /** @var list<string> */
            public array $connectCalls = [];
            /** @var list<string> */
            public array $writes = [];

            private int $connects = 0;
            /** @var list<list<string>> */
            private array $reads;

            public function __construct(string $info)
            {
                $this->reads = [
                    // Initial connection succeeds.
                    [$info, "PONG\r\n", '__EOF__'],
                    // Reconnect attempt succeeds.
                    [$info, "PONG\r\n"],
                ];
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function () use ($dsn, $timeoutMs): void {
                    $this->connectCalls[] = $dsn . '|' . $timeoutMs;
                    $this->connects++;
                });
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                });
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $conn = max(0, $this->connects - 1);
                    $next = array_shift($this->reads[$conn]) ?? '';
                    if ($next === '__EOF__') {
                        throw new TransportClosedException('eof');
                    }

                    return $next;
                });
            }

            public function close(): Future
            {
                return async(static function (): void {
                    // Always throw: performRecovery() must swallow this and keep retrying.
                    throw new \RuntimeException('close failed intentionally');
                });
            }
        };

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );

        $connection->connect()->await();

        // processIncoming() reads EOF -> triggers recoverConnection() -> performRecovery() ->
        // transport.close() throws -> swallowed -> reconnect attempt succeeds.
        $connection->processIncoming()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->connectCalls);
    }
}
