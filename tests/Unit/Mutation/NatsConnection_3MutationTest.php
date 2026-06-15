<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\Cancellation;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\AuthenticationException;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\FlakyTransport;
use IDCT\NATS\Transport\TransportClosedException;
use IDCT\NATS\Transport\TransportInterface;
use PHPUnit\Framework\TestCase;

use function Amp\async;

final class NatsConnection_3MutationTest extends TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /**
     * isNoRespondersStatus uses an anchored regex (`^NATS/1.0 503`). A header whose first line merely
     * CONTAINS the 503 status (not at the start) is a normal reply, NOT a no-responders sentinel.
     *
     * Removing the `^` (PregMatchRemoveCaret) makes the regex match anywhere, so this reply would be
     * mis-classified as a no-responders sentinel and the collection would return empty.
     */
    public function testNoRespondersRequiresStatusAtStartOfLine(): void
    {
        // Header block whose FIRST line embeds "NATS/1.0 503" but does not start with it.
        $status = "x NATS/1.0 503\r\n\r\n";
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            'HMSG _INBOX.any 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        // kills PregMatchRemoveCaret @ 982: real treats this as a normal reply (collected),
        // the mutant treats it as a no-responders sentinel (empty result).
        $replies = $connection->requestMany('svc.scan', 'q', null, null, 1000)->await();
        self::assertCount(1, $replies);
        self::assertSame('', $replies[0]->payload);
    }

    /**
     * A genuine "NATS/1.0 503" status line IS a no-responders sentinel: collection returns empty.
     * (Pins the positive side so the regex still recognizes the real sentinel.)
     */
    public function testNoRespondersStatusReturnsEmpty(): void
    {
        $status = "NATS/1.0 503\r\n\r\n";
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            'HMSG _INBOX.any 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $replies = $connection->requestMany('svc.scan', 'q', null, null, 1000)->await();
        self::assertSame([], $replies);
    }

    /**
     * A lone userinfo component in the server URL (token@host, no password) must be applied to CONNECT
     * as an `auth_token`, NOT as a user/password pair.
     */
    public function testUrlTokenCredentialEncodedAsAuthToken(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);

        $connection = new NatsConnection(
            new NatsOptions(servers: ['nats://s3cr3t-token@127.0.0.1:4222']),
            $transport,
        );
        $connection->connect()->await();

        $connect = $transport->writes[0];
        // kills LogicalAnd @ 1036: real (&&) returns a token (no password present) -> auth_token only;
        // the mutant (||) returns a user/pass pair (pass = rawurldecode(null) = '') -> "user" appears.
        self::assertStringStartsWith('CONNECT ', $connect);
        self::assertStringContainsString('"auth_token":"s3cr3t-token"', $connect);
        self::assertStringNotContainsString('"user"', $connect);
        self::assertStringNotContainsString('"pass"', $connect);
    }

    /**
     * When TLS is required but the transport never activated TLS, connectOnce throws a ConnectionException
     * whose message concatenates two distinct halves in a fixed order.
     */
    public function testTlsRequiredButInactiveThrowsExactMessage(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $transport->canUpgrade = false; // upgradeTls() leaves tlsActive false

        $connection = new NatsConnection(
            new NatsOptions(servers: ['nats://127.0.0.1:4222'], tlsRequired: true, reconnectEnabled: false),
            $transport,
        );

        // kills Concat + both ConcatOperandRemoval @ 1077: the exact full message (both halves, in order)
        // distinguishes the real concat from a reordered or truncated one.
        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage(
            'Server requires TLS but the TLS handshake was not established; '
            . 'configure TLS materials (NatsOptions tlsRequired / tlsCaFile / tlsCertFile) for this connection',
        );

        $connection->connect()->await();
    }

    /**
     * connectOnce only seeds the discovered-servers set from the initial INFO when that INFO actually
     * carries connect_urls. On a reconnect to a server whose INFO has NO connect_urls, the previously
     * discovered peers must be retained (the guard's second operand must hold).
     */
    public function testSeedDiscoveredServersRequiresNonEmptyConnectUrls(): void
    {
        $infoWithPeers = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true,"connect_urls":["10.9.9.9:4222"]}' . "\r\n";
        $infoNoPeers = self::INFO;

        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [$infoWithPeers, "PONG\r\n", '__EOF__'],
                [$infoNoPeers, "PONG\r\n"],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $connection = new NatsConnection(
            new NatsOptions(
                servers: ['nats://127.0.0.1:4222'],
                reconnectEnabled: true,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );
        $connection->connect()->await();

        // First connection advertised a peer: it is seeded.
        self::assertSame(['10.9.9.9:4222'], $connection->discoveredServers());

        // EOF -> reconnect to a server whose INFO has NO connect_urls.
        $connection->processIncoming()->await();
        self::assertSame(ConnectionState::Open, $connection->state());

        // kills LogicalAnd @ 1093: real (&&) leaves the prior peers intact because connect_urls === [];
        // the mutant (||) overwrites them with [] (first operand alone is true).
        self::assertSame(['10.9.9.9:4222'], $connection->discoveredServers());
    }

    /**
     * requestMany with no maxResponses and no stall runs until the total deadline. When the per-slice
     * read times out (CancelledException) and there is NO external cancellation, the loop must continue
     * and ultimately return the collected messages - never rethrow / dereference a null cancellation.
     */
    public function testRequestManyTotalTimeoutWithoutExternalCancellationReturnsCollected(): void
    {
        $transport = new FakeTransport(
            [
                self::INFO,
                "PONG\r\n",
                "MSG _INBOX.any 1 1\r\nA\r\n",
            ],
            blockWhenEmpty: true, // after the single reply, reads suspend until the slice cancellation fires
        );

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        // No maxResponses, no stall: the loop ends on the total deadline. Each idle slice read is
        // cancelled, hitting the catch with $cancellation === null.
        // kills NotIdentical / LogicalAnd / LogicalAndAllSubExprNegation / LogicalAndNegation @ 947:
        //  - && -> ||, or any negation that makes the branch fire (or dereference null), would throw
        //    instead of returning the one collected message.
        $replies = $connection->requestMany('svc.scan', 'q', null, null, 40)->await();

        self::assertCount(1, $replies);
        self::assertSame('A', $replies[0]->payload);
    }

    /**
     * Same as above but WITH an external cancellation that is present yet NOT requested. A per-slice
     * timeout must still be swallowed (loop continues), because the external token has not fired.
     */
    public function testRequestManyTotalTimeoutWithUnrequestedExternalCancellationReturnsCollected(): void
    {
        $transport = new FakeTransport(
            [
                self::INFO,
                "PONG\r\n",
                "MSG _INBOX.any 1 1\r\nA\r\n",
            ],
            blockWhenEmpty: true,
        );

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $deferredCancellation = new DeferredCancellation(); // present but never cancelled

        // kills LogicalAndSingleSubExprNegation @ 947: real ($cancellation->isRequested() === false) ->
        // continue; the mutant (`&& !isRequested()`) would treat the slice timeout as an external
        // cancellation and rethrow, dropping the collected message.
        $replies = $connection
            ->requestMany('svc.scan', 'q', null, null, 40, null, $deferredCancellation->getCancellation())
            ->await();

        self::assertCount(1, $replies);
        self::assertSame('A', $replies[0]->payload);
    }

    /**
     * retryInitialConnect (reconnect disabled, retryOnFailedInitialConnect on) attempts EXACTLY
     * max(1, maxReconnectAttempts) reconnect tries after the failed initial connect. With
     * maxReconnectAttempts = 1 and every connect failing, the total transport connect() calls are
     * 1 (initial) + 1 (single retry) = 2.
     */
    public function testRetryInitialConnectAttemptCountIsExact(): void
    {
        // Every connect fails -> retryInitialConnect exhausts its attempts and connect() throws.
        $transport = new FlakyTransport(
            readQueuesByConnection: [],
            connectFailures: 10,
            readFailures: 0,
        );

        $connection = new NatsConnection(
            new NatsOptions(
                servers: ['nats://127.0.0.1:4222'],
                reconnectEnabled: false,
                retryOnFailedInitialConnect: true,
                maxReconnectAttempts: 1,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
            ),
            $transport,
        );

        try {
            $connection->connect()->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException) {
            // expected: all connects failed
        }

        // kills IncrementInteger @ 1190 (max(2,..) -> 3 calls), IncrementInteger @ 1192 (start at 2 -> 1
        // call), LessThanOrEqualTo @ 1192 (< -> 1 call): real performs exactly 1 initial + 1 retry.
        self::assertCount(2, $transport->connectCalls);
    }

    /**
     * retryInitialConnect aborts immediately (rethrows) on an authentication failure instead of
     * swallowing it and retrying to exhaustion.
     */
    public function testRetryInitialConnectRethrowsAuthError(): void
    {
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                // connection 0: initial connect succeeds at socket level but handshake -ERR (recoverable)
                ['__THROW__'],
                // connection 1: the retry's handshake hits an auth violation
                [self::INFO, "-ERR Authorization Violation\r\n"],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $connection = new NatsConnection(
            new NatsOptions(
                servers: ['nats://127.0.0.1:4222'],
                reconnectEnabled: false,
                retryOnFailedInitialConnect: true,
                maxReconnectAttempts: 5,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
            ),
            $transport,
        );

        // kills CatchBlockRemoval @ 1201: real rethrows AuthenticationException (fail fast); the mutant
        // (no auth catch) swallows it via the generic catch and keeps retrying -> ConnectionException.
        $this->expectException(AuthenticationException::class);
        $connection->connect()->await();
    }

    /**
     * A successful retryInitialConnect emits a Connected event.
     */
    public function testRetryInitialConnectEmitsConnectedOnSuccess(): void
    {
        $events = [];
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [self::INFO, "PONG\r\n"],
            ],
            connectFailures: 1, // initial connect fails, the retry succeeds
            readFailures: 0,
        );

        $connection = new NatsConnection(
            new NatsOptions(
                servers: ['nats://127.0.0.1:4222'],
                reconnectEnabled: false,
                retryOnFailedInitialConnect: true,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                connectionListener: static function (ConnectionEvent $e) use (&$events): void {
                    $events[] = $e;
                },
            ),
            $transport,
        );

        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        // kills MethodCallRemoval @ 1203: real emits Connected after the retry succeeds.
        self::assertContains(ConnectionEvent::Connected, $events);
    }

    /**
     * performRecovery, when reconnect is DISABLED, must emit a Closed event before throwing
     * 'Reconnect is disabled'.
     */
    public function testPerformRecoveryEmitsClosedWhenReconnectDisabled(): void
    {
        $events = [];
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [self::INFO, "PONG\r\n", '__EOF__'],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: false,
                pingIntervalSeconds: 0,
                connectionListener: static function (ConnectionEvent $e) use (&$events): void {
                    $events[] = $e;
                },
            ),
            $transport,
        );
        $connection->connect()->await();

        // EOF triggers recovery; reconnectEnabled=false -> Closed event then 'Reconnect is disabled'
        // throw, which propagates out of processIncoming and leaves the connection Closed.
        try {
            $connection->processIncoming()->await();
        } catch (ConnectionException) {
            // expected: reconnect disabled
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        // kills MethodCallRemoval @ 1233: real emits Closed in the reconnect-disabled branch.
        self::assertContains(ConnectionEvent::Closed, $events);
    }

    /**
     * performRecovery is a no-op when close-intent is set: it must NOT emit Disconnected nor dial a new
     * server (the early return guard).
     */
    public function testPerformRecoveryBailsWhenClosingWithoutDisconnectEvent(): void
    {
        $events = [];
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [self::INFO, "PONG\r\n"],
                [self::INFO, "PONG\r\n"],
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

        (new \ReflectionProperty(NatsConnection::class, 'closing'))->setValue($connection, true);

        async(static function () use ($connection): void {
            (new \ReflectionMethod(NatsConnection::class, 'performRecovery'))->invoke($connection);
        })->await();

        // kills ReturnRemoval @ 1228: real returns immediately (no Disconnected, no extra connect); the
        // mutant falls through to the reconnect loop -> Disconnected event + a second dial.
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertNotContains(ConnectionEvent::Disconnected, $events);
        self::assertCount(1, $transport->connectCalls);
    }

    /**
     * recoverConnection is a no-op while close-intent is set, even if there are buffered messages: the
     * early return must short-circuit BEFORE the post-recovery drain.
     */
    public function testRecoverConnectionEarlyReturnSkipsPendingDrain(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $delivered = [];
        $sid = $connection->subscribe('updates', static function (NatsMessage $m) use (&$delivered): void {
            $delivered[] = $m->payload;
        })->await();

        // Buffer a message for that sid without delivering it.
        $queue = new \SplQueue();
        $queue->enqueue(new NatsMessage('updates', $sid, null, 'buffered'));
        $pendingProp = new \ReflectionProperty(NatsConnection::class, 'pendingMessages');
        $pendingProp->setValue($connection, [$sid => $queue]);

        // Close-intent set: recoverConnection must bail at the top and never drain.
        (new \ReflectionProperty(NatsConnection::class, 'closing'))->setValue($connection, true);

        async(static function () use ($connection): void {
            (new \ReflectionMethod(NatsConnection::class, 'recoverConnection'))->invoke($connection);
        })->await();

        // kills ReturnRemoval @ 1148: real returns before drainAllPending(); the mutant falls through,
        // runs performRecovery (which bails) AND then drainAllPending(), delivering the buffered message.
        self::assertSame([], $delivered);
    }

    /**
     * When a reconnect is already in progress, recoverConnection awaits that in-progress future and
     * surfaces its outcome. If the in-progress attempt errored, the awaiting caller must observe that
     * error (proving the await is not skipped).
     */
    public function testRecoverConnectionAwaitsInProgressReconnect(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $deferred = new DeferredFuture();
        $deferred->error(new \RuntimeException('in-progress reconnect failed'));
        (new \ReflectionProperty(NatsConnection::class, 'reconnecting'))->setValue($connection, $deferred);

        // kills MethodCallRemoval @ 1153: real awaits the in-progress future (which throws); the mutant
        // skips the await and returns normally.
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('in-progress reconnect failed');

        async(static function () use ($connection): void {
            (new \ReflectionMethod(NatsConnection::class, 'recoverConnection'))->invoke($connection);
        })->await();
    }

    /**
     * After a FAILED recovery (all reconnect attempts exhausted), recoverConnection must clear its
     * `reconnecting` slot via the finally clause, even though performRecovery rethrew.
     */
    public function testReconnectingClearedAfterFailedRecovery(): void
    {
        // Initial connect succeeds; the read throws -> recovery; every reconnect connect fails -> exhausted.
        $info = self::INFO;
        $transport = new class ($info) implements TransportInterface {
            /** @var list<string> */
            public array $connectCalls = [];
            private int $connects = 0;
            /** @var list<string> */
            private array $reads;

            public function __construct(string $info)
            {
                $this->reads = [$info, "PONG\r\n", '__THROW__'];
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function () use ($dsn): void {
                    $this->connectCalls[] = $dsn;
                    $this->connects++;
                    if ($this->connects > 1) {
                        throw new \RuntimeException('reconnect connect failed');
                    }
                });
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(static fn(): null => null);
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $next = array_shift($this->reads) ?? '__THROW__';
                    if ($next === '__THROW__') {
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

        // Drive the read loop: it hits the read failure, recovers (all attempts fail), goes Closed.
        try {
            $connection->processIncoming()->await();
        } catch (ConnectionException) {
            // expected: reconnect attempts exhausted
        }

        self::assertSame(ConnectionState::Closed, $connection->state());

        // kills UnwrapFinally @ 1164 and Finally_ @ 1171: the finally clears `reconnecting` even on the
        // rethrow path; without it (or with it moved past the throw) the errored deferred would linger.
        $reconnecting = (new \ReflectionProperty(NatsConnection::class, 'reconnecting'))->getValue($connection);
        self::assertNull($reconnecting);
    }

    /**
     * performRecovery closes the existing transport before each reconnect attempt, and a successful
     * reconnect cancels the active ping timer (its own startPingTimer runs in connectOnce, but the
     * pre-loop cancelPingTimer keeps the original watchdog from surviving).
     */
    public function testRecoveryClosesTransportBeforeReconnect(): void
    {
        $info = self::INFO;
        $transport = new class ($info) implements TransportInterface {
            public int $closeCalls = 0;
            /** @var list<string> */
            public array $connectCalls = [];
            private int $connects = 0;
            /** @var list<list<string>> */
            private array $reads;

            public function __construct(string $info)
            {
                // connection 0: handshake then EOF; connection 1: reconnect handshake.
                $this->reads = [
                    [$info, "PONG\r\n", '__EOF__'],
                    [$info, "PONG\r\n"],
                ];
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function () use ($dsn): void {
                    $this->connectCalls[] = $dsn;
                    $this->connects++;
                });
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(static fn(): null => null);
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $idx = max(0, $this->connects - 1);
                    $next = array_shift($this->reads[$idx]) ?? '';
                    if ($next === '__EOF__') {
                        throw new TransportClosedException('eof');
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
                maxReconnectAttempts: 3,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );
        $connection->connect()->await();

        $closesBefore = $transport->closeCalls;
        $connection->processIncoming()->await(); // EOF -> reconnect

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->connectCalls);
        // kills MethodCallRemoval @ 1252: real closes the dead transport before the reconnect attempt;
        // the mutant skips the close, so the close-call count would not increase across the reconnect.
        self::assertGreaterThan($closesBefore, $transport->closeCalls);
    }

    /**
     * retryInitialConnect closes the transport before each retry attempt. With the initial connect
     * failing and the retry succeeding, the transport's close() must be invoked (once) before the
     * successful reconnect.
     */
    public function testRetryInitialConnectClosesTransportBetweenAttempts(): void
    {
        $info = self::INFO;
        $transport = new class ($info) implements TransportInterface {
            public int $closeCalls = 0;
            /** @var list<string> */
            public array $connectCalls = [];
            private int $connects = 0;
            /** @var list<string> */
            private array $reads;

            public function __construct(string $info)
            {
                // The (successful) retry connection's handshake reads.
                $this->reads = [$info, "PONG\r\n"];
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function () use ($dsn): void {
                    $this->connectCalls[] = $dsn;
                    $this->connects++;
                    if ($this->connects === 1) {
                        throw new \RuntimeException('initial connect failed');
                    }
                });
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(static fn(): null => null);
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(fn(): string => array_shift($this->reads) ?? '');
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
                servers: ['nats://127.0.0.1:4222'],
                reconnectEnabled: false,
                retryOnFailedInitialConnect: true,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );

        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        // kills MethodCallRemoval @ 1196: real closes the transport before each retry attempt.
        self::assertGreaterThanOrEqual(1, $transport->closeCalls);
    }

    /**
     * performRecovery cancels the active ping watchdog timer before attempting reconnects. After a
     * recovery that exhausts every attempt (so connectOnce never restarts a timer), the ping timer id
     * must have been cleared.
     */
    public function testPerformRecoveryCancelsPingTimer(): void
    {
        // Initial connect succeeds (and arms a ping timer); the read throws; every reconnect fails.
        $info = self::INFO;
        $transport = new class ($info) implements TransportInterface {
            private int $connects = 0;
            /** @var list<string> */
            private array $reads;

            public function __construct(string $info)
            {
                $this->reads = [$info, "PONG\r\n", '__THROW__'];
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function (): void {
                    $this->connects++;
                    if ($this->connects > 1) {
                        throw new \RuntimeException('reconnect connect failed');
                    }
                });
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(static fn(): null => null);
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $next = array_shift($this->reads) ?? '__THROW__';
                    if ($next === '__THROW__') {
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
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 1,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 30, // arm a (slow, never-firing) ping timer on the initial connect
            ),
            $transport,
        );
        $connection->connect()->await();

        $pingProp = new \ReflectionProperty(NatsConnection::class, 'pingTimerId');
        self::assertNotNull($pingProp->getValue($connection), 'precondition: ping timer armed');

        try {
            $connection->processIncoming()->await(); // read throws -> recovery exhausts -> Closed
        } catch (ConnectionException) {
            // expected: reconnect attempts exhausted
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        // kills MethodCallRemoval @ 1237: real cancels the ping timer before reconnecting (and, on a
        // failed recovery, never re-arms it), so the timer id is cleared; the mutant leaves it set.
        self::assertNull($pingProp->getValue($connection));
    }

    /**
     * performRecovery uses max(1, maxReconnectAttempts) reconnect attempts. With maxReconnectAttempts=0
     * and reconnect enabled, recovery (reached via the read-error path, which does not gate on the
     * attempt count) must still make exactly ONE attempt.
     */
    public function testPerformRecoveryMakesAtLeastOneAttemptWhenMaxIsZero(): void
    {
        $reconnectConnects = 0;
        $transport = new class ($reconnectConnects) implements TransportInterface {
            private int $connects = 0;
            /** @var list<string> */
            private array $reads;

            public function __construct(public int &$reconnectConnects)
            {
                $this->reads = [self::INFO_LINE, "PONG\r\n", '__THROW__'];
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function (): void {
                    $this->connects++;
                    if ($this->connects > 1) {
                        $this->reconnectConnects++;
                        throw new \RuntimeException('reconnect connect failed');
                    }
                });
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(static fn(): null => null);
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $next = array_shift($this->reads) ?? '__THROW__';
                    if ($next === '__THROW__') {
                        throw new TransportClosedException('eof');
                    }

                    return $next;
                });
            }

            public function close(): Future
            {
                return async(static fn(): null => null);
            }

            public const INFO_LINE = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";
        };

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 0,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );
        $connection->connect()->await();

        try {
            $connection->processIncoming()->await(); // read throws -> recovery
        } catch (ConnectionException) {
            // expected: exhausted
        }

        // kills DecrementInteger @ 1240: real performs max(1, 0) = 1 reconnect attempt; the mutant
        // (max(0, 0) = 0) performs ZERO attempts.
        self::assertSame(1, $reconnectConnects);
    }

    /**
     * performRecovery uses EXACTLY max(1, maxReconnectAttempts) attempts: with maxReconnectAttempts=1
     * and every reconnect failing, precisely one reconnect attempt is made before giving up.
     */
    public function testPerformRecoveryAttemptCountIsExactWhenMaxIsOne(): void
    {
        $reconnectConnects = 0;
        $transport = new class ($reconnectConnects) implements TransportInterface {
            private int $connects = 0;
            /** @var list<string> */
            private array $reads;

            public function __construct(public int &$reconnectConnects)
            {
                $this->reads = [self::INFO_LINE, "PONG\r\n", '__THROW__'];
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function (): void {
                    $this->connects++;
                    if ($this->connects > 1) {
                        $this->reconnectConnects++;
                        throw new \RuntimeException('reconnect connect failed');
                    }
                });
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(static fn(): null => null);
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $next = array_shift($this->reads) ?? '__THROW__';
                    if ($next === '__THROW__') {
                        throw new TransportClosedException('eof');
                    }

                    return $next;
                });
            }

            public function close(): Future
            {
                return async(static fn(): null => null);
            }

            public const INFO_LINE = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";
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

        try {
            $connection->processIncoming()->await();
        } catch (ConnectionException) {
            // expected: exhausted
        }

        // kills IncrementInteger @ 1240: real makes exactly 1 attempt (max(1,1)); the mutant (max(2,1))
        // makes 2 attempts.
        self::assertSame(1, $reconnectConnects);
    }
}
