<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\Cancellation;
use Amp\Future;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\AuthenticationException;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Transport\TransportClosedException;
use IDCT\NATS\Transport\TransportInterface;
use Psr\Log\AbstractLogger;
use RuntimeException;

use function Amp\async;

/**
 * Mutation-killing tests for src/Connection/NatsConnection.php (chunk 4: lines ~1283-1473).
 *
 * Covers the reconnect/backoff loop (performRecovery), flushReconnectBuffer, drainImmediateServerFrames,
 * and the handshake read loops (awaitInitialPong / awaitServerInfo).
 */
final class NatsConnection_4MutationTest extends \PHPUnit\Framework\TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /**
     * Drives the read loop (which triggers recoverConnection on read failure) to completion,
     * swallowing the exception a failed/aborted reconnect re-throws out of processIncoming().
     */
    private static function pumpSwallowing(NatsConnection $connection): \Throwable|int
    {
        try {
            return $connection->processIncoming()->await();
        } catch (\Throwable $e) {
            return $e;
        }
    }

    // ---------------------------------------------------------------------------------------------
    // performRecovery - AuthenticationException branch (line 1283)
    // ---------------------------------------------------------------------------------------------

    /**
     * kills MethodCallRemoval @ line 1283
     *
     * When a reconnect handshake fails with an auth error, performRecovery must notify the error
     * listener via emitError($e) BEFORE emitting Closed and re-throwing. The read failure that
     * triggered recovery emits one error; the auth abort emits a second. Removing emitError($e)
     * leaves only the read-failure error.
     */
    public function testReconnectAuthFailureNotifiesErrorListener(): void
    {
        $errors = [];
        // connection 0: handshake OK then a read throw -> recovery; connection 1: -ERR auth -> abort.
        $transport = new TwoPhaseReconnectTransport(
            firstReads: [self::INFO, "PONG\r\n", '__THROW__'],
            secondReads: ["-ERR Authorization Violation\r\n"],
        );

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                errorListener: static function (\Throwable $e) use (&$errors): void {
                    $errors[] = $e->getMessage();
                },
            ),
            $transport,
        );
        $connection->connect()->await();

        $result = self::pumpSwallowing($connection);

        self::assertInstanceOf(AuthenticationException::class, $result);
        // Real code emits two errors: the triggering read failure AND the auth failure.
        self::assertCount(2, $errors);
        self::assertTrue(
            (bool) array_filter($errors, static fn(string $m): bool => str_contains(strtolower($m), 'authentication')),
            'auth failure must be surfaced to the error listener',
        );
    }

    // ---------------------------------------------------------------------------------------------
    // performRecovery - exhaustion branch (lines 1299, 1302)
    // ---------------------------------------------------------------------------------------------

    /**
     * kills MethodCallRemoval @ line 1299
     *
     * When reconnect attempts are exhausted, performRecovery must emit a Closed lifecycle event.
     */
    public function testReconnectExhaustionEmitsClosedEvent(): void
    {
        $events = [];
        $transport = new TwoPhaseReconnectTransport(
            firstReads: [self::INFO, "PONG\r\n", '__THROW__'],
            secondReads: [], // unused: every reconnect connect() fails
            failReconnectConnects: true,
        );

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 2,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                connectionListener: static function (ConnectionEvent $e) use (&$events): void {
                    $events[] = $e;
                },
            ),
            $transport,
        );
        $connection->connect()->await();

        $result = self::pumpSwallowing($connection);

        self::assertInstanceOf(ConnectionException::class, $result);
        self::assertContains(ConnectionEvent::Closed, $events, 'exhausted reconnect must emit Closed');
    }

    /**
     * kills DecrementInteger @ line 1302, IncrementInteger @ line 1302
     *
     * The "Reconnect attempts exhausted" exception must be constructed with code exactly 0.
     */
    public function testReconnectExhaustionThrowsWithZeroCode(): void
    {
        $transport = new TwoPhaseReconnectTransport(
            firstReads: [self::INFO, "PONG\r\n", '__THROW__'],
            secondReads: [],
            failReconnectConnects: true,
        );

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 2,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
            ),
            $transport,
        );
        $connection->connect()->await();

        $result = self::pumpSwallowing($connection);

        self::assertInstanceOf(ConnectionException::class, $result);
        self::assertSame('Reconnect attempts exhausted', $result->getMessage());
        self::assertSame(0, $result->getCode());
    }

    /**
     * kills ArrayItemRemoval @ line 1292 (log context 'attempt'),
     * kills DecrementInteger @ line 1376, IncrementInteger @ line 1376 (backoff base = max(1, delay))
     *
     * Each failed (non-final) reconnect attempt logs a warning whose context records the attempt
     * number and the computed delayMs. With reconnectDelayMs=1, jitter=0, the first attempt's
     * backoff is exactly 1ms (base=max(1,1)=1, exponential=1, capped=1). The mutated base values
     * (max(0,1)=1 has no effect at delay=1, but max(2,1)=2 yields delayMs=2) and a removed 'attempt'
     * key are both observable in the captured log context.
     */
    public function testReconnectWarningLogsAttemptAndDelay(): void
    {
        $logger = new CapturingLogger();
        // 2 reconnect connect failures -> two warning logs, then exhaustion.
        $transport = new TwoPhaseReconnectTransport(
            firstReads: [self::INFO, "PONG\r\n", '__THROW__'],
            secondReads: [],
            failReconnectConnects: true,
        );

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 2,
                reconnectDelayMs: 1,
                reconnectMaxDelayMs: 10_000,
                reconnectJitterMs: 0,
                logger: $logger,
            ),
            $transport,
        );
        $connection->connect()->await();

        self::pumpSwallowing($connection);

        $warnings = array_values(array_filter(
            $logger->records,
            static fn(array $r): bool => $r['level'] === 'warning'
                && str_contains((string) $r['message'], 'reconnect attempt'),
        ));
        self::assertNotEmpty($warnings);

        $first = $warnings[0];
        // 'attempt' key must be present and correct (ArrayItemRemoval @ 1292).
        self::assertArrayHasKey('attempt', $first['context']);
        self::assertSame(1, $first['context']['attempt']);
        self::assertSame(2, $first['context']['maxAttempts']);
        // delayMs for attempt 1 with base=max(1,1)=1 -> exactly 1 (kills max(2,...) mutant @ 1376).
        self::assertSame(1, $first['context']['delayMs']);
    }

    /**
     * kills DecrementInteger @ line 1376 (max(1, reconnectDelayMs) -> max(0, ...))
     *
     * With reconnectDelayMs=0, the floor max(1, 0)=1 keeps the first backoff at 1ms; the mutant
     * max(0, 0)=0 collapses it to 0ms. Observed via the logged delayMs.
     */
    public function testBackoffFloorsBaseAtOne(): void
    {
        $logger = new CapturingLogger();
        $transport = new TwoPhaseReconnectTransport(
            firstReads: [self::INFO, "PONG\r\n", '__THROW__'],
            secondReads: [],
            failReconnectConnects: true,
        );

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 1,
                reconnectDelayMs: 0,
                reconnectMaxDelayMs: 10_000,
                reconnectJitterMs: 0,
                logger: $logger,
            ),
            $transport,
        );
        $connection->connect()->await();

        self::pumpSwallowing($connection);

        $warnings = array_values(array_filter(
            $logger->records,
            static fn(array $r): bool => $r['level'] === 'warning'
                && str_contains((string) $r['message'], 'reconnect attempt'),
        ));
        self::assertNotEmpty($warnings);
        // base=max(1,0)=1, exponential=1, capped=1, jitter=0 -> delayMs=1 (mutant max(0,0)=0 -> 0).
        self::assertSame(1, $warnings[0]['context']['delayMs']);
    }

    // ---------------------------------------------------------------------------------------------
    // flushReconnectBuffer - empty-buffer early return (line 1313)
    // ---------------------------------------------------------------------------------------------

    /**
     * kills ReturnRemoval @ line 1313
     *
     * On a clean reconnect with NO buffered publishes, flushReconnectBuffer must early-return so it
     * never writes anything. Removing the return writes an empty string ('') to the transport.
     */
    public function testReconnectWithEmptyBufferWritesNothingExtra(): void
    {
        $transport = new TwoPhaseReconnectTransport(
            firstReads: [self::INFO, "PONG\r\n", '__THROW__'],
            secondReads: [self::INFO, "PONG\r\n"],
        );

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
            ),
            $transport,
        );
        $connection->connect()->await();

        self::pumpSwallowing($connection);

        self::assertSame(ConnectionState::Open, $connection->state());
        // No empty-string write may appear: flushReconnectBuffer must skip the write when buffer is empty.
        self::assertNotContains('', $transport->writes, 'empty buffer must not produce an empty write');
    }

    // ---------------------------------------------------------------------------------------------
    // drainImmediateServerFrames - frame handling (lines 1357, 1363)
    // ---------------------------------------------------------------------------------------------

    /**
     * kills Continue_ @ line 1363
     *
     * During reconnect subscription replay, drainImmediateServerFrames skips +OK frames with
     * `continue` and still processes the remaining frames in the same chunk. Mutating to `break`
     * abandons the rest of the chunk, so a MSG co-chunked after the +OK is lost entirely.
     */
    public function testDrainProcessesMsgAfterOkInSameChunk(): void
    {
        $transport = new TwoPhaseReconnectTransport(
            firstReads: [self::INFO, "PONG\r\n", '__THROW__'],
            // reconnect handshake, then the resubscribe poll reads one chunk: +OK then a MSG.
            secondReads: [self::INFO, "PONG\r\n", "+OK\r\nMSG updates 1 5\r\nhello\r\n"],
        );

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
            ),
            $transport,
        );
        $connection->connect()->await();

        $received = [];
        $connection->subscribe('updates', static function (NatsMessage $m) use (&$received): void {
            $received[] = $m->payload;
        })->await();

        self::pumpSwallowing($connection);
        // Deliver anything still buffered.
        self::pumpSwallowing($connection);

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertContains('hello', $received, 'MSG after +OK in the drain chunk must still be delivered');
    }

    /**
     * kills ReturnRemoval @ line 1357
     *
     * drainImmediateServerFrames returns on an empty read, so a MSG queued AFTER an empty chunk is
     * NOT drained during replay - it is left for the next processIncoming(). Removing the return
     * keeps polling and drains (delivers) that MSG eagerly during the reconnect critical section.
     */
    public function testDrainStopsOnEmptyChunkLeavingLaterMsgForNextRead(): void
    {
        $transport = new TwoPhaseReconnectTransport(
            firstReads: [self::INFO, "PONG\r\n", '__THROW__'],
            // After the resubscribe SUB, the poll reads '' (stop), then the MSG remains queued.
            secondReads: [self::INFO, "PONG\r\n", '', "MSG updates 1 5\r\nhello\r\n"],
        );

        $connection = new NatsConnection(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
            ),
            $transport,
        );
        $connection->connect()->await();

        $received = [];
        $connection->subscribe('updates', static function (NatsMessage $m) use (&$received): void {
            $received[] = $m->payload;
        })->await();

        // The processIncoming() that triggers recovery: drain stops at '' so the MSG is NOT yet drained.
        self::pumpSwallowing($connection);
        self::assertSame([], $received, 'MSG after empty chunk must not be drained during replay');
        self::assertSame(ConnectionState::Open, $connection->state());

        // The next read picks up the still-queued MSG.
        self::pumpSwallowing($connection);
        self::assertSame(['hello'], $received);
    }

    // ---------------------------------------------------------------------------------------------
    // awaitInitialPong - handshake loop (lines 1395, 1413)
    // ---------------------------------------------------------------------------------------------

    /**
     * kills Continue_ @ line 1395
     *
     * awaitInitialPong must `continue` past an empty handshake chunk and keep polling for the PONG.
     * Mutating to `break` exits the while loop on the first empty read, failing the handshake.
     */
    public function testHandshakeContinuesPastEmptyChunkBeforePong(): void
    {
        // awaitServerInfo consumes INFO; awaitInitialPong then sees '' (continue) then PONG.
        $transport = new FakeTransport([
            self::INFO,
            '',
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * kills Continue_ @ line 1413
     *
     * In awaitInitialPong, after applying an INFO update the loop must `continue` to the next frame in
     * the SAME chunk (where the PONG lives). Mutating to `break` abandons the rest of the chunk and
     * loses the co-chunked PONG, so the handshake never completes.
     */
    public function testHandshakeContinuesAfterInfoToReachPongInSameChunk(): void
    {
        $secondInfo = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","max_payload":1048576,"headers":true}' . "\r\n";
        // awaitServerInfo consumes the first INFO; awaitInitialPong reads a chunk holding INFO + PONG.
        $transport = new FakeTransport([
            self::INFO,
            $secondInfo . "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            // short timeout keeps the mutant's doomed poll loop bounded and fast.
            new NatsOptions(reconnectEnabled: false, connectTimeoutMs: 50),
            $transport,
        );
        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
    }

    // ---------------------------------------------------------------------------------------------
    // awaitServerInfo - handshake loop (line 1456)
    // ---------------------------------------------------------------------------------------------

    /**
     * kills Continue_ @ line 1456
     *
     * In awaitServerInfo, after answering a server PING with PONG the loop must `continue` to the next
     * frame in the same chunk (the INFO). Mutating to `break` abandons the chunk and loses the INFO,
     * so the connect cannot obtain server info and fails.
     */
    public function testAwaitServerInfoContinuesAfterPingToReachInfoInSameChunk(): void
    {
        // First handshake read carries PING then INFO in one chunk; PONG follows for awaitInitialPong.
        $transport = new FakeTransport([
            "PING\r\n" . self::INFO,
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(reconnectEnabled: false, connectTimeoutMs: 50),
            $transport,
        );
        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        $serverInfo = $connection->serverInfo();
        self::assertNotNull($serverInfo);
        self::assertSame('S1', $serverInfo->serverId);
        // The server PING was answered with a PONG during handshake.
        self::assertContains("PONG\r\n", $transport->writes);
    }
}

/**
 * A transport that serves one read queue for the initial connection, then a second read queue for the
 * (single) reconnect. It can also force every reconnect connect() to fail to exercise exhaustion.
 */
final class TwoPhaseReconnectTransport implements TransportInterface
{
    /** @var list<string> */
    public array $writes = [];

    /** @var list<string> */
    public array $connectCalls = [];

    private int $connects = 0;

    /** @var list<string> */
    private array $firstReads;

    /** @var list<string> */
    private array $secondReads;

    /**
     * @param list<string> $firstReads
     * @param list<string> $secondReads
     */
    public function __construct(
        array $firstReads,
        array $secondReads,
        private bool $failReconnectConnects = false,
    ) {
        $this->firstReads = $firstReads;
        $this->secondReads = $secondReads;
    }

    public function connect(string $dsn, int $timeoutMs): Future
    {
        return async(function () use ($dsn, $timeoutMs): void {
            $this->connectCalls[] = $dsn . '|' . $timeoutMs;
            $isReconnect = $this->connects >= 1;
            $this->connects++;

            if ($isReconnect && $this->failReconnectConnects) {
                throw new RuntimeException('reconnect connect failed');
            }
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
            // connects==1 -> still on the first connection; connects>=2 -> reconnect connection.
            if ($this->connects <= 1) {
                $next = array_shift($this->firstReads);
            } else {
                $next = array_shift($this->secondReads);
            }

            if ($next === null) {
                return '';
            }

            if ($next === '__THROW__') {
                throw new TransportClosedException('read failed');
            }

            return $next;
        });
    }

    public function close(): Future
    {
        return async(static fn(): null => null);
    }
}

/**
 * Minimal PSR-3 logger that captures every record for assertions.
 */
final class CapturingLogger extends AbstractLogger
{
    /** @var list<array{level:mixed,message:string|\Stringable,context:array<mixed>}> */
    public array $records = [];

    /**
     * @param array<mixed> $context
     */
    public function log($level, string|\Stringable $message, array $context = []): void
    {
        $this->records[] = ['level' => $level, 'message' => $message, 'context' => $context];
    }
}
