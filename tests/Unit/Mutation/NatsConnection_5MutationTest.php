<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\Cancellation;
use Amp\Future;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Transport\TransportInterface;
use PHPUnit\Framework\TestCase;

use function Amp\async;

/**
 * Targeted unit tests that kill surviving Infection mutants in src/Connection/NatsConnection.php
 * (chunk 5: handshake poll budget + handleFrame frame dispatch).
 */
final class NatsConnection_5MutationTest extends TestCase
{
    /**
     * A transport whose readLine() always returns an empty string, counting the calls. Used to count
     * how many handshake polls awaitServerInfo() performs before giving up - which equals the value
     * returned by handshakePollBudget(). The connect() handshake never receives an INFO and fails with
     * "Expected INFO during connect" after exactly handshakePollBudget() reads.
     *
     * @return TransportInterface&object{reads: int}
     */
    private function countingTransport(): TransportInterface
    {
        return new class implements TransportInterface {
            public int $reads = 0;

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
                return async(static fn(): null => null);
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $this->reads++;

                    return '';
                });
            }

            public function close(): Future
            {
                return async(static fn(): null => null);
            }
        };
    }

    /**
     * Runs a doomed handshake (no INFO ever arrives) and returns the number of readLine() calls the
     * handshake performed, which is exactly handshakePollBudget(). The deadline is large relative to
     * the empty-read loop, so the poll budget - not the wall-clock deadline - bounds the loop.
     */
    private function handshakePollCount(int $connectTimeoutMs): int
    {
        $transport = $this->countingTransport();

        $connection = new NatsConnection(
            new NatsOptions(
                connectTimeoutMs: $connectTimeoutMs,
                reconnectEnabled: false,
                retryOnFailedInitialConnect: false,
            ),
            $transport,
        );

        try {
            $connection->connect()->await();
            self::fail('Expected the doomed handshake to throw');
        } catch (ConnectionException $e) {
            self::assertStringContainsString('Expected INFO during connect', $e->getMessage());
        }

        return $transport->reads;
    }

    /**
     * The handshake poll budget is max(16, ceil(max(1, connectTimeoutMs) / 10)).
     *
     * With connectTimeoutMs=150 the budget is max(16, ceil(15.0)) = max(16, 15) = 16, so exactly 16
     * handshake reads happen before "Expected INFO during connect".
     *
     * // kills DecrementInteger (max(15, ...) -> 15) @ line 1483
     * // kills IncrementInteger (max(17, ...) -> 17) @ line 1483
     * // kills Division (ceil(.../10) -> ceil(.../9) = 17) @ line 1483
     * // kills Division (ceil(.../10) -> ceil(... * 10) = 1500) @ line 1483
     */
    public function testHandshakePollBudgetIsSixteenAtConnectTimeout150(): void
    {
        self::assertSame(16, $this->handshakePollCount(150));
    }

    /**
     * With connectTimeoutMs=203 the ceil term dominates the max(16, ...) floor and the division is
     * non-exact: budget = max(16, ceil(20.3)) = max(16, 21) = 21. This pins the rounding direction
     * (ceil, not floor/round) and the /10 divisor.
     *
     * // kills RoundingFamily (ceil -> floor = 20) @ line 1483
     * // kills RoundingFamily (ceil -> round = 20) @ line 1483
     * // kills DecrementInteger (/10 -> /11 = 19) @ line 1483
     * // kills IncrementInteger (/10 -> /9 = 23) @ line 1483
     * // kills Division (/10 -> *10 = 2030) @ line 1483
     */
    public function testHandshakePollBudgetIsTwentyOneAtConnectTimeout203(): void
    {
        self::assertSame(21, $this->handshakePollCount(203));
    }

    /**
     * A non-recoverable server -ERR frame received after connect must throw a ConnectionException whose
     * message is "Server sent error frame: " followed by the verbatim error text. Pins both operands and
     * their order in the concatenation at line 1552.
     *
     * // kills Concat (operand order swap) @ line 1552
     * // kills ConcatOperandRemoval (drop $error operand) @ line 1552
     */
    public function testNonRecoverableServerErrorFrameThrowsWithErrorTextAppended(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "-ERR 'My Fatal Boom'\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        try {
            $connection->processIncoming()->await();
            self::fail('Expected a ConnectionException from a non-recoverable -ERR frame');
        } catch (ConnectionException $e) {
            $message = $e->getMessage();
            // Order: the static prefix comes first (kills the swap mutant which would start with the error).
            self::assertStringStartsWith('Server sent error frame: ', $message);
            // The error text is appended (kills the operand-removal mutant which would drop it).
            self::assertStringContainsString('My Fatal Boom', $message);
        }
    }

    /**
     * A recoverable server -ERR frame (permissions violation) must be surfaced to the error listener with
     * the message "Server sent recoverable error frame: " followed by the error text - connection stays
     * open. Pins the concatenation order at line 1547.
     *
     * // kills Concat (operand order swap) @ line 1547
     */
    public function testRecoverableServerErrorFrameMessageStartsWithStaticPrefix(): void
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
        // The static prefix comes first (kills the swap mutant which would start with the error text).
        self::assertStringStartsWith('Server sent recoverable error frame: ', $errors[0]);
        self::assertStringContainsString('Permissions Violation for Subscription to foo', $errors[0]);
    }

    /**
     * A malformed async INFO frame must be surfaced to the error listener with the message
     * "Discarding malformed async INFO frame: " followed by the underlying JSON error - without tearing
     * down the read loop. Pins the emitError call, the concatenation order, and both operands at line 1533.
     *
     * // kills MethodCallRemoval (drop emitError call entirely) @ line 1533
     * // kills Concat (operand order swap) @ line 1533
     * // kills ConcatOperandRemoval (drop the static prefix) @ line 1533
     * // kills ConcatOperandRemoval (drop $e->getMessage()) @ line 1533
     */
    public function testMalformedAsyncInfoEmitsPrefixedErrorWithJsonDetail(): void
    {
        $errors = [];
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "INFO {not-json\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(errorListener: static function (\Throwable $err) use (&$errors): void {
                $errors[] = $err->getMessage();
            }),
            $transport,
        );
        $connection->connect()->await();

        // Must not throw out of the read loop despite the malformed INFO.
        $connection->processIncoming()->await();

        // Exactly one error is emitted (kills MethodCallRemoval, which would emit none).
        self::assertCount(1, $errors);
        $prefix = 'Discarding malformed async INFO frame: ';
        // The static prefix comes first (kills the swap and the prefix-drop mutants).
        self::assertStringStartsWith($prefix, $errors[0]);
        // The JSON error detail is appended after the prefix (kills the $e->getMessage()-drop mutant).
        self::assertGreaterThan(strlen($prefix), strlen($errors[0]));
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * inBytes accumulates across messages (+=), it is not overwritten per message (=). Two messages of
     * different sizes must yield the SUM of their payload lengths.
     *
     * // kills Assignment (+= -> =) @ line 1572
     */
    public function testInBytesAccumulatesAcrossMessages(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG updates 1 5\r\nhello\r\nMSG updates 1 6\r\nworld!\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $connection->subscribe('updates', static function (NatsMessage $message): void {})->await();
        $connection->processIncoming()->await();

        $stats = $connection->statistics();
        // 5 ("hello") + 6 ("world!") = 11; with the "=" mutant this would be 6 (last message only).
        self::assertSame(11, $stats->inBytes);
        self::assertSame(2, $stats->inMsgs);
    }

    /**
     * An HMSG frame with a header-bytes count of exactly 0 must be treated as "no headers" - rawHeaders
     * stays null (the <= 0 short-circuit), not an empty string (which the < 0 mutant would produce by
     * falling through to substr()). Pins the boundary at line 1586.
     *
     * // kills LessThanOrEqualTo (<= 0 -> < 0) @ line 1586
     */
    public function testHmsgWithZeroHeaderBytesHasNullRawHeaders(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // HMSG with header-bytes = 0, total-bytes = 5: the whole payload is body, no header block.
            "HMSG updates 1 0 5\r\nhello\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        $received = null;
        $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received = $message;
        })->await();

        $connection->processIncoming()->await();

        self::assertInstanceOf(NatsMessage::class, $received);
        /** @var NatsMessage $received */
        // Real code returns [null, payload] at headerBytes <= 0; the < 0 mutant returns ['', payload].
        self::assertNull($received->rawHeaders);
        self::assertSame('hello', $received->payload);
    }
}
