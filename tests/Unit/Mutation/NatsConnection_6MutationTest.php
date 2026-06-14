<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\AuthenticationException;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\FakeTransport;

/**
 * Targeted tests that kill surviving Infection mutants in src/Connection/NatsConnection.php
 * (chunk 6: lines 1586-1786). Each assertion group is annotated with the mutant it pins.
 */
final class NatsConnection_6MutationTest extends \PHPUnit\Framework\TestCase
{
    private const INFO_TEMPLATE
        = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":%d,"headers":true}'
        . "\r\n";

    /**
     * Builds a JSON object string nested $depth levels deep, e.g. depth 2 => {"x":{"x":1}}.
     */
    private static function nestedJson(int $depth): string
    {
        return str_repeat('{"x":', $depth) . '1' . str_repeat('}', $depth);
    }

    /**
     * kills LogicalOr @ line 1586 ( `||` -> `&&` between headerBytes===null and headerBytes<=0 ).
     *
     * An HMSG with headerBytes=0 hits the third sub-condition only (headerBytes<=0 true, ===null false).
     * Real (OR): returns rawHeaders=null. Mutant (AND): both must hold, so it splits at offset 0 and
     * returns an empty-string header block ('') instead of null.
     */
    public function testHmsgWithZeroHeaderBytesYieldsNullRawHeaders(): void
    {
        $transport = new FakeTransport([
            sprintf(self::INFO_TEMPLATE, 1048576),
            "PONG\r\n",
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
        self::assertSame('hello', $received->payload);
        // Real returns null; the AND-mutant returns '' (empty header block).
        self::assertNull($received->rawHeaders);
    }

    /**
     * kills Concat + ConcatOperandRemoval @ line 1617 (drop-oldest slow-consumer message text).
     *
     * With maxPending=1 and two messages for the same sid, the second triggers DropOldest: dequeue the
     * first, emit a debug error, enqueue the second. The error message must be the exact, in-order text
     * including the sid; reordering/removing operands changes it.
     */
    public function testDropOldestEmitsExactMessageWithSid(): void
    {
        $errors = [];
        $transport = new FakeTransport([
            sprintf(self::INFO_TEMPLATE, 1048576),
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

        $delivered = [];
        $connection->subscribe('updates', static function (NatsMessage $m) use (&$delivered): void {
            $delivered[] = $m->payload;
        })->await();

        $connection->processIncoming()->await();

        self::assertCount(1, $errors);
        self::assertSame('Slow consumer on sid 1: dropped oldest message', $errors[0]);
        // The newest survived (oldest was dropped).
        self::assertSame(['B'], $delivered);
    }

    /**
     * kills Concat + ConcatOperandRemoval + MethodCallRemoval @ line 1619 (drop-newest message + emit).
     *
     * The second message triggers DropNewest: emit a debug error and return (the newest is NOT enqueued).
     * MethodCallRemoval removes the emit entirely (0 errors); the concat mutants change the text.
     */
    public function testDropNewestEmitsExactMessageWithSid(): void
    {
        $errors = [];
        $transport = new FakeTransport([
            sprintf(self::INFO_TEMPLATE, 1048576),
            "PONG\r\n",
            "MSG updates 1 1\r\nA\r\nMSG updates 1 1\r\nB\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(
                maxPendingMessagesPerSubscription: 1,
                slowConsumerPolicy: SlowConsumerPolicy::DropNewest,
                errorListener: static function (\Throwable $err) use (&$errors): void {
                    $errors[] = $err->getMessage();
                },
            ),
            $transport,
        );
        $connection->connect()->await();

        $delivered = [];
        $connection->subscribe('updates', static function (NatsMessage $m) use (&$delivered): void {
            $delivered[] = $m->payload;
        })->await();

        $connection->processIncoming()->await();

        self::assertCount(1, $errors);
        self::assertSame('Slow consumer on sid 1: dropped newest message', $errors[0]);
        // The earliest survived (newest was dropped); confirms the `return` short-circuit too.
        self::assertSame(['A'], $delivered);
    }

    /**
     * kills Concat + ConcatOperandRemoval @ line 1623 and MethodCallRemoval @ line 1624 (overflow error).
     *
     * With the Error policy, overflow builds a ConnectionException, emits it to the error listener, then
     * throws it. The thrown message must be the exact in-order text including the sid (1623); the listener
     * must receive exactly that error (1624 removes the emit but still throws).
     */
    public function testOverflowErrorPolicyEmitsAndThrowsExactMessage(): void
    {
        $errors = [];
        $transport = new FakeTransport([
            sprintf(self::INFO_TEMPLATE, 1048576),
            "PONG\r\n",
            "MSG updates 1 1\r\nA\r\nMSG updates 1 1\r\nB\r\n",
        ]);

        $connection = new NatsConnection(
            new NatsOptions(
                maxPendingMessagesPerSubscription: 1,
                slowConsumerPolicy: SlowConsumerPolicy::Error,
                errorListener: static function (\Throwable $err) use (&$errors): void {
                    $errors[] = $err->getMessage();
                },
            ),
            $transport,
        );
        $connection->connect()->await();
        $connection->subscribe('updates', static function (NatsMessage $m): void {})->await();

        $thrown = null;
        try {
            $connection->processIncoming()->await();
        } catch (ConnectionException $e) {
            $thrown = $e;
        }

        self::assertInstanceOf(ConnectionException::class, $thrown);
        // kills Concat/ConcatOperandRemoval @ 1623
        self::assertSame('Subscription queue overflow for sid 1', $thrown->getMessage());
        // kills MethodCallRemoval @ 1624 (emitError must have fired with the same overflow error)
        self::assertCount(1, $errors);
        self::assertSame('Subscription queue overflow for sid 1', $errors[0]);
    }

    /**
     * kills the down-side line-1657 mutants (1023*1024, 1024*1023, 1024/1024, Plus `-`).
     *
     * With max_payload=7340033 (just over 7 MiB), the real inbound bound is
     * max(8 MiB, max_payload + 1 MiB) = 8388609. A MSG of exactly 8388609 bytes sits ON the bound, so the
     * real parser accepts it and delivers. Every down-mutant lowers the +1 MiB term so the bound collapses
     * to the 8 MiB floor (8388608) and the same frame is rejected (parser throws -> recover -> not
     * delivered). reconnectEnabled=false makes a rejection surface as a thrown ConnectionException.
     */
    public function testInboundFrameAtExactNegotiatedBoundIsAccepted(): void
    {
        $maxPayload = 7340033; // > 7 MiB so (max_payload + 1 MiB) wins the max() against the 8 MiB floor.
        $size = $maxPayload + 1024 * 1024; // 8388609 == the real inbound bound.
        $payload = str_repeat('A', $size);
        $frame = sprintf("MSG big 1 %d\r\n%s\r\n", $size, $payload);

        $transport = new FakeTransport([
            sprintf(self::INFO_TEMPLATE, $maxPayload),
            "PONG\r\n",
            $frame,
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        $received = null;
        $connection->subscribe('big', static function (NatsMessage $m) use (&$received): void {
            $received = $m;
        })->await();

        $frames = $connection->processIncoming()->await();

        self::assertSame(1, $frames);
        self::assertInstanceOf(NatsMessage::class, $received);
        /** @var NatsMessage $received */
        self::assertSame($size, strlen($received->payload));
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * kills the up-side line-1657 mutants (1025*1024, 1024*1025) AND the line-1653 condition mutants that
     * make a normal server return the 64 MiB fallback (Identical `!==`, LessThanOrEqualToNegotiation `>0`,
     * LogicalOrAllSubExprNegation, LogicalOrNegation).
     *
     * Same server (max_payload=7340033, real bound 8388609). A MSG of 8388610 bytes is ONE byte over the
     * bound: the real parser rejects it (throws -> recover; reconnect disabled => ConnectionException).
     * The up-mutants raise the bound (8389633) and any 1653 mutant that returns 64 MiB would instead
     * ACCEPT and deliver the frame, so the absence of a delivery / presence of the throw pins them.
     */
    public function testInboundFrameOneByteOverNegotiatedBoundIsRejected(): void
    {
        $maxPayload = 7340033;
        $size = $maxPayload + 1024 * 1024 + 1; // 8388610 == real bound + 1.
        $payload = str_repeat('A', $size);
        $frame = sprintf("MSG big 1 %d\r\n%s\r\n", $size, $payload);

        $transport = new FakeTransport([
            sprintf(self::INFO_TEMPLATE, $maxPayload),
            "PONG\r\n",
            $frame,
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        $received = null;
        $connection->subscribe('big', static function (NatsMessage $m) use (&$received): void {
            $received = $m;
        })->await();

        // Real code: the oversized frame is rejected by the parser; with reconnect disabled the recovery
        // raises a ConnectionException out of processIncoming(). Any mutant that widens the bound (or
        // returns the 64 MiB fallback) would deliver the frame and NOT throw.
        $thrown = null;
        try {
            $connection->processIncoming()->await();
        } catch (ConnectionException $e) {
            $thrown = $e;
        }

        self::assertInstanceOf(ConnectionException::class, $thrown);
        self::assertNull($received, 'oversized frame must not be delivered');
    }

    /**
     * kills line-1653 mutants whose only distinguishing input is max_payload==0
     * (LessThanOrEqualTo `<0`, LogicalOr `&&`, LogicalOrNegation, LessThanOrEqualToNegotiation `>0`).
     *
     * With max_payload=0 the real bound is the 64 MiB fallback, so an 8388609-byte frame (> the 8 MiB
     * default) is accepted and delivered. Each of these mutants instead falls through to
     * max(8 MiB, 0 + 1 MiB) = 8 MiB, which rejects the frame.
     */
    public function testZeroMaxPayloadUsesLargeFallbackBoundAndAcceptsAboveEightMiB(): void
    {
        $size = 8 * 1024 * 1024 + 1; // 8388609, one byte over the 8 MiB default floor.
        $payload = str_repeat('A', $size);
        $frame = sprintf("MSG big 1 %d\r\n%s\r\n", $size, $payload);

        $transport = new FakeTransport([
            sprintf(self::INFO_TEMPLATE, 0),
            "PONG\r\n",
            $frame,
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        self::assertSame(0, $connection->maxPayload());

        $received = null;
        $connection->subscribe('big', static function (NatsMessage $m) use (&$received): void {
            $received = $m;
        })->await();

        $frames = $connection->processIncoming()->await();

        self::assertSame(1, $frames);
        self::assertInstanceOf(NatsMessage::class, $received);
        /** @var NatsMessage $received */
        self::assertSame($size, strlen($received->payload));
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * kills GreaterThan @ line 1670 ( `$max > 0` -> `$max >= 0` ) in enforceMaxPayload().
     *
     * With max_payload=0 the real guard `$max > 0` is false, so outbound publishes are NOT size-checked
     * and a non-empty publish succeeds. The mutant `$max >= 0` is true, turning the check into
     * `totalBytes > 0`, which rejects every non-empty payload with a ProtocolException.
     */
    public function testPublishNotEnforcedWhenServerMaxPayloadIsZero(): void
    {
        $transport = new FakeTransport([
            sprintf(self::INFO_TEMPLATE, 0),
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(), $transport);
        $connection->connect()->await();

        // Real: succeeds (no enforcement when max_payload == 0). Mutant: throws ProtocolException.
        $connection->publish('a.b', 'hi')->await();

        $pubs = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_starts_with($w, 'PUB a.b '),
        ));
        self::assertSame(["PUB a.b 2\r\nhi\r\n"], $pubs);
    }

    /**
     * kills DecrementInteger @ line 1685 ( json_decode depth 512 -> 511 ).
     *
     * A second INFO frame (decoded via decodeServerInfoPayload at line 1411 during awaitInitialPong, with
     * no JsonException swallowing) nested exactly 511 levels deep parses at depth 512 (real) but throws at
     * depth 511 (mutant). Real: the handshake completes (Open). Mutant: JsonException aborts connect().
     */
    public function testHandshakeAcceptsInfoNested511DeepUnderDepthLimit(): void
    {
        $secondInfo = 'INFO ' . self::nestedJson(511) . "\r\n";
        $transport = new FakeTransport([
            sprintf(self::INFO_TEMPLATE, 1048576),
            $secondInfo,
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);
        $connection->connect()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * kills IncrementInteger @ line 1685 ( json_decode depth 512 -> 513 ).
     *
     * A second INFO nested exactly 512 levels deep is at the depth limit: real (512) THROWS a JsonException
     * (so connect fails), while the mutant (513) would parse it and complete the handshake.
     */
    public function testHandshakeRejectsInfoNested512DeepAtDepthLimit(): void
    {
        $secondInfo = 'INFO ' . self::nestedJson(512) . "\r\n";
        $transport = new FakeTransport([
            sprintf(self::INFO_TEMPLATE, 1048576),
            $secondInfo,
            "PONG\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);

        // Real: the depth-512 INFO exceeds the depth-512 limit and aborts the handshake. The mutant
        // (depth 513) would accept it and reach Open.
        $thrown = null;
        try {
            $connection->connect()->await();
        } catch (\Throwable $e) {
            $thrown = $e;
        }

        self::assertNotNull($thrown, 'depth-512 INFO must abort the handshake on real code');
        self::assertNotSame(ConnectionState::Open, $connection->state());
    }

    /**
     * kills Concat + ConcatOperandRemoval @ line 1701 (authentication connect-error message).
     *
     * An authorization -ERR during connect builds an AuthenticationException whose message is
     * 'Server rejected authentication during connect: ' . $error. Reordering or dropping an operand
     * changes the exact text.
     */
    public function testAuthConnectErrorMessageIsExactAndInOrder(): void
    {
        $transport = new FakeTransport([
            sprintf(self::INFO_TEMPLATE, 1048576),
            "-ERR Authorization Violation\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);

        $thrown = null;
        try {
            $connection->connect()->await();
        } catch (AuthenticationException $e) {
            $thrown = $e;
        }

        self::assertInstanceOf(AuthenticationException::class, $thrown);
        self::assertSame(
            'Server rejected authentication during connect: Authorization Violation',
            $thrown->getMessage(),
        );
    }

    /**
     * kills Concat + ConcatOperandRemoval @ line 1704 (non-auth connect-error message).
     *
     * A non-authorization -ERR during connect builds a ConnectionException whose message is
     * 'Server error during connect: ' . $error. Reordering or dropping an operand changes the exact text.
     */
    public function testServerConnectErrorMessageIsExactAndInOrder(): void
    {
        $transport = new FakeTransport([
            sprintf(self::INFO_TEMPLATE, 1048576),
            "-ERR Maximum Connections Exceeded\r\n",
        ]);

        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false), $transport);

        $thrown = null;
        try {
            $connection->connect()->await();
        } catch (ConnectionException $e) {
            $thrown = $e;
        }

        self::assertInstanceOf(ConnectionException::class, $thrown);
        self::assertNotInstanceOf(AuthenticationException::class, $thrown);
        self::assertSame(
            'Server error during connect: Maximum Connections Exceeded',
            $thrown->getMessage(),
        );
    }
}
