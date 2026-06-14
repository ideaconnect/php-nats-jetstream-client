<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Protocol\Enum\ProtocolFrameType;
use IDCT\NATS\Protocol\ProtocolParser;
use PHPUnit\Framework\TestCase;

/**
 * Tests crafted specifically to kill mutants Infection left surviving in ProtocolParser.
 *
 * Each method pins the exact observable behavior a mutation would change: a boundary value, an
 * exception message substring/ordering, a substr offset, a removed side effect, or a label swap.
 */
final class ProtocolParserMutationTest extends TestCase
{
    /**
     * The unterminated-control-line OOM guard checks `($bufferLength - $offset) > MAX` where MAX is
     * exactly 1 MiB. By first consuming a complete PING frame (advancing $offset to 6) and then
     * appending exactly MAX bytes of CRLF-less tail in the SAME chunk, the remaining unterminated
     * length is exactly MAX. Real code uses `>` so it does NOT throw; the boundary and operand
     * mutants both flip this to a throw.
     *
     * kills GreaterThan (`>` -> `>=`) @ line 101 : `>=` would throw at the exact MAX boundary.
     * kills Minus (`-` -> `+`) @ line 101 : `(6 + 1048576 + 6) > 1048576` would throw.
     */
    public function testUnterminatedControlLineAtExactBoundaryWithOffsetIsBuffered(): void
    {
        $parser = new ProtocolParser();

        // "PING\r\n" (6 bytes, fully parsed -> offset advances to 6) + exactly 1 MiB of non-CRLF tail.
        $frames = $parser->push("PING\r\n" . str_repeat('A', 1048576));

        // Real code: the PING parses and the exact-boundary tail is buffered (no throw).
        self::assertCount(1, $frames);
        self::assertSame(ProtocolFrameType::Ping, $frames[0]->type);
    }

    /**
     * Distinguishes Minus (`-` -> `+`) from the real subtraction independently of the `>=` boundary.
     * With $offset = 6 (after a PING) and a tail of 1048570 bytes: real `(1048576-6 ... ) = 1048570 > 1048576`
     * is false (buffered); mutant `+` gives `1048576+6 = 1048582 > 1048576` -> true (throws). The `>=`
     * mutant also stays false here (1048570 >= 1048576 is false), so this isolates the operand sign.
     *
     * kills Minus (`-` -> `+`) @ line 101
     */
    public function testUnterminatedControlLineBelowBoundaryWithOffsetIsBufferedNotThrown(): void
    {
        $parser = new ProtocolParser();

        // 6 consumed + 1048570 unterminated => real remaining length 1048570 (< 1 MiB), so buffered.
        $frames = $parser->push("PING\r\n" . str_repeat('A', 1048570));

        self::assertCount(1, $frames);
        self::assertSame(ProtocolFrameType::Ping, $frames[0]->type);
    }

    /**
     * INFO payload is read via `ltrim(substr($line, 4))`: strip the 4-char "INFO" verb then trim the
     * single separating space. Incrementing the offset to 5 would also swallow the first payload byte.
     * Using a payload whose first char is significant (`{`) detects the off-by-one.
     *
     * kills IncrementInteger (substr offset 4 -> 5) @ line 155
     */
    public function testInfoPayloadKeepsFirstByteAfterVerb(): void
    {
        $parser = new ProtocolParser();

        $frames = $parser->push("INFO {\"server_id\":\"S1\"}\r\n");

        self::assertCount(1, $frames);
        self::assertSame(ProtocolFrameType::Info, $frames[0]->type);
        // Real: '{"server_id":"S1"}'. Mutant (substr 5): '"server_id":"S1"}' (leading '{' lost).
        self::assertSame('{"server_id":"S1"}', $frames[0]->infoPayload);
    }

    /**
     * -ERR payload is read via `trim(substr($line, 4))`. The verb is "-ERR" (4 chars). Incrementing
     * the offset to 5 would drop the first character of the error text. Use a single-quoted error so
     * the leading quote is the load-bearing first byte.
     *
     * kills IncrementInteger (substr offset 4 -> 5) @ line 162
     */
    public function testErrPayloadKeepsFirstByteAfterVerb(): void
    {
        $parser = new ProtocolParser();

        $frames = $parser->push("-ERR 'Authorization Violation'\r\n");

        self::assertCount(1, $frames);
        self::assertSame(ProtocolFrameType::Err, $frames[0]->type);
        // Real: "'Authorization Violation'". Mutant (substr 5): "Authorization Violation'" (leading ' lost).
        self::assertSame("'Authorization Violation'", $frames[0]->error);
    }

    /**
     * Malformed MSG line message is `'Invalid MSG frame line: ' . $line`. The Concat-swap mutant emits
     * `$line . 'Invalid MSG frame line: '` and the operand-removal mutants drop one half. Asserting the
     * full concatenated string (prefix then the line) pins the order and both operands.
     *
     * kills Concat (operand swap) @ line 205
     * kills ConcatOperandRemoval (drop $line) @ line 205
     * kills ConcatOperandRemoval (drop prefix) @ line 205
     */
    public function testMalformedMsgLineMessageIsPrefixThenLine(): void
    {
        $parser = new ProtocolParser();

        $this->expectException(ProtocolException::class);
        // Real message is exactly the prefix immediately followed by the original line.
        $this->expectExceptionMessage('Invalid MSG frame line: MSG only-two-fields');

        $parser->push("MSG only-two-fields\r\n");
    }

    /**
     * MSG oversize message is `'MSG frame payload size is invalid: ' . $size`. Pin prefix-then-size
     * ordering and both operands. maxFrameSize=10, size=20.
     *
     * kills Concat (operand swap) @ line 211
     * kills ConcatOperandRemoval (drop $size) @ line 211
     */
    public function testMsgOversizeMessageIsPrefixThenSize(): void
    {
        $parser = new ProtocolParser(maxFrameSize: 10);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('MSG frame payload size is invalid: 20');

        $parser->push("MSG subject 1 20\r\n" . str_repeat('x', 20) . "\r\n");
    }

    /**
     * Malformed HMSG line message is `'Invalid HMSG frame line: ' . $line`. Pin order + both operands.
     *
     * kills Concat (operand swap) @ line 232
     * kills ConcatOperandRemoval (drop $line) @ line 232
     * kills ConcatOperandRemoval (drop prefix) @ line 232
     */
    public function testMalformedHmsgLineMessageIsPrefixThenLine(): void
    {
        $parser = new ProtocolParser();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('Invalid HMSG frame line: HMSG too short');

        $parser->push("HMSG too short\r\n");
    }

    /**
     * HMSG total-bytes overflow is checked with `$totalBytes > $this->maxFrameSize`. With
     * maxFrameSize = 26 and totalBytes = 26 exactly, the real `>` does NOT throw and the frame parses;
     * the `>=` mutant would reject the exact-boundary frame.
     *
     * kills GreaterThan (`>` -> `>=`) @ line 247
     */
    public function testHmsgTotalBytesAtExactMaxFrameSizeIsAccepted(): void
    {
        $parser = new ProtocolParser(maxFrameSize: 26);
        $headersAndPayload = "NATS/1.0\r\nStatus:100\r\n\r\nok"; // 26 bytes total

        // headerBytes=24, totalBytes=26 (== maxFrameSize): boundary must be accepted by real code.
        $frames = $parser->push("HMSG hb 3 24 26\r\n" . $headersAndPayload . "\r\n");

        self::assertCount(1, $frames);
        self::assertSame(ProtocolFrameType::HMsg, $frames[0]->type);
        self::assertSame(26, $frames[0]->totalBytes);
        self::assertSame($headersAndPayload, $frames[0]->payload);
    }

    /**
     * HMSG oversize message is `'HMSG frame payload size is invalid: ' . $totalBytes`. Pin
     * prefix-then-size ordering and both operands. maxFrameSize=10, totalBytes=20.
     *
     * kills Concat (operand swap) @ line 248
     * kills ConcatOperandRemoval (drop $totalBytes) @ line 248
     */
    public function testHmsgOversizeMessageIsPrefixThenTotalBytes(): void
    {
        $parser = new ProtocolParser(maxFrameSize: 10);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('HMSG frame payload size is invalid: 20');

        $parser->push("HMSG subject 1 5 20\r\n" . str_repeat('x', 20) . "\r\n");
    }

    /**
     * HMSG header-bytes-exceed-total message is `'HMSG header bytes exceed total bytes: ' . $line`.
     * Pin prefix-then-line ordering and both operands. headerBytes=20 > totalBytes=10.
     *
     * kills Concat (operand swap) @ line 252
     * kills ConcatOperandRemoval (drop $line) @ line 252
     */
    public function testHmsgHeaderBytesExceedTotalMessageIsPrefixThenLine(): void
    {
        $parser = new ProtocolParser();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('HMSG header bytes exceed total bytes: HMSG subject 1 20 10');

        $parser->push("HMSG subject 1 20 10\r\n1234567890\r\n");
    }

    /**
     * parseUnsignedInt's out-of-range guard must actually `throw` the constructed exception. The
     * Throw_ mutant constructs the ProtocolException but discards it (no `throw`), so an overflowing
     * 20-digit size would fall through and be (int)-coerced/accepted. Real code throws.
     *
     * kills Throw_ (drop `throw`) @ line 280
     */
    public function testOutOfRangeSizeActuallyThrows(): void
    {
        $parser = new ProtocolParser();

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('out of range in frame line');

        // 20 digits > strlen(PHP_INT_MAX)=19, so the range guard fires and MUST throw.
        $parser->push("MSG subject 1 99999999999999999999\r\nhello\r\n");
    }

    /**
     * The pending-payload CRLF-terminator error labels the frame by type:
     * `$pending['type'] === HMsg ? 'HMSG' : 'MSG'`. The Ternary mutant swaps the branches, so an MSG
     * frame would be labelled 'HMSG'. Drive an MSG payload without a trailing CRLF and assert the
     * message names 'MSG' (and the full concatenated text), which also covers the line-299 concat
     * mutants for the MSG branch.
     *
     * kills Ternary (label branch swap) @ line 298
     * kills Concat (operand swap) @ line 299
     * kills ConcatOperandRemoval (drop $label) @ line 299
     */
    public function testMsgMissingTerminatorErrorIsLabelledMsg(): void
    {
        $parser = new ProtocolParser();

        try {
            // 5-byte payload "hello" followed by "XX" instead of CRLF.
            $parser->push("MSG s 1 5\r\nhelloXX");
            self::fail('Expected ProtocolException for a payload without a terminating CRLF');
        } catch (ProtocolException $e) {
            // Real: "MSG frame payload must be terminated by CRLF".
            // Ternary mutant: would start with "HMSG ...".
            self::assertSame('MSG frame payload must be terminated by CRLF', $e->getMessage());
            self::assertStringStartsWith('MSG ', $e->getMessage());
        }
    }

    /**
     * Same CRLF-terminator error for the HMSG branch. Confirms the ternary picks 'HMSG' for HMsg
     * frames and that the label concat keeps `$label . ' frame payload...'` ordering. The
     * ConcatOperandRemoval that drops the suffix (leaving only $label = 'HMSG') is killed by asserting
     * the full message, not just the prefix.
     *
     * kills Ternary (label branch swap) @ line 298
     * kills Concat (operand swap) @ line 299
     * kills ConcatOperandRemoval (drop suffix, leaving only $label) @ line 299
     */
    public function testHmsgMissingTerminatorErrorIsLabelledHmsg(): void
    {
        $parser = new ProtocolParser();

        try {
            // headerBytes=12, totalBytes=17; supply 17 payload bytes then "XX" instead of CRLF.
            $headersAndPayload = "NATS/1.0\r\n\r\nhello"; // 17 bytes
            $parser->push("HMSG orders 10 12 17\r\n" . $headersAndPayload . 'XX');
            self::fail('Expected ProtocolException for an HMSG payload without a terminating CRLF');
        } catch (ProtocolException $e) {
            self::assertSame('HMSG frame payload must be terminated by CRLF', $e->getMessage());
        }
    }
}
