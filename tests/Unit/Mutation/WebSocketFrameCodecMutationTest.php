<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Transport\WebSocketFrameCodec;

/**
 * Targeted unit tests that pin behaviour Infection mutants of {@see WebSocketFrameCodec} would change.
 *
 * Each test asserts the SPECIFIC observable difference (a boundary header byte, an emitted/withheld
 * frame, an exact exception message, the exact deflate output) so the assertion passes on the real
 * code and fails on the mutated code. Tests are pure and deterministic (no sockets, no sleeping).
 */
final class WebSocketFrameCodecMutationTest extends \PHPUnit\Framework\TestCase
{
    /**
     * encode()'s $mask default must be true: calling it without the flag produces a masked client frame
     * (mask bit set, a 4-byte key included). The mutant flips the default to false -> unmasked frame.
     */
    public function testEncodeDefaultsToMaskedClientFrame(): void
    {
        // kills TrueValue @ line 38
        $encoded = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'hi');

        // Mask bit (0x80) must be set on byte 2 when masking is on by default.
        self::assertSame(0x80, ord($encoded[1]) & 0x80, 'default encode must set the mask bit');
        // 2 header + 4 mask key + 2 payload bytes = 8. An unmasked frame would be only 4 bytes.
        self::assertSame(8, strlen($encoded), 'default masked frame must carry the 4-byte mask key');
    }

    /**
     * The 7-bit length form is inclusive at 125: a 125-byte payload encodes its length directly in byte 2
     * (single-byte header tail). The "<=" -> "<" mutant would push 125 into the 16-bit (126-marker) form.
     */
    public function testLength125UsesSingleByteLength(): void
    {
        // kills LessThanOrEqualTo @ line 45
        $encoded = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, str_repeat('a', 125), false);

        self::assertSame(125, ord($encoded[1]) & 0x7F, '125 must be stored directly in the 7-bit length');
        // Header is exactly 2 bytes (no extended length field) for length 125.
        self::assertSame(2, strlen($encoded) - 125, 'length 125 must not use an extended length field');
    }

    /**
     * The 16-bit length form is inclusive at 0xFFFF: a 65535-byte payload uses the 126 marker + 2 bytes.
     * The "<=" -> "<" mutant would push 65535 into the 64-bit (127-marker) form.
     */
    public function testLength65535UsesSixteenBitLength(): void
    {
        // kills LessThanOrEqualTo @ line 47
        $encoded = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, str_repeat('a', 65535), false);

        self::assertSame(126, ord($encoded[1]) & 0x7F, '65535 must use the 126 (16-bit) length marker');
        // 126 marker means a 2-byte length field -> 4-byte header total (not the 10-byte 64-bit header).
        self::assertSame(4, strlen($encoded) - 65535, 'length 65535 must use a 2-byte extended length');
    }

    /**
     * decode() reads FIN strictly from bit 0x80 and RSV1 strictly from bit 0x40. A non-final, non-RSV1
     * frame whose opcode is TEXT (0x1) sets bit 0 of byte 1 only. The 0x80->129 and 0x40->65 mutants
     * would let that opcode bit leak into the FIN / RSV1 flags.
     */
    public function testFinAndRsv1ReadOnlyTheirOwnBits(): void
    {
        // byte1 = 0x01: FIN=0, RSV1=0, opcode=TEXT(0x1). byte2 = 3 (unmasked, length 3).
        $buffer = chr(0x01) . chr(3) . 'abc';
        $frames = WebSocketFrameCodec::decode($buffer);

        self::assertCount(1, $frames);
        // kills IncrementInteger @ line 90  (0x80 -> 129 would see opcode bit 0)
        self::assertFalse($frames[0]['fin'], 'FIN must come from 0x80 only, not the opcode low bit');
        // kills IncrementInteger @ line 91  (0x40 -> 65 would see opcode bit 0)
        self::assertFalse($frames[0]['rsv1'], 'RSV1 must come from 0x40 only, not the opcode low bit');
        self::assertSame(WebSocketFrameCodec::OP_TEXT, $frames[0]['opcode']);
    }

    /**
     * A 16-bit-marker frame needs offset(2)+2 = 4 bytes for the header before the payload. With exactly
     * 4 bytes available and a declared length of 0, the real code reads the length and emits one empty
     * frame. The "+2"->"+3" and "<"->"<=" mutants both demand a 5th byte and would withhold the frame.
     */
    public function testSixteenBitHeaderAcceptedAtExactFourBytes(): void
    {
        // header(2) + 2-byte length declaring 0 = exactly 4 bytes, no payload.
        $buffer = pack('CC', 0x82, 126) . pack('n', 0);
        $frames = WebSocketFrameCodec::decode($buffer);

        // kills IncrementInteger @ line 98  and  LessThan @ line 98
        self::assertCount(1, $frames, '4 bytes is exactly enough to read a 16-bit length header');
        self::assertSame('', $frames[0]['payload']);
        self::assertSame('', $buffer, 'the complete (empty) frame must be fully consumed');
    }

    /**
     * A 64-bit-marker frame needs offset(2)+8 = 10 bytes for the length header. With only 9 bytes (header
     * + 7 of the 8 length bytes) the real code waits: no frame, buffer untouched. The "+8"->"+7" mutant
     * would proceed and try to unpack a 64-bit value from 7 bytes.
     */
    public function testSixtyFourBitHeaderWaitsForFullEightLengthBytes(): void
    {
        // header(2) + only 7 of the required 8 length bytes = 9 bytes total.
        $buffer = pack('CC', 0x82, 127) . str_repeat("\x00", 7);
        $original = $buffer;

        $frames = WebSocketFrameCodec::decode($buffer);

        // kills DecrementInteger @ line 106
        self::assertSame([], $frames, 'a 64-bit length header is incomplete with only 7 of 8 length bytes');
        self::assertSame($original, $buffer, 'buffer must be left untouched until all 8 length bytes arrive');
    }

    /**
     * MAX_FRAME_PAYLOAD (64 MiB) is an inclusive upper bound: a frame declaring exactly that length is
     * accepted (it simply waits for the payload). The ">"->">=" mutant would reject the boundary value.
     */
    public function testMaxFramePayloadBoundaryIsAccepted(): void
    {
        // kills GreaterThan @ line 146 (> -> >= would flag exactly-MAX as a terminal violation)
        $max = 64 * 1024 * 1024; // MAX_FRAME_PAYLOAD
        $buffer = pack('CC', 0x82, 127) . pack('J', $max); // declares length == MAX, no payload yet
        $original = $buffer;

        $frames = WebSocketFrameCodec::decode($buffer, $terminal); // must NOT flag at exactly MAX

        self::assertSame([], $frames, 'a frame at exactly MAX length is valid and just waits for payload');
        self::assertSame($original, $buffer);
        self::assertNull($terminal, 'exactly MAX is within bounds - no terminal violation may be reported');
    }

    /**
     * RFC 6455 5.5 applies to EVERY control opcode, including OP_CLOSE (0x8, the lowest): a Close
     * frame with FIN cleared is a fragmented control frame and must be reported terminal. The
     * ">= 0x8" -> "> 0x8" mutant would exempt exactly the Close opcode from the control-frame rules.
     */
    public function testFragmentedCloseFrameIsATerminalControlViolation(): void
    {
        // kills GreaterThanOrEqualTo @ line 161 (opcode >= 0x8 -> > 0x8 skips OP_CLOSE)
        $close = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_CLOSE, pack('n', 1000), false);
        $close[0] = chr(ord($close[0]) & 0x7F); // clear FIN on the control frame
        $buffer = $close;

        $frames = WebSocketFrameCodec::decode($buffer, $terminal);

        self::assertSame([], $frames, 'a fragmented Close must not be decoded as a data-like frame');
        self::assertInstanceOf(ProtocolException::class, $terminal);
        self::assertStringContainsString('control frame', $terminal->getMessage());
    }

    /**
     * RFC 6455 5.5's control-frame payload cap is INCLUSIVE at 125 bytes: a ping carrying exactly
     * 125 bytes is legal and must decode, not be flagged as oversized. The "> 125" -> ">= 125"
     * mutant would reject the boundary payload.
     */
    public function testControlFrameWithExactly125BytePayloadIsAccepted(): void
    {
        // kills GreaterThan @ line 161 (length > 125 -> >= 125 rejects the inclusive boundary)
        $payload = str_repeat('p', 125);
        $buffer = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_PING, $payload, false);

        $frames = WebSocketFrameCodec::decode($buffer, $terminal);

        self::assertNull($terminal, 'a 125-byte control payload is exactly at the inclusive cap');
        self::assertCount(1, $frames);
        self::assertSame(WebSocketFrameCodec::OP_PING, $frames[0]['opcode']);
        self::assertSame($payload, $frames[0]['payload']);
    }

    /**
     * One byte over MAX_FRAME_PAYLOAD throws with the exact message "<prefix>: <length>". The Concat
     * mutant would reorder it to "<length><prefix>"; the ConcatOperandRemoval mutant would drop the
     * length entirely. Asserting the full message string kills both.
     */
    public function testOutOfBoundsLengthReportsExactTerminalMessage(): void
    {
        $tooLarge = 64 * 1024 * 1024 + 1; // MAX + 1
        $buffer = pack('CC', 0x82, 127) . pack('J', $tooLarge);

        WebSocketFrameCodec::decode($buffer, $terminal);

        // kills Concat / ConcatOperandRemoval on the terminal message construction
        self::assertInstanceOf(ProtocolException::class, $terminal);
        self::assertSame(
            'WebSocket frame payload length out of bounds: ' . $tooLarge,
            $terminal->getMessage(),
        );
    }

    /**
     * A masked frame with a 0-length payload needs offset(2)+4 = 6 bytes (header + 4 mask key). With
     * exactly 6 bytes the real code reads the mask key and emits one empty frame. The "+4"->"+5" mutant
     * and the "<"->"<=" mutant both demand a 7th byte and would withhold the frame.
     */
    public function testMaskKeyAcceptedAtExactSixBytes(): void
    {
        // header(2) + 4-byte mask key, byte2 = 0x80|0 (masked, length 0) = exactly 6 bytes.
        $buffer = pack('CC', 0x82, 0x80) . 'ABCD';
        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);

        // kills IncrementInteger @ line 121 (+4 -> +5)  and  LessThan @ line 121 (< -> <=)
        self::assertCount(1, $frames, '6 bytes is exactly enough for a masked, empty-payload frame');
        self::assertSame('', $frames[0]['payload']);
        self::assertSame('', $buffer, 'the complete masked frame must be fully consumed');
    }

    /**
     * deflate() strips exactly the trailing 4-byte empty DEFLATE block (0x00 0x00 0xff 0xff). The output
     * must therefore (a) NOT end with that marker and (b) be exactly 4 bytes shorter than the raw
     * sync-flushed stream. This pins the IfNegation (skip the trim), DecrementInteger (-4 -> -5, trims a
     * 5th byte) and UnwrapSubstr (no trim at all) mutants.
     */
    public function testDeflateStripsExactlyFourByteEmptyBlock(): void
    {
        $payload = 'hello world repeated content content content';

        // Reference: the raw sync-flushed DEFLATE stream the codec produces before trimming.
        $ctx = deflate_init(ZLIB_ENCODING_RAW);
        self::assertNotFalse($ctx);
        $raw = deflate_add($ctx, $payload, ZLIB_SYNC_FLUSH);
        self::assertIsString($raw);
        self::assertStringEndsWith("\x00\x00\xff\xff", $raw, 'sync-flush should end with the empty block');

        $compressed = WebSocketFrameCodec::deflate($payload);

        // kills IfNegation @ line 168  and  UnwrapSubstr @ line 169 (both would keep the marker)
        self::assertStringEndsNotWith("\x00\x00\xff\xff", $compressed, 'the empty-block tail must be removed');
        // kills DecrementInteger @ line 169 (-4 -> -5 would drop a 5th byte, making it 1 byte shorter)
        self::assertSame(strlen($raw) - 4, strlen($compressed), 'exactly 4 trailing bytes must be removed');
        // The codec output must equal the raw stream with precisely its last 4 bytes removed.
        self::assertSame(substr($raw, 0, -4), $compressed);

        // Sanity: it still round-trips through inflate().
        self::assertSame($payload, WebSocketFrameCodec::inflate($compressed));
    }
}
