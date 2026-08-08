<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Transport\WebSocketFrameCodec;
use PHPUnit\Framework\TestCase;

final class WebSocketFrameCodecTest extends TestCase
{
    /**
     * Verifies a masked client frame round-trips through decode() (#31).
     */
    public function testEncodeMaskedFrameRoundTrips(): void
    {
        $encoded = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, "PING\r\n", true, 'ABCD');

        // FIN+binary, mask bit set, length 6.
        self::assertSame(0x82, ord($encoded[0]));
        self::assertSame(0x80 | 6, ord($encoded[1]));

        $buffer = $encoded;
        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);

        self::assertSame('', $buffer);
        self::assertCount(1, $frames);
        self::assertSame(WebSocketFrameCodec::OP_BINARY, $frames[0]['opcode']);
        self::assertSame("PING\r\n", $frames[0]['payload']);
        self::assertTrue($frames[0]['fin']);
    }

    /**
     * Verifies the RFC 6455 §1.3 Sec-WebSocket-Accept example vector (#31).
     */
    public function testAcceptKeyMatchesRfcExample(): void
    {
        self::assertSame(
            's3pPLMBiTxaQ9kYGzzhZRbK+xOo=',
            WebSocketFrameCodec::acceptKey('dGhlIHNhbXBsZSBub25jZQ=='),
        );
    }

    /**
     * Verifies decode() leaves an incomplete trailing frame in the buffer (#31).
     */
    public function testDecodeKeepsIncompleteTrailingFrame(): void
    {
        $full = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'hello', true, 'WXYZ');
        // Feed everything but the last byte.
        $buffer = substr($full, 0, -1);
        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);

        self::assertSame([], $frames);
        self::assertSame(substr($full, 0, -1), $buffer);

        // Deliver the final byte; the frame now decodes.
        $buffer .= substr($full, -1);
        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);
        self::assertCount(1, $frames);
        self::assertSame('hello', $frames[0]['payload']);
        self::assertSame('', $buffer);
    }

    /**
     * Verifies decode() returns multiple frames present in one buffer (#31).
     */
    public function testDecodeReturnsMultipleFrames(): void
    {
        $buffer = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'INFO {}', false)
            . WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_PING, 'hb', false)
            . WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, '+OK', false);

        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);

        self::assertCount(3, $frames);
        self::assertSame('INFO {}', $frames[0]['payload']);
        self::assertSame(WebSocketFrameCodec::OP_PING, $frames[1]['opcode']);
        self::assertSame('+OK', $frames[2]['payload']);
    }

    /**
     * Verifies an extended (16-bit length) unmasked server frame decodes (#31).
     */
    public function testDecodeExtended16BitLengthFrame(): void
    {
        $payload = str_repeat('x', 300);
        $buffer = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $payload, false);

        // Length marker 126 + 2-byte length.
        self::assertSame(126, ord($buffer[1]) & 0x7F);

        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);
        self::assertCount(1, $frames);
        self::assertSame($payload, $frames[0]['payload']);
    }

    /**
     * Verifies a fragmented (FIN=0) data frame is reported with fin=false (#31).
     */
    public function testDecodeReportsFragmentationFlag(): void
    {
        // Manually craft a non-final binary frame (FIN=0, opcode binary, unmasked, len 3).
        $buffer = chr(WebSocketFrameCodec::OP_BINARY) . chr(3) . 'abc';
        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);

        self::assertCount(1, $frames);
        self::assertFalse($frames[0]['fin']);
        self::assertSame('abc', $frames[0]['payload']);
    }

    /**
     * Verifies permessage-deflate deflate/inflate round-trips (#61).
     */
    public function testDeflateInflateRoundTrip(): void
    {
        $payload = str_repeat('NATS-over-websocket payload ', 20);

        $compressed = WebSocketFrameCodec::deflate($payload);
        self::assertNotSame($payload, $compressed);
        self::assertLessThan(strlen($payload), strlen($compressed));
        self::assertSame($payload, WebSocketFrameCodec::inflate($compressed));
    }

    /**
     * permessage-deflate inflate() must cap the decompressed size: a small frame that inflates far
     * beyond the cap (a decompression bomb) throws a ProtocolException instead of allocating
     * unboundedly. Regression for #121 (inflate_add() had no decompressed-size cap - a hostile
     * server could OOM-spike the client, inconsistent with the file's #89 threat model).
     */
    public function testInflateEnforcesDecompressedSizeCap(): void
    {
        // ~1 MiB of a single repeated byte compresses to ~1 KiB - well under the 4096-byte cap, so
        // only the decompressed size (not the compressed input) can trip the guard.
        $bomb = WebSocketFrameCodec::deflate(str_repeat('A', 1024 * 1024));
        self::assertLessThan(4096, strlen($bomb));

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessageMatches('/maximum/i');
        // A cap far below the ~1 MiB decompressed size must abort rather than allocate it all.
        WebSocketFrameCodec::inflate($bomb, 4096);
    }

    /**
     * A payload under the cap still inflates byte-identically even when its COMPRESSED form spans many
     * bounded inflate slices - the multi-slice reassembly must not corrupt or truncate legitimate
     * output (#121). The input must be incompressible: a repetitive payload deflates to well under
     * INFLATE_INPUT_CHUNK_BYTES (8192) and the loop runs a single iteration, so it would never
     * exercise the multi-slice path. 64 KiB of random bytes deflates to slightly MORE than 64 KiB, so
     * inflate() feeds it in ~9 slices, genuinely iterating the bounded loop and reassembling across it.
     */
    public function testInflateReturnsFullPayloadWithinCap(): void
    {
        $original = random_bytes(64 * 1024); // incompressible: deflates to > 64 KiB, no shrink
        $compressed = WebSocketFrameCodec::deflate($original);

        // Prove the input genuinely spans several INFLATE_INPUT_CHUNK_BYTES (8192-byte) slices, so the
        // bounded loop iterates many times rather than collapsing to one pass.
        self::assertGreaterThanOrEqual(strlen($original), strlen($compressed), 'random data must not shrink');
        self::assertGreaterThan(8192 * 4, strlen($compressed), 'compressed input must span several inflate slices');

        // A cap exactly at the payload size must accept the full reassembled output byte-for-byte.
        self::assertSame($original, WebSocketFrameCodec::inflate($compressed, strlen($original)));
    }

    /**
     * The decompressed-size cap is enforced WHILE the bounded loop iterates, not just on a single-pass
     * frame: an incompressible payload whose compressed form spans many INFLATE_INPUT_CHUNK_BYTES
     * slices, inflated under a cap far below its decompressed size, must abort with a ProtocolException
     * partway through the loop rather than reassembling the whole output first (#121).
     */
    public function testInflateAbortsMidLoopWhenMultiSliceOutputExceedsCap(): void
    {
        $original = random_bytes(96 * 1024); // ~13 compressed slices, decompresses to 96 KiB
        $compressed = WebSocketFrameCodec::deflate($original);
        self::assertGreaterThan(8192 * 4, strlen($compressed), 'compressed input must span several inflate slices');

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessageMatches('/maximum/i');
        // 16 KiB cap << 96 KiB output: the accumulated output crosses the cap mid-loop and aborts.
        WebSocketFrameCodec::inflate($compressed, 16 * 1024);
    }

    /**
     * Verifies a compressed frame carries RSV1 and decodes back to the original payload (#61).
     */
    public function testCompressedFrameRoundTrip(): void
    {
        $payload = 'PUB orders.created 5\r\nhello\r\n';
        $encoded = WebSocketFrameCodec::encode(
            WebSocketFrameCodec::OP_BINARY,
            WebSocketFrameCodec::deflate($payload),
            true,
            'MASK',
            true,
        );

        // RSV1 bit set on the first byte.
        self::assertSame(0x40, ord($encoded[0]) & 0x40);

        $buffer = $encoded;
        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);
        self::assertCount(1, $frames);
        self::assertTrue($frames[0]['rsv1']);
        self::assertSame($payload, WebSocketFrameCodec::inflate($frames[0]['payload']));
    }

    /**
     * Verifies a non-4-byte mask key is rejected by encode() (#31).
     */
    public function testEncodeRejectsBadMaskKey(): void
    {
        $this->expectException(ProtocolException::class);
        WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'x', true, 'AB');
    }

    /**
     * Verifies encode() uses a 64-bit length header for payloads > 65535 bytes,
     * and that decode() correctly reassembles the same frame.
     */
    public function testEncode64BitLengthFrameRoundTrips(): void
    {
        // Payload is 65536 bytes - exactly one past the 16-bit threshold.
        $payload = str_repeat('A', 65536);
        $encoded = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $payload, false);

        // Byte 1 must carry the 127 length marker (no mask bit since mask=false).
        self::assertSame(127, ord($encoded[1]) & 0x7F);
        // The 8-byte big-endian length starting at offset 2 must equal 65536.
        /** @var array{1:int} $unpacked */
        $unpacked = unpack('J', substr($encoded, 2, 8));
        self::assertSame(65536, $unpacked[1]);

        // Decode must reconstruct the original payload from the 64-bit-length frame.
        $buffer = $encoded;
        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);
        self::assertCount(1, $frames);
        self::assertSame($payload, $frames[0]['payload']);
        self::assertSame('', $buffer);
    }

    /**
     * Verifies decode() halts and preserves the buffer when a 64-bit length header is
     * incomplete: the first 2 bytes say length=127 but fewer than 10 bytes are available.
     */
    public function testDecode64BitLengthHeaderIncompleteWaits(): void
    {
        // 2-byte frame header with length marker 127, then only 3 of the required 8 length bytes.
        // decode() needs at least offset(2) + 8 = 10 bytes to read the 64-bit length, so 5 bytes
        // is not enough and the loop must break without consuming anything.
        $buffer = pack('CC', 0x82, 127) . str_repeat("\x00", 3);
        $original = $buffer;

        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);

        self::assertSame([], $frames, 'No complete frame should be decoded');
        self::assertSame($original, $buffer, 'Buffer must be left unchanged when header is incomplete');
    }

    /**
     * Verifies decode() halts and preserves the buffer when a 16-bit length header is
     * incomplete: the first 2 bytes say length=126 but only one of the two length bytes is present.
     */
    public function testDecode16BitLengthHeaderIncompleteWaits(): void
    {
        // 2-byte frame header with length marker 126, then only 1 byte of the 2-byte length field.
        $buffer = pack('CC', 0x82, 126) . "\x01";
        $original = $buffer;

        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);

        self::assertSame([], $frames, 'No complete frame should be decoded');
        self::assertSame($original, $buffer, 'Buffer must not be consumed when 16-bit length is incomplete');
    }

    /**
     * A declared payload length beyond MAX_FRAME_PAYLOAD no longer THROWS mid-batch (which
     * discarded frames already decoded from the same read and left the buffer untrimmed): decode()
     * reports it via the $terminal out-param, returns the valid frames parsed before it, and trims
     * only their bytes - the caller delivers the data first and surfaces the violation deferred.
     */
    public function testDecodePayloadLengthOutOfBoundsReportsTerminalAfterValidFrames(): void
    {
        // MAX_FRAME_PAYLOAD is 64 * 1024 * 1024 = 67108864. Use one byte over.
        $tooLarge = 64 * 1024 * 1024 + 1;
        $valid = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'ok', false);
        $poison = pack('CC', 0x82, 127) . pack('J', $tooLarge);
        $buffer = $valid . $poison;

        $frames = WebSocketFrameCodec::decode($buffer, $terminal);

        self::assertCount(1, $frames, 'the valid frame before the violation must be returned');
        self::assertSame('ok', $frames[0]['payload']);
        self::assertInstanceOf(ProtocolException::class, $terminal);
        self::assertMatchesRegularExpression('/payload length out of bounds/', $terminal->getMessage());
        self::assertSame($poison, $buffer, 'only the valid frame\'s bytes are consumed');
    }

    /**
     * RFC 6455 5.1: a server-to-client frame MUST NOT be masked - the default (server-frame) decode
     * mode reports it as a terminal violation instead of silently unmasking; the explicit
     * allowMasked mode (client-written frames: pong/close assertions, server-side harnesses) still
     * unmasks.
     */
    public function testDecodeRejectsMaskedServerFrameUnlessAllowed(): void
    {
        $masked = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'data', true, 'MKEY');

        $buffer = $masked;
        $frames = WebSocketFrameCodec::decode($buffer, $terminal);
        self::assertSame([], $frames);
        self::assertInstanceOf(ProtocolException::class, $terminal);
        self::assertStringContainsString('masked', $terminal->getMessage());

        $buffer = $masked;
        $frames = WebSocketFrameCodec::decode($buffer, $terminal, allowMasked: true);
        self::assertNull($terminal);
        self::assertSame('data', $frames[0]['payload'] ?? null);
    }

    /**
     * RFC 6455 5.5: control frames MUST NOT be fragmented and MUST carry at most 125 payload
     * bytes. A fragmented PING used to be answered and its continuation spliced into an
     * in-progress fragmented DATA message - silent corruption; it is now a terminal violation.
     */
    public function testDecodeRejectsFragmentedOrOversizedControlFrames(): void
    {
        // PING with FIN cleared (fragmented control frame).
        $ping = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_PING, 'abc', false);
        $ping[0] = chr(ord($ping[0]) & 0x7F);
        $buffer = $ping;
        WebSocketFrameCodec::decode($buffer, $terminal);
        self::assertInstanceOf(ProtocolException::class, $terminal);
        self::assertStringContainsString('control frame', $terminal->getMessage());

        // PING with a 126-byte payload (oversized control frame).
        $buffer = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_PING, str_repeat('x', 126), false);
        WebSocketFrameCodec::decode($buffer, $terminal);
        self::assertInstanceOf(ProtocolException::class, $terminal);
        self::assertStringContainsString('control frame', $terminal->getMessage());
    }

    /**
     * Verifies decode() halts and preserves the buffer when a masked frame has a complete header
     * but its 4-byte mask key is not yet fully buffered.
     */
    public function testDecodeMaskedFrameWaitsForMaskKey(): void
    {
        // Craft a masked frame: FIN+binary, mask bit set, 7-bit length = 5.
        // Full frame would be: 2 header + 4 mask key + 5 payload = 11 bytes.
        // Provide header + only 2 of the 4 mask key bytes - not enough.
        $buffer = pack('CC', 0x82, 0x80 | 5) . "\xAB\xCD";
        $original = $buffer;

        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);

        self::assertSame([], $frames, 'No frame decoded when mask key is incomplete');
        self::assertSame($original, $buffer, 'Buffer unchanged when mask key bytes are missing');
    }

    /**
     * Verifies inflate() throws a ProtocolException when the input is not valid DEFLATE data.
     *
     * PHPUnit's error handler is disabled so that the native PHP E_WARNING emitted by inflate_add()
     * on a corrupt stream does not short-circuit into a test error; the ProtocolException thrown
     * immediately after is what the test actually asserts.
     */
    #[\PHPUnit\Framework\Attributes\WithoutErrorHandler]
    public function testInflateInvalidDataThrows(): void
    {
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessageMatches('/inflate compressed WebSocket frame/');

        // Pass data that is definitely not a valid raw DEFLATE stream.
        // inflate() appends \x00\x00\xff\xff before calling inflate_add; even so, this garbage
        // is not valid and inflate_add returns false, triggering the ProtocolException.
        WebSocketFrameCodec::inflate('this is not compressed data at all !!!!!!');
    }

    /**
     * Verifies frameRequiredBytes() returns the exact full wire size for every length form (7-bit
     * unmasked, 7-bit masked, 16-bit, 64-bit), the last two from a header prefix alone - including a
     * masked frame whose mask-key bytes have not arrived yet (the size only needs the length bytes),
     * so the transport can size a pending frame as early as possible (#164).
     */
    public function testFrameRequiredBytesComputesFullWireSizePerLengthForm(): void
    {
        // 7-bit unmasked: 2-byte header + 5 payload bytes.
        $small = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'hello', false);
        self::assertSame(strlen($small), WebSocketFrameCodec::frameRequiredBytes($small));

        // 7-bit masked: header + 4-byte mask key + payload.
        $masked = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'hello', true, 'ABCD');
        self::assertSame(strlen($masked), WebSocketFrameCodec::frameRequiredBytes($masked));

        // 16-bit length (126 marker), sized from its 4 header bytes with the mask key still in flight.
        $medium = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, str_repeat('m', 300), true, 'ABCD');
        self::assertSame(strlen($medium), WebSocketFrameCodec::frameRequiredBytes(substr($medium, 0, 4)));

        // 64-bit length (127 marker): a 10-byte header prefix is enough to size the whole frame.
        $large = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, str_repeat('l', 65536), false);
        self::assertSame(strlen($large), WebSocketFrameCodec::frameRequiredBytes(substr($large, 0, 10)));
    }

    /**
     * Verifies frameRequiredBytes() returns null while the length bytes are incomplete (#164).
     */
    public function testFrameRequiredBytesReturnsNullOnIncompleteHeader(): void
    {
        self::assertNull(WebSocketFrameCodec::frameRequiredBytes(''));
        self::assertNull(WebSocketFrameCodec::frameRequiredBytes(chr(0x82)));
        // 126 marker with only 1 of its 2 extended-length bytes.
        self::assertNull(WebSocketFrameCodec::frameRequiredBytes(pack('CC', 0x82, 126) . "\x01"));
        // 127 marker with only 7 of its 8 extended-length bytes.
        self::assertNull(WebSocketFrameCodec::frameRequiredBytes(pack('CC', 0x82, 127) . str_repeat("\x00", 7)));
    }

    /**
     * Verifies frameRequiredBytes() enforces the same declared-length bound as decode(), throwing
     * the moment the hostile length bytes arrive (#164).
     */
    public function testFrameRequiredBytesRejectsOversizedDeclaredLength(): void
    {
        $tooLarge = 64 * 1024 * 1024 + 1; // MAX_FRAME_PAYLOAD + 1

        try {
            WebSocketFrameCodec::frameRequiredBytes(pack('CC', 0x82, 127) . pack('J', $tooLarge));
            self::fail('Expected a ProtocolException for the oversized declared length');
        } catch (ProtocolException $e) {
            // Full-message pin: the offending length is the operator's diagnostic for a hostile frame.
            self::assertSame('WebSocket frame payload length out of bounds: ' . $tooLarge, $e->getMessage());
        }
    }

    /**
     * Verifies parseFrameHeader() sizes a frame from its header alone for every length form
     * (7-bit, 16-bit, 64-bit; masked and unmasked): headerBytes + payloadLength equals the frame's
     * full wire size, and the flag/opcode/mask fields match what was encoded (#164).
     */
    public function testParseFrameHeaderComputesFullWireSizePerLengthForm(): void
    {
        // 7-bit unmasked: 2-byte header + 5 payload bytes.
        $small = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'hello', false);
        $header = WebSocketFrameCodec::parseFrameHeader($small);
        self::assertNotNull($header);
        self::assertSame(strlen($small), $header['headerBytes'] + $header['payloadLength']);
        self::assertSame(WebSocketFrameCodec::OP_BINARY, $header['opcode']);
        self::assertTrue($header['fin']);
        self::assertFalse($header['rsv1']);
        self::assertFalse($header['masked']);

        // 7-bit masked: header + 4-byte mask key + payload; the key is surfaced for spanning-frame consumers.
        $masked = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'hello', true, 'ABCD');
        $header = WebSocketFrameCodec::parseFrameHeader($masked);
        self::assertNotNull($header);
        self::assertSame(strlen($masked), $header['headerBytes'] + $header['payloadLength']);
        self::assertTrue($header['masked']);
        self::assertSame('ABCD', $header['maskKey']);

        // 16-bit length (126 marker).
        $medium = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, str_repeat('m', 300), false);
        $header = WebSocketFrameCodec::parseFrameHeader($medium);
        self::assertNotNull($header);
        self::assertSame(strlen($medium), $header['headerBytes'] + $header['payloadLength']);
        self::assertSame(300, $header['payloadLength']);

        // 64-bit length (127 marker): a 10-byte header prefix is enough to size the whole frame.
        $large = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, str_repeat('l', 65536), false);
        $header = WebSocketFrameCodec::parseFrameHeader(substr($large, 0, 10));
        self::assertNotNull($header);
        self::assertSame(strlen($large), $header['headerBytes'] + $header['payloadLength']);
        self::assertSame(65536, $header['payloadLength']);
    }

    /**
     * Verifies parseFrameHeader() returns null while the header itself is incomplete, including a
     * masked frame whose mask-key bytes have not fully arrived (#164).
     */
    public function testParseFrameHeaderReturnsNullOnIncompleteHeader(): void
    {
        self::assertNull(WebSocketFrameCodec::parseFrameHeader(''));
        self::assertNull(WebSocketFrameCodec::parseFrameHeader(chr(0x82)));
        // 126 marker with only 1 of its 2 extended-length bytes.
        self::assertNull(WebSocketFrameCodec::parseFrameHeader(pack('CC', 0x82, 126) . "\x01"));
        // 127 marker with only 7 of its 8 extended-length bytes.
        self::assertNull(WebSocketFrameCodec::parseFrameHeader(pack('CC', 0x82, 127) . str_repeat("\x00", 7)));
        // Masked 7-bit frame with only 3 of its 4 mask-key bytes.
        self::assertNull(WebSocketFrameCodec::parseFrameHeader(pack('CC', 0x82, 0x80 | 5) . 'ABC'));
    }

    /**
     * Verifies parseFrameHeader() enforces the same declared-length bound as decode(), and does so
     * the moment the length bytes arrive - even on a masked frame whose mask key is still in flight -
     * rather than after accumulating payload bytes (#164).
     */
    public function testParseFrameHeaderRejectsOversizedDeclaredLength(): void
    {
        $tooLarge = 64 * 1024 * 1024 + 1; // MAX_FRAME_PAYLOAD + 1
        $maskedHeaderWithoutKey = pack('CC', 0x82, 0x80 | 127) . pack('J', $tooLarge);

        try {
            WebSocketFrameCodec::parseFrameHeader($maskedHeaderWithoutKey);
            self::fail('Expected a ProtocolException for the oversized declared length');
        } catch (ProtocolException $e) {
            // Full-message pin: the offending length is the operator's diagnostic for a hostile frame.
            self::assertSame('WebSocket frame payload length out of bounds: ' . $tooLarge, $e->getMessage());
        }
    }

    /**
     * Verifies the @deprecated unmask() helper still inverts the masking encode() applies (#164):
     * it is production-dead (masked server frames are a terminal RFC 6455 5.1 violation) but public
     * since 2.7.x, so it stays behavior-pinned until a major release removes it.
     */
    public function testDeprecatedUnmaskStillInvertsEncodeMasking(): void
    {
        $payload = random_bytes(133);
        $frame = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $payload, true, "\x01\x7F\xAA\x00");

        // 16-bit length form: 4 header bytes + 4 mask-key bytes precede the masked payload.
        $maskedPayload = substr($frame, 8);
        self::assertNotSame($payload, $maskedPayload);
        self::assertSame($payload, WebSocketFrameCodec::unmask($maskedPayload, "\x01\x7F\xAA\x00"));
    }

    /**
     * #100: a well-behaved error handler (one that respects the @ operator via error_reporting()) must
     * NOT observe the native inflate_add() warning, so an application that promotes warnings to
     * exceptions still receives the typed ProtocolException rather than an ErrorException leaking from
     * inside the codec. Before the @ suppression this raised an ErrorException from the inflate_add()
     * call, never reaching the ProtocolException.
     */
    public function testInflateInvalidDataSuppressesNativeWarningForRespectfulHandlers(): void
    {
        set_error_handler(static function (int $errno, string $errstr): bool {
            if ((error_reporting() & $errno) === 0) {
                return false; // suppressed via @ - respect it, do not promote to an exception
            }

            throw new \ErrorException($errstr, 0, $errno);
        });

        try {
            WebSocketFrameCodec::inflate('this is not compressed data at all !!!!!!');
            self::fail('Expected a ProtocolException');
        } catch (ProtocolException $e) {
            self::assertStringContainsString('inflate compressed WebSocket frame', $e->getMessage());
        } finally {
            restore_error_handler();
        }
    }

    /**
     * parseFrameHeader() must decode a complete two-byte 7-bit header (the minimum sufficient prefix):
     * the two-byte availability guard is < 2, so exactly two bytes are enough. A non-final text frame
     * with FIN=0 pins the fin/rsv1/opcode bit-field extraction - each bit is read from a distinct mask
     * (0x80/0x40/0x0F), and byte1=0x01 makes those masks disagree so a mutated mask flips a field.
     */
    public function testParseFrameHeaderDecodesTwoByteNonFinalTextHeader(): void
    {
        // byte1=0x01: FIN=0, RSV1=0, opcode=1 (text). byte2=0x03: unmasked, 7-bit length 3.
        $header = WebSocketFrameCodec::parseFrameHeader("\x01\x03");

        self::assertNotNull($header, 'two header bytes are sufficient to parse a 7-bit frame');
        self::assertFalse($header['fin'], 'FIN bit (0x80) is clear on byte1=0x01');
        self::assertFalse($header['rsv1'], 'RSV1 bit (0x40) is clear on byte1=0x01');
        self::assertSame(WebSocketFrameCodec::OP_TEXT, $header['opcode'], 'opcode is the low nibble of byte1');
        self::assertFalse($header['masked']);
        self::assertSame('', $header['maskKey']);
        self::assertSame(2, $header['headerBytes']);
        self::assertSame(3, $header['payloadLength']);
    }

    /**
     * frameRequiredBytes() must size a frame from a complete two-byte 7-bit header alone: the guard is
     * < 2, so exactly two bytes suffice. Pins that boundary against a <= 2 off-by-one that would report
     * the frame unsized (null) when its whole header is already present.
     */
    public function testFrameRequiredBytesSizesTwoByteHeader(): void
    {
        // FIN+binary, unmasked, 7-bit length 3: full wire size is 2 header + 3 payload = 5.
        self::assertSame(5, WebSocketFrameCodec::frameRequiredBytes("\x82\x03"));
    }

    /**
     * A zero-length frame (e.g. an empty PING) is valid: frameRequiredBytes() must accept declared
     * length 0 and return the header size, not treat 0 as out of bounds. Pins the lower length bound
     * against a <= 0 mutation that would reject an empty frame.
     */
    public function testFrameRequiredBytesAcceptsZeroLengthFrame(): void
    {
        // 0x89 = FIN+ping, 0x00 = unmasked length 0: whole frame is just its 2 header bytes.
        self::assertSame(2, WebSocketFrameCodec::frameRequiredBytes("\x89\x00"));
    }

    /**
     * A frame declaring exactly MAX_FRAME_PAYLOAD bytes is the largest permitted and must be accepted,
     * not rejected. frameRequiredBytes() sizes it from its 10-byte 64-bit-length header (no payload
     * needed). Pins the upper bound against a >= MAX mutation that would reject the boundary length.
     */
    public function testFrameRequiredBytesAcceptsExactlyMaxDeclaredLength(): void
    {
        $max = 64 * 1024 * 1024; // MAX_FRAME_PAYLOAD
        // 127 marker + 8-byte big-endian length == MAX; full wire size is 10 header + MAX payload.
        $header = pack('CC', 0x82, 127) . pack('J', $max);

        self::assertSame(10 + $max, WebSocketFrameCodec::frameRequiredBytes($header));
    }

    /**
     * parseFrameHeader() must parse a 16-bit (126-marker) header the moment its 4 length-bearing bytes
     * arrive: the guard is < 4, so exactly four bytes suffice. Pins that boundary against a <= 4
     * off-by-one that would report null when the extended length is fully present.
     */
    public function testParseFrameHeaderDecodes16BitHeaderAtExactlyFourBytes(): void
    {
        // FIN+binary, 126 marker, then the 2-byte length 300 - exactly 4 bytes, no mask/payload yet.
        $header = WebSocketFrameCodec::parseFrameHeader(pack('CC', 0x82, 126) . pack('n', 300));

        self::assertNotNull($header, 'four bytes are enough to read a 16-bit length');
        self::assertSame(300, $header['payloadLength']);
        self::assertSame(4, $header['headerBytes']);
    }

    /**
     * parseFrameHeader() must accept a zero-length frame rather than treating declared length 0 as out
     * of bounds. Pins the lower length bound against a <= 0 mutation.
     */
    public function testParseFrameHeaderAcceptsZeroLengthFrame(): void
    {
        $header = WebSocketFrameCodec::parseFrameHeader("\x89\x00");

        self::assertNotNull($header);
        self::assertSame(0, $header['payloadLength']);
        self::assertSame(2, $header['headerBytes']);
    }

    /**
     * parseFrameHeader() must accept a frame declaring exactly MAX_FRAME_PAYLOAD bytes (the largest
     * permitted), sizing it from its 64-bit-length header alone. Pins the upper bound against a >= MAX
     * mutation that would reject the boundary length.
     */
    public function testParseFrameHeaderAcceptsExactlyMaxDeclaredLength(): void
    {
        $max = 64 * 1024 * 1024; // MAX_FRAME_PAYLOAD
        $header = WebSocketFrameCodec::parseFrameHeader(pack('CC', 0x82, 127) . pack('J', $max));

        self::assertNotNull($header);
        self::assertSame($max, $header['payloadLength']);
        self::assertSame(10, $header['headerBytes']);
    }

    /**
     * parseFrameHeader() must surface a masked frame's mask key as soon as the header and its 4 mask-key
     * bytes are present - the guard is available < offset + 4, so a header of exactly offset+4 bytes
     * (no payload yet) suffices. Pins that boundary against a +5 / <= off-by-one that would report null
     * when the mask key is already fully buffered.
     */
    public function testParseFrameHeaderReadsMaskKeyAtExactHeaderFit(): void
    {
        // FIN+binary, mask bit + 7-bit length 5, then the 4 mask-key bytes: 6 bytes == offset(2) + 4.
        $header = WebSocketFrameCodec::parseFrameHeader(pack('CC', 0x82, 0x80 | 5) . 'ABCD');

        self::assertNotNull($header, 'header plus mask key (6 bytes) is enough to parse');
        self::assertTrue($header['masked']);
        self::assertSame('ABCD', $header['maskKey']);
        self::assertSame(5, $header['payloadLength']);
        self::assertSame(6, $header['headerBytes']);
    }

    /**
     * Sec-WebSocket-Key must carry exactly 16 random bytes per RFC 6455 §4.1: generateClientKey() must
     * base64-encode 16 bytes. Pins the byte count (the base64 string length is 24 for both 15 and 17
     * bytes, so the decoded length is what distinguishes them) against +/-1 mutations.
     */
    public function testGenerateClientKeyEncodesSixteenRandomBytes(): void
    {
        $decoded = base64_decode(WebSocketFrameCodec::generateClientKey(), true);

        self::assertIsString($decoded);
        self::assertSame(16, strlen($decoded));
    }
}
