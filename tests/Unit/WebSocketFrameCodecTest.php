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
        $frames = WebSocketFrameCodec::decode($buffer);

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
        $frames = WebSocketFrameCodec::decode($buffer);

        self::assertSame([], $frames);
        self::assertSame(substr($full, 0, -1), $buffer);

        // Deliver the final byte; the frame now decodes.
        $buffer .= substr($full, -1);
        $frames = WebSocketFrameCodec::decode($buffer);
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

        $frames = WebSocketFrameCodec::decode($buffer);

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

        $frames = WebSocketFrameCodec::decode($buffer);
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
        $frames = WebSocketFrameCodec::decode($buffer);

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
        $frames = WebSocketFrameCodec::decode($buffer);
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
        $frames = WebSocketFrameCodec::decode($buffer);
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

        $frames = WebSocketFrameCodec::decode($buffer);

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

        $frames = WebSocketFrameCodec::decode($buffer);

        self::assertSame([], $frames, 'No complete frame should be decoded');
        self::assertSame($original, $buffer, 'Buffer must not be consumed when 16-bit length is incomplete');
    }

    /**
     * Verifies decode() throws when the declared payload length exceeds MAX_FRAME_PAYLOAD.
     * We craft a 64-bit-length frame header with a length value of 64 MiB + 1.
     */
    public function testDecodePayloadLengthOutOfBoundsThrows(): void
    {
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessageMatches('/payload length out of bounds/');

        // MAX_FRAME_PAYLOAD is 64 * 1024 * 1024 = 67108864. Use one byte over.
        $tooLarge = 64 * 1024 * 1024 + 1;
        // FIN+binary, 64-bit length marker, 8-byte big-endian length (no payload needed - decode
        // throws before trying to read the payload).
        $buffer = pack('CC', 0x82, 127) . pack('J', $tooLarge);

        WebSocketFrameCodec::decode($buffer);
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

        $frames = WebSocketFrameCodec::decode($buffer);

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

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessageMatches('/payload length out of bounds/');
        WebSocketFrameCodec::frameRequiredBytes(pack('CC', 0x82, 127) . pack('J', $tooLarge));
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

        // 7-bit masked: header + 4-byte mask key + payload; the key is surfaced for unmask().
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

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessageMatches('/payload length out of bounds/');
        WebSocketFrameCodec::parseFrameHeader($maskedHeaderWithoutKey);
    }

    /**
     * Verifies unmask() inverts the masking encode() applies: the masked payload bytes taken from an
     * encoded client frame unmask back to the original payload with the frame's key (#164).
     */
    public function testUnmaskInvertsEncodeMasking(): void
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
}
