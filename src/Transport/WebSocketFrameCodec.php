<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

use IDCT\NATS\Exception\ProtocolException;

/**
 * Minimal RFC 6455 WebSocket frame codec for the NATS WebSocket transport.
 *
 * NATS-over-WebSocket carries raw NATS protocol bytes as WebSocket binary frame payloads. This codec
 * encodes masked client frames (clients MUST mask), decodes server frames (which are unmasked),
 * reassembles fragmented messages, and surfaces control frames (ping/pong/close) to the caller. It is
 * deliberately self-contained (no HTTP/WebSocket dependency) and pure, so it is fully unit-testable.
 */
final class WebSocketFrameCodec
{
    public const OP_CONTINUATION = 0x0;
    public const OP_TEXT = 0x1;
    public const OP_BINARY = 0x2;
    public const OP_CLOSE = 0x8;
    public const OP_PING = 0x9;
    public const OP_PONG = 0xA;

    /** GUID appended to the client key when computing the server's accept value (RFC 6455 §1.3). */
    private const HANDSHAKE_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

    /** Hard cap on a single frame's declared payload length, to bound memory on a hostile/garbled stream. */
    private const MAX_FRAME_PAYLOAD = 64 * 1024 * 1024;

    /** Longest possible frame header: 2 base bytes + 8 extended-length bytes + 4 mask-key bytes. */
    public const HEADER_MAX_BYTES = 14;

    /**
     * Encodes a single final ({@code FIN=1}) frame. When $mask is true (the default - required for
     * client->server frames) the payload is masked with a fresh 4-byte key.
     *
     * @param string|null $maskKey Optional fixed mask key (4 bytes) for deterministic tests; otherwise random.
     */
    public static function encode(int $opcode, string $payload, bool $mask = true, ?string $maskKey = null, bool $rsv1 = false): string
    {
        $length = strlen($payload);
        // FIN=1, RSV1 marks a permessage-deflate-compressed payload (RFC 7692), then the opcode.
        $frame = pack('C', 0x80 | ($rsv1 ? 0x40 : 0x00) | ($opcode & 0x0F));

        $maskBit = $mask ? 0x80 : 0x00;
        if ($length <= 125) {
            $frame .= pack('C', $maskBit | $length);
        } elseif ($length <= 0xFFFF) {
            $frame .= pack('C', $maskBit | 126) . pack('n', $length);
        } else {
            // 64-bit length. pack('J') needs PHP 64-bit ints, which this library already requires.
            $frame .= pack('C', $maskBit | 127) . pack('J', $length);
        }

        if (!$mask) {
            return $frame . $payload;
        }

        $key = $maskKey ?? random_bytes(4);
        if (strlen($key) !== 4) {
            throw new ProtocolException('WebSocket mask key must be exactly 4 bytes');
        }

        return $frame . $key . ($payload === '' ? '' : (string) (self::applyMask($payload, $key)));
    }

    /**
     * Decodes as many complete frames as are present in $buffer, removing the consumed bytes (an
     * incomplete trailing frame is left in $buffer for the next read).
     *
     * @return list<array{opcode:int,payload:string,fin:bool,rsv1:bool}>
     */
    public static function decode(string &$buffer): array
    {
        $frames = [];
        $total = strlen($buffer);
        // Advance a single cursor instead of re-slicing the whole remaining buffer per frame, so a read
        // carrying K coalesced frames costs O(N) total rather than O(K*N). The buffer is trimmed once at
        // the end (an incomplete trailing frame is left for the next read).
        $consumed = 0;

        while (true) {
            $base = $consumed;
            $available = $total - $base;
            if ($available < 2) {
                break;
            }

            $byte1 = ord($buffer[$base]);
            $byte2 = ord($buffer[$base + 1]);
            $fin = ($byte1 & 0x80) !== 0;
            $rsv1 = ($byte1 & 0x40) !== 0;
            $opcode = $byte1 & 0x0F;
            $masked = ($byte2 & 0x80) !== 0;
            $length = $byte2 & 0x7F;

            $offset = 2;
            if ($length === 126) {
                if ($available < $offset + 2) {
                    break;
                }
                /** @var array{1:int} $unpacked */
                $unpacked = unpack('n', substr($buffer, $base + $offset, 2));
                $length = $unpacked[1];
                $offset += 2;
            } elseif ($length === 127) {
                if ($available < $offset + 8) {
                    break;
                }
                /** @var array{1:int} $unpacked */
                $unpacked = unpack('J', substr($buffer, $base + $offset, 8));
                $length = $unpacked[1];
                $offset += 8;
            }

            if ($length < 0 || $length > self::MAX_FRAME_PAYLOAD) {
                throw new ProtocolException('WebSocket frame payload length out of bounds: ' . $length);
            }

            $maskKey = '';
            if ($masked) {
                if ($available < $offset + 4) {
                    break;
                }
                $maskKey = substr($buffer, $base + $offset, 4);
                $offset += 4;
            }

            if ($available < $offset + $length) {
                // Full payload not yet buffered; wait for more bytes.
                break;
            }

            $payload = $length > 0 ? substr($buffer, $base + $offset, $length) : '';
            if ($masked && $payload !== '') {
                $payload = (string) self::applyMask($payload, $maskKey);
            }

            $consumed = $base + $offset + $length;
            $frames[] = ['opcode' => $opcode, 'payload' => $payload, 'fin' => $fin, 'rsv1' => $rsv1];
        }

        if ($consumed > 0) {
            $buffer = substr($buffer, $consumed);
        }

        return $frames;
    }

    /**
     * Returns the full wire size (header plus declared payload) of the frame starting at the head of
     * $buffer, or null while too few header bytes have arrived to know it. Reads at most the leading
     * {@see HEADER_MAX_BYTES} bytes and allocates nothing but the int - it sizes the pending frame on
     * the transport's per-read hot path (#164). Enforces the same declared-length bound as
     * {@see decode()}, and does so before the mask-key bytes arrive, so a hostile length is rejected
     * the moment its length bytes are in.
     */
    public static function frameRequiredBytes(string $buffer): ?int
    {
        $available = strlen($buffer);
        if ($available < 2) {
            return null;
        }

        $byte2 = ord($buffer[1]);
        $length = $byte2 & 0x7F;

        $offset = 2;
        if ($length === 126) {
            if ($available < 4) {
                return null;
            }
            /** @var array{1:int} $unpacked */
            $unpacked = unpack('n', substr($buffer, 2, 2));
            $length = $unpacked[1];
            $offset = 4;
        } elseif ($length === 127) {
            if ($available < 10) {
                return null;
            }
            /** @var array{1:int} $unpacked */
            $unpacked = unpack('J', substr($buffer, 2, 8));
            $length = $unpacked[1];
            $offset = 10;
        }

        if ($length < 0 || $length > self::MAX_FRAME_PAYLOAD) {
            throw new ProtocolException('WebSocket frame payload length out of bounds: ' . $length);
        }

        if (($byte2 & 0x80) !== 0) {
            $offset += 4; // mask key
        }

        return $offset + $length;
    }

    /**
     * Parses the frame header (and mask key, when present) from a prefix of the frame's bytes, or
     * returns null while too few bytes have arrived. A prefix of {@see HEADER_MAX_BYTES} bytes is
     * always sufficient. Lets the transport consume a frame that spans accumulated read chunks
     * without joining its payload tail (#164). Enforces the same declared-length bound as
     * {@see decode()} - and does so before requiring the mask-key bytes, so a hostile length is
     * rejected the moment its length bytes arrive.
     *
     * @return array{fin: bool, rsv1: bool, opcode: int, masked: bool, maskKey: string, headerBytes: int, payloadLength: int}|null
     */
    public static function parseFrameHeader(string $prefix): ?array
    {
        $available = strlen($prefix);
        if ($available < 2) {
            return null;
        }

        $byte1 = ord($prefix[0]);
        $byte2 = ord($prefix[1]);
        $masked = ($byte2 & 0x80) !== 0;
        $length = $byte2 & 0x7F;

        $offset = 2;
        if ($length === 126) {
            if ($available < 4) {
                return null;
            }
            /** @var array{1:int} $unpacked */
            $unpacked = unpack('n', substr($prefix, 2, 2));
            $length = $unpacked[1];
            $offset = 4;
        } elseif ($length === 127) {
            if ($available < 10) {
                return null;
            }
            /** @var array{1:int} $unpacked */
            $unpacked = unpack('J', substr($prefix, 2, 8));
            $length = $unpacked[1];
            $offset = 10;
        }

        if ($length < 0 || $length > self::MAX_FRAME_PAYLOAD) {
            throw new ProtocolException('WebSocket frame payload length out of bounds: ' . $length);
        }

        $maskKey = '';
        if ($masked) {
            if ($available < $offset + 4) {
                return null;
            }
            $maskKey = substr($prefix, $offset, 4);
            $offset += 4;
        }

        return [
            'fin' => ($byte1 & 0x80) !== 0,
            'rsv1' => ($byte1 & 0x40) !== 0,
            'opcode' => $byte1 & 0x0F,
            'masked' => $masked,
            'maskKey' => $maskKey,
            'headerBytes' => $offset,
            'payloadLength' => $length,
        ];
    }

    /**
     * Removes a client mask from a payload (masking is its own XOR inverse). Companion to
     * {@see parseFrameHeader()} for callers that extract a spanning frame's payload themselves;
     * {@see decode()} keeps unmasking inline.
     */
    public static function unmask(string $payload, string $key): string
    {
        return self::applyMask($payload, $key);
    }

    /**
     * Compresses a payload for permessage-deflate (RFC 7692, no context takeover): raw DEFLATE with a
     * sync flush, trailing empty block (0x00 0x00 0xff 0xff) removed.
     */
    public static function deflate(string $payload): string
    {
        $ctx = deflate_init(ZLIB_ENCODING_RAW);
        if ($ctx === false) {
            throw new ProtocolException('Failed to initialize DEFLATE context');
        }

        // Suppress the native E_WARNING: the false-check below already turns a failure into a typed
        // ProtocolException. Without @, an app that promotes warnings to exceptions would see a generic
        // ErrorException from inside the codec instead of the intended ProtocolException (#100).
        $out = @deflate_add($ctx, $payload, ZLIB_SYNC_FLUSH);
        if ($out === false) {
            throw new ProtocolException('Failed to deflate WebSocket frame');
        }

        if (str_ends_with($out, "\x00\x00\xff\xff")) {
            $out = substr($out, 0, -4);
        }

        return $out;
    }

    /**
     * Decompresses a permessage-deflate payload (the inverse of {@see deflate()}): re-append the empty
     * block tail, then raw INFLATE.
     */
    public static function inflate(string $payload): string
    {
        $ctx = inflate_init(ZLIB_ENCODING_RAW);
        if ($ctx === false) {
            throw new ProtocolException('Failed to initialize INFLATE context');
        }

        // Suppress the native E_WARNING ("inflate_add(): data error") on a corrupt peer-controlled frame:
        // the false-check below already raises a typed ProtocolException. Without @, an app that promotes
        // warnings to exceptions would get an ErrorException from inside the codec instead (#100).
        $result = @inflate_add($ctx, $payload . "\x00\x00\xff\xff");
        if ($result === false) {
            throw new ProtocolException('Failed to inflate compressed WebSocket frame');
        }

        return $result;
    }

    /**
     * Computes the value the server must return in `Sec-WebSocket-Accept` for a given client key.
     */
    public static function acceptKey(string $clientKey): string
    {
        return base64_encode(sha1($clientKey . self::HANDSHAKE_GUID, true));
    }

    /**
     * Generates a fresh base64 client key for `Sec-WebSocket-Key` (16 random bytes).
     */
    public static function generateClientKey(): string
    {
        return base64_encode(random_bytes(16));
    }

    /**
     * XORs $payload with the repeating 4-byte $key (masking is its own inverse).
     */
    private static function applyMask(string $payload, string $key): string
    {
        $masked = $payload ^ str_repeat($key, intdiv(strlen($payload), 4) + 1);

        return substr($masked, 0, strlen($payload));
    }
}
