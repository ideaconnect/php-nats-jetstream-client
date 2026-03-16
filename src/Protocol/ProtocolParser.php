<?php

declare(strict_types=1);

namespace IDCT\NATS\Protocol;

use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Protocol\Enum\ProtocolFrameType;

/**
 * Streaming parser for NATS protocol frames read from transport.
 */
final class ProtocolParser
{
    private string $buffer = '';

    /** Maximum total frame size (headers + payload) accepted from the server. */
    private int $maxFrameSize;

    /**
     * Creates a parser for line and payload frames produced by the NATS server.
     *
     * @param int $maxFrameSize Maximum total MSG/HMSG bytes accepted per frame to limit memory usage.
     */
    public function __construct(int $maxFrameSize = 8 * 1024 * 1024)
    {
        $this->maxFrameSize = $maxFrameSize;
    }

    /**
     * Appends a raw socket chunk and emits all complete frames currently available.
     *
     * @return list<ProtocolFrame>
     */
    public function push(string $chunk): array
    {
        $this->buffer .= $chunk;
        $frames = [];
        $offset = 0;
        $bufferLength = strlen($this->buffer);

        while ($offset < $bufferLength) {
            $lineEndPos = strpos($this->buffer, "\r\n", $offset);
            if ($lineEndPos === false) {
                break;
            }

            $line = substr($this->buffer, $offset, $lineEndPos - $offset);
            $nextOffset = $lineEndPos + 2;

            if (str_starts_with($line, 'MSG ')) {
                [$frame, $consumed] = $this->parseMsgFrame($line, $nextOffset, $bufferLength);
                if ($consumed === 0) {
                    // Payload is incomplete; keep buffered bytes for the next chunk.
                    break;
                }

                $frames[] = $frame;
                $offset = $consumed;
                continue;
            }

            if (str_starts_with($line, 'HMSG ')) {
                [$frame, $consumed] = $this->parseHMsgFrame($line, $nextOffset, $bufferLength);
                if ($consumed === 0) {
                    // Payload is incomplete; keep buffered bytes for the next chunk.
                    break;
                }

                $frames[] = $frame;
                $offset = $consumed;
                continue;
            }

            $frames[] = $this->parseControlFrame($line);
            $offset = $nextOffset;
        }

        $this->buffer = substr($this->buffer, $offset);

        return $frames;
    }

    /**
     * Parses line-based control frames that do not carry trailing payload bytes.
     */
    private function parseControlFrame(string $line): ProtocolFrame
    {
        if (str_starts_with($line, 'INFO ')) {
            return new ProtocolFrame(
                type: ProtocolFrameType::Info,
                infoPayload: substr($line, 5),
            );
        }

        if ($line === 'PING') {
            return new ProtocolFrame(type: ProtocolFrameType::Ping);
        }

        if ($line === 'PONG') {
            return new ProtocolFrame(type: ProtocolFrameType::Pong);
        }

        if ($line === '+OK') {
            return new ProtocolFrame(type: ProtocolFrameType::Ok);
        }

        if (str_starts_with($line, '-ERR')) {
            return new ProtocolFrame(
                type: ProtocolFrameType::Err,
                error: trim(substr($line, 4)),
            );
        }

        throw new ProtocolException('Unsupported control frame: ' . $line);
    }

    /**
     * Parses an MSG line and payload when enough buffered bytes are available.
     *
     * @return array{0: ProtocolFrame, 1: int}
     */
    private function parseMsgFrame(string $line, int $payloadOffset, int $bufferLength): array
    {
        $parts = preg_split('/\s+/', $line);
        if ($parts === false || count($parts) < 4 || count($parts) > 5) {
            throw new ProtocolException('Invalid MSG frame line: ' . $line);
        }

        $subject = $parts[1];
        $sid = (int) $parts[2];
        $replyTo = count($parts) === 5 ? $parts[3] : null;
        $size = (int) $parts[count($parts) - 1];

        if ($size < 0 || $size > $this->maxFrameSize) {
            throw new ProtocolException('MSG frame payload size is invalid: ' . $size);
        }

        $required = $payloadOffset + $size + 2;
        if ($bufferLength < $required) {
            return [new ProtocolFrame(type: ProtocolFrameType::Msg), 0];
        }

        $payload = substr($this->buffer, $payloadOffset, $size);
        $crlf = substr($this->buffer, $payloadOffset + $size, 2);
        if ($crlf !== "\r\n") {
            throw new ProtocolException('MSG frame payload must be terminated by CRLF');
        }

        return [
            new ProtocolFrame(
                type: ProtocolFrameType::Msg,
                subject: $subject,
                sid: $sid,
                replyTo: $replyTo,
                payload: $payload,
            ),
            $required,
        ];
    }

    /**
     * Parses an HMSG line and combined headers+payload section.
     *
     * @return array{0: ProtocolFrame, 1: int}
     */
    private function parseHMsgFrame(string $line, int $payloadOffset, int $bufferLength): array
    {
        $parts = preg_split('/\s+/', $line);
        if ($parts === false || count($parts) < 5 || count($parts) > 6) {
            throw new ProtocolException('Invalid HMSG frame line: ' . $line);
        }

        $subject = $parts[1];
        $sid = (int) $parts[2];

        if (count($parts) === 6) {
            $replyTo = $parts[3];
            $headerBytes = (int) $parts[4];
            $totalBytes = (int) $parts[5];
        } else {
            $replyTo = null;
            $headerBytes = (int) $parts[3];
            $totalBytes = (int) $parts[4];
        }

        if ($totalBytes < 0 || $totalBytes > $this->maxFrameSize) {
            throw new ProtocolException('HMSG frame payload size is invalid: ' . $totalBytes);
        }

        $required = $payloadOffset + $totalBytes + 2;
        if ($bufferLength < $required) {
            return [new ProtocolFrame(type: ProtocolFrameType::HMsg), 0];
        }

        $payload = substr($this->buffer, $payloadOffset, $totalBytes);
        $crlf = substr($this->buffer, $payloadOffset + $totalBytes, 2);
        if ($crlf !== "\r\n") {
            throw new ProtocolException('HMSG frame payload must be terminated by CRLF');
        }

        return [
            new ProtocolFrame(
                type: ProtocolFrameType::HMsg,
                subject: $subject,
                sid: $sid,
                replyTo: $replyTo,
                payload: $payload,
                headerBytes: $headerBytes,
                totalBytes: $totalBytes,
            ),
            $required,
        ];
    }
}
