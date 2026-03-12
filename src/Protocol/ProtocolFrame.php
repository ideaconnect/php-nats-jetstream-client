<?php

declare(strict_types=1);

namespace IDCT\NATS\Protocol;

use IDCT\NATS\Protocol\Enum\ProtocolFrameType;

/**
 * Immutable parsed NATS protocol frame.
 */
final class ProtocolFrame
{
    /**
     * Represents one parsed protocol frame emitted by ProtocolParser.
     *
     * @param ProtocolFrameType $type Parsed frame type token (INFO, PING, PONG, MSG, HMSG, +OK, -ERR).
     * @param string|null $subject Subject for MSG/HMSG deliveries.
     * @param int|null $sid Subscription ID from MSG/HMSG control line.
     * @param string|null $replyTo Optional reply subject from MSG/HMSG control line.
     * @param string|null $payload Payload bytes (for MSG) or merged header+payload bytes (for HMSG).
     * @param string|null $error Error string payload parsed from `-ERR` control frames.
     * @param string|null $infoPayload JSON payload parsed from INFO frames.
     * @param int|null $headerBytes Header byte length advertised by HMSG control line.
     * @param int|null $totalBytes Total byte length (headers + payload) advertised by HMSG control line.
     */
    public function __construct(
        public readonly ProtocolFrameType $type,
        public readonly ?string $subject = null,
        public readonly ?int $sid = null,
        public readonly ?string $replyTo = null,
        public readonly ?string $payload = null,
        public readonly ?string $error = null,
        public readonly ?string $infoPayload = null,
        public readonly ?int $headerBytes = null,
        public readonly ?int $totalBytes = null,
    ) {
    }
}
