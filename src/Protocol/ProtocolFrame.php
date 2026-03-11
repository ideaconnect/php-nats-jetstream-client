<?php

declare(strict_types=1);

namespace Idct\Nats\Protocol;

final class ProtocolFrame
{
    /**
     * Represents one parsed protocol frame emitted by ProtocolParser.
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
