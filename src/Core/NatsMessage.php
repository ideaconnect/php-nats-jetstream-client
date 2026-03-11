<?php

declare(strict_types=1);

namespace IDCT\NATS\Core;

final class NatsMessage
{
    /**
     * Represents a normalized delivery passed to user subscription handlers.
     */
    public function __construct(
        public readonly string $subject,
        public readonly int $sid,
        public readonly ?string $replyTo,
        public readonly string $payload,
        public readonly ?string $rawHeaders = null,
    ) {
    }
}
