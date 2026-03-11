<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

final class KeyValueEntry
{
    /**
     * Represents a KV entry snapshot delivered by get/watch operations.
     */
    public function __construct(
        public readonly string $bucket,
        public readonly string $key,
        public readonly ?string $value,
        public readonly string $operation,
        public readonly ?int $revision = null,
    ) {
    }
}
