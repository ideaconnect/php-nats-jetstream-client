<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\KeyValue;

/**
 * Immutable representation of a Key-Value entry revision.
 */
final class KeyValueEntry
{
    /**
     * Represents a KV entry snapshot delivered by get/watch operations.
     *
     * @param string $bucket KV bucket name.
     * @param string $key KV entry key.
     * @param ?string $value Entry payload, or null for delete/purge tombstones.
     * @param string $operation KV operation marker (for example `PUT`, `DEL`, `PURGE`).
     * @param ?int $revision Stream sequence used as KV revision.
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
