<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\ObjectStore;

/**
 * Container for object metadata and optional downloaded payload bytes.
 */
final class ObjectData
{
    /**
     * Represents an object read result with metadata and optional payload.
     *
     * @param ObjectInfo $info Object metadata returned from Object Store.
     * @param ?string $data Object payload bytes, or null when only metadata was requested.
     */
    public function __construct(
        public readonly ObjectInfo $info,
        public readonly ?string $data,
    ) {
    }
}
