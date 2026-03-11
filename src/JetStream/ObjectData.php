<?php

declare(strict_types=1);

namespace Idct\Nats\JetStream;

final class ObjectData
{
    /**
     * Represents an object read result with metadata and optional payload.
     */
    public function __construct(
        public readonly ObjectInfo $info,
        public readonly ?string $data,
    ) {
    }
}
