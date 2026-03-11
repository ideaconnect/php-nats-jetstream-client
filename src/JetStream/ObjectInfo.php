<?php

declare(strict_types=1);

namespace Idct\Nats\JetStream;

final class ObjectInfo
{
    /**
     * Represents Object Store metadata for a single object revision.
     *
     * @param array<string,string> $metadata
     */
    public function __construct(
        public readonly string $bucket,
        public readonly string $name,
        public readonly int $size,
        public readonly string $digest,
        public readonly string $modified,
        public readonly bool $deleted,
        public readonly string $chunkSubject,
        public readonly array $metadata = [],
    ) {
    }

    /**
     * @param array<string,mixed> $data
     */
    public static function fromArray(string $bucket, array $data): self
    {
        /** @var array<string,string> $metadata */
        $metadata = is_array($data['metadata'] ?? null) ? array_map('strval', $data['metadata']) : [];

        return new self(
            bucket: $bucket,
            name: (string) ($data['name'] ?? ''),
            size: (int) ($data['size'] ?? 0),
            digest: (string) ($data['digest'] ?? ''),
            modified: (string) ($data['mtime'] ?? ''),
            deleted: (bool) ($data['deleted'] ?? false),
            chunkSubject: (string) ($data['chunk_subject'] ?? ''),
            metadata: $metadata,
        );
    }
}
