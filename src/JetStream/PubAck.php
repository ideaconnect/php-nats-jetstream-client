<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

final class PubAck
{
    /**
     * Represents an acknowledgment for a JetStream publish request.
     */
    public function __construct(
        public readonly string $stream,
        public readonly int $seq,
        public readonly bool $duplicate,
        /** @var array<string,mixed> */
        public readonly array $raw,
    ) {
    }

    /**
     * Hydrates publish acknowledgment from JetStream API JSON.
     *
     * @param array<string,mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            stream: (string) ($data['stream'] ?? ''),
            seq: (int) ($data['seq'] ?? 0),
            duplicate: (bool) ($data['duplicate'] ?? false),
            raw: $data,
        );
    }
}
