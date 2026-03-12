<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Models;

/**
 * Immutable acknowledgment returned by JetStream publish APIs.
 */
final class PubAck
{
    /**
     * Represents an acknowledgment for a JetStream publish request.
     *
     * @param string $stream Stream that accepted the publish.
     * @param int $seq Assigned stream sequence number for the stored message.
     * @param bool $duplicate Indicates server detected duplicate publish by message ID.
     * @param array<string,mixed> $raw Full publish acknowledgment payload.
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
