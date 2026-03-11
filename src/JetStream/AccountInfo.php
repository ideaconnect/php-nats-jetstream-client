<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

final class AccountInfo
{
    /**
     * Represents account-level JetStream usage details.
     */
    public function __construct(
        public readonly int $memory,
        public readonly int $storage,
        public readonly int $streams,
        public readonly int $consumers,
        /** @var array<string,mixed> */
        public readonly array $raw,
    ) {
    }

    /**
     * Hydrates account info from JetStream API JSON.
     *
     * @param array<string,mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            memory: (int) ($data['memory'] ?? 0),
            storage: (int) ($data['storage'] ?? 0),
            streams: (int) ($data['streams'] ?? 0),
            consumers: (int) ($data['consumers'] ?? 0),
            raw: $data,
        );
    }
}
