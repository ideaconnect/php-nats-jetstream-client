<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Models;

/**
 * Immutable account-level JetStream resource usage snapshot.
 */
final class AccountInfo
{
    /**
     * Represents account-level JetStream usage details.
     *
     * @param int $memory Current memory bytes used by JetStream account resources.
     * @param int $storage Current storage bytes used by JetStream account resources.
     * @param int $streams Number of streams currently owned by the account.
     * @param int $consumers Number of consumers currently owned by the account.
     * @param array<string,mixed> $raw Full raw JetStream account info payload for advanced fields not mapped explicitly.
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
