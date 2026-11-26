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
     * @param int|null $apiLevel The JetStream API level (ADR-44) the reply reports in `api.level`, or null when it
     *        reports none (servers before 2.11). `$JS.API.INFO` is answered by the meta leader, so in a cluster this
     *        is the level taken to represent the cluster, unlike the INFO version of the server the client is
     *        connected to. ADR-8 and ADR-48 ask a client to check this level, not a version, before it uses limit
     *        markers (#272).
     */
    public function __construct(
        public readonly int $memory,
        public readonly int $storage,
        public readonly int $streams,
        public readonly int $consumers,
        /** @var array<string,mixed> */
        public readonly array $raw,
        public readonly ?int $apiLevel = null,
    ) {}

    /**
     * Hydrates account info from JetStream API JSON.
     *
     * @param array<string,mixed> $data
     */
    public static function fromArray(array $data): self
    {
        $api = $data['api'] ?? null;
        $level = is_array($api) ? ($api['level'] ?? null) : null;

        return new self(
            memory: (int) ($data['memory'] ?? 0),
            storage: (int) ($data['storage'] ?? 0),
            streams: (int) ($data['streams'] ?? 0),
            consumers: (int) ($data['consumers'] ?? 0),
            raw: $data,
            apiLevel: is_int($level) && $level >= 0 ? $level : null,
        );
    }
}
