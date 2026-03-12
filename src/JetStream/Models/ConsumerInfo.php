<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Models;

/**
 * Immutable subset of JetStream consumer metadata.
 */
final class ConsumerInfo
{
    /**
     * Represents a subset of consumer metadata returned by JetStream APIs.
     *
     * @param string $streamName Stream name that owns this consumer.
     * @param string $name Consumer name (durable name for durable consumers).
     * @param bool $push Whether consumer is push-based (`deliver_subject` configured) versus pull-based.
     * @param array<string,mixed> $raw Full consumer info payload returned by `$JS.API.CONSUMER.INFO`.
     */
    public function __construct(
        public readonly string $streamName,
        public readonly string $name,
        public readonly bool $push,
        /** @var array<string,mixed> */
        public readonly array $raw,
    ) {
    }

    /**
     * Hydrates consumer info from JetStream API JSON.
     *
     * @param array<string,mixed> $data
     */
    public static function fromArray(array $data): self
    {
        /** @var array<string,mixed> $config */
        $config = is_array($data['config'] ?? null) ? $data['config'] : [];
        $deliverSubject = (string) ($config['deliver_subject'] ?? '');

        return new self(
            streamName: (string) ($data['stream_name'] ?? ''),
            name: (string) ($data['name'] ?? ($config['durable_name'] ?? '')),
            push: $deliverSubject !== '',
            raw: $data,
        );
    }
}
