<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

final class ConsumerInfo
{
    /**
     * Represents a subset of consumer metadata returned by JetStream APIs.
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
