<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Models;

/**
 * Immutable subset of JetStream stream metadata.
 */
final class StreamInfo
{
    /**
     * Represents the selected stream metadata returned by JetStream APIs.
     *
     * @param string $name Stream name from stream configuration.
     * @param list<string> $subjects Subject filters bound to the stream.
     * @param array<string,mixed> $raw Full stream info payload returned by `$JS.API.STREAM.INFO`.
     */
    public function __construct(
        public readonly string $name,
        /** @var list<string> */
        public readonly array $subjects,
        /** @var array<string,mixed> */
        public readonly array $raw,
    ) {
    }

    /**
     * Hydrates stream information from JetStream API JSON.
     *
     * @param array<string,mixed> $data
     */
    public static function fromArray(array $data): self
    {
        /** @var array<string,mixed> $config */
        $config = is_array($data['config'] ?? null) ? $data['config'] : [];
        /** @var list<string> $subjects */
        $subjects = array_values(array_filter(
            is_array($config['subjects'] ?? null) ? $config['subjects'] : [],
            static fn (mixed $value): bool => is_string($value),
        ));

        return new self(
            name: (string) ($config['name'] ?? ''),
            subjects: $subjects,
            raw: $data,
        );
    }
}
