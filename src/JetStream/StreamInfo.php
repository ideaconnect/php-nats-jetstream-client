<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

final class StreamInfo
{
    /**
     * Represents the selected stream metadata returned by JetStream APIs.
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
