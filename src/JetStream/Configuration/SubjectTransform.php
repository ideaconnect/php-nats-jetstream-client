<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Configuration;

/**
 * Configures subject transform rules for streams.
 *
 * Usage:
 *   $js->createStream('MAPPED', ['raw.>'], [
 *       'subject_transform' => SubjectTransform::create('raw.>', 'mapped.>')->toArray(),
 *   ]);
 */
final class SubjectTransform
{
    /**
     * @param string $src Source subject pattern to match.
     * @param string $dest Destination subject pattern after transform.
     */
    private function __construct(
        private readonly string $src,
        private readonly string $dest,
    ) {
    }

    /**
     * Creates a subject transform mapping a source pattern to a destination pattern.
     */
    public static function create(string $src, string $dest): self
    {
        return new self($src, $dest);
    }

    /**
     * Serializes to the NATS JetStream API subject_transform configuration array.
     *
     * @return array<string, string>
     */
    public function toArray(): array
    {
        return [
            'src' => $this->src,
            'dest' => $this->dest,
        ];
    }
}
