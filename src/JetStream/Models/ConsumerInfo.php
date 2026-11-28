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
     * @param int|null $resetSequence The stream sequence a consumer reset moved the consumer to (`reset_seq`): the
     *        next message delivered is the first one at or after it. Only the reply of
     *        {@see \IDCT\NATS\JetStream\JetStreamContext::resetConsumer()} carries it; null otherwise.
     */
    public function __construct(
        public readonly string $streamName,
        public readonly string $name,
        public readonly bool $push,
        /** @var array<string,mixed> */
        public readonly array $raw,
        public readonly ?int $resetSequence = null,
    ) {}

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
        $resetSequence = $data['reset_seq'] ?? null;

        return new self(
            streamName: (string) ($data['stream_name'] ?? ''),
            name: (string) ($data['name'] ?? ($config['durable_name'] ?? '')),
            push: $deliverSubject !== '',
            raw: $data,
            resetSequence: is_int($resetSequence) ? $resetSequence : null,
        );
    }
}
