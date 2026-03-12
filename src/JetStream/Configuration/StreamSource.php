<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Configuration;

/**
 * Describes a stream source or mirror origin for stream replication.
 *
 * Used with `createStream()` options:
 *   $js->createStream('MIRROR', [], [
 *       'mirror' => StreamSource::mirror('ORIGIN')->toArray(),
 *   ]);
 *
 *   $js->createStream('AGG', ['agg.>'], [
 *       'sources' => [
 *           StreamSource::source('ORDERS')->filterSubject('orders.>')->toArray(),
 *           StreamSource::source('EVENTS')->startSeq(100)->toArray(),
 *       ],
 *   ]);
 */
final class StreamSource
{
    private ?int $optStartSeq = null;
    private ?string $optStartTime = null;
    private ?string $filterSubject = null;
    /** @var array<string,string>|null */
    private ?array $externalApi = null;

    /**
     * @param string $name Origin stream name used by mirror/source configuration.
     */
    private function __construct(
        private readonly string $name,
    ) {
    }

    /**
     * Creates a mirror source reference.
     */
    public static function mirror(string $streamName): self
    {
        return new self($streamName);
    }

    /**
     * Creates a source reference for stream aggregation.
     */
    public static function source(string $streamName): self
    {
        return new self($streamName);
    }

    /**
     * Start replication from a specific sequence number.
     *
     * @return $this
     */
    public function startSeq(int $seq): self
    {
        $this->optStartSeq = $seq;

        return $this;
    }

    /**
     * Start replication from a specific time (ISO 8601).
     *
     * @return $this
     */
    public function startTime(string $isoTime): self
    {
        $this->optStartTime = $isoTime;

        return $this;
    }

    /**
     * Filter replicated subjects.
     *
     * @return $this
     */
    public function filterSubject(string $subject): self
    {
        $this->filterSubject = $subject;

        return $this;
    }

    /**
     * Sets external API reference for cross-account/cross-cluster sourcing.
     *
     * @return $this
     */
    public function external(string $apiPrefix, ?string $deliverPrefix = null): self
    {
        $ext = ['api' => $apiPrefix];
        if ($deliverPrefix !== null) {
            $ext['deliver'] = $deliverPrefix;
        }
        $this->externalApi = $ext;

        return $this;
    }

    /**
     * Serializes to the NATS JetStream API mirror/source configuration array.
     *
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        $result = ['name' => $this->name];

        if ($this->optStartSeq !== null) {
            $result['opt_start_seq'] = $this->optStartSeq;
        }

        if ($this->optStartTime !== null) {
            $result['opt_start_time'] = $this->optStartTime;
        }

        if ($this->filterSubject !== null) {
            $result['filter_subject'] = $this->filterSubject;
        }

        if ($this->externalApi !== null) {
            $result['external'] = $this->externalApi;
        }

        return $result;
    }
}
