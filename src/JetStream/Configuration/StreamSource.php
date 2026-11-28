<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Configuration;

use IDCT\NATS\Exception\JetStreamException;

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
 *
 * Durable sourcing (ADR-60, NATS server 2.14+) through a consumer of your own, created first on the origin:
 *   $js->createStream('AGG', [], [
 *       'sources' => [StreamSource::source('WQ')->consumer('SRC', 'deliver.wq')->toArray()],
 *   ]);
 */
final class StreamSource
{
    private ?int $optStartSeq = null;
    private ?string $optStartTime = null;
    private ?string $filterSubject = null;
    /** @var array<string,string>|null */
    private ?array $externalApi = null;
    /** @var array{name: string, deliver_subject: string}|null */
    private ?array $consumer = null;

    /**
     * @param string $name Origin stream name used by mirror/source configuration.
     */
    private function __construct(
        private readonly string $name,
    ) {}

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
     * Sources or mirrors through a durable consumer of your own on the origin stream (ADR-60, NATS server 2.14+),
     * emitted as `consumer: {name, deliver_subject}`. Create the consumer first, on the origin, with
     * {@see \IDCT\NATS\JetStream\Enum\AckPolicy::FlowControl} and this deliver subject (a literal subject outside
     * the origin's own subjects): the server checks neither when the stream is created, and a stream naming a
     * consumer that is missing or that delivers elsewhere stays empty without an error.
     *
     * Without this setter a 2.14+ server sources or mirrors a WorkQueue or Interest stream through a durable
     * consumer it creates and removes itself. Name your own when its configuration should be yours: the start
     * sequence, start time and filter subject then belong on that consumer, so {@see startSeq()},
     * {@see startTime()} and {@see filterSubject()} cannot be combined with this setter ({@see toArray()} refuses
     * it). It can be combined with {@see external()}.
     *
     * A server before 2.14 does not know the field: 2.12 refuses the stream (400, err_code 10025,
     * `unknown field "consumer"`), while 2.10 and 2.11 create it without the field and source the old way, which
     * the client does not notice.
     *
     * @return $this
     *
     * @throws JetStreamException When the consumer name or the deliver subject is empty.
     */
    public function consumer(string $name, string $deliverSubject): self
    {
        if ($name === '') {
            throw new JetStreamException('A stream source consumer needs a name');
        }

        if ($deliverSubject === '') {
            throw new JetStreamException('A stream source consumer needs a deliver subject');
        }

        $this->consumer = ['name' => $name, 'deliver_subject' => $deliverSubject];

        return $this;
    }

    /**
     * Serializes to the NATS JetStream API mirror/source configuration array.
     *
     * @return array<string, mixed>
     *
     * @throws JetStreamException When {@see consumer()} is combined with {@see startSeq()}, {@see startTime()} or
     *                            {@see filterSubject()}.
     */
    public function toArray(): array
    {
        $this->assertConsumerHasNoOwnStartOrFilter();

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

        if ($this->consumer !== null) {
            $result['consumer'] = $this->consumer;
        }

        return $result;
    }

    /**
     * With a named consumer the start sequence, start time and filter subject belong on that consumer; the server
     * refuses the combination with an error that names no field (10215 for a source, 10213 for a mirror).
     *
     * @throws JetStreamException
     */
    private function assertConsumerHasNoOwnStartOrFilter(): void
    {
        if ($this->consumer === null) {
            return;
        }

        $clashing = [];
        if ($this->optStartSeq !== null) {
            $clashing[] = 'startSeq()';
        }

        if ($this->optStartTime !== null) {
            $clashing[] = 'startTime()';
        }

        if ($this->filterSubject !== null) {
            $clashing[] = 'filterSubject()';
        }

        if ($clashing !== []) {
            throw new JetStreamException(sprintf(
                'Stream source "%s": consumer() cannot be combined with %s; set the start and the filter on the consumer "%s" instead',
                $this->name,
                implode(', ', $clashing),
                $this->consumer['name'],
            ));
        }
    }
}
