<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Models;

/**
 * What {@see \IDCT\NATS\JetStream\FastPublisher::add()} returns for one message of a fast-ingest batch (ADR-50).
 */
final class FastPubAck
{
    /**
     * @param int $batchSequence The message's sequence within its batch (1 for the first message).
     * @param int $ackSequence   The highest batch sequence the server had acknowledged when add() returned. In gap
     *                           mode `fail` every message up to and including it was stored; in gap mode `ok` some
     *                           of them may have been lost (see the publisher's `onError` notices).
     */
    public function __construct(
        public readonly int $batchSequence,
        public readonly int $ackSequence,
    ) {}
}
