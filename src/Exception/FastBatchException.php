<?php

declare(strict_types=1);

namespace IDCT\NATS\Exception;

use IDCT\NATS\JetStream\Models\PubAck;

/**
 * A fast-ingest batch (ADR-50, {@see \IDCT\NATS\JetStream\FastPublisher}) that the server ended before its commit, or a
 * notice the server sent about one message of a batch.
 *
 * - The batch ended before its commit: {@see $pubAck} is the PubAck the server ended it with. Its `batchCount` is the
 *   batch sequence the server stopped at; in gap mode `fail` every message up to it was stored, in order. That
 *   happens after a gap or a failed per-message check (`Nats-Expected-*`) in gap mode `fail`, and after a repeated
 *   batch sequence in either mode. {@see getPrevious()} is the notice that announced the end, when it came.
 * - A notice: a gap (messages lost between two batch sequences) or a message the stream refused (its `err_code`, for
 *   instance 10071 for a wrong last sequence). {@see $batchSequence} is the batch sequence it is about. In gap mode
 *   `ok` notices go to the publisher's `onError` callback and the batch goes on.
 *
 * It is a {@see JetStreamException}, so existing `catch (JetStreamException)` handlers catch it.
 */
final class FastBatchException extends JetStreamException
{
    /**
     * @param PubAck|null $pubAck        The PubAck that ended the batch before its commit, or null for a notice.
     * @param int|null    $batchSequence The batch sequence a notice is about, or null.
     */
    public function __construct(
        string $message,
        int $code = 0,
        ?\Throwable $previous = null,
        ?int $errCode = null,
        public readonly ?PubAck $pubAck = null,
        public readonly ?int $batchSequence = null,
    ) {
        parent::__construct($message, $code, $previous, $errCode);
    }
}
