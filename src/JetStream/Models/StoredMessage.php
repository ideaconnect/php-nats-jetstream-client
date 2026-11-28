<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Models;

/**
 * A message as stored in a stream, read over the leader `$JS.API.STREAM.MSG.GET` API together with
 * the stream sequence and the store time the server reports for it.
 *
 * Returned by {@see \IDCT\NATS\JetStream\JetStreamContext::getNextMessageForSubject()} (#36, #15), so
 * that a caller can walk a subject by asking again from `$sequence + 1`. The payload and the header
 * block are decoded as for {@see \IDCT\NATS\JetStream\JetStreamContext::getStreamMessage()}: a body
 * that is exactly `"0"` is kept, and `$rawHeaders` holds the stored header block (null when the
 * message has none).
 */
final class StoredMessage
{
    /**
     * @param string      $subject    Subject the message was published to.
     * @param int         $sequence   Stream sequence of the stored message.
     * @param string      $time       Store time as the server sent it (RFC 3339 with nanoseconds, UTC).
     * @param string      $payload    Message body.
     * @param string|null $rawHeaders Stored header block (`NATS/1.0` ...), or null when there is none.
     */
    public function __construct(
        public readonly string $subject,
        public readonly int $sequence,
        public readonly string $time,
        public readonly string $payload,
        public readonly ?string $rawHeaders = null,
    ) {}
}
