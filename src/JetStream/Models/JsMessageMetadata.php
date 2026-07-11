<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Models;

use IDCT\NATS\Core\NatsMessage;

/**
 * Parsed JetStream delivery metadata carried by a message's `$JS.ACK` reply subject.
 *
 * Mirrors nats.go `MsgMetadata` and nats.java `NatsJetStreamMetaData`: stream/consumer sequences,
 * redelivery count, pending backlog, server timestamp, and (for domain/account-prefixed deliveries)
 * the JetStream domain.
 */
final class JsMessageMetadata
{
    /**
     * @param string      $stream            Stream the message was stored in.
     * @param string      $consumer          Consumer that delivered the message.
     * @param int         $numDelivered      Delivery count (1 on first delivery, >1 after redelivery).
     * @param int         $streamSequence    Sequence of the message within the stream.
     * @param int         $consumerSequence  Sequence of this delivery within the consumer.
     * @param int         $numPending        Messages still pending for the consumer after this one.
     * @param int         $timestampNanos    Server store timestamp, in nanoseconds since the Unix epoch.
     * @param string|null $domain            JetStream domain, or null for the non-domain reply form.
     */
    public function __construct(
        public readonly string $stream,
        public readonly string $consumer,
        public readonly int $numDelivered,
        public readonly int $streamSequence,
        public readonly int $consumerSequence,
        public readonly int $numPending,
        public readonly int $timestampNanos,
        public readonly ?string $domain = null,
    ) {}

    /**
     * Parses metadata from a delivered message's `$JS.ACK` reply subject, or returns null when the
     * message was not delivered by a JetStream consumer (no parseable ack subject).
     */
    public static function fromMessage(NatsMessage $message): ?self
    {
        if ($message->replyTo === null) {
            return null;
        }

        $parts = explode('.', $message->replyTo);
        if ($parts[0] !== '$JS' || ($parts[1] ?? null) !== 'ACK') {
            return null;
        }

        // Two ack reply-subject shapes. The v1 form is exactly 9 tokens; the expanded v2 form is
        // matched with >= 11 tokens - field offsets anchor from the front and everything after
        // index 10 is ignored, because servers may append tokens (the 12th, a random suffix, was
        // itself a later addition) and nats.go's parser deliberately tolerates extras (#155).
        // Literal offsets per count() branch so the token-count guard provably covers every
        // access (a shared base-offset arithmetic loses that correlation for static analysis):
        //   9 tokens:  $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending>
        //  >= 11 tokens: $JS.ACK.<domain>.<account>.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending>[.<extra>...]
        $count = count($parts);

        if ($count === 9) {
            return new self(
                stream: $parts[2],
                consumer: $parts[3],
                numDelivered: (int) $parts[4],
                streamSequence: (int) $parts[5],
                consumerSequence: (int) $parts[6],
                numPending: (int) $parts[8],
                timestampNanos: (int) $parts[7],
                domain: null,
            );
        }

        if ($count < 11) {
            return null;
        }

        // The server uses "_" as the placeholder domain when none is configured.
        $domain = $parts[2] === '_' ? null : $parts[2];

        return new self(
            stream: $parts[4],
            consumer: $parts[5],
            numDelivered: (int) $parts[6],
            streamSequence: (int) $parts[7],
            consumerSequence: (int) $parts[8],
            numPending: (int) $parts[10],
            timestampNanos: (int) $parts[9],
            domain: $domain,
        );
    }

    /**
     * The server store timestamp as a UTC {@see \DateTimeImmutable} (microsecond resolution; the
     * nanosecond remainder is available via {@see $timestampNanos}).
     */
    public function timestamp(): \DateTimeImmutable
    {
        $seconds = intdiv($this->timestampNanos, 1_000_000_000);
        $micros = intdiv($this->timestampNanos % 1_000_000_000, 1_000);

        $dt = \DateTimeImmutable::createFromFormat(
            'U.u',
            sprintf('%d.%06d', $seconds, $micros),
            new \DateTimeZone('UTC'),
        );

        // createFromFormat returns false only on a malformed format string, which cannot happen for
        // the fixed numeric format above; fall back defensively to keep the return type total.
        return $dt !== false ? $dt : (new \DateTimeImmutable('@' . $seconds));
    }
}
