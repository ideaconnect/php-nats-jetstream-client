<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\JetStream\Models\JsMessageMetadata;
use PHPUnit\Framework\TestCase;

final class JsMessageMetadataTest extends TestCase
{
    // ---------------------------------------------------------------------------
    // Helper
    // ---------------------------------------------------------------------------

    /**
     * Builds a minimal NatsMessage whose replyTo is the given string.
     */
    private function makeMessage(?string $replyTo): NatsMessage
    {
        return new NatsMessage(
            subject: 'test.subject',
            sid: 1,
            replyTo: $replyTo,
            payload: '',
        );
    }

    // ---------------------------------------------------------------------------
    // null / guard branches (lines 46, 51, 64-65)
    // ---------------------------------------------------------------------------

    /**
     * When a message has no reply subject, fromMessage() returns null (line 46).
     */
    public function testFromMessageReturnsNullWhenReplyToIsNull(): void
    {
        $result = JsMessageMetadata::fromMessage($this->makeMessage(null));

        self::assertNull($result);
    }

    /**
     * When the first token is not "$JS", fromMessage() returns null (line 51).
     */
    public function testFromMessageReturnsNullWhenFirstTokenIsNotJs(): void
    {
        $result = JsMessageMetadata::fromMessage($this->makeMessage('FOO.ACK.stream.consumer.1.1.1.0.0'));

        self::assertNull($result);
    }

    /**
     * When the second token is not "ACK", fromMessage() returns null (line 51).
     */
    public function testFromMessageReturnsNullWhenSecondTokenIsNotAck(): void
    {
        $result = JsMessageMetadata::fromMessage($this->makeMessage('$JS.NAK.stream.consumer.1.1.1.0.0'));

        self::assertNull($result);
    }

    /**
     * When the token count is not 9, 11, or 12 (e.g. 7 tokens), fromMessage()
     * returns null via the default branch of the match expression (lines 61, 64-65).
     */
    public function testFromMessageReturnsNullForUnrecognisedTokenCount(): void
    {
        // "$JS.ACK" + 5 more tokens = 7 total - not 9/11/12
        $result = JsMessageMetadata::fromMessage($this->makeMessage('$JS.ACK.a.b.c.d.e'));

        self::assertNull($result);
    }

    // ---------------------------------------------------------------------------
    // 9-token form (line 59, plus 92-93, 95-99, 103 via timestamp())
    // ---------------------------------------------------------------------------

    /**
     * Parses a canonical 9-token $JS.ACK reply subject and verifies every field.
     * Exercises the base=2 branch (line 59) and the constructor (lines 74-84).
     */
    public function testFromMessageParses9TokenForm(): void
    {
        // $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending>
        $replyTo = '$JS.ACK.mystream.myconsumer.3.42.7.1700000000000000000.5';
        $meta = JsMessageMetadata::fromMessage($this->makeMessage($replyTo));

        self::assertNotNull($meta);
        self::assertSame('mystream', $meta->stream);
        self::assertSame('myconsumer', $meta->consumer);
        self::assertSame(3, $meta->numDelivered);
        self::assertSame(42, $meta->streamSequence);
        self::assertSame(7, $meta->consumerSequence);
        self::assertSame(1700000000000000000, $meta->timestampNanos);
        self::assertSame(5, $meta->numPending);
        self::assertNull($meta->domain);
    }

    // ---------------------------------------------------------------------------
    // 11-token domain form (line 60)
    // ---------------------------------------------------------------------------

    /**
     * Parses the 11-token domain-prefixed reply subject and populates the domain field.
     * Covers the base=4 branch (line 60) and $domain = $parts[2] (line 68).
     */
    public function testFromMessageParses11TokenFormWithRealDomain(): void
    {
        // $JS.ACK.<domain>.<account>.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending>
        $replyTo = '$JS.ACK.hub.ACC.orders.push.2.10.2.1700000000000000000.0';
        $meta = JsMessageMetadata::fromMessage($this->makeMessage($replyTo));

        self::assertNotNull($meta);
        self::assertSame('hub', $meta->domain);
        self::assertSame('orders', $meta->stream);
        self::assertSame('push', $meta->consumer);
        self::assertSame(2, $meta->numDelivered);
        self::assertSame(10, $meta->streamSequence);
        self::assertSame(2, $meta->consumerSequence);
        self::assertSame(1700000000000000000, $meta->timestampNanos);
        self::assertSame(0, $meta->numPending);
    }

    /**
     * When the domain token is "_" the placeholder is normalized to null (lines 69-72).
     */
    public function testFromMessageNormalizesUnderscoreDomainToNull(): void
    {
        // domain token = "_" (server placeholder)
        $replyTo = '$JS.ACK._.ACC.orders.push.1.1.1.1700000000000000000.0';
        $meta = JsMessageMetadata::fromMessage($this->makeMessage($replyTo));

        self::assertNotNull($meta);
        self::assertNull($meta->domain, 'Underscore domain placeholder must be normalized to null');
    }

    // ---------------------------------------------------------------------------
    // 12-token form (line 60 - trailing random token)
    // ---------------------------------------------------------------------------

    /**
     * Parses the 12-token reply subject (domain form + trailing random token).
     * The 12th token must be silently ignored; all other fields must parse correctly.
     */
    public function testFromMessageParses12TokenForm(): void
    {
        // $JS.ACK.<domain>.<account>.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending>.<rand>
        $replyTo = '$JS.ACK.hub.ACC.events.worker.5.99.5.1700000000000000000.3.RANDOMTOKEN';
        $meta = JsMessageMetadata::fromMessage($this->makeMessage($replyTo));

        self::assertNotNull($meta);
        self::assertSame('hub', $meta->domain);
        self::assertSame('events', $meta->stream);
        self::assertSame('worker', $meta->consumer);
        self::assertSame(5, $meta->numDelivered);
        self::assertSame(99, $meta->streamSequence);
        self::assertSame(5, $meta->consumerSequence);
        self::assertSame(3, $meta->numPending);
    }

    // ---------------------------------------------------------------------------
    // timestamp() method (lines 92-93, 95-99, 103)
    // ---------------------------------------------------------------------------

    /**
     * timestamp() converts a nanosecond epoch value to the correct UTC DateTimeImmutable
     * (lines 92-93, 95-99).
     */
    public function testTimestampReturnsCorrectUtcDatetime(): void
    {
        // 1_700_000_000 seconds = 2023-11-14 22:13:20 UTC  (well-known epoch second)
        $nanos = 1_700_000_000_000_000_000;
        $replyTo = '$JS.ACK.s.c.1.1.1.' . $nanos . '.0';
        $meta = JsMessageMetadata::fromMessage($this->makeMessage($replyTo));

        self::assertNotNull($meta);
        $dt = $meta->timestamp();

        self::assertInstanceOf(\DateTimeImmutable::class, $dt);
        // createFromFormat with 'U.u' uses offset +00:00, which is equivalent to UTC
        self::assertSame(0, $dt->getOffset(), 'Timezone offset must be zero (UTC)');
        self::assertSame('2023-11-14 22:13:20', $dt->format('Y-m-d H:i:s'));
    }

    /**
     * timestamp() preserves sub-second precision down to microseconds (lines 95-99).
     */
    public function testTimestampPreservesMicrosecondPrecision(): void
    {
        // 500_000 nanoseconds = 500 microseconds past the whole second
        $seconds = 1_700_000_000;
        $nanos = $seconds * 1_000_000_000 + 500_000;
        $replyTo = '$JS.ACK.s.c.1.1.1.' . $nanos . '.0';
        $meta = JsMessageMetadata::fromMessage($this->makeMessage($replyTo));

        self::assertNotNull($meta);
        $dt = $meta->timestamp();

        // Microsecond portion: 500_000 ns / 1_000 = 500 µs -> "000500"
        self::assertSame('000500', $dt->format('u'));
    }

    /**
     * timestamp() still returns a DateTimeImmutable for a zero nanosecond value
     * (exercises the happy path and the fallback guard on line 103).
     */
    public function testTimestampHandlesZeroNanoseconds(): void
    {
        $replyTo = '$JS.ACK.s.c.1.1.1.0.0';
        $meta = JsMessageMetadata::fromMessage($this->makeMessage($replyTo));

        self::assertNotNull($meta);
        $dt = $meta->timestamp();

        self::assertInstanceOf(\DateTimeImmutable::class, $dt);
        self::assertSame('1970-01-01 00:00:00', $dt->format('Y-m-d H:i:s'));
    }
}
