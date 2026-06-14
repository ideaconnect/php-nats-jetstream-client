<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\JetStream\Models\JsMessageMetadata;

/**
 * Targeted tests that pin behaviour Infection's surviving mutants would change in
 * {@see JsMessageMetadata}. Each assertion group documents the mutant it kills.
 */
final class JsMessageMetadataMutationTest extends \PHPUnit\Framework\TestCase
{
    private function makeMessage(?string $replyTo): NatsMessage
    {
        return new NatsMessage(
            subject: 'test.subject',
            sid: 1,
            replyTo: $replyTo,
            payload: '',
        );
    }

    /**
     * The sub-second remainder must be divided by exactly 1000 (ns -> us). With a
     * remainder of 999000 ns the real code yields 999 us; dividing by 999 (the
     * DecrementInteger mutant) yields 1000 us, i.e. microsecond field "001000".
     */
    public function testTimestampMicrosecondsUseDivisorOf1000(): void
    {
        // kills DecrementInteger @ line 93
        $seconds = 1_700_000_000;
        $nanos = $seconds * 1_000_000_000 + 999_000; // 999000 ns remainder
        $meta = JsMessageMetadata::fromMessage(
            $this->makeMessage('$JS.ACK.s.c.1.1.1.' . $nanos . '.0'),
        );

        self::assertNotNull($meta);
        // intdiv(999000, 1000) = 999  vs  intdiv(999000, 999) = 1000
        self::assertSame('000999', $meta->timestamp()->format('u'));
    }

    /**
     * The defensive fallback on line 103 is reachable: when timestampNanos is
     * PHP_INT_MIN the modulo is negative, sprintf produces a malformed
     * "-9223372036.-854775" and createFromFormat() returns false, so the
     * ternary takes the `new \DateTimeImmutable('@' . $seconds)` branch.
     *
     * The "@<unix-seconds>" spelling is what makes this work:
     *   - Concat mutant  ($seconds . '@')  -> "-9223372036@" -> throws.
     *   - ConcatOperandRemoval ('@')       -> "@"            -> throws.
     *   - ConcatOperandRemoval ($seconds)  -> "-9223372036"  -> a wildly
     *     different (positive, far-future) date, not the epoch second.
     */
    public function testTimestampFallbackUsesAtPrefixedUnixSeconds(): void
    {
        // kills Concat @ line 103, ConcatOperandRemoval('@') @ line 103,
        //       ConcatOperandRemoval($seconds) @ line 103
        $nanos = PHP_INT_MIN;
        $seconds = intdiv($nanos, 1_000_000_000); // -9223372036

        // Sanity: this input really does drive createFromFormat() to false,
        // so the fallback branch (and thus the mutated expression) executes.
        $probe = \DateTimeImmutable::createFromFormat(
            'U.u',
            sprintf('%d.%06d', $seconds, intdiv($nanos % 1_000_000_000, 1_000)),
            new \DateTimeZone('UTC'),
        );
        self::assertFalse($probe, 'PHP_INT_MIN must drive the defensive fallback branch');

        $meta = JsMessageMetadata::fromMessage(
            $this->makeMessage('$JS.ACK.s.c.1.1.1.' . $nanos . '.0'),
        );
        self::assertNotNull($meta);
        self::assertSame($nanos, $meta->timestampNanos);

        // Real code: new \DateTimeImmutable('@' . -9223372036) === unix second -9223372036.
        $dt = $meta->timestamp();
        self::assertInstanceOf(\DateTimeImmutable::class, $dt);
        self::assertSame($seconds, $dt->getTimestamp());
        self::assertSame('1677-09-21 00:12:44', $dt->format('Y-m-d H:i:s'));
    }
}
