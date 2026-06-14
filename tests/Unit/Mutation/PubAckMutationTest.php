<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\JetStream\Models\PubAck;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing unit tests for {@see PubAck::fromArray()}.
 *
 * PubAck is a pure immutable value object. Its factory maps:
 *   - seq via `(int) ($data['seq'] ?? 0)`            (Increment/Decrement target the 0 default)
 *   - duplicate via `(bool) ($data['duplicate'] ?? false)` (FalseValue flips the default to true)
 *   - batchCount via `isset(...) ? (int) $data['count'] : null`  (CastInt drops the (int) cast)
 *   - batchId via `isset(...) ? (string) $data['batch'] : null`  (CastString drops the (string) cast)
 *
 * Each test pins the exact observable value the corresponding mutation would change. Source must NOT
 * be modified. Pure construction + strict value assertions: deterministic, no I/O, no timing.
 */
final class PubAckMutationTest extends TestCase
{
    /**
     * With 'seq' absent, the assigned sequence must default to exactly 0.
     * DecrementInteger shifts the default to -1; IncrementInteger shifts it to 1. assertSame
     * (strict int compare) fails under either mutant, which would yield -1 or 1 respectively.
     */
    public function testMissingSeqDefaultsToExactlyZero(): void
    {
        $ack = PubAck::fromArray([]);

        // kills DecrementInteger @ line 41 and IncrementInteger @ line 41
        self::assertSame(0, $ack->seq);
    }

    /**
     * With 'duplicate' absent, the flag must default to exactly false.
     * FalseValue rewrites the default to true; assertFalse fails under that mutant.
     */
    public function testMissingDuplicateDefaultsToFalse(): void
    {
        $ack = PubAck::fromArray([]);

        // kills FalseValue @ line 42
        self::assertFalse($ack->duplicate);
    }

    /**
     * 'count' supplied as a non-integer scalar string must be coerced to int by the `(int)` cast.
     * CastInt removes the cast, leaving batchCount as the raw string "5"; assertSame against the
     * int 5 (strict type+value) fails because string "5" !== int 5.
     */
    public function testBatchCountIsCastToInt(): void
    {
        $ack = PubAck::fromArray(['count' => '5']);

        // kills CastInt @ line 44
        self::assertSame(5, $ack->batchCount);
    }

    /**
     * 'batch' supplied as a non-string scalar (int) must be coerced to string by the `(string)` cast.
     * CastString removes the cast, leaving batchId as the raw int 42; assertSame against the
     * string "42" (strict type+value) fails because int 42 !== string "42".
     */
    public function testBatchIdIsCastToString(): void
    {
        $ack = PubAck::fromArray(['batch' => 42]);

        // kills CastString @ line 45
        self::assertSame('42', $ack->batchId);
    }
}
