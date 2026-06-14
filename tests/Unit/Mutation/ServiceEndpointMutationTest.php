<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Services\ServiceEndpoint;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing unit tests for {@see ServiceEndpoint}.
 *
 * ServiceEndpoint is a plain runtime-state container. The surviving Infection mutants in this chunk
 * both target the `processingTimeNs` constructor default on line 38:
 *   - IncrementInteger shifts the default from 0 to 1.
 *   - DecrementInteger shifts the default from 0 to -1.
 *
 * A freshly-registered endpoint has not accumulated any processing time, so the default must be
 * exactly 0. Source must NOT be modified. Tests are pure construction + value assertions.
 */
final class ServiceEndpointMutationTest extends TestCase
{
    /**
     * Constructing an endpoint without specifying processing time must leave the accumulator at
     * exactly 0 (a brand-new endpoint has handled nothing yet). Both the IncrementInteger (=> 1) and
     * DecrementInteger (=> -1) mutants on the default would break this exact-value assertion.
     */
    public function testProcessingTimeNsDefaultsToExactlyZero(): void
    {
        $endpoint = new ServiceEndpoint(
            name: 'echo',
            subject: 'svc.echo',
            queueGroup: null,
        );

        // kills IncrementInteger + DecrementInteger @ line 38 (processingTimeNs default)
        self::assertSame(0, $endpoint->processingTimeNs);
    }

    /**
     * Sanity guard against an equivalent-mutant escape: prove the field is genuinely wired to the
     * constructor argument and is not coincidentally always 0. A non-zero explicit value must be
     * stored verbatim, while leaving the other runtime counters at their own zero defaults.
     */
    public function testProcessingTimeNsHonoursExplicitValueAndOtherCountersDefaultToZero(): void
    {
        $endpoint = new ServiceEndpoint(
            name: 'echo',
            subject: 'svc.echo',
            queueGroup: null,
            processingTimeNs: 12_345,
        );

        // explicit value is read from the argument, not forced to a constant default
        self::assertSame(12_345, $endpoint->processingTimeNs);
        // the other runtime counters remain at their own zero defaults (cross-wiring guard)
        self::assertSame(0, $endpoint->requests);
        self::assertSame(0, $endpoint->errors);
    }
}
