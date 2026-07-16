<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\JetStream\Consumers\PullConsumerIterator;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing tests for the #153 idle-backoff curve on
 * {@see \IDCT\NATS\JetStream\Consumers\PullConsumerIterator::idleBackoffMs()}.
 *
 * idleBackoffMs() is a pure, public static helper shared with the pipelined engine
 * ({@see \IDCT\NATS\JetStream\JetStreamContext::consumePipelined()}). It computes the pacing delay for
 * the Nth consecutive immediately answered empty pull:
 *
 *   $exponent = min(6, max(0, $consecutiveEmptyPulls - 1));  // clamp the shift into [0, 6]
 *   return min(IDLE_BACKOFF_MAX_MS (500), IDLE_BACKOFF_INITIAL_MS (10) << $exponent);
 *
 * The exact curve (n = 0..) is: 10, 10, 20, 40, 80, 160, 320, 500, 500, ... - it starts at the 10ms
 * initial delay, doubles per consecutive empty pull, and clamps at the 500ms ceiling. Each test below
 * pins one boundary of that curve so a surviving arithmetic mutant on the exponent expression changes
 * an observable return value (or throws). No transport is needed - the helper is called directly.
 */
final class PullConsumerIterator_1MutationTest extends TestCase
{
    /**
     * Pins the max(0, ...) floor that guards the bit shift against a negative exponent.
     *
     * At n = 0 the inner term is (0 - 1) = -1; the max(0, ...) clamps it to 0, so idleBackoffMs(0) is
     * the 10ms initial delay. The mutant lowers the floor to max(-1, ...), leaving the exponent at -1,
     * which makes `10 << -1` throw an ArithmeticError instead of returning 10.
     *
     * Kills: DecrementInteger @ 442  (max(0, ...) -> max(-1, ...)).
     */
    public function testIdleBackoffFloorGuardsNegativeShiftAtZero(): void
    {
        // Real code clamps the exponent to 0 and returns the 10ms initial delay; the mutant would
        // evaluate `10 << -1` and throw ArithmeticError instead.
        self::assertSame(10, PullConsumerIterator::idleBackoffMs(0));
    }

    /**
     * Pins the initial-delay plateau at the FIRST empty pull. With n = 1 the exponent is
     * min(6, max(0, 1 - 1)) = 0, so the delay must still be the 10ms initial value - the backoff has
     * not escalated yet. Three separate mutants on the exponent all inflate n = 1 above 10ms:
     *
     *  - IncrementInteger @ 442  max(0, ...) -> max(1, ...):  exponent 1 => 10 << 1 = 20
     *  - DecrementInteger @ 442  ($n - 1)    -> ($n - 0):     exponent 1 => 20
     *  - Minus          @ 442  ($n - 1)    -> ($n + 1):     exponent 2 => 10 << 2 = 40
     *
     * Kills: IncrementInteger @ 442, DecrementInteger @ 442 (the -1 -> -0), Minus @ 442.
     */
    public function testIdleBackoffHoldsInitialDelayForFirstEmptyPull(): void
    {
        // Real code: exponent 0 => 10. Every listed mutant escalates this to 20 or 40.
        self::assertSame(10, PullConsumerIterator::idleBackoffMs(1));
    }

    /**
     * Pins the ceiling clamp and the exponent cap. The last un-clamped step is n = 6
     * (min(6, max(0, 5)) = 5 => 10 << 5 = 320); at n = 7 the exponent caps at 6, 10 << 6 = 640, which
     * clamps to the 500ms ceiling and stays there. The mutant lowers the exponent cap to min(5, ...),
     * so n = 7 yields 10 << 5 = 320 instead of the clamped 500.
     *
     * Kills: DecrementInteger @ 442  (min(6, ...) -> min(5, ...)).
     */
    public function testIdleBackoffClampsAtCeiling(): void
    {
        // Last pre-clamp step.
        self::assertSame(320, PullConsumerIterator::idleBackoffMs(6));
        // Real code: exponent caps at 6 (10 << 6 = 640) and clamps to 500; the min(5) mutant returns 320.
        self::assertSame(500, PullConsumerIterator::idleBackoffMs(7));
        self::assertSame(500, PullConsumerIterator::idleBackoffMs(8));
    }
}
