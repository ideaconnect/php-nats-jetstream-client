<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\MessageTtl;
use PHPUnit\Framework\TestCase;

/**
 * Targets Infection survivors in src/JetStream/MessageTtl.php. Each test pins the exact
 * observable behavior a mutation would change (boundary, regex anchor, cast).
 */
final class MessageTtlMutationTest extends TestCase
{
    /**
     * The integer boundary is exactly 1: one second is the smallest accepted TTL.
     *
     * Real `$ttl < 1` accepts 1 and returns "1s"; the `<=` mutant would reject 1.
     */
    public function testAcceptsIntegerOneSecondAtBoundary(): void
    {
        // kills LessThan @ line 24
        self::assertSame('1s', MessageTtl::format(1));
    }

    /**
     * A bare-integer string of exactly "1" is the smallest accepted seconds value.
     *
     * Real `(int) $ttl < 1` accepts "1" and returns "1s"; the `<=` mutant would reject "1".
     */
    public function testAcceptsIntegerStringOneSecondAtBoundary(): void
    {
        // kills LessThan @ line 43
        self::assertSame('1s', MessageTtl::format('1'));
    }

    /**
     * The bare-integer regex is anchored at the start (`^`): a string that merely ends in
     * digits is NOT a bare integer and must be passed through to the server unchanged.
     *
     * Real `/^\d+$/` does not match "abc5", so "abc5" falls through and is returned verbatim.
     * The caret-removed mutant `/\d+$/` matches the trailing "5", then `(int) "abc5" === 0`
     * is `< 1`, so it would throw instead of returning the value.
     */
    public function testNonNumericPrefixWithTrailingDigitsPassesThrough(): void
    {
        // kills PregMatchRemoveCaret @ line 42
        self::assertSame('abc5', MessageTtl::format('abc5'));
    }

    /**
     * The zero-duration regex is anchored at the end (`$`): only a string that is ENTIRELY a
     * zero-valued duration is rejected. A string that merely starts with a zero duration but
     * has trailing junk is not a recognized zero and is passed through to the server.
     *
     * Real `/^0+...$/` does not match "0sX" (trailing "X"), so "0sX" is returned verbatim.
     * The dollar-removed mutant `/^0+.../` matches the "0s" prefix and would throw.
     */
    public function testZeroDurationWithTrailingJunkPassesThrough(): void
    {
        // kills PregMatchRemoveDollar @ line 56
        self::assertSame('0sX', MessageTtl::format('0sX'));
    }

    /**
     * Sanity guard for the line-56 anchor: a fully-zero duration string IS still rejected,
     * so the passthrough above is genuinely about the trailing-junk distinction, not a
     * regex that stopped matching zeros altogether.
     */
    public function testFullyZeroDurationStillRejected(): void
    {
        // supports PregMatchRemoveDollar @ line 56 (the real regex still rejects pure zeros)
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Per-message TTL must be at least 1 second');

        MessageTtl::format('0s');
    }
}
