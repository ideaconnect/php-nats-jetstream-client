<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\JetStream\Schedule;

final class ScheduleMutationTest extends \PHPUnit\Framework\TestCase
{
    /**
     * cron() must trim surrounding whitespace before validating and returning the
     * expression. Without the trim, the returned string keeps the leading/trailing
     * spaces, and (because preg_split on a non-empty padded string still yields the
     * same field count after collapsing \s+) the function would return the padded
     * value verbatim.
     */
    public function testCronTrimsSurroundingWhitespace(): void
    {
        // kills UnwrapTrim @ line 68
        // Real code: trim -> exactly the 6-field expression. Mutant: returns padded string.
        self::assertSame('0 0 0 * * *', Schedule::cron('  0 0 0 * * *  '));
    }

    /**
     * predefined() must trim the alias before lowercasing/normalizing. With the trim
     * removed, a padded alias like " daily " normalizes to "@ daily " which fails the
     * regex and throws, instead of returning "@daily".
     */
    public function testPredefinedTrimsAliasBeforeNormalizing(): void
    {
        // kills UnwrapTrim @ line 87
        self::assertSame('@daily', Schedule::predefined('  daily  '));
    }

    /**
     * The anchoring caret in the validation regex requires the matched alias to start
     * at the very beginning of the normalized string. Removing "^" would let an alias
     * with junk before an embedded "@alias" pass. "x@daily" normalizes to "@x@daily":
     * the real (anchored) regex rejects it, the un-anchored mutant would accept it.
     */
    public function testPredefinedRejectsAliasWithEmbeddedAtPrefix(): void
    {
        // kills PregMatchRemoveCaret @ line 89
        $this->expectException(\InvalidArgumentException::class);

        Schedule::predefined('x@daily');
    }

    /**
     * The trailing dollar in the validation regex requires the alias to end the string.
     * Removing "$" would let trailing junk pass. "dailyx" normalizes to "@dailyx":
     * the real (end-anchored) regex rejects it, the mutant (prefix match) would accept it.
     */
    public function testPredefinedRejectsAliasWithTrailingJunk(): void
    {
        // kills PregMatchRemoveDollar @ line 89
        $this->expectException(\InvalidArgumentException::class);

        Schedule::predefined('dailyx');
    }

    /**
     * The exception message for an unknown alias must be exactly
     * 'Unknown schedule alias "<alias>": expected ...'. Every Concat / ConcatOperandRemoval
     * mutation on line 91 reorders or drops one of the three concatenation operands,
     * producing a different message. Asserting the exact full message kills all of them.
     */
    public function testUnknownAliasExceptionMessageIsExact(): void
    {
        // kills Concat (x2) and ConcatOperandRemoval (x3) @ line 91
        try {
            Schedule::predefined('fortnightly');
            self::fail('Expected InvalidArgumentException for unknown alias');
        } catch (\InvalidArgumentException $e) {
            self::assertSame(
                'Unknown schedule alias "fortnightly": expected hourly, daily, weekly, monthly, yearly, annually, or midnight',
                $e->getMessage(),
            );
        }
    }
}
