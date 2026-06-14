<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Core\Inbox;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing tests for Inbox::generate().
 *
 * The only logic is `return $prefix . '.' . bin2hex(random_bytes(12));`.
 * Infection flips the byte count 12 -> 11 (DecrementInteger) and 12 -> 13
 * (IncrementInteger). bin2hex emits exactly two hex chars per byte, so the
 * random suffix is 24 hex chars for the real code, 22 for the decrement and 26
 * for the increment. Pinning the exact suffix length therefore kills both.
 */
final class InboxMutationTest extends TestCase
{
    /**
     * The random suffix must be exactly 24 lowercase hex characters
     * (bin2hex of 12 random bytes).
     *
     * kills DecrementInteger @ line 17 (random_bytes(11) -> 22 hex chars)
     * kills IncrementInteger @ line 17 (random_bytes(13) -> 26 hex chars)
     */
    public function testRandomSuffixIsExactlyTwentyFourHexChars(): void
    {
        $inbox = Inbox::generate('_INBOX');

        // Structure: "_INBOX." + 24 hex chars.
        self::assertStringStartsWith('_INBOX.', $inbox);

        $suffix = substr($inbox, strlen('_INBOX.'));

        // Exact boundary: 24 chars, not 22 (decrement) and not 26 (increment).
        self::assertSame(24, strlen($suffix));
        self::assertMatchesRegularExpression('/^[0-9a-f]{24}$/', $suffix);
    }

    /**
     * The total generated subject length is deterministic for a given prefix:
     * strlen(prefix) + 1 (the dot) + 24 hex chars. Re-pins the suffix length
     * via the full string so the mutants cannot survive on any prefix.
     *
     * kills DecrementInteger @ line 17
     * kills IncrementInteger @ line 17
     */
    public function testTotalLengthIsPrefixPlusDotPlusTwentyFourHex(): void
    {
        $prefix = '_INBOX.JS.PUSH';
        $inbox = Inbox::generate($prefix);

        $expectedLength = strlen($prefix) + 1 + 24;

        self::assertSame($expectedLength, strlen($inbox));
        self::assertMatchesRegularExpression(
            '/^' . preg_quote($prefix, '/') . '\.[0-9a-f]{24}$/',
            $inbox,
        );
    }
}
