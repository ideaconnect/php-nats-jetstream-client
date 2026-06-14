<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\JetStream\Models\AccountInfo;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing unit tests for {@see AccountInfo::fromArray()}.
 *
 * AccountInfo is a pure immutable value object whose factory maps four integer fields, each via
 * `(int) ($data[$key] ?? 0)`, plus the full raw payload. Every surviving Infection mutant in this
 * chunk targets those coalesce defaults:
 *   - IncrementInteger / DecrementInteger shift the absent-key default from 0 to 1 / -1.
 *   - Coalesce swaps the operands to `(0 ?? $data[$key])`, which always yields 0 and thus drops a
 *     present, non-zero value.
 *
 * Source must NOT be modified. Tests are pure construction + value assertions: deterministic, no I/O.
 */
final class AccountInfoMutationTest extends TestCase
{
    /**
     * With no keys present, every mapped field must default to exactly 0. This pins the
     * Increment/Decrement mutants on all four lines: each would shift its field's absent default to
     * 1 or -1, breaking one of these assertions.
     */
    public function testAllMissingFieldsDefaultToExactlyZero(): void
    {
        $info = AccountInfo::fromArray([]);

        // kills IncrementInteger + DecrementInteger @ line 38 (memory)
        self::assertSame(0, $info->memory);
        // kills IncrementInteger + DecrementInteger @ line 39 (storage)
        self::assertSame(0, $info->storage);
        // kills IncrementInteger + DecrementInteger @ line 40 (streams)
        self::assertSame(0, $info->streams);
        // kills IncrementInteger + DecrementInteger @ line 41 (consumers)
        self::assertSame(0, $info->consumers);
    }

    /**
     * 'streams' present with a non-zero value must be read from the payload, not replaced by the
     * default. The Coalesce mutant rewrites this to `(int) (0 ?? $data['streams'])`, which always
     * evaluates to 0 and would therefore return 0 here.
     */
    public function testStreamsReadsPresentNonZeroValue(): void
    {
        // kills Coalesce @ line 40 (streams)
        $info = AccountInfo::fromArray(['streams' => 7]);

        self::assertSame(7, $info->streams);
    }

    /**
     * 'consumers' present with a non-zero value must be read from the payload, not replaced by the
     * default. The Coalesce mutant rewrites this to `(int) (0 ?? $data['consumers'])`, which always
     * evaluates to 0 and would therefore return 0 here.
     */
    public function testConsumersReadsPresentNonZeroValue(): void
    {
        // kills Coalesce @ line 41 (consumers)
        $info = AccountInfo::fromArray(['consumers' => 3]);

        self::assertSame(3, $info->consumers);
    }

    /**
     * Belt-and-braces: each field also reads its own present non-zero value, proving the
     * memory/storage default branches are not silently always-applied and that fields are not
     * cross-wired. (Reinforces the Increment/Decrement coverage with non-zero payloads.)
     */
    public function testEachFieldReadsItsOwnPresentValue(): void
    {
        // kills IncrementInteger + DecrementInteger @ lines 38-41 with non-zero inputs
        $info = AccountInfo::fromArray([
            'memory' => 100,
            'storage' => 200,
            'streams' => 5,
            'consumers' => 9,
        ]);

        self::assertSame(100, $info->memory);
        self::assertSame(200, $info->storage);
        self::assertSame(5, $info->streams);
        self::assertSame(9, $info->consumers);
    }
}
