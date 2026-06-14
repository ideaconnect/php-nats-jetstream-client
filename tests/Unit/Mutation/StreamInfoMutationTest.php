<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\JetStream\Models\StreamInfo;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing unit tests for {@see StreamInfo::fromArray()}.
 *
 * Line 37 builds the subjects list:
 *   $subjects = array_values(array_filter(
 *       is_array($config['subjects'] ?? null) ? $config['subjects'] : [],
 *       static fn(mixed $value): bool => is_string($value),
 *   ));
 *
 *   - UnwrapArrayFilter drops the array_filter(is_string) guard, so non-string entries
 *     would survive into $subjects.
 *   - UnwrapArrayValues drops the array_values() re-indexing, so the surviving keys would
 *     not be normalised to a 0-based sequential list.
 *
 * Source must NOT be modified. Tests are pure construction + value assertions: deterministic, no I/O.
 */
final class StreamInfoMutationTest extends TestCase
{
    /**
     * A subjects array mixing strings with non-string entries (int, bool, null, nested array)
     * must keep ONLY the string entries.
     *
     * The UnwrapArrayFilter mutant removes the is_string() filter, so every entry would survive
     * and $subjects would contain the non-string values, changing both contents and count.
     */
    public function testNonStringSubjectsAreFilteredOut(): void
    {
        // kills UnwrapArrayFilter @ line 37
        $info = StreamInfo::fromArray([
            'config' => [
                'name' => 'orders',
                'subjects' => ['orders.created', 42, 'orders.updated', false, null, ['nested']],
            ],
        ]);

        self::assertSame(['orders.created', 'orders.updated'], $info->subjects);
    }

    /**
     * When a non-string entry sits BETWEEN two string entries, filtering leaves a gap in the
     * keys (0 => 'a', 2 => 'b'). array_values() must re-index these to a contiguous 0-based list.
     *
     * The UnwrapArrayValues mutant removes array_values(), so the result would keep keys [0, 2]
     * and would NOT be a list. We assert the array is a proper list AND has sequential keys.
     */
    public function testSurvivingSubjectsAreReindexedToAList(): void
    {
        // kills UnwrapArrayValues @ line 37
        $info = StreamInfo::fromArray([
            'config' => [
                'name' => 'orders',
                // index 1 (the int) is dropped by the filter, leaving keys 0 and 2.
                'subjects' => ['orders.a', 999, 'orders.b'],
            ],
        ]);

        self::assertSame([0, 1], array_keys($info->subjects));
        // array_is_list() on $info->subjects is pre-narrowed to true by PHPStan (the property
        // is declared list<string>), so assert list-ness on a value PHPStan treats as dynamic.
        // Without array_values() the surviving keys would be [0, 2] and this would be false,
        // so the UnwrapArrayValues mutant stays killed.
        $subjects = json_decode((string) json_encode($info->subjects), true);
        self::assertTrue(array_is_list($subjects));
        self::assertSame(['orders.a', 'orders.b'], $info->subjects);
    }
}
