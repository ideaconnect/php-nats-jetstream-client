<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\JetStream\Configuration\StreamConfiguration;

final class StreamConfigurationMutationTest extends \PHPUnit\Framework\TestCase
{
    /**
     * subjects() must reindex the variadic args via array_values() so the emitted
     * payload always carries a sequential, list-shaped array. When an associative
     * array is splatted into the variadic parameter, PHP preserves the string keys
     * inside $subjects; only array_values() collapses them back to 0,1,...
     *
     * The mutant drops array_values() and stores the splatted array verbatim, so the
     * 'subjects' entry would keep the string keys instead of integer 0,1.
     */
    public function testSubjectsReindexesSplattedAssociativeKeysToAList(): void
    {
        // kills UnwrapArrayValues @ line 41
        $assoc = ['first' => 'orders.created', 'second' => 'orders.updated'];

        $array = StreamConfiguration::create('ORDERS')
            ->subjects(...$assoc)
            ->toArray();

        // Real code: array_values() reindexes -> exact list with integer keys 0,1.
        // assertSame is key- and order-sensitive, so the mutant (string keys
        // 'first','second') fails this assertion.
        self::assertSame(
            [0 => 'orders.created', 1 => 'orders.updated'],
            $array['subjects'],
        );
        self::assertSame([0, 1], array_keys($array['subjects']));
    }

    /**
     * sealed() defaults its $sealed parameter to true, so calling it with no argument
     * must record sealed = true in the config payload. The mutant flips the default to
     * false, which would record sealed = false for the same no-argument call.
     */
    public function testSealedDefaultsToTrueWhenCalledWithoutArgument(): void
    {
        // kills TrueValue @ line 160
        $array = StreamConfiguration::create('SEALED')
            ->sealed()
            ->toArray();

        // Real code: default true. Mutant: default false -> this assertion fails.
        self::assertTrue($array['sealed']);
    }
}
