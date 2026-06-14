<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Exception\UnsupportedFeatureException;
use IDCT\NATS\JetStream\FeatureSupport;

final class FeatureSupportMutationTest extends \PHPUnit\Framework\TestCase
{
    /**
     * The "unknown field" phrase is matched case-insensitively (the `/i` flag). A server may
     * report it with different casing, e.g. "Unknown Field". With `/i` this matches and maps to
     * the typed exception; without `/i` (the mutation) the regex would not match and the method
     * would return null. The captured field name itself stays lowercase so it resolves to a
     * known version-gated requirement.
     */
    public function testMatchesUnknownFieldPhraseCaseInsensitively(): void
    {
        // kills PregMatchRemoveFlags @ line 54
        $e = FeatureSupport::unsupportedFromApiError(
            'invalid JSON: json: UNKNOWN FIELD "allow_atomic"',
            400,
            '2.10.5',
        );

        self::assertInstanceOf(UnsupportedFeatureException::class, $e);
        self::assertSame('allow_atomic', $e->feature);
        self::assertSame('2.12', $e->requiredVersion);

        // Mixed-case variant also exercises the difference between /i and no-flag.
        $e2 = FeatureSupport::unsupportedFromApiError(
            'Unknown Field "allow_msg_ttl"',
            400,
            '2.10.5',
        );

        self::assertInstanceOf(UnsupportedFeatureException::class, $e2);
        self::assertSame('allow_msg_ttl', $e2->feature);
        self::assertSame('2.11', $e2->requiredVersion);
    }
}
