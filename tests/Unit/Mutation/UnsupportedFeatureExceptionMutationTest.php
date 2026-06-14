<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Exception\UnsupportedFeatureException;

/**
 * Kills the surviving DecrementInteger / IncrementInteger mutants on the default value of the
 * $code constructor parameter (src/Exception/UnsupportedFeatureException.php:28).
 */
final class UnsupportedFeatureExceptionMutationTest extends \PHPUnit\Framework\TestCase
{
    public function testDefaultExceptionCodeIsZeroWhenCodeArgOmitted(): void
    {
        // Construct WITHOUT passing $code so the default value on line 28 is exercised.
        $exception = new UnsupportedFeatureException(
            feature: 'allow_atomic',
            requiredVersion: '2.12',
            serverVersion: '2.10',
            message: 'feature unavailable',
        );

        // kills DecrementInteger @ line 28 (default -1) and IncrementInteger @ line 28 (default 1):
        // the real default is exactly 0, which Throwable::getCode() must report verbatim.
        self::assertSame(0, $exception->getCode());

        // Pin the surrounding state too, so the assertion is unambiguously about the default code.
        self::assertSame('allow_atomic', $exception->feature);
        self::assertSame('2.12', $exception->requiredVersion);
        self::assertSame('2.10', $exception->serverVersion);
        self::assertSame('feature unavailable', $exception->getMessage());
    }

    public function testExplicitNonDefaultCodeStillFlowsThrough(): void
    {
        // Guards that getCode() faithfully reflects the constructor argument, so the default-value
        // test above is a real boundary at 0 rather than a hard-coded return.
        $exception = new UnsupportedFeatureException(
            feature: 'allow_atomic',
            requiredVersion: '2.12',
            serverVersion: null,
            message: 'feature unavailable',
            code: 42,
        );

        self::assertSame(42, $exception->getCode());
    }
}
