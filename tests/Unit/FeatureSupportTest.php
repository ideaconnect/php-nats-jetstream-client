<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\UnsupportedFeatureException;
use IDCT\NATS\JetStream\FeatureSupport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class FeatureSupportTest extends TestCase
{
    /**
     * Verifies the version registry returns the documented minimum versions (and null for unknowns).
     */
    public function testRequiredVersion(): void
    {
        self::assertSame('2.10', FeatureSupport::requiredVersion('filter_subjects'));
        self::assertSame('2.11', FeatureSupport::requiredVersion('allow_msg_ttl'));
        self::assertSame('2.12', FeatureSupport::requiredVersion('allow_atomic'));
        self::assertNull(FeatureSupport::requiredVersion('not_a_real_field'));
    }

    /**
     * Verifies an "unknown field" error for a version-gated feature maps to a typed exception that
     * carries the feature, required version, server version, and is catchable as a JetStreamException.
     */
    public function testUnsupportedFromApiErrorMapsKnownField(): void
    {
        $e = FeatureSupport::unsupportedFromApiError(
            'invalid JSON: json: unknown field "allow_atomic"',
            400,
            '2.10.5',
        );

        self::assertInstanceOf(UnsupportedFeatureException::class, $e);
        self::assertInstanceOf(JetStreamException::class, $e);
        self::assertSame('allow_atomic', $e->feature);
        self::assertSame('2.12', $e->requiredVersion);
        self::assertSame('2.10.5', $e->serverVersion);
        self::assertSame(400, $e->getCode());
        self::assertStringContainsString('requires NATS server 2.12+', $e->getMessage());
        self::assertStringContainsString('2.10.5', $e->getMessage());
    }

    /**
     * Verifies an unknown field that is not in the registry is left as an ordinary error (null).
     */
    public function testUnsupportedFromApiErrorIgnoresUnregisteredField(): void
    {
        self::assertNull(FeatureSupport::unsupportedFromApiError('unknown field "totally_made_up"', 400, '2.12.0'));
    }

    /**
     * Verifies a non-"unknown field" error is not treated as a feature gap.
     */
    public function testUnsupportedFromApiErrorIgnoresOtherErrors(): void
    {
        self::assertNull(FeatureSupport::unsupportedFromApiError('stream not found', 404, '2.12.0'));
    }

    /**
     * Verifies an unknown server version renders as "unknown" in the message.
     */
    public function testUnsupportedFromApiErrorWithUnknownServerVersion(): void
    {
        $e = FeatureSupport::unsupportedFromApiError('unknown field "allow_msg_ttl"', 400, null);

        self::assertInstanceOf(UnsupportedFeatureException::class, $e);
        self::assertNull($e->serverVersion);
        self::assertStringContainsString('reports unknown', $e->getMessage());
    }

    /**
     * Verifies the scheduler features that came after the first scheduler release report 2.14 (#267).
     */
    public function testRequiredVersionOfTheScheduleFeatures(): void
    {
        self::assertSame('2.12', FeatureSupport::requiredVersion('allow_msg_schedules'));

        foreach (['schedule_every', 'schedule_cron', 'schedule_source', 'schedule_time_zone', 'schedule_rollup'] as $feature) {
            self::assertSame('2.14', FeatureSupport::requiredVersion($feature), $feature);
        }
    }

    /**
     * @return iterable<string, array{string|null, string, bool}>
     */
    public static function serverVersions(): iterable
    {
        yield 'older minor' => ['2.12.15', '2.14', true];
        yield 'one minor below' => ['2.13.9', '2.14', true];
        yield 'same minor' => ['2.14.0', '2.14', false];
        yield 'same minor, later patch' => ['2.14.10', '2.14', false];
        yield 'same minor, pre-release' => ['2.14.0-beta.1', '2.14', false];
        yield 'newer minor' => ['2.15.3', '2.14', false];
        yield 'older major, higher minor' => ['1.99.0', '2.14', true];
        yield 'newer major, lower minor' => ['3.0.0', '2.14', false];
        yield 'v prefix, older' => ['v2.12.0', '2.14', true];
        yield 'v prefix, same' => ['v2.14.0', '2.14', false];
        yield 'major only, older' => ['2', '2.1', true];
        yield 'major only, same' => ['2', '2.0', false];
        yield 'requirement without minor' => ['1.9.0', '2', true];
        yield 'not starting with a number' => ['release-2.12.0', '2.14', false];
        yield 'custom build' => ['custom-build', '2.14', false];
        yield 'empty' => ['', '2.14', false];
        yield 'none' => [null, '2.14', false];
    }

    /**
     * Verifies the version comparison reads the numeric major.minor prefix, ignores pre-release tags and never
     * calls an unparseable or missing version older.
     */
    #[DataProvider('serverVersions')]
    public function testServerIsOlderThan(?string $serverVersion, string $required, bool $older): void
    {
        self::assertSame($older, FeatureSupport::serverIsOlderThan($serverVersion, $required));
    }

    /**
     * Verifies a requirement without a numeric prefix is a programming error, not a silent "not older".
     */
    public function testServerIsOlderThanRejectsAnUnparseableRequirement(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Required version "latest" has no numeric major.minor prefix');

        FeatureSupport::serverIsOlderThan('2.12.0', 'latest');
    }
}
