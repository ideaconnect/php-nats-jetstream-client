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
        self::assertSame('2.14', FeatureSupport::requiredVersion('allow_batched'));
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

    /**
     * Verifies only config fields are read from a config: the schedule_* keys of REQUIREMENTS are
     * scheduler features of publishScheduled(), not config fields, so a config carrying them asks for
     * nothing, the pre-flight on a 2.10 server and the reply check let it through, and a strict
     * server's "unknown field" rejection of one is not mapped to a 2.14 requirement. Every config field
     * of REQUIREMENTS is still read (#266).
     */
    public function testScheduleFeaturesAreNotConfigFields(): void
    {
        $scheduleKeys = [
            'schedule_every' => true,
            'schedule_cron' => '0 0 * * * *',
            'schedule_source' => 'orders.latest',
            'schedule_time_zone' => 'Europe/Warsaw',
            'schedule_rollup' => 'sub',
        ];

        self::assertSame([], FeatureSupport::requestedFields($scheduleKeys));
        self::assertSame(['allow_msg_schedules'], FeatureSupport::requestedFields($scheduleKeys + ['allow_msg_schedules' => true]));
        self::assertSame(
            ['filter_subjects', 'allow_msg_ttl', 'subject_delete_marker_ttl', 'priority_groups', 'priority_policy', 'priority_timeout', 'allow_msg_schedules', 'allow_atomic', 'allow_msg_counter', 'allow_batched'],
            FeatureSupport::requestedFields(array_fill_keys(array_keys(FeatureSupport::REQUIREMENTS), true)),
        );

        FeatureSupport::assertSupportedBy($scheduleKeys, '2.10.29', 'stream');
        FeatureSupport::assertHonored($scheduleKeys, null, '2.10.29', 'stream "S"');
        self::assertNull(FeatureSupport::unsupportedFromApiError('invalid JSON: json: unknown field "schedule_every"', 400, '2.12.0'));

        // The registry still answers for them.
        self::assertSame('2.14', FeatureSupport::requiredVersion('schedule_every'));
        self::assertSame('2.14', FeatureSupport::REQUIREMENTS['schedule_rollup']);
        $this->addToAssertionCount(2);
    }

    /**
     * Verifies a gated field set to its default asks for nothing, and every other value asks for its
     * feature, listed in REQUIREMENTS order (#266).
     */
    public function testRequestedFieldsSkipDefaultValues(): void
    {
        self::assertSame([], FeatureSupport::requestedFields([
            'filter_subjects' => null,
            'allow_msg_ttl' => false,
            'subject_delete_marker_ttl' => 0,
            'priority_groups' => [],
            'priority_policy' => 'none',
            'priority_timeout' => -1,
            'allow_msg_schedules' => '',
            'allow_atomic' => 0.0,
            'max_msgs' => 10,
        ]));

        self::assertSame(
            ['filter_subjects', 'allow_msg_ttl', 'subject_delete_marker_ttl', 'priority_groups', 'priority_policy', 'priority_timeout', 'allow_atomic', 'allow_msg_counter'],
            FeatureSupport::requestedFields([
                'allow_msg_counter' => true,
                'priority_timeout' => 1,
                'allow_atomic' => 0.5,
                'priority_policy' => 'overflow',
                'priority_groups' => ['g1'],
                'subject_delete_marker_ttl' => 60_000_000_000,
                'allow_msg_ttl' => true,
                'filter_subjects' => ['a.>', 'b.>'],
                'allow_msg_schedules' => false,
            ]),
        );
    }

    /**
     * Verifies a negative subject_delete_marker_ttl asks for its feature: it is an invalid value, not
     * "markers off", and a server without the field would drop it without a word; 0 still asks for
     * nothing. A negative priority_timeout asks for nothing, like 0: 2.12.15 and 2.15.1 treat it as
     * unset (#266).
     */
    public function testNegativeMarkerTtlAsksForItsFeature(): void
    {
        self::assertSame(['subject_delete_marker_ttl'], FeatureSupport::requestedFields(['subject_delete_marker_ttl' => -1]));
        self::assertSame(['subject_delete_marker_ttl'], FeatureSupport::requestedFields(['subject_delete_marker_ttl' => -0.5]));
        self::assertSame([], FeatureSupport::requestedFields(['subject_delete_marker_ttl' => 0, 'priority_timeout' => -1]));
    }

    /**
     * @return iterable<string,array{array<string,mixed>,string,string,string}>
     */
    public static function tooOldServers(): iterable
    {
        yield 'allow_msg_ttl on 2.10' => [['allow_msg_ttl' => true], '2.10.29', 'allow_msg_ttl', '2.11'];
        yield 'subject_delete_marker_ttl on 2.10' => [['subject_delete_marker_ttl' => 60_000_000_000], '2.10.29', 'subject_delete_marker_ttl', '2.11'];
        yield 'negative subject_delete_marker_ttl on 2.10' => [['subject_delete_marker_ttl' => -1], '2.10.29', 'subject_delete_marker_ttl', '2.11'];
        yield 'allow_msg_counter on 2.11' => [['allow_msg_counter' => true], '2.11.17', 'allow_msg_counter', '2.12'];
        yield 'allow_msg_schedules on 2.11' => [['allow_msg_schedules' => true], '2.11.17', 'allow_msg_schedules', '2.12'];
        yield 'allow_atomic on 2.11' => [['allow_atomic' => true], '2.11.17', 'allow_atomic', '2.12'];
        yield 'filter_subjects on 2.9' => [['filter_subjects' => ['a.>', 'b.>']], '2.9.25', 'filter_subjects', '2.10'];
        yield 'priority_groups on 2.10' => [['priority_groups' => ['g1']], '2.10.29', 'priority_groups', '2.11'];
        yield 'filter_subjects on a 1.x server' => [['filter_subjects' => ['a.>']], '1.4.1', 'filter_subjects', '2.10'];
        yield 'two-segment version' => [['allow_atomic' => true], '2.11', 'allow_atomic', '2.12'];
        yield 'v-prefixed version' => [['allow_msg_ttl' => true], 'v2.10.0', 'allow_msg_ttl', '2.11'];
        yield 'second field too old' => [['allow_msg_ttl' => true, 'allow_msg_counter' => true], '2.11.5', 'allow_msg_counter', '2.12'];
    }

    /**
     * Verifies the pre-flight refuses a gated field the connected server's version is too old for,
     * naming the field, its minimum version and the server's version (#266).
     *
     * @param array<string,mixed> $config
     */
    #[DataProvider('tooOldServers')]
    public function testAssertSupportedByRefusesFieldTheServerIsTooOldFor(array $config, string $version, string $feature, string $required): void
    {
        try {
            FeatureSupport::assertSupportedBy($config, $version, 'stream');
            self::fail('Expected UnsupportedFeatureException');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame($feature, $e->feature);
            self::assertSame($required, $e->requiredVersion);
            self::assertSame($version, $e->serverVersion);
            self::assertSame(0, $e->getCode());
            self::assertSame(
                sprintf(
                    'The "%s" feature requires NATS server %s+, but the connected server reports %s (nothing was sent: an older server ignores the field and would set up the stream without it)',
                    $feature,
                    $required,
                    $version,
                ),
                $e->getMessage(),
            );
        }
    }

    /**
     * @return iterable<string,array{array<string,mixed>,?string}>
     */
    public static function serversThePreflightLetsThrough(): iterable
    {
        yield 'allow_msg_ttl on 2.11.0' => [['allow_msg_ttl' => true, 'subject_delete_marker_ttl' => 60_000_000_000], '2.11.0'];
        yield 'allow_atomic on a 2.12 pre-release' => [['allow_atomic' => true], '2.12.0-beta.1'];
        yield 'every field on 2.12' => [['filter_subjects' => ['a.>'], 'allow_msg_counter' => true, 'priority_groups' => ['g1']], 'v2.12.1'];
        yield 'a 3.0 server' => [['allow_msg_counter' => true], '3.0.0'];
        yield 'unparseable version' => [['allow_msg_counter' => true], 'synadia-custom'];
        // The version must start with its number: "release-1.5" is a custom build, not a 1.5 server.
        yield 'number not at the start' => [['allow_msg_ttl' => true], 'release-1.5'];
        yield 'empty version' => [['allow_msg_counter' => true], ''];
        yield 'no version' => [['allow_msg_counter' => true], null];
        yield 'defaults on 2.10' => [['allow_msg_ttl' => false, 'subject_delete_marker_ttl' => 0, 'allow_atomic' => false, 'priority_policy' => 'none'], '2.10.29'];
    }

    /**
     * Verifies the pre-flight passes a server new enough for every requested field, a version it
     * cannot parse (the reply check covers that case), and fields set to their defaults (#266).
     *
     * @param array<string,mixed> $config
     */
    #[DataProvider('serversThePreflightLetsThrough')]
    public function testAssertSupportedByPassesWhatItCannotRuleOut(array $config, ?string $version): void
    {
        FeatureSupport::assertSupportedBy($config, $version, 'stream');

        $this->addToAssertionCount(1);
    }

    /**
     * Verifies the reply check names the first requested field the returned config lacks or carries
     * at its default, says only what the reply shows (the target exists without the field, and may
     * have existed before the request), and reports the server's version (#266).
     */
    public function testAssertHonoredRefusesConfigThatLacksRequestedField(): void
    {
        try {
            FeatureSupport::assertHonored(
                ['allow_msg_ttl' => true, 'allow_msg_counter' => true],
                ['name' => 'C', 'allow_msg_ttl' => true],
                'synadia-custom',
                'stream "C"',
            );
            self::fail('Expected UnsupportedFeatureException');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame('allow_msg_counter', $e->feature);
            self::assertSame('2.12', $e->requiredVersion);
            self::assertSame('synadia-custom', $e->serverVersion);
            self::assertSame(0, $e->getCode());
            self::assertSame(
                'The "allow_msg_counter" feature requires NATS server 2.12+, and the server (connected server reports synadia-custom) ignored the field: stream "C" exists without it (it may have existed before this request: inspect it before changing or deleting it)',
                $e->getMessage(),
            );
        }

        try {
            // 2.11 encodes allow_msg_ttl without omitempty: a dropped request comes back as false.
            FeatureSupport::assertHonored(['allow_msg_ttl' => true], ['name' => 'T', 'allow_msg_ttl' => false], null, 'stream "T"');
            self::fail('Expected UnsupportedFeatureException');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame('allow_msg_ttl', $e->feature);
            self::assertNull($e->serverVersion);
            self::assertStringContainsString('(connected server reports unknown)', $e->getMessage());
            self::assertStringContainsString('stream "T" exists without it', $e->getMessage());
        }

        try {
            FeatureSupport::assertHonored(['priority_groups' => ['g1']], ['durable_name' => 'P'], '', 'consumer "P" on stream "S"');
            self::fail('Expected UnsupportedFeatureException');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame('priority_groups', $e->feature);
            self::assertStringContainsString('(connected server reports unknown)', $e->getMessage());
            self::assertStringContainsString('consumer "P" on stream "S" exists without it', $e->getMessage());
        }
    }

    /**
     * Verifies a negative subject_delete_marker_ttl cannot pass an old responder that dropped it: the
     * reply check refuses a config without it, as for any other request for the feature (#266).
     */
    public function testAssertHonoredRefusesDroppedNegativeMarkerTtl(): void
    {
        try {
            FeatureSupport::assertHonored(['subject_delete_marker_ttl' => -1], ['name' => 'M'], 'synadia-custom', 'stream "M"');
            self::fail('Expected UnsupportedFeatureException');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame('subject_delete_marker_ttl', $e->feature);
            self::assertSame('2.11', $e->requiredVersion);
            self::assertStringContainsString('stream "M" exists without it', $e->getMessage());
        }
    }

    /**
     * @return iterable<string,array{mixed}>
     */
    public static function repliesWithoutAConfig(): iterable
    {
        // The call sites pass `$response['config'] ?? null`, so a missing config arrives as null.
        yield 'missing or null' => [null];
        yield 'empty object' => [[]];
        yield 'a list' => [[true]];
        yield 'a string' => ['allow_msg_counter'];
        yield 'a number' => [1];
    }

    /**
     * Verifies a reply without a config object fails a request for a gated feature: nothing confirms
     * the feature, so the check says the request was sent and the target's state is unknown, instead
     * of treating less evidence as success (#266).
     */
    #[DataProvider('repliesWithoutAConfig')]
    public function testAssertHonoredRefusesReplyWithoutConfig(mixed $returned): void
    {
        try {
            FeatureSupport::assertHonored(['allow_msg_ttl' => false, 'allow_msg_counter' => true], $returned, 'synadia-custom', 'stream "C"');
            self::fail('Expected UnsupportedFeatureException');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame('allow_msg_counter', $e->feature);
            self::assertSame('2.12', $e->requiredVersion);
            self::assertSame('synadia-custom', $e->serverVersion);
            self::assertSame(0, $e->getCode());
            self::assertSame(
                'The "allow_msg_counter" feature requires NATS server 2.12+, and the server\'s reply (connected server reports synadia-custom) carries no config to confirm it: the request was sent and the state of stream "C" is unknown (inspect it before changing or deleting it)',
                $e->getMessage(),
            );
        }

        try {
            FeatureSupport::assertHonored(['priority_groups' => ['g1']], $returned, null, 'the consumer on stream "S"');
            self::fail('Expected UnsupportedFeatureException');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame('priority_groups', $e->feature);
            self::assertStringContainsString('(connected server reports unknown)', $e->getMessage());
            self::assertStringContainsString('the state of the consumer on stream "S" is unknown', $e->getMessage());
        }
    }

    /**
     * Verifies the reply check passes a config that echoes every requested field, and leaves a request
     * that asks for no gated feature alone, whatever the reply carries: among them a negative
     * priority_timeout, which 2.12.15 drops from an overflow consumer's config (#266).
     */
    public function testAssertHonoredPassesEchoedFieldsAndRequestsForNothing(): void
    {
        FeatureSupport::assertHonored(
            ['allow_msg_ttl' => true, 'subject_delete_marker_ttl' => 60_000_000_000, 'filter_subjects' => ['a.>', 'b.>']],
            ['filter_subjects' => ['a.>', 'b.>'], 'allow_msg_ttl' => true, 'subject_delete_marker_ttl' => 60_000_000_000],
            '2.12.0',
            'stream "S"',
        );
        FeatureSupport::assertHonored(
            ['priority_groups' => ['g1'], 'priority_policy' => 'overflow', 'priority_timeout' => -1],
            ['durable_name' => 'P', 'priority_groups' => ['g1'], 'priority_policy' => 'overflow'],
            '2.12.15',
            'consumer "P" on stream "S"',
        );
        FeatureSupport::assertHonored(['allow_msg_ttl' => false, 'max_msgs' => 5], [], '2.10.29', 'stream "S"');
        FeatureSupport::assertHonored(['subject_delete_marker_ttl' => 0, 'max_msgs' => 5], ['name' => 'S'], '2.10.29', 'stream "S"');
        FeatureSupport::assertHonored(['max_msgs' => 5], null, 'synadia-custom', 'stream "S"');

        $this->addToAssertionCount(5);
    }
}
