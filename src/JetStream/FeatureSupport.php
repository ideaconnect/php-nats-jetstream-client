<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

use IDCT\NATS\Exception\UnsupportedFeatureException;

/**
 * Maps version-gated JetStream features (stream/consumer config fields) to the minimum NATS server
 * version that provides them, and turns a server "unknown field" rejection into a clear
 * {@see UnsupportedFeatureException}.
 *
 * This is reactive: the library does not probe the server version before each request. Instead, when a
 * request fails, the central API error path consults this map so the caller gets an actionable message
 * ("feature X requires NATS Y") instead of an opaque `unknown field "X"`.
 */
final class FeatureSupport
{
    /**
     * Minimum NATS server version per version-gated config field / feature key. A modern server accepts
     * the field; an older one rejects an unknown stream/consumer config field with `unknown field "X"`.
     *
     * The `schedule_*` keys are not config fields but scheduler features of `publishScheduled()` (ADR-51)
     * that came after the first scheduler release: nats-server 2.12 accepts only `@at` schedules, and the
     * recurring patterns and the source, time-zone and rollup headers arrived in 2.14.0 (#267).
     *
     * @var array<string,string>
     */
    public const REQUIREMENTS = [
        'filter_subjects' => '2.10',
        'allow_msg_ttl' => '2.11',
        'subject_delete_marker_ttl' => '2.11',
        'priority_groups' => '2.11',
        'priority_policy' => '2.11',
        'priority_timeout' => '2.11',
        'allow_msg_schedules' => '2.12',
        'allow_atomic' => '2.12',
        'allow_msg_counter' => '2.12',
        // An "@every <duration>" schedule.
        'schedule_every' => '2.14',
        // A 6-field cron schedule or a predefined alias (@hourly, @daily, ...).
        'schedule_cron' => '2.14',
        // The Nats-Schedule-Source header.
        'schedule_source' => '2.14',
        // The Nats-Schedule-Time-Zone header (cron schedules only).
        'schedule_time_zone' => '2.14',
        // The Nats-Schedule-Rollup header.
        'schedule_rollup' => '2.14',
    ];

    /**
     * Returns the minimum NATS server version for a feature/config field, or null if not version-gated.
     */
    public static function requiredVersion(string $feature): ?string
    {
        return self::REQUIREMENTS[$feature] ?? null;
    }

    /**
     * Whether an INFO-advertised server version is known to be older than $required (a "major.minor"
     * such as "2.14"). Only the numeric major.minor prefix counts: pre-release tags are ignored (a
     * 2.14.0-beta server counts as 2.14), matching the {@see BatchPublisher} comparison. A missing or
     * unparseable version (proxies, custom builds) is never "older": the caller then leaves the
     * decision to the server.
     *
     * @throws \InvalidArgumentException When $required has no numeric major.minor prefix.
     */
    public static function serverIsOlderThan(?string $serverVersion, string $required): bool
    {
        $minimum = self::majorMinor($required)
            ?? throw new \InvalidArgumentException('Required version "' . $required . '" has no numeric major.minor prefix');
        $server = self::majorMinor($serverVersion);
        if ($server === null) {
            return false;
        }

        return $server[0] < $minimum[0] || ($server[0] === $minimum[0] && $server[1] < $minimum[1]);
    }

    /**
     * Numeric [major, minor] prefix of a version string, or null when it has no leading number.
     *
     * @return array{int,int}|null
     */
    private static function majorMinor(?string $version): ?array
    {
        if ($version === null || preg_match('/^v?(\d+)(?:\.(\d+))?/', $version, $match) !== 1) {
            return null;
        }

        return [(int) $match[1], (int) ($match[2] ?? 0)];
    }

    /**
     * If a JetStream API error reports an unknown config field that maps to a known version-gated
     * feature, returns an {@see UnsupportedFeatureException} to throw; otherwise null (so the caller
     * raises the ordinary error).
     */
    public static function unsupportedFromApiError(string $description, int $code, ?string $serverVersion, ?int $errCode = null): ?UnsupportedFeatureException
    {
        // A strict-JSON server rejects an unrecognized stream/consumer config field as: unknown field "X"
        if (preg_match('/unknown field ["\']([^"\']+)["\']/i', $description, $matches) !== 1) {
            return null;
        }

        $field = $matches[1];
        $required = self::requiredVersion($field);
        if ($required === null) {
            return null;
        }

        $reported = ($serverVersion !== null && $serverVersion !== '') ? $serverVersion : 'unknown';

        return new UnsupportedFeatureException(
            $field,
            $required,
            $serverVersion,
            sprintf(
                'The "%s" feature requires NATS server %s+, but the connected server reports %s. (%s)',
                $field,
                $required,
                $reported,
                $description,
            ),
            $code,
            errCode: $errCode,
        );
    }
}
