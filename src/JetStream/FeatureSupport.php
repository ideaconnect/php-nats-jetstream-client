<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

use IDCT\NATS\Exception\UnsupportedFeatureException;

/**
 * Maps version-gated JetStream features (stream/consumer config fields) to the minimum NATS server
 * version that provides them, and turns a server that cannot honor such a field into a clear
 * {@see UnsupportedFeatureException}.
 *
 * A server that predates a field does not, by default, reject it: nats-server rejects unknown JSON
 * fields only in strict mode, which 2.10 does not have, 2.11 has only with `jetstream { strict: true }`,
 * and 2.12 turns on by default. A default 2.10 or 2.11 server drops the field and creates or updates
 * the stream or consumer without the feature (#266). The stream and consumer create/update calls of
 * {@see JetStreamContext} therefore judge the server's reply: {@see assertHonored()} checks that the
 * config the server returned still carries each requested field, and fails naming the field the server
 * dropped, or saying the reply carries no config to confirm it. A strict server's own
 * `unknown field "X"` rejection is mapped as well ({@see unsupportedFromApiError()}).
 *
 * Nothing is refused before a request because of the server version (ADR-44, #272): a JetStream request
 * is answered by the meta leader or by the server hosting the asset, and the server the client is
 * connected to, whose version INFO reports, may be neither. {@see REQUIREMENTS} and
 * {@see requiredVersion()} stay as documentation of what each feature needs.
 */
final class FeatureSupport
{
    /**
     * Minimum NATS server version per version-gated feature key: the stream/consumer config fields
     * ({@see CONFIG_FIELDS}), then the scheduler features of `publishScheduled()`
     * ({@see SCHEDULE_FEATURES}), then the JetStream API calls an older server does not answer
     * ({@see API_FEATURES}); neither of the last two is a config field. {@see requiredVersion()} answers
     * for all of them; only the config fields are read from a config ({@see requestedFields()}) or mapped
     * from a strict server's `unknown field "X"` rejection ({@see unsupportedFromApiError()}).
     *
     * @var array<string,string>
     */
    public const REQUIREMENTS = self::CONFIG_FIELDS + self::SCHEDULE_FEATURES + self::API_FEATURES;

    /**
     * Minimum NATS server version per version-gated stream/consumer config field. An older server does
     * not know the field: by default it ignores it, and only in strict mode does it reject it with
     * `unknown field "X"`.
     *
     * @var array<string,string>
     */
    private const CONFIG_FIELDS = [
        'filter_subjects' => '2.10',
        'allow_msg_ttl' => '2.11',
        'subject_delete_marker_ttl' => '2.11',
        'priority_groups' => '2.11',
        'priority_policy' => '2.11',
        'priority_timeout' => '2.11',
        'allow_msg_schedules' => '2.12',
        'allow_atomic' => '2.12',
        'allow_msg_counter' => '2.12',
    ];

    /**
     * Minimum NATS server version per scheduler feature of `publishScheduled()` (ADR-51) that came after
     * the first scheduler release: nats-server 2.12 accepts only `@at` schedules, and the recurring
     * patterns and the source, time-zone and rollup headers arrived in 2.14.0 (#267). These keys are
     * not config fields: no stream or consumer config carries them.
     *
     * @var array<string,string>
     */
    private const SCHEDULE_FEATURES = [
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
     * Minimum NATS server version per JetStream API call that came after the API's first release. An
     * older server does not reject such a request: nothing answers it, and the call times out. These
     * keys are not config fields either.
     *
     * @var array<string,string>
     */
    private const API_FEATURES = [
        // $JS.API.CONSUMER.RESET (ADR-60), JetStreamContext::resetConsumer().
        'consumer_reset' => '2.14',
    ];

    /**
     * Numeric fields whose negative value is not "off" but invalid, so it asks for the feature: a server
     * that knows the field rejects it (2.11.17: `subject delete marker TTL must not be negative`), and
     * one that does not would drop it without a word. A negative `priority_timeout` is not listed: a
     * 2.12+ server treats it as unset (0, or the two-minute default for a pinned client), so it asks
     * for nothing, like 0.
     *
     * @var array<string,true>
     */
    private const NEGATIVE_IS_A_REQUEST = [
        'subject_delete_marker_ttl' => true,
    ];

    /**
     * Returns the minimum NATS server version for a feature/config field, or null if not version-gated.
     */
    public static function requiredVersion(string $feature): ?string
    {
        return self::REQUIREMENTS[$feature] ?? null;
    }

    /**
     * The version-gated fields a stream or consumer config asks for, in {@see REQUIREMENTS} order: each
     * config field ({@see CONFIG_FIELDS}) the config sets to something other than its default. The
     * `schedule_*` scheduler features are not config fields, so a config key of that name asks for
     * nothing. `null`, `false`, `0`, `''`, `'none'` (the default priority policy) and an empty array
     * ask for nothing, since a server without the field behaves as one that has it at that value. A
     * negative number asks for nothing either, except for a field where it is invalid
     * ({@see NEGATIVE_IS_A_REQUEST}): a negative `subject_delete_marker_ttl` is not the default, and a
     * server without the field would drop it without a word.
     *
     * @param array<string,mixed> $config
     * @return list<string>
     */
    public static function requestedFields(array $config): array
    {
        $requested = [];
        foreach (array_keys(self::CONFIG_FIELDS) as $field) {
            if (array_key_exists($field, $config) && self::asksForFeature($field, $config[$field])) {
                $requested[] = $field;
            }
        }

        return $requested;
    }

    /**
     * Reply check of a stream or consumer create/update (#266): throws an {@see UnsupportedFeatureException}
     * for the first field of $requested that asks for a feature the server's reply does not confirm. The
     * request has been sent by then, so the message says only what the reply shows about $target:
     *
     * - the returned config lacks the field, or carries it at its default: the server ignored the field
     *   and $target exists without the feature. That does not make it new: a create returns an existing
     *   stream or consumer whose config matches the request, and dropping the field can make it match;
     * - the reply carries no config object (missing, null, empty or a list): nothing confirms the
     *   feature, and the state of $target is unknown.
     *
     * A request that asks for no version-gated feature is not checked.
     *
     * @internal For the stream and consumer create/update calls of {@see JetStreamContext} (#266); not part of
     *           the supported API.
     *
     * @param array<string,mixed> $requested The config that was sent.
     * @param mixed               $returned  The `config` object of the server's reply.
     * @param string              $target    What the request set up, e.g. `stream "ORDERS"`.
     */
    public static function assertHonored(array $requested, mixed $returned, ?string $serverVersion, string $target): void
    {
        $fields = self::requestedFields($requested);
        if ($fields === []) {
            return;
        }

        $reported = ($serverVersion !== null && $serverVersion !== '') ? $serverVersion : 'unknown';

        if (!is_array($returned) || array_is_list($returned)) {
            $field = $fields[0];
            $required = self::REQUIREMENTS[$field];

            throw new UnsupportedFeatureException(
                $field,
                $required,
                $serverVersion,
                sprintf(
                    'The "%s" feature requires NATS server %s+, and the server\'s reply (connected server reports %s) carries no config to confirm it: the request was sent and the state of %s is unknown (inspect it before changing or deleting it)',
                    $field,
                    $required,
                    $reported,
                    $target,
                ),
            );
        }

        foreach ($fields as $field) {
            if (array_key_exists($field, $returned) && self::asksForFeature($field, $returned[$field])) {
                continue;
            }

            $required = self::REQUIREMENTS[$field];

            throw new UnsupportedFeatureException(
                $field,
                $required,
                $serverVersion,
                sprintf(
                    'The "%s" feature requires NATS server %s+, and the server (connected server reports %s) ignored the field: %s exists without it (it may have existed before this request: inspect it before changing or deleting it)',
                    $field,
                    $required,
                    $reported,
                    $target,
                ),
            );
        }
    }

    /**
     * Whether an INFO-advertised server version is known to be older than $required (a "major.minor"
     * such as "2.14"). Only the numeric major.minor prefix counts: pre-release tags are ignored (a
     * 2.14.0-beta server counts as 2.14). A missing or unparseable version (proxies, custom builds) is
     * never "older".
     *
     * The library itself no longer calls it (#272).
     *
     * @deprecated ADR-44 forbids refusing a feature, choosing a request or changing a configuration because of
     *             the server version a client is connected to: a JetStream request is answered by the meta leader
     *             or by the server hosting the asset, which may run another release. This will be removed in the
     *             next major release. Send the request and judge the server's reply; where a feature level must
     *             be checked, use the API level `$JS.API.INFO` reports ({@see Models\AccountInfo::$apiLevel}).
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
     * config field ({@see CONFIG_FIELDS}), returns an {@see UnsupportedFeatureException} to throw;
     * otherwise null (so the caller raises the ordinary error). A `schedule_*` key is not a config
     * field: a strict server rejects it in a config whatever its version, so it is not mapped.
     */
    public static function unsupportedFromApiError(string $description, int $code, ?string $serverVersion, ?int $errCode = null): ?UnsupportedFeatureException
    {
        // A strict-JSON server (2.12+ by default, 2.11 with `jetstream { strict: true }`) rejects an
        // unrecognized stream/consumer config field as: unknown field "X"
        if (preg_match('/unknown field ["\']([^"\']+)["\']/i', $description, $matches) !== 1) {
            return null;
        }

        $field = $matches[1];
        $required = self::CONFIG_FIELDS[$field] ?? null;
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

    /**
     * Whether a config value asks for its feature: anything but the field's default. A number asks when
     * it is positive, or negative for a field where a negative value is invalid
     * ({@see NEGATIVE_IS_A_REQUEST}).
     */
    private static function asksForFeature(string $field, mixed $value): bool
    {
        if (is_int($value) || is_float($value)) {
            return $value > 0 || ($value < 0 && isset(self::NEGATIVE_IS_A_REQUEST[$field]));
        }

        if (is_string($value)) {
            return $value !== '' && $value !== 'none';
        }

        if (is_array($value)) {
            return $value !== [];
        }

        return $value !== null && $value !== false;
    }
}
