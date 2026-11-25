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
 * {@see JetStreamContext} therefore check twice:
 *
 * - before the request, {@see assertSupportedBy()} compares the fields the config asks for with the
 *   connected server's INFO-advertised version, and fails before anything is sent;
 * - after it, {@see assertHonored()} checks that the config the server returned still carries each of
 *   those fields (an unparseable version, or a JetStream API answered by an older server than the
 *   connected one), and fails naming the field the server dropped, or saying the reply carries no
 *   config to confirm it.
 *
 * A strict server's own `unknown field "X"` rejection is mapped as well ({@see unsupportedFromApiError()}).
 */
final class FeatureSupport
{
    /**
     * Minimum NATS server version per version-gated config field / feature key. An older server does
     * not know the field: by default it ignores it, and only in strict mode does it reject it with
     * `unknown field "X"`.
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
     * field the config sets to something other than its default. `null`, `false`, `0`, `''`, `'none'`
     * (the default priority policy) and an empty array ask for nothing, since a server without the
     * field behaves as one that has it at that value. A negative number asks for nothing either,
     * except for a field where it is invalid ({@see NEGATIVE_IS_A_REQUEST}): a negative
     * `subject_delete_marker_ttl` is not the default, and a server without the field would drop it
     * without a word.
     *
     * @param array<string,mixed> $config
     * @return list<string>
     */
    public static function requestedFields(array $config): array
    {
        $requested = [];
        foreach (array_keys(self::REQUIREMENTS) as $field) {
            if (array_key_exists($field, $config) && self::asksForFeature($field, $config[$field])) {
                $requested[] = $field;
            }
        }

        return $requested;
    }

    /**
     * Pre-flight of a stream or consumer create/update (#266): throws an {@see UnsupportedFeatureException}
     * for the first field of $config that asks for a feature the connected server's version is too old
     * for, before anything is sent. The version is the INFO-advertised one, compared on its numeric
     * `major.minor` prefix as the atomic batch pre-flight does ({@see BatchPublisher::commit()}); in a
     * cluster it stands for the servers that will hold the stream. An unknown or unparseable version (a
     * proxy, a custom build) passes, and {@see assertHonored()} checks the reply instead.
     *
     * @param array<string,mixed> $config The config about to be sent.
     * @param string              $target What the request sets up, e.g. `stream` or `consumer`.
     */
    public static function assertSupportedBy(array $config, ?string $serverVersion, string $target): void
    {
        $version = self::versionComponents($serverVersion);
        if ($version === null) {
            return;
        }

        foreach (self::requestedFields($config) as $field) {
            $required = self::REQUIREMENTS[$field];
            if (!self::isOlderThan($version, $required)) {
                continue;
            }

            throw new UnsupportedFeatureException(
                $field,
                $required,
                $serverVersion,
                sprintf(
                    'The "%s" feature requires NATS server %s+, but the connected server reports %s (nothing was sent: an older server ignores the field and would set up the %s without it)',
                    $field,
                    $required,
                    $serverVersion,
                    $target,
                ),
            );
        }
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
     * If a JetStream API error reports an unknown config field that maps to a known version-gated
     * feature, returns an {@see UnsupportedFeatureException} to throw; otherwise null (so the caller
     * raises the ordinary error).
     */
    public static function unsupportedFromApiError(string $description, int $code, ?string $serverVersion, ?int $errCode = null): ?UnsupportedFeatureException
    {
        // A strict-JSON server (2.12+ by default, 2.11 with `jetstream { strict: true }`) rejects an
        // unrecognized stream/consumer config field as: unknown field "X"
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

    /**
     * Whether a parsed server version is below a `major.minor` requirement.
     *
     * @param array{int,int} $version
     */
    private static function isOlderThan(array $version, string $required): bool
    {
        [$major, $minor] = array_map('intval', explode('.', $required));

        return $version[0] < $major || ($version[0] === $major && $version[1] < $minor);
    }

    /**
     * Numeric [major, minor] prefix of an INFO-advertised server version, or null when the string has
     * no leading number (proxies and custom builds advertise arbitrary strings). Pre-release tags are
     * ignored, as in {@see BatchPublisher}.
     *
     * @return array{int,int}|null
     */
    private static function versionComponents(?string $version): ?array
    {
        if ($version === null || preg_match('/^v?(\d+)(?:\.(\d+))?/', $version, $match) !== 1) {
            return null;
        }

        return [(int) $match[1], (int) ($match[2] ?? 0)];
    }
}
