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
     * Returns the minimum NATS server version for a feature/config field, or null if not version-gated.
     */
    public static function requiredVersion(string $feature): ?string
    {
        return self::REQUIREMENTS[$feature] ?? null;
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
