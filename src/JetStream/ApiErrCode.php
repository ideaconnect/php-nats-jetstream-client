<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

/**
 * Stable JetStream API error numbers (the envelope's `err_code`, ADR-1) - the field error kinds are
 * discriminated on, since `code` carries only an HTTP-like class and `description` wording varies
 * between server versions. Mirrors nats.go `JSErrCode*`.
 */
final class ApiErrCode
{
    public const STREAM_NAME_IN_USE = 10058;
    public const STREAM_WRONG_LAST_SEQUENCE = 10071;

    /**
     * Extracts `err_code` from a decoded error envelope, or null when absent (a server predating the
     * field). A zero is treated as absent: real servers never emit `err_code: 0` (Go omitempty), so
     * it can only come from a malformed proxy - taking it as authoritative would disable the
     * description fallback and silently break error discrimination.
     *
     * @param array<string,mixed> $error
     */
    public static function fromEnvelope(array $error): ?int
    {
        $code = isset($error['err_code']) ? (int) $error['err_code'] : 0;

        return $code !== 0 ? $code : null;
    }
}
