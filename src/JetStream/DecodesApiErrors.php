<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

use IDCT\NATS\Exception\JetStreamException;

/**
 * Shared decode-and-throw for the JetStream `error` envelope carried by an API/publish response.
 * KeyValueBucket and ObjectStoreBucket both parse this envelope by hand at several call sites (the
 * default description had already drifted between "JetStream API error" and "JetStream publish
 * error"); this trait gives them one implementation with a caller-chosen default description.
 */
trait DecodesApiErrors
{
    /**
     * If $data carries a JetStream `error` envelope, throws the matching {@see JetStreamException}
     * (description, numeric code, and typed {@see ApiErrCode}); otherwise returns. $defaultDescription
     * fills in when the envelope omits `description`.
     *
     * @param array<string,mixed> $data
     */
    private function throwIfApiError(array $data, string $defaultDescription = 'JetStream API error'): void
    {
        /** @var array<string,mixed>|null $error */
        $error = is_array($data['error'] ?? null) ? $data['error'] : null;
        if ($error === null) {
            return;
        }

        throw new JetStreamException(
            (string) ($error['description'] ?? $defaultDescription),
            (int) ($error['code'] ?? 0),
            null,
            ApiErrCode::fromEnvelope($error),
        );
    }
}
