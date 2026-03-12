<?php

declare(strict_types=1);

namespace IDCT\NATS\Services;

/**
 * Runtime state container for a registered service endpoint.
 */
final class ServiceEndpoint
{
    /**
     * Represents one registered service endpoint.
     *
     * @param string $name Endpoint logical name for INFO/STATS payloads.
     * @param string $subject NATS subject subscribed by this endpoint.
     * @param ?string $queueGroup Optional queue group for shared subscription dispatch.
     * @param array<string,mixed>|null $schema Optional request schema for validator hooks.
     * @param int $requests Runtime counter of handled requests.
     * @param int $errors Runtime counter of handler/validation errors.
     * @param ?string $lastError Most recent endpoint error message.
     * @param int $processingTimeNs Accumulated processing time in nanoseconds.
     */
    public function __construct(
        public readonly string $name,
        public readonly string $subject,
        public readonly ?string $queueGroup,
        /** @var array<string,mixed>|null */
        public readonly ?array $schema = null,
        public int $requests = 0,
        public int $errors = 0,
        public ?string $lastError = null,
        public int $processingTimeNs = 0,
    ) {
    }
}
