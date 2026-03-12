<?php

declare(strict_types=1);

namespace IDCT\NATS\Services;

final class ServiceEndpoint
{
    /**
     * Represents one registered service endpoint.
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
