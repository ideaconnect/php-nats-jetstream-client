<?php

declare(strict_types=1);

namespace Idct\Nats\Services;

final class ServiceEndpoint
{
    /**
     * Represents one registered service endpoint.
     */
    public function __construct(
        public readonly string $name,
        public readonly string $subject,
        public readonly ?string $queueGroup,
        public int $requests = 0,
        public int $errors = 0,
    ) {
    }
}
