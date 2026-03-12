<?php

declare(strict_types=1);

namespace IDCT\NATS\Services;

use IDCT\NATS\Core\NatsMessage;

/**
 * Contract for object-based service endpoint handlers.
 */
interface ServiceEndpointHandlerInterface
{
    /**
     * @return string|array<string,mixed>|null
     */
    public function handle(NatsMessage $message): string|array|null;
}
