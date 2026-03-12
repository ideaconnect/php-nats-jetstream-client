<?php

declare(strict_types=1);

namespace IDCT\NATS\Services;

use IDCT\NATS\Core\NatsMessage;

/**
 * Contract for validating service requests against endpoint schemas.
 */
interface ServiceSchemaValidatorInterface
{
    /**
     * Validates a request payload against endpoint schema definition.
     *
     * Returns null when valid, otherwise an error message describing
     * the validation failure.
     *
     * @param array<string,mixed> $schema
     */
    public function validate(NatsMessage $message, array $schema): ?string;
}
