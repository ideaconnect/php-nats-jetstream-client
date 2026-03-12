<?php

declare(strict_types=1);

namespace IDCT\NATS\Exception;

/**
 * Thrown when operations exceed configured or explicit timeout limits.
 */
final class TimeoutException extends NatsException
{
}
