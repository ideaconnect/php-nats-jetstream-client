<?php

declare(strict_types=1);

namespace IDCT\NATS\Exception;

use RuntimeException;

/**
 * Base exception type for all library-level NATS errors.
 */
class NatsException extends RuntimeException
{
}
