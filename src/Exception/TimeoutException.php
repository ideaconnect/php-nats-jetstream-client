<?php

declare(strict_types=1);

namespace Idct\Nats\Exception;

final class TimeoutException extends NatsException
{
	/**
	 * Raised when time-bounded operations exceed their configured deadline.
	 */
}
