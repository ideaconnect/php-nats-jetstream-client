<?php

declare(strict_types=1);

namespace IDCT\NATS\Exception;

final class TimeoutException extends NatsException
{
	/**
	 * Raised when time-bounded operations exceed their configured deadline.
	 */
}
