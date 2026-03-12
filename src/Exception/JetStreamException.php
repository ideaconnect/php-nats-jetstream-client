<?php

declare(strict_types=1);

namespace IDCT\NATS\Exception;

/**
 * Thrown when JetStream API responses indicate an application-level failure.
 */
final class JetStreamException extends NatsException
{
}
