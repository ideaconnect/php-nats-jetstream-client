<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Psr\Log\AbstractLogger;

/**
 * A PSR-3 logger that throws while logging any message containing a needle - a broken user logger.
 * Everything else is discarded.
 */
final class ThrowingLogger extends AbstractLogger
{
    public function __construct(private readonly string $needle) {}

    /** @param array<mixed> $context */
    public function log($level, string|\Stringable $message, array $context = []): void
    {
        if (str_contains((string) $message, $this->needle)) {
            throw new \RuntimeException('logger failed');
        }
    }
}
