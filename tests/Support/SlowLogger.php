<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use IDCT\NATS\Connection\Enum\ConnectionEvent;
use Psr\Log\AbstractLogger;

use function Amp\delay;

/**
 * A PSR-3 logger that records what it logs and, the first time it logs a message containing a needle, runs a
 * hook and then takes its time - like a logger writing to a slow stream, which suspends the fiber that logs.
 */
final class SlowLogger extends AbstractLogger
{
    /** @var list<string> Every message, in order. */
    public array $messages = [];

    /** Whether it is taking its time right now. */
    public bool $suspended = false;

    private bool $armed = true;

    /** @param (\Closure(): void)|null $hook */
    public function __construct(
        private readonly string $needle,
        private readonly float $seconds,
        private readonly ?\Closure $hook = null,
    ) {}

    /** @param array<mixed> $context */
    public function log($level, string|\Stringable $message, array $context = []): void
    {
        $message = (string) $message;
        $this->messages[] = $message;
        if (!$this->armed || !str_contains($message, $this->needle)) {
            return;
        }

        $this->armed = false;
        if ($this->hook !== null) {
            ($this->hook)();
        }

        $this->suspended = true;
        try {
            delay($this->seconds);
        } finally {
            $this->suspended = false;
        }
    }

    /**
     * The lifecycle events it logged ("NATS connection Reconnected", ...), in order.
     *
     * @return list<ConnectionEvent>
     */
    public function lifecycleEvents(): array
    {
        $events = [];
        foreach ($this->messages as $message) {
            foreach (ConnectionEvent::cases() as $event) {
                if ($message === 'NATS connection ' . $event->name) {
                    $events[] = $event;
                }
            }
        }

        return $events;
    }
}
