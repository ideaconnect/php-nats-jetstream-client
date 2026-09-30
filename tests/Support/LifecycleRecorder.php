<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use IDCT\NATS\Connection\Enum\ConnectionEvent;

/**
 * Records what a connection reports through its connection and error listeners.
 */
final class LifecycleRecorder
{
    /** @var list<ConnectionEvent> */
    public array $events = [];

    /** @var list<?string> The error each Closed event carried - its message, null for none - in order. */
    public array $closedErrors = [];

    /** @var list<string> Error messages, in the order they were reported. */
    public array $errors = [];

    /** @return \Closure(ConnectionEvent, ?\Throwable): void */
    public function connectionListener(): \Closure
    {
        return function (ConnectionEvent $event, ?\Throwable $error = null): void {
            $this->events[] = $event;
            if ($event === ConnectionEvent::Closed) {
                $this->closedErrors[] = $error?->getMessage();
            }
        };
    }

    /** @return \Closure(\Throwable): void */
    public function errorListener(): \Closure
    {
        return function (\Throwable $error): void {
            $this->errors[] = $error->getMessage();
        };
    }

    public function closedEvents(): int
    {
        return count(array_filter($this->events, static fn(ConnectionEvent $event): bool => $event === ConnectionEvent::Closed));
    }

    /** @return list<string> The reported errors containing $needle. */
    public function errorsContaining(string $needle): array
    {
        return array_values(array_filter($this->errors, static fn(string $error): bool => str_contains($error, $needle)));
    }
}
