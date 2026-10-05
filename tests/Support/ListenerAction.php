<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\NatsConnection;

/**
 * A connection listener that records every event through a {@see LifecycleRecorder} and, the first time a given
 * event is announced, runs an action with the connection - what an application does from its Reconnected
 * listener, say. It records how long the action took, what it returned and what it threw.
 *
 * Events announced before {@see $connection} is set are only recorded.
 */
final class ListenerAction
{
    public ?NatsConnection $connection = null;

    /** Seconds the action took, once it has returned or thrown. */
    public ?float $seconds = null;

    public mixed $result = null;

    public ?\Throwable $failure = null;

    private bool $armed = true;

    /**
     * @param \Closure(NatsConnection): mixed $action
     */
    public function __construct(
        public readonly LifecycleRecorder $recorder,
        private readonly \Closure $action,
        private readonly ConnectionEvent $trigger = ConnectionEvent::Reconnected,
    ) {}

    /** @return \Closure(ConnectionEvent, ?\Throwable): void */
    public function listener(): \Closure
    {
        $record = $this->recorder->connectionListener();

        return function (ConnectionEvent $event, ?\Throwable $error = null) use ($record): void {
            $record($event, $error);
            $connection = $this->connection;
            if ($event !== $this->trigger || !$this->armed || $connection === null) {
                return;
            }

            $this->armed = false;
            $start = hrtime(true);
            try {
                $this->result = ($this->action)($connection);
            } catch (\Throwable $e) {
                $this->failure = $e;
            }

            $this->seconds = (hrtime(true) - $start) / 1e9;
        };
    }
}
