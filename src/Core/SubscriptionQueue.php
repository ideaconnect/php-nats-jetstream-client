<?php

declare(strict_types=1);

namespace IDCT\NATS\Core;

use SplQueue;
use function Amp\delay;

/**
 * A queue-based subscription that collects messages for polling access.
 *
 * Provides `fetch()`, `next()`, and `fetchAll()` methods similar to
 * basis-company/nats.php queue interface.
 */
final class SubscriptionQueue
{
    /** @var SplQueue<NatsMessage> */
    private SplQueue $messages;
    private float $timeout = 0;

    /**
     * @param NatsClient $client Client used to drive processIncoming polling while waiting for queued deliveries.
     * @param int $sid Subscription ID backing this queue instance.
     */
    public function __construct(
        private readonly NatsClient $client,
        public readonly int $sid,
    ) {
        $this->messages = new SplQueue();
    }

    /**
     * Enqueues a message received from the subscription callback.
     *
     * @internal Called by the subscribe handler.
     */
    public function enqueue(NatsMessage $message): void
    {
        $this->messages->enqueue($message);
    }

    /**
     * Sets the timeout in seconds for blocking operations.
     *
     * @return $this
     */
    public function setTimeout(float $seconds): self
    {
        $this->timeout = $seconds;

        return $this;
    }

    /**
     * Returns the next buffered message without blocking, or null if none available.
     */
    public function fetch(): ?NatsMessage
    {
        if (!$this->messages->isEmpty()) {
            return $this->messages->dequeue();
        }

        // Try one processIncoming cycle to see if anything arrives.
        $this->client->processIncoming()->await();

        return $this->messages->count() > 0 ? $this->messages->dequeue() : null;
    }

    /**
     * Blocks until a message is available, up to configured timeout.
     *
     * @throws \IDCT\NATS\Exception\TimeoutException If timeout is exceeded without a message.
     */
    public function next(): ?NatsMessage
    {
        if (!$this->messages->isEmpty()) {
            return $this->messages->dequeue();
        }

        $deadline = $this->timeout > 0 ? microtime(true) + $this->timeout : PHP_FLOAT_MAX;

        do {
            $this->client->processIncoming()->await();

            if ($this->messages->count() > 0) {
                break;
            }

            // Nothing arrived — yield briefly to avoid a tight spin.
            delay(0.001);
        } while (microtime(true) < $deadline);

        return $this->messages->count() > 0 ? $this->messages->dequeue() : null;
    }

    /**
     * Fetches all available messages up to the given limit.
     *
     * Uses the configured timeout to wait for messages. Returns early
     * if no new messages arrive after a processIncoming cycle.
     *
     * @param int|null $limit Maximum messages to return (null = unlimited).
     * @return list<NatsMessage>
     */
    public function fetchAll(?int $limit = null): array
    {
        $collected = [];
        $deadline = $this->timeout > 0 ? microtime(true) + $this->timeout : PHP_FLOAT_MAX;

        while (($limit === null || count($collected) < $limit) && microtime(true) < $deadline) {
            // Drain already queued messages first.
            while (!$this->messages->isEmpty() && ($limit === null || count($collected) < $limit)) {
                $collected[] = $this->messages->dequeue();
            }

            if ($limit !== null && count($collected) >= $limit) {
                break;
            }

            $frames = $this->client->processIncoming()->await();
            if ($frames === 0 && $this->messages->isEmpty()) {
                break;
            }
        }

        // Final drain of any remaining buffered messages.
        while (!$this->messages->isEmpty() && ($limit === null || count($collected) < $limit)) {
            $collected[] = $this->messages->dequeue();
        }

        return $collected;
    }
}
