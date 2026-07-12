<?php

declare(strict_types=1);

namespace IDCT\NATS\Core;

use Amp\CancelledException;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Exception\NatsException;
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
    /**
     * Bounds a single "non-blocking" / no-timeout poll so the underlying socket read cannot suspend
     * the calling fiber indefinitely on an idle subject. Small enough to be effectively immediate,
     * large enough not to race past bytes already in flight.
     */
    private const NON_BLOCKING_TIMEOUT = 0.001;

    /** @var SplQueue<NatsMessage> */
    private SplQueue $messages;
    private float $timeout = 0;

    /** Monotonic count of messages discarded by the slow-consumer policy; never resets. */
    private int $droppedCount = 0;

    /**
     * @param NatsClient $client Client used to drive processIncoming polling while waiting for queued deliveries.
     * @param int $sid Subscription ID backing this queue instance.
     */
    public function __construct(
        private readonly NatsClient $client,
        public readonly int $sid,
        private readonly int $maxPending = 1024,
        private readonly SlowConsumerPolicy $slowConsumerPolicy = SlowConsumerPolicy::DropOldest,
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
        // Bound the polling backlog with the same cap + slow-consumer policy as the connection's
        // callback queue. Without this the connection's per-chunk drain empties its (capped) queue
        // into this one unbounded, so a queue fed faster than it is polled would grow until OOM.
        $limit = max(1, $this->maxPending);

        if ($this->messages->count() >= $limit) {
            // A drop must never be silent (#134): this queue is where connection-level drains land
            // for polling consumers, so overflow here is the real slow-consumer signal. Count it and
            // route it through the client's error listener exactly like the connection layer does
            // for its own queue (same message shape, same debug level - drops are a per-message hot
            // path and must not flood error logs).
            if ($this->slowConsumerPolicy === SlowConsumerPolicy::DropOldest) {
                $this->messages->dequeue();
                $this->droppedCount++;
                $this->client->emitError(new NatsException('Slow consumer on sid ' . $this->sid . ': dropped oldest message'), 'debug');
            } elseif ($this->slowConsumerPolicy === SlowConsumerPolicy::DropNewest) {
                $this->droppedCount++;
                $this->client->emitError(new NatsException('Slow consumer on sid ' . $this->sid . ': dropped newest message'), 'debug');

                return;
            } else {
                // Error policy: the overflowing message is dropped and the loss is surfaced loudly.
                // The drop must still be observable and counted, exactly like DropOldest/DropNewest,
                // so droppedCount()/the error listener never miss it (#134/#159); the throw then
                // propagates the failure to the caller.
                $this->droppedCount++;
                $overflow = new NatsException('Subscription queue overflow for sid ' . $this->sid);
                $this->client->emitError($overflow, 'debug');

                throw $overflow;
            }
        }

        $this->messages->enqueue($message);
    }

    /**
     * Monotonic count of messages this queue has discarded under the slow-consumer policy
     * (DropOldest dequeues, DropNewest rejections, and Error-policy overflows before the throw).
     * Never resets for the lifetime of the queue, so applications can detect delivery gaps by
     * observing increases (#134/#159).
     */
    public function droppedCount(): int
    {
        return $this->droppedCount;
    }

    /**
     * Cancels the underlying subscription so this queue stops receiving messages.
     *
     * Convenience for `$client->unsubscribe($queue->sid)`.
     *
     * @return Future<void>
     */
    public function unsubscribe(): Future
    {
        return $this->client->unsubscribe($this->sid);
    }

    /**
     * Alias of {@see unsubscribe()}.
     *
     * @return Future<void>
     */
    public function close(): Future
    {
        return $this->client->unsubscribe($this->sid);
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
     * Monotonic clock in seconds (hrtime-based) for deadline math, immune to wall-clock jumps
     * (the underlying read is also bounded by a monotonic TimeoutCancellation) (#70).
     */
    private function monotonicSeconds(): float
    {
        return hrtime(true) / 1e9;
    }

    /**
     * Returns the next buffered message without blocking, or null if none available.
     */
    public function fetch(): ?NatsMessage
    {
        if (!$this->messages->isEmpty()) {
            return $this->messages->dequeue();
        }

        // Try one bounded processIncoming cycle so an idle socket read cannot park the caller.
        $cancellation = new TimeoutCancellation(self::NON_BLOCKING_TIMEOUT);

        try {
            $this->client->processIncoming($cancellation)->await();
        } catch (CancelledException) {
            // Nothing was immediately available; honor the non-blocking contract.
        }

        return $this->messages->count() > 0 ? $this->messages->dequeue() : null;
    }

    /**
     * Blocks until a message is available, up to the configured timeout.
     *
     * Returns the next message, or `null` when no message arrives within the configured timeout.
     * When no timeout is configured (`timeout <= 0`) a single `processIncoming()` cycle is
     * attempted, matching {@see fetch()} rather than blocking indefinitely.
     */
    public function next(): ?NatsMessage
    {
        if (!$this->messages->isEmpty()) {
            return $this->messages->dequeue();
        }

        if ($this->timeout <= 0) {
            // No timeout configured: a single bounded cycle, matching fetch() rather than blocking.
            $cancellation = new TimeoutCancellation(self::NON_BLOCKING_TIMEOUT);

            try {
                $this->client->processIncoming($cancellation)->await();
            } catch (CancelledException) {
                // Nothing was immediately available.
            }

            return $this->messages->count() > 0 ? $this->messages->dequeue() : null;
        }

        // Bound the wait with a cancellation so the underlying socket read cannot block past the
        // configured timeout, then yield cooperatively between cycles to avoid a tight spin.
        $cancellation = new TimeoutCancellation($this->timeout);
        $deadline = $this->monotonicSeconds() + $this->timeout;

        try {
            do {
                $read = $this->client->readIncoming($cancellation)->await();

                if ($this->messages->count() > 0) {
                    break;
                }

                if (!$read->consumedBytes) {
                    // Genuinely idle read: yield so the deadline can fire without a busy-spin. A read
                    // that consumed bytes but did not complete a message yet (a large payload arriving
                    // in chunks) loops immediately - no 1 ms sleep is paid per partial chunk (#119).
                    delay(0.001, cancellation: $cancellation);
                }
            } while ($this->monotonicSeconds() < $deadline);
        } catch (CancelledException) {
            // Timeout elapsed without a message.
        }

        return $this->messages->count() > 0 ? $this->messages->dequeue() : null;
    }

    /**
     * Fetches all available messages up to the given limit.
     *
     * With a configured timeout (see {@see setTimeout()}), waits up to the full window for up to
     * `$limit` messages, tolerating transient empty reads within the window. With no timeout
     * configured it performs a single best-effort `processIncoming()` cycle and returns whatever is
     * buffered (it does not block).
     *
     * @param int|null $limit Maximum messages to return (null = unlimited).
     * @return list<NatsMessage>
     */
    public function fetchAll(?int $limit = null): array
    {
        $collected = [];

        // Drain anything already buffered first.
        while (!$this->messages->isEmpty() && ($limit === null || count($collected) < $limit)) {
            $collected[] = $this->messages->dequeue();
        }

        if ($limit !== null && count($collected) >= $limit) {
            return $collected;
        }

        // A finite cancellation always bounds each read so a real socket cannot block past the
        // timeout window - and, when no timeout is configured, past a single non-blocking cycle.
        $hasTimeout = $this->timeout > 0;
        $cancellation = new TimeoutCancellation($hasTimeout ? $this->timeout : self::NON_BLOCKING_TIMEOUT);

        try {
            while ($limit === null || count($collected) < $limit) {
                $read = $this->client->readIncoming($cancellation)->await();
                $frames = $read->frames;

                while (!$this->messages->isEmpty() && ($limit === null || count($collected) < $limit)) {
                    $collected[] = $this->messages->dequeue();
                }

                if ($frames === 0 && $this->messages->isEmpty()) {
                    if (!$hasTimeout) {
                        // No timeout configured: best-effort single cycle, return what is buffered.
                        break;
                    }

                    if (!$read->consumedBytes) {
                        // With a timeout, keep waiting for the full window instead of bailing on a
                        // transient idle read (e.g. the heartbeat self-read briefly owning the socket).
                        // The cancellation ends the wait by throwing CancelledException. A read that
                        // consumed bytes but completed no frame yet (a large payload arriving in chunks)
                        // loops immediately - no 1 ms sleep is paid per partial chunk (#119).
                        delay(0.001, cancellation: $cancellation);
                    }
                }
            }
        } catch (CancelledException) {
            // Timeout window elapsed; return what was collected.
        }

        // Final drain of any remaining buffered messages.
        while (!$this->messages->isEmpty() && ($limit === null || count($collected) < $limit)) {
            $collected[] = $this->messages->dequeue();
        }

        return $collected;
    }
}
