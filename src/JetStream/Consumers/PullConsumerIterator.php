<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Consumers;

use Amp\Future;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\JetStreamContext;

use function Amp\async;

/**
 * Fluent builder for pull-consumer batch iteration.
 *
 * Usage:
 *   $js->pullConsumer('STREAM', 'CONSUMER')
 *      ->setBatching(10)
 *      ->setExpiresMs(5000)
 *      ->setIterations(3)
 *      ->handle(function (NatsMessage $msg, JetStreamContext $js) {
 *          $js->ack($msg)->await();
 *      });
 */
final class PullConsumerIterator
{
    private int $batch = 1;
    private int $expiresMs = 3000;
    private ?int $iterations = null;
    private ?string $group = null;
    private ?int $priority = null;
    private ?int $minPending = null;
    private ?int $minAckPending = null;
    private ?int $maxBytes = null;
    private bool $noWait = false;

    /** Runtime pin id captured from the first delivered message of a pinned group. */
    private ?string $pinId = null;

    /** Set by stop(): break the consume loop promptly, abandoning the rest of the in-flight batch. */
    private bool $stopRequested = false;

    /** Set by drain(): stop after the in-flight batch finishes processing; do not pull again. */
    private bool $drainRequested = false;

    /** Optional diagnostics callback fired when the consume loop stops on a non-routine error (#63). */
    private ?\Closure $onError = null;

    /**
     * @param JetStreamContext $context JetStream context used to issue pull requests and ACK-related commands.
     * @param string $stream Stream name that owns the target consumer.
     * @param string $consumer Durable/ephemeral consumer name used for `CONSUMER.MSG.NEXT` pulls.
     */
    public function __construct(
        private readonly JetStreamContext $context,
        private readonly string $stream,
        private readonly string $consumer,
    ) {}

    /**
     * Sets the number of messages to fetch per pull request.
     *
     * @return $this
     */
    public function setBatching(int $batch): self
    {
        if ($batch <= 0) {
            throw new JetStreamException('Batch size must be greater than zero');
        }
        $this->batch = $batch;

        return $this;
    }

    /**
     * Sets the server-side expiration timeout in milliseconds for each pull request.
     *
     * @return $this
     */
    public function setExpiresMs(int $expiresMs): self
    {
        if ($expiresMs <= 0) {
            throw new JetStreamException('ExpiresMs must be greater than zero');
        }
        $this->expiresMs = $expiresMs;

        return $this;
    }

    /**
     * Sets the number of fetch iterations (null = infinite loop).
     *
     * @return $this
     */
    public function setIterations(?int $iterations): self
    {
        if ($iterations !== null && $iterations <= 0) {
            throw new JetStreamException('Iterations must be greater than zero or null for infinite');
        }
        $this->iterations = $iterations;

        return $this;
    }

    /**
     * Sets the ADR-42 priority group this consumer pulls under (required for priority policies).
     *
     * @return $this
     */
    public function setGroup(?string $group): self
    {
        if ($group !== null && preg_match('/^[A-Za-z0-9\-_\/=]{1,16}$/', $group) !== 1) {
            throw new JetStreamException('Pull group must be 1..16 characters of [A-Za-z0-9-_/=]');
        }

        $this->group = $group;

        return $this;
    }

    /**
     * Sets the pull priority (0-9) for a `prioritized` priority policy.
     *
     * @return $this
     */
    public function setPriority(?int $priority): self
    {
        if ($priority !== null && ($priority < 0 || $priority > 9)) {
            throw new JetStreamException('Pull priority must be an integer between 0 and 9');
        }

        $this->priority = $priority;

        return $this;
    }

    /**
     * Sets the `overflow` policy `min_pending` threshold (only pull when at least this many messages
     * are pending).
     *
     * @return $this
     */
    public function setMinPending(?int $minPending): self
    {
        $this->minPending = $minPending;

        return $this;
    }

    /**
     * Sets the `overflow` policy `min_ack_pending` threshold.
     *
     * @return $this
     */
    public function setMinAckPending(?int $minAckPending): self
    {
        $this->minAckPending = $minAckPending;

        return $this;
    }

    /**
     * Caps the total bytes returned per pull request.
     *
     * @return $this
     */
    public function setMaxBytes(?int $maxBytes): self
    {
        $this->maxBytes = $maxBytes;

        return $this;
    }

    /**
     * Enables `no_wait` mode (return immediately rather than waiting for the expiry).
     *
     * @return $this
     */
    public function setNoWait(bool $noWait = true): self
    {
        $this->noWait = $noWait;

        return $this;
    }

    /**
     * Registers a diagnostics callback invoked when the consume loop terminates on a non-routine error
     * (e.g. 409 "Consumer Deleted", a server error) - as opposed to a routine empty window (404/408) or
     * an explicit stop()/drain(). Mirrors nats.go's `ConsumeErrHandler` for surfacing why a consumer
     * stopped (#63).
     *
     * @param callable(JetStreamException):void $handler
     * @return $this
     */
    public function setOnError(callable $handler): self
    {
        $this->onError = \Closure::fromCallable($handler);

        return $this;
    }

    /**
     * Returns configured batch size.
     */
    public function getBatching(): int
    {
        return $this->batch;
    }

    /**
     * Returns configured server-side expiration.
     */
    public function getExpiresMs(): int
    {
        return $this->expiresMs;
    }

    /**
     * Returns configured iterations (null = infinite).
     */
    public function getIterations(): ?int
    {
        return $this->iterations;
    }

    /**
     * Signals a running {@see handle()} loop to stop promptly: it breaks before the next pull and
     * abandons any messages remaining in the in-flight batch (already-fetched but not yet handled).
     * Safe to call from inside the handler or from another fiber. Mirrors nats.go
     * `ConsumeContext.Stop()`.
     */
    public function stop(): void
    {
        $this->stopRequested = true;
    }

    /**
     * Signals a running {@see handle()} loop to drain: it finishes processing the in-flight batch
     * (so no fetched message is dropped) and then stops without issuing another pull. Mirrors nats.go
     * `ConsumeContext.Drain()`.
     */
    public function drain(): void
    {
        $this->drainRequested = true;
    }

    /**
     * Runs the fetch loop, invoking the handler for each received message. The loop ends when the
     * configured iteration count is reached, a terminal error occurs, or {@see stop()}/{@see drain()}
     * is signalled.
     *
     * @param callable(NatsMessage, JetStreamContext):void $handler
     * @return Future<int> Total number of messages processed.
     */
    public function handle(callable $handler): Future
    {
        return async(function () use ($handler): int {
            // Reset lifecycle flags so a reused iterator is not pre-stopped from an earlier run.
            $this->resetLifecycle();
            $totalProcessed = 0;
            $iteration = 0;

            while (($this->iterations === null || $iteration < $this->iterations)
                && !$this->stopRequested
                && !$this->drainRequested
            ) {
                ++$iteration;

                try {
                    $messages = $this->context->fetchBatch(
                        $this->stream,
                        $this->consumer,
                        $this->batch,
                        $this->expiresMs,
                        $this->buildPull(),
                    )->await();
                } catch (JetStreamException $e) {
                    // A stale pin (423) is never terminal: drop the pin id and re-pull without it so
                    // the server re-pins this client (or hands the pin to another). Applies in both
                    // finite and infinite mode.
                    if ($e->getCode() === 423) {
                        $this->pinId = null;

                        continue;
                    }

                    // In infinite mode keep polling through routine/transient conditions so a
                    // long-running worker is not killed by a quiet period, backpressure, or a
                    // failover: 404 (no messages) and 408 (request timeout) are routine empty
                    // windows, and a 409 may be transient (MaxAckPending exceeded / leadership
                    // change / server shutdown / max-waiting) rather than terminal (Consumer
                    // Deleted). Finite mode keeps the existing stop-on-any-error behavior.
                    if ($this->iterations === null) {
                        $code = $e->getCode();
                        if (in_array($code, [404, 408], true) || ($code === 409 && self::isTransientPullStatus($e->getMessage()))) {
                            continue;
                        }
                    }

                    // Finite mode, or a terminal error (e.g. 409 Consumer Deleted): stop iterating.
                    // Surface non-routine terminations (consumer deleted, server error, ...) to the
                    // diagnostics callback so the app learns WHY the consume loop stopped (#63); routine
                    // empty windows (404/408) are not errors.
                    if ($this->onError !== null && !in_array($e->getCode(), [404, 408], true)) {
                        ($this->onError)($e);
                    }

                    break;
                }

                // Capture the pin id from the first message of a newly pinned group so subsequent
                // pulls retain the pin.
                if ($this->group !== null && $this->pinId === null && $messages !== []) {
                    $this->pinId = $this->context->pinIdOf($messages[0]);
                }

                foreach ($messages as $message) {
                    $handler($message, $this->context);
                    ++$totalProcessed;

                    // A hard stop abandons the rest of this batch; a drain lets it finish (the
                    // while-condition then ends the loop before the next pull). Read through the
                    // accessor so the flag the handler may have just set is observed.
                    if ($this->isStopRequested()) {
                        break;
                    }
                }
            }

            return $totalProcessed;
        });
    }

    /**
     * Whether a hard {@see stop()} has been requested. Exposed as an accessor so the consume loop reads
     * the live flag (which the handler may set mid-batch) rather than a value narrowed by control flow.
     */
    private function isStopRequested(): bool
    {
        return $this->stopRequested;
    }

    /**
     * Clears the stop/drain flags at the start of a {@see handle()} run.
     */
    private function resetLifecycle(): void
    {
        $this->stopRequested = false;
        $this->drainRequested = false;
    }

    /**
     * Builds the optional pull-request fields from the configured priority/group options plus the
     * current pin id.
     *
     * @return array<string,mixed>
     */
    private function buildPull(): array
    {
        $pull = [];

        if ($this->group !== null) {
            $pull['group'] = $this->group;
        }

        if ($this->pinId !== null) {
            $pull['id'] = $this->pinId;
        }

        if ($this->priority !== null) {
            $pull['priority'] = $this->priority;
        }

        if ($this->minPending !== null) {
            $pull['min_pending'] = $this->minPending;
        }

        if ($this->minAckPending !== null) {
            $pull['min_ack_pending'] = $this->minAckPending;
        }

        if ($this->maxBytes !== null) {
            $pull['max_bytes'] = $this->maxBytes;
        }

        if ($this->noWait) {
            $pull['no_wait'] = true;
        }

        return $pull;
    }

    /**
     * Whether a 409 pull status describes a transient, self-clearing condition that an infinite
     * worker should keep polling through (as opposed to a terminal one such as "Consumer Deleted").
     */
    private static function isTransientPullStatus(string $message): bool
    {
        foreach (['MaxAckPending', 'Leadership Change', 'Server Shutdown', 'Exceeded MaxWaiting'] as $needle) {
            if (stripos($message, $needle) !== false) {
                return true;
            }
        }

        return false;
    }
}
