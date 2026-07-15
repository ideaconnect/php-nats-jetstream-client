<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Consumers;

use Amp\Future;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\JetStreamContext;

/**
 * Fluent builder for pull-consumer batch iteration.
 *
 * handle() snapshots the configuration into an immutable {@see PullPipelineConfig}, binds a live
 * {@see PullPipelineControl} to this iterator, and delegates to the pipelined pull engine on
 * {@see JetStreamContext::consumePipelined()} - which overlaps up to {@see setDepth()} concurrent
 * pull round-trips with handler processing while preserving message order (#120). All fluent
 * configuration, lifecycle (stop/drain/pin), and the pull-status/backoff helpers stay here.
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
    /**
     * First idle-backoff delay (ms) after an immediately answered empty pull in infinite mode
     * (a no_wait 404/408 or a non-terminal 409). Doubles per consecutive empty pull (#153).
     */
    private const IDLE_BACKOFF_INITIAL_MS = 10;

    /**
     * Idle-backoff ceiling (ms): an idle no_wait consumer (or one stuck behind an oversized
     * pending message under max_bytes) settles at ~2 pulls per second instead of an unthrottled
     * re-pull storm (#153).
     */
    private const IDLE_BACKOFF_MAX_MS = 500;

    private int $batch = 100;
    private int $expiresMs = 3000;
    private int $depth = 2;
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
     * Sets the number of messages to fetch per pull request. Independent of {@see setDepth()}: even
     * batch=1 still pipelines, issuing up to `depth` single-message pulls concurrently (#120).
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
     * Sets the pipeline depth: the maximum number of pull requests {@see handle()} keeps in flight at
     * once in infinite, ungrouped, non-idle steady state, so a fresh pull's network round-trip
     * overlaps handler processing of the previous pull (#120). depth=1 forces the classic serial
     * one-pull-at-a-time behavior. Ignored in finite ({@see setIterations()}) mode, which is always
     * strictly serial, and clamped to 1 until a pinned group's pin id is resolved and while idle.
     *
     * @return $this
     */
    public function setDepth(int $depth): self
    {
        if ($depth <= 0) {
            throw new JetStreamException('Pipeline depth must be greater than zero');
        }
        $this->depth = $depth;

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
        if ($group !== null) {
            JetStreamContext::assertValidPriorityGroupName($group, 'Pull group');
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
     * Enables `no_wait` mode (return immediately rather than waiting for the expiry). In infinite
     * mode consecutive empty no_wait pulls are paced with an escalating idle backoff (see
     * {@see self::IDLE_BACKOFF_INITIAL_MS}/{@see self::IDLE_BACKOFF_MAX_MS}) so an idle consumer
     * is not busy-polled (#153).
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
     * Runs the pull loop, invoking the handler for each received message. Thin adapter over the
     * pipelined engine on {@see JetStreamContext::consumePipelined()}: it clears the stop/drain flags,
     * freezes the current configuration into an immutable {@see PullPipelineConfig}, and binds a live
     * {@see PullPipelineControl} so the engine still sees stop()/drain() the handler sets mid-run and
     * writes any captured pin back onto this iterator. The engine overlaps up to {@see setDepth()}
     * concurrent pulls while preserving order; behavior is otherwise identical to the classic serial
     * loop (finite count, 404/408/409/423 handling, #153 idle backoff, onError). The run ends when the
     * configured iteration count is reached, a terminal error occurs, or {@see stop()}/{@see drain()}
     * is signalled (#120).
     *
     * @param callable(NatsMessage, JetStreamContext):void $handler
     * @return Future<int> Total number of messages processed.
     */
    public function handle(callable $handler): Future
    {
        // Reset lifecycle flags so a reused iterator is not pre-stopped from an earlier run. The pin
        // is deliberately NOT reset here: a pinned group keeps its pin across runs.
        $this->resetLifecycle();

        $config = new PullPipelineConfig(
            batch: $this->batch,
            expiresMs: $this->expiresMs,
            depth: $this->depth,
            iterations: $this->iterations,
            noWait: $this->noWait,
            grouped: $this->group !== null,
            pullFields: $this->buildPull(),
            // The iterator exposes no idle_heartbeat knob; buildPull() never sets it. Kept explicit so
            // the engine's route-wide (non-terminal) heartbeat handling stays wired for future use.
            idleHeartbeatNs: null,
            onError: $this->onError,
        );

        $control = new PullPipelineControl(
            stopFn: fn(): bool => $this->isStopRequested(),
            drainFn: fn(): bool => $this->isDrainRequested(),
            getPinFn: fn(): ?string => $this->pinId,
            setPinFn: function (?string $pinId): void {
                $this->pinId = $pinId;
            },
        );

        return $this->context->consumePipelined($this->stream, $this->consumer, $config, $handler, $control);
    }

    /**
     * Whether a hard {@see stop()} has been requested. Read through this accessor (by the bound
     * {@see PullPipelineControl}) so the engine observes the live flag the handler may set mid-run
     * rather than a value narrowed by control flow.
     */
    private function isStopRequested(): bool
    {
        return $this->stopRequested;
    }

    /**
     * Whether a {@see drain()} has been requested. Read through this accessor (by the bound
     * {@see PullPipelineControl}) so the engine observes the live flag the handler may set mid-run.
     */
    private function isDrainRequested(): bool
    {
        return $this->drainRequested;
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
     * Whether a 409 pull status describes a non-terminal condition an infinite worker should keep
     * polling through: either a pull-COMPLETION status ("Batch Completed", "Message Size Exceeds
     * MaxBytes" - nats.go excludes ErrBatchCompleted/ErrMaxBytesExceeded from terminal handling) or
     * a transient, self-clearing one (backpressure/failover). Terminal 409s such as "Consumer
     * Deleted" or "Consumer is push based" stay terminal.
     *
     * @internal Shared with the pipelined engine on {@see JetStreamContext::consumePipelined()}; not
     *           part of the supported public API.
     */
    public static function isNonTerminalPullStatus(string $message): bool
    {
        $needles = [
            // Pull-completion statuses: the request ended without messages, the consumer is fine.
            'Batch Completed', 'Message Size Exceeds MaxBytes',
            // Transient conditions that clear on their own.
            'MaxAckPending', 'Leadership Change', 'Server Shutdown', 'Exceeded MaxWaiting',
        ];

        foreach ($needles as $needle) {
            if (stripos($message, $needle) !== false) {
                return true;
            }
        }

        return false;
    }

    /**
     * Escalating idle backoff for the Nth consecutive immediately answered empty pull: doubles from
     * {@see self::IDLE_BACKOFF_INITIAL_MS} up to {@see self::IDLE_BACKOFF_MAX_MS}.
     *
     * @internal Shared with the pipelined engine on {@see JetStreamContext::consumePipelined()}; not
     *           part of the supported public API.
     */
    public static function idleBackoffMs(int $consecutiveEmptyPulls): int
    {
        // The shift is capped so a long idle streak cannot overflow the integer before min() clamps.
        $exponent = min(6, max(0, $consecutiveEmptyPulls - 1));

        return min(self::IDLE_BACKOFF_MAX_MS, self::IDLE_BACKOFF_INITIAL_MS << $exponent);
    }
}
