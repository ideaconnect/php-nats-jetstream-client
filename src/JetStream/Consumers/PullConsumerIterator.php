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
 * {@see PullPipelineControl} to the run's own stop/drain state ({@see PullRunLifecycle}, #189) and to
 * this iterator's pin, and delegates to the pipelined pull engine on
 * {@see JetStreamContext::consumePipelined()} - which overlaps up to {@see setDepth()} concurrent
 * pull round-trips with handler processing while preserving message order (#120). All fluent
 * configuration, lifecycle (stop/drain of the active runs, pin), and the pull-status/backoff helpers
 * stay here.
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

    /**
     * The stop/drain state of each {@see handle()} run still active, keyed by its object id (#189): added once the
     * engine has taken the run on, removed as the run's future resolves. {@see stop()} and {@see drain()} signal every
     * run here, and each run reads only its own, so a later handle() cannot clear a signal meant for an earlier run.
     * Holding each one also keeps its wake-ups alive for the whole run, since Amp fires a DeferredCancellation as it is
     * destructed.
     *
     * @var array<int, PullRunLifecycle>
     */
    private array $activeRuns = [];

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
     * Signals every {@see handle()} run active on this iterator to stop promptly: each breaks before its next pull and
     * abandons any messages remaining in its in-flight batch (already-fetched but not yet handled). Safe to call from
     * inside the handler or from another fiber: a call from another fiber - a signal handler's timer, a supervisor -
     * also ends each engine's wait on the socket, or in its idle backoff, at once, instead of at the earliest in-flight
     * pull's deadline (#181). It acts on the runs active when it is called (#189): a run a later handle() starts, in
     * the same tick or not, starts clean and is not stopped, so stop() followed by handle() restarts the consumer, the
     * earlier run ending on its stop. A handler already running when it is called finishes its message. Idempotent; a
     * call outside a run has no effect on a later run. Mirrors nats.go `ConsumeContext.Stop()`.
     */
    public function stop(): void
    {
        foreach ($this->activeRuns as $run) {
            $run->stop();
        }
    }

    /**
     * Signals every {@see handle()} run active on this iterator to drain: each finishes processing its in-flight batch
     * (so no fetched message is dropped, across a reconnect too, #187) and then stops without issuing another pull.
     * From another fiber the call wakes each engine once, so it stops issuing pulls at once and, with nothing in
     * flight, returns at once; the in-flight pulls still complete or reach their deadline first, as from inside the
     * handler (#181). Like {@see stop()} it acts on the runs active when it is called (#189): a run a later handle()
     * starts pulls as usual, and the drained run goes on delivering its in-flight pulls to its own handler next to it,
     * without pulling again. Mirrors nats.go `ConsumeContext.Drain()`.
     */
    public function drain(): void
    {
        foreach ($this->activeRuns as $run) {
            $run->drain();
        }
    }

    /**
     * Runs the pull loop, invoking the handler for each received message. Thin adapter over the
     * pipelined engine on {@see JetStreamContext::consumePipelined()}: it gives the run stop/drain state
     * of its own ({@see PullRunLifecycle}, #189), freezes the current configuration into an immutable
     * {@see PullPipelineConfig}, and binds a live {@see PullPipelineControl} so the engine still sees a
     * stop()/drain() the handler makes mid-run and writes any captured pin back onto this iterator. The
     * control also carries the run's two one-shot wake-ups, which stop() and drain() fire, so a call from
     * another fiber ends the engine's wait on the socket or in its idle backoff at once (#181); each run
     * has its own, so a wake-up fired in an earlier run cannot end a wait of this one. The engine
     * overlaps up to {@see setDepth()} concurrent pulls while preserving order; behavior is otherwise
     * identical to the classic serial loop (finite count, 404/408/409/423 handling, #153 idle backoff,
     * onError). The run ends when the configured iteration count is reached, a terminal error occurs,
     * or {@see stop()}/{@see drain()} is signalled (#120).
     *
     * A run that fails first runs the handler for what its pulls have received (#197): when its read fails because the
     * connection is going (lost with waitForReconnect off, a reconnect that gave up, reconnect off, a fatal -ERR or a
     * PONG the socket would not take that the run does not go on past, see below), when a pull's write fails, when the
     * server rejects the run's reply inbox, or when the read fails for a reason the options make its own
     * (handlerErrorsFailOperations, slowConsumerErrorsFailOperations). Those messages go to the handler in the order
     * they arrived, unless {@see stop()} was called, or the application closed the connection in a way that discards
     * them (see below), and the future then fails with that error, unchanged. A handler that throws during that
     * delivery ends it, and its exception goes to the connection's error listener and logger, since the run's own
     * failure is the one thrown; on a closed connection that includes a handler whose ack failed. A handler that throws
     * at any other time ends the run at once with its own exception, the other messages left undelivered. A rejection
     * of the inbox ends the run whichever fiber's read records it (#206): one recorded while the handler or onError
     * runs, or while a pull is written, ends it as soon as that code returns, a finite run that has delivered its last
     * batch and one a later pull's terminal status would have ended included, and no pull goes out on the inbox once
     * the run knows of it; a stop() made by then still ends the run with its count.
     *
     * The client's drain() hands over what the run's pulls hold, and its disconnect() discards it, wherever the run is
     * (#207), as they do with what the connection itself has received. Once the drain's flush is done, the run hands
     * every pull in flight to the handler in order while the connection is Draining, so that the acks the handler
     * publishes still go out (a request, such as ackSync(), fails there with "Connection is not open", the connection
     * refusing requests while it is Draining), and the future then resolves with the count, as after {@see drain()};
     * the client's drain() waits for that, the handler included, within its budget, and when the budget runs out first
     * it closes the connection, the rest left undelivered from its "drain deadline exceeded" report on, which names it,
     * and the future still resolves with what was handed over, or fails with the error of a pull's write that the close
     * cut short. A handler, onError or error listener that awaits the client's drain() while the run calls it holds
     * that drain for its whole budget, since the drain waits for this run, as it waits for a subscription's handler
     * that awaits it: stop() or drain() the iterator there, and drain the client once the future has resolved, or from
     * another fiber. A handler that throws during the hand-over ends the run with its own exception, and a stop()
     * leaves the rest undelivered. While the client's drain() is under way the run issues no new pull, and with none
     * left in flight it ends with its count. A disconnect(), from the handler or from another fiber, ends whatever
     * delivery is under way, the retire of a full pull included, before the next message: the rest stays unacked, for
     * the server to deliver again after the ack wait (lost on a consumer without acks), and the future fails, as it
     * does when the connection is closed under the run, with "Connection is not open" or the error of the read or write
     * that met the close; it resolves with the count where the client's drain() had already asked the run for its
     * hand-over, or where a finite run has no pull left to issue. A drain() that finds no connection to drain (its
     * budget ran out before the reconnect it waited for was done, or that reconnect gave up) discards the same way, the
     * future failing with "Connection is not open" or the reconnect's own error.
     *
     * An infinite run with reconnect on and waitForReconnect enabled goes on after a fatal -ERR ('Stale Connection',
     * 'User Authentication Expired') or a PONG the socket would not take, once the reconnect has reopened the
     * connection, as it goes on after a lost connection (#210): the frame's error goes to the error listener and the
     * logger instead of failing the future, the handler gets what the pulls held as part of the run (counted in its
     * total, a handler that throws there ending the run with its own exception), and the run pulls again on the new
     * connection. A finite run still fails with that error, and so does an infinite one with reconnect off or waiting
     * disabled, whose connection the application is closing, or whose reconnect gave up while the read that met the
     * frame still waited for it; a reconnect that gives up later ends the run with its own error ("Reconnect attempts
     * exhausted"), as after a lost connection.
     *
     * An infinite run hands the handler every message it receives, across a reconnect too (#187). Once the connection
     * has reconnected, the run ends its pulls in flight and pulls again at once, the server holding some of them or
     * not: what they received reaches the handler first, in order, and a status one of them got on the old connection
     * neither ends the run nor drops a group's pin. A server that outlived the connection still serves the requests the
     * run issued before the reconnect, and a pull written during the outage goes out on the new connection: what such a
     * request brings goes to the run's newer pulls, and what none of them has room for is held for the handler, in
     * arrival order, behind what the pulls hold, counted in the future's result, the run issuing no pull until it has
     * handed it over. So across a reconnect the handler can get more messages than the batch times the depth. A
     * terminal status also hands over what the pulls behind the one it ended hold, before the future resolves. stop()
     * and a close that discards leave such messages undelivered, as they leave any; the run used to drop them, for the
     * server to deliver again after the ack wait, or never on a consumer without acks or with max_deliver 1. A finite
     * run keeps its exact count, and drops what its last pull has no room for, as fetchBatch() does.
     *
     * Each run has stop/drain flags and wake-ups of its own (#189): {@see stop()} and {@see drain()} act on every run
     * active when they are called, and handle() leaves the runs already active alone. So stop() followed by handle()
     * restarts the consumer, in the same tick too, without awaiting the earlier run: that run ends on its stop with its
     * pull inbox released, what it holds left undelivered as after any stop(), in each of its hand-overs too, and the
     * new run starts clean, with the configuration of its own handle(). After drain() and handle() the earlier run
     * delivers its pulls in flight to its own handler, next to the new run, and then ends without pulling again. The
     * earlier run's handler may still be running when the new run starts (it awaits while another fiber stops and
     * restarts, or it restarts its own consumer): await the earlier run's future first when two handlers must never run
     * at once. Runs started without a stop() in between all go on, sharing the iterator's pin, and one stop() ends them
     * all, each at once; a stop() or drain() that an earlier run's handler makes once a new run is under way reaches the
     * new run too. The runs used to share the iterator's two flags, so a handle() while a run was still active cleared a
     * stop() or drain() that run had not seen yet, and the run went on next to the new one, with its own handler, until
     * a later stop() reached it at its pulls' deadline.
     *
     * @param callable(NatsMessage, JetStreamContext):void $handler
     * @return Future<int> Total number of messages processed. It resolves, or fails, once the run is over
     *         and stop() and drain() no longer reach it.
     */
    public function handle(callable $handler): Future
    {
        // This run's own stop/drain state, which no other run's stop(), drain() or handle() can reset (#189). The pin
        // is deliberately the iterator's, NOT the run's: a pinned group keeps its pin across runs.
        $lifecycle = new PullRunLifecycle();

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

        // The engine reads the run's flags through closures, so that it observes a flag the handler, or another fiber,
        // sets mid-run rather than a value narrowed by control flow, and the pin through closures over this iterator.
        $control = new PullPipelineControl(
            stopFn: static fn(): bool => $lifecycle->isStopRequested(),
            drainFn: static fn(): bool => $lifecycle->isDrainRequested(),
            getPinFn: fn(): ?string => $this->pinId,
            setPinFn: function (?string $pinId): void {
                $this->pinId = $pinId;
            },
            stopInterruption: $lifecycle->stopInterruption(),
            drainInterruption: $lifecycle->drainInterruption(),
        );

        // Registered only once the engine has taken the run on: consumePipelined() throws on an invalid name before it
        // returns, which would leave an entry nothing removes. Nothing suspends in between, and the engine's fiber
        // starts only once this one suspends, so the run is registered before it can read its flags.
        $run = $this->context->consumePipelined($this->stream, $this->consumer, $config, $handler, $control);
        $runId = spl_object_id($lifecycle);
        $this->activeRuns[$runId] = $lifecycle;

        // The future handed back completes only once the run has left the registry, with the run's own result or
        // failure: a caller that resumes from it finds that stop() and drain() no longer reach the run. Cancelling a
        // wait on it does not end the run, which stays registered until it is over.
        return $run->finally(function () use ($runId): void {
            unset($this->activeRuns[$runId]);
        });
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
