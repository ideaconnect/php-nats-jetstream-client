<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Consumers;

use Amp\Cancellation;
use Amp\NullCancellation;

/**
 * Mutable control surface the pipelined pull engine
 * ({@see \IDCT\NATS\JetStream\JetStreamContext::consumePipelined()}) shares with the live
 * {@see PullConsumerIterator} that launched it.
 *
 * Every accessor reads/writes live state through the closures the iterator supplies: the stop/drain
 * flags of this run's own {@see PullRunLifecycle} (#189), so a handler, or another fiber, that calls
 * stop()/drain() mid-run is observed on the very next check, and the iterator's pin, so a pin captured
 * mid-run is written straight back to the iterator. Each handle() run has its own flags, which no later
 * run touches, while the pin is the iterator's, so a captured pin survives into the next run (#120).
 *
 * Besides the flags, the run has two one-shot wake-ups, {@see stopInterruption()} and
 * {@see drainInterruption()}: cancellations that stop() and drain() fire right after setting their flag,
 * so that a call from another fiber ends the engine's wait on the socket, or in its idle backoff, at
 * once instead of at the earliest pull's deadline (#181). The engine composes a wake-up into a wait only
 * while it has not fired: once stop() or drain() fired it, their flag is seen at the top of the loop, and
 * a fired wake-up left out of the later waits cannot end them at once. The wake-ups are the run's own,
 * like its flags, and live as long as the run (#189): a later handle() on the iterator no longer replaces
 * them, so neither fires without its flag while the run lasts. Given none, a wake-up never fires.
 *
 * @internal Not part of the supported public API.
 */
final class PullPipelineControl
{
    /**
     * @param \Closure():bool $stopFn Reads the run's live stop flag (#189).
     * @param \Closure():bool $drainFn Reads the run's live drain flag (#189).
     * @param \Closure():?string $getPinFn Reads the iterator's current pin id.
     * @param \Closure(?string):void $setPinFn Writes the captured/cleared pin id back onto the iterator.
     * @param Cancellation $stopInterruption Fires once stop() has set the run's stop flag (#181).
     * @param Cancellation $drainInterruption Fires once drain() has set the run's drain flag (#181).
     */
    public function __construct(
        private readonly \Closure $stopFn,
        private readonly \Closure $drainFn,
        private readonly \Closure $getPinFn,
        private readonly \Closure $setPinFn,
        private readonly Cancellation $stopInterruption = new NullCancellation(),
        private readonly Cancellation $drainInterruption = new NullCancellation(),
    ) {}

    /**
     * Whether a hard stop() was requested: abandon the rest of the in-flight generation, issue no new
     * pull, break the per-message drain.
     *
     * @phpstan-impure Reads a flag the handler, or another fiber, sets while the engine runs.
     */
    public function isStopRequested(): bool
    {
        return ($this->stopFn)();
    }

    /**
     * Whether a drain() was requested: issue no new pull but let every in-flight pull complete and
     * deliver all its buffered messages first (the per-message drain does NOT break on drain).
     *
     * @phpstan-impure Reads a flag the handler, or another fiber, sets while the engine runs.
     */
    public function isDrainRequested(): bool
    {
        return ($this->drainFn)();
    }

    /**
     * The run's stop() wake-up: requested once stop() has set its flag, so a wait composed with it ends
     * at once (#181). Compose it only while it is not requested yet: a wait that included a fired one
     * would end before it began, and so would every later wait. Once it has fired the flag is set, and
     * {@see isStopRequested()} ends the run.
     */
    public function stopInterruption(): Cancellation
    {
        return $this->stopInterruption;
    }

    /**
     * The run's drain() wake-up: requested once drain() has set its flag (#181). Compose it only while it
     * is not requested yet, for the same reason as {@see stopInterruption()}: a drain is latched for the
     * rest of the run, and the engine goes on pumping the in-flight pulls after it.
     */
    public function drainInterruption(): Cancellation
    {
        return $this->drainInterruption;
    }

    /**
     * The pin id currently held by the iterator (null until captured / after a 423 drop).
     */
    public function getPinId(): ?string
    {
        return ($this->getPinFn)();
    }

    /**
     * Writes a captured pin id back onto the iterator (or null to drop it after a stale-pin 423).
     */
    public function setPinId(?string $pinId): void
    {
        ($this->setPinFn)($pinId);
    }
}
