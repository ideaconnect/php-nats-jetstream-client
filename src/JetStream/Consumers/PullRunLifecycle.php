<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Consumers;

use Amp\Cancellation;
use Amp\DeferredCancellation;

/**
 * The stop/drain state of one {@see PullConsumerIterator::handle()} run (#189): its two flags and the two one-shot
 * wake-ups that end the engine's waits (#181). Each run gets its own, so the runs of one iterator never share a signal:
 * the iterator's stop() and drain() set the flag of every run active when they are called, and a later handle() starts
 * a fresh one without touching an earlier run's, where the runs used to share the iterator's flags, which the next
 * handle() cleared before the earlier run saw them.
 *
 * A flag stays set for the rest of its run. stop() and drain() set it first and then fire its wake-up, with no
 * suspension in between: the engine composes a wake-up into a wait only while it has not fired, so a wait either holds
 * the wake-up or the check after the wait sees the flag. Amp fires a DeferredCancellation as it is destructed, so the
 * object lives exactly as long as its run: the iterator holds it until the run's future resolves, and the run's
 * {@see PullPipelineControl} closes over it.
 *
 * @internal Not part of the supported public API.
 */
final class PullRunLifecycle
{
    /** Set by stop(): break the consume loop promptly, abandoning the rest of the in-flight batch. */
    private bool $stopRequested = false;

    /** Set by drain(): stop after the in-flight batch finishes processing; do not pull again. */
    private bool $drainRequested = false;

    /** Fired right after {@see $stopRequested} is set, so the engine's wait on the socket or in its idle backoff ends at once. */
    private readonly DeferredCancellation $stopWakeUp;

    /** Fired right after {@see $drainRequested} is set, as {@see $stopWakeUp}. */
    private readonly DeferredCancellation $drainWakeUp;

    public function __construct()
    {
        $this->stopWakeUp = new DeferredCancellation();
        $this->drainWakeUp = new DeferredCancellation();
    }

    /** Asks the run to stop: the flag first, then the wake-up. Idempotent. */
    public function stop(): void
    {
        $this->stopRequested = true;
        $this->stopWakeUp->cancel();
    }

    /** Asks the run to drain: the flag first, then the wake-up. Idempotent. */
    public function drain(): void
    {
        $this->drainRequested = true;
        $this->drainWakeUp->cancel();
    }

    /** Whether {@see stop()} was called for this run. */
    public function isStopRequested(): bool
    {
        return $this->stopRequested;
    }

    /** Whether {@see drain()} was called for this run. */
    public function isDrainRequested(): bool
    {
        return $this->drainRequested;
    }

    /** The run's stop() wake-up, {@see PullPipelineControl::stopInterruption()}. */
    public function stopInterruption(): Cancellation
    {
        return $this->stopWakeUp->getCancellation();
    }

    /** The run's drain() wake-up, {@see PullPipelineControl::drainInterruption()}. */
    public function drainInterruption(): Cancellation
    {
        return $this->drainWakeUp->getCancellation();
    }
}
