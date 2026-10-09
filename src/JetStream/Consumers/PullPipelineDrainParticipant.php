<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Consumers;

use Amp\Cancellation;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use IDCT\NATS\Connection\DrainParticipant;

/**
 * A pull consumer run's part in a drain() of the client's connection (#207), one per run of the pipelined pull engine
 * ({@see \IDCT\NATS\JetStream\JetStreamContext::consumePipelined()}). The engine registers it once the run's inbox is
 * subscribed and removes it as the run ends, before the inbox's UNSUB.
 *
 * Once its flush is done the drain asks for the run's hand-over ({@see handOver()}), which fires the run's wake-up
 * ({@see wakeUp()}): the pump read and the idle backoff wait with it while it has not fired, as they wait with the
 * wake-ups of the iterator's stop() and drain() (#181), and once fired it is the request itself, latched for the rest
 * of the run ({@see isHandOverRequested()}). The engine then hands every pull in flight over at the top of its loop,
 * issues nothing more, and ends the run with its count; {@see handedOver()} completes the future the drain waits for,
 * which the engine calls as the run ends, however it ends, so that a drain never waits for a run that has gone. The
 * request lives on this object, not in a local variable of the engine that a closure sets, and is read through a
 * method that static analysis treats as impure, so that the engine's later checks of it are not taken as settled by
 * its earlier ones, as the iterator's flags are read through {@see PullPipelineControl}.
 *
 * @internal Not part of the supported public API.
 */
final class PullPipelineDrainParticipant implements DrainParticipant
{
    /** @var DeferredFuture<null>|null Made when the drain asks; completed once the run has handed over, or ended. */
    private ?DeferredFuture $handOver = null;

    /** Fired when the drain asks, and only then while the run lasts: Amp fires it on destruction, after the run. */
    private readonly DeferredCancellation $wakeUp;

    /**
     * @param \Closure(): int $held Counts the messages the run's pulls hold that the handler has not got yet, the pull
     *        the retire phase is handing over included.
     */
    public function __construct(private readonly \Closure $held)
    {
        $this->wakeUp = new DeferredCancellation();
    }

    /**
     * The drain asks for the run's hand-over: the run's wake-up is fired, and the future of that hand-over returned.
     * Does not suspend: the wake-up's callbacks are queued, not run. Asked again, it returns the same future.
     *
     * @return Future<null>
     */
    public function handOver(): Future
    {
        $this->handOver ??= new DeferredFuture();
        $this->wakeUp->cancel();

        return $this->handOver->getFuture();
    }

    /** What the run's pulls hold that the handler has not got yet, for the drain's deadline report. */
    public function undelivered(): int
    {
        return ($this->held)();
    }

    /**
     * Whether the drain has asked for the run's hand-over: then the engine hands its pulls over at the top of its loop
     * and ends the run, issuing no pull meanwhile.
     *
     * @phpstan-impure The drain asks from another fiber while the engine runs, which ends the engine's waits.
     */
    public function isHandOverRequested(): bool
    {
        return $this->wakeUp->isCancelled();
    }

    /**
     * The run's wake-up for the drain: fired once the drain asks, so that a wait composed with it ends at once. As for
     * the wake-ups of {@see PullPipelineControl}, compose it only while it has not fired: once it has, the request is
     * seen at the top of the loop, and a fired wake-up composed into a wait would end it, and every later one, at once.
     */
    public function wakeUp(): Cancellation
    {
        return $this->wakeUp->getCancellation();
    }

    /**
     * Completes the future of the hand-over the drain asked for, if it did and it is not complete yet: once the run has
     * handed its pulls over, or as it ends otherwise (a stop(), a handler that threw, a failure of its own).
     */
    public function handedOver(): void
    {
        if ($this->handOver !== null && !$this->handOver->isComplete()) {
            $this->handOver->complete();
        }
    }
}
