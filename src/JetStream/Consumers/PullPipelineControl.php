<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\Consumers;

/**
 * Mutable control surface the pipelined pull engine
 * ({@see \IDCT\NATS\JetStream\JetStreamContext::consumePipelined()}) shares with the live
 * {@see PullConsumerIterator} that launched it.
 *
 * Every accessor reads/writes the iterator's own fields through the closures the iterator supplies,
 * so a handler that calls stop()/drain() mid-run is observed on the very next check, and a pin
 * captured mid-run is written straight back to the iterator. handle()'s resetLifecycle() clears the
 * stop/drain flags between runs but NOT the pin, so a captured pin survives into the next run (#120).
 *
 * @internal Not part of the supported public API.
 */
final class PullPipelineControl
{
    /**
     * @param \Closure():bool $stopFn Reads the iterator's live stop flag.
     * @param \Closure():bool $drainFn Reads the iterator's live drain flag.
     * @param \Closure():?string $getPinFn Reads the iterator's current pin id.
     * @param \Closure(?string):void $setPinFn Writes the captured/cleared pin id back onto the iterator.
     */
    public function __construct(
        private readonly \Closure $stopFn,
        private readonly \Closure $drainFn,
        private readonly \Closure $getPinFn,
        private readonly \Closure $setPinFn,
    ) {}

    /**
     * Whether a hard stop() was requested: abandon the rest of the in-flight generation, issue no new
     * pull, break the per-message drain.
     */
    public function isStopRequested(): bool
    {
        return ($this->stopFn)();
    }

    /**
     * Whether a drain() was requested: issue no new pull but let every in-flight pull complete and
     * deliver all its buffered messages first (the per-message drain does NOT break on drain).
     */
    public function isDrainRequested(): bool
    {
        return ($this->drainFn)();
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
