<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\DeferredFuture;
use Amp\Future;
use IDCT\NATS\Connection\DrainParticipant;

/**
 * A drain participant whose hand-over lasts until the test releases it: it holds a drain()'s delivery phase open, as a
 * pull consumer run whose handler is still at work does, with no handler holding up the reads meanwhile. It reports
 * {@see $held} messages as not handed over, for the drain's deadline report.
 */
final class HeldDrainParticipant implements DrainParticipant
{
    /** How many messages it says it still holds ({@see undelivered()}). */
    public int $held = 0;

    /** How many times a drain asked it to hand over. */
    public int $asked = 0;

    /** @var DeferredFuture<null> */
    private readonly DeferredFuture $handOver;

    public function __construct()
    {
        $this->handOver = new DeferredFuture();
    }

    /** @return Future<null> */
    public function handOver(): Future
    {
        ++$this->asked;

        return $this->handOver->getFuture();
    }

    public function undelivered(): int
    {
        return $this->held;
    }

    /** Ends the hand-over: the drain stops waiting for this participant. */
    public function release(): void
    {
        if (!$this->handOver->isComplete()) {
            $this->handOver->complete();
        }
    }
}
