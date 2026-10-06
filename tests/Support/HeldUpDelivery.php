<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\DeferredFuture;
use Revolt\EventLoop;

/**
 * A subscription handler that holds up the delivery it runs in, the way one that awaits an HTTP or a database call
 * does: the first message it gets starts the hold-up, and it returns once {@see end()} is called, or once its fallback
 * time is up. {@see holdUp()} does the same from any other code a delivery runs, such as an error listener. The read slot is free meanwhile, and what that delivery still has to deliver waits behind it. Paired with
 * {@see WatchedTransport::$onRead} through {@see endWhenARead()}, the hold-up ends as soon as a read takes the socket
 * during it, which tells a test the order of events without timing them.
 */
final class HeldUpDelivery
{
    /** Whether the handler is holding a delivery up right now. */
    public bool $held = false;

    /** What ended the hold-up. */
    public string $endedBy = 'nothing';

    /** @var DeferredFuture<null> Completed when the hold-up starts. */
    public readonly DeferredFuture $began;

    /** @var DeferredFuture<null> Completed when the handler returns, with the delivery behind it under way again. */
    public readonly DeferredFuture $returned;

    /**
     * Called by the handler once it was let go, right before it returns: whatever it queues runs ahead of anything
     * the rest of the delivery queues.
     *
     * @var (\Closure(): void)|null
     */
    public ?\Closure $beforeReturning = null;

    private bool $over = false;

    /** @var DeferredFuture<null> */
    private readonly DeferredFuture $released;

    public function __construct(private readonly float $fallbackSeconds = 1.0)
    {
        $this->began = new DeferredFuture();
        $this->returned = new DeferredFuture();
        $this->released = new DeferredFuture();
    }

    /** @return \Closure(): void The handler to subscribe with. */
    public function handler(): \Closure
    {
        return fn() => $this->holdUp();
    }

    /**
     * Holds the delivery, or whatever else runs this, up until {@see end()} is called or the fallback time is up: for
     * code that is not a subscription handler, such as an error listener a dropped message is reported to. The first
     * call starts the hold-up; a call during it, or after it ended, returns at once.
     */
    public function holdUp(): void
    {
        if ($this->held || $this->over) {
            return;
        }

        $this->held = true;
        $this->began->complete();
        $fallback = EventLoop::delay($this->fallbackSeconds, fn() => $this->end('its own time running out'));
        try {
            $this->released->getFuture()->await();
        } finally {
            EventLoop::cancel($fallback);
        }

        if ($this->beforeReturning !== null) {
            ($this->beforeReturning)();
        }

        $this->returned->complete();
    }

    /** @return \Closure(): void For {@see WatchedTransport::$onRead}: ends the hold-up when a read takes the socket during it. */
    public function endWhenARead(): \Closure
    {
        return function (): void {
            if ($this->held) {
                $this->end('a read taking the socket');
            }
        };
    }

    public function end(string $by): void
    {
        if ($this->over) {
            return;
        }

        $this->over = true;
        $this->held = false;
        $this->endedBy = $by;
        $this->released->complete();
    }
}
