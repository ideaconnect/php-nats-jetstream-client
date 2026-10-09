<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

use Amp\Cancellation;
use Amp\DeferredCancellation;
use Revolt\EventLoop;

/** @internal A fixed fetch deadline or a renewable Direct Get no-progress deadline. */
final class BatchReadBudget
{
    private readonly DeferredCancellation $source;
    private readonly int $intervalNs;
    private int $deadlineNs;
    private ?string $timer = null;

    public function __construct(int $timeoutMs, private readonly bool $rolling = false)
    {
        $this->source = new DeferredCancellation();
        $this->intervalNs = $timeoutMs * 1_000_000;
        $this->deadlineNs = hrtime(true) + $this->intervalNs;
        $this->arm();
    }

    public function cancellation(): Cancellation
    {
        return $this->source->getCancellation();
    }

    public function remainingNs(): int
    {
        // Wire durations are integer nanoseconds, including on platforms with a float hrtime clock.
        return (int) max(0, $this->deadlineNs - hrtime(true));
    }

    public function touch(): void
    {
        if ($this->rolling && !$this->cancellation()->isRequested()) {
            $this->deadlineNs = hrtime(true) + $this->intervalNs;
        }
    }

    public function stop(): void
    {
        if ($this->timer !== null) {
            EventLoop::cancel($this->timer);
            $this->timer = null;
        }

        // A detached writer must not start another attempt after its operation has ended.
        $this->source->cancel();
    }

    private function arm(): void
    {
        $this->timer = EventLoop::delay(max(0.000001, $this->remainingNs() / 1e9), function (): void {
            $this->timer = null;
            if ($this->remainingNs() > 0) {
                // Replies extend a rolling budget without allocating a timer for every message.
                // Also tolerate drivers whose cached loop time fires the timer slightly early.
                $this->arm();
            } else {
                $this->source->cancel();
            }
        });
        EventLoop::unreference($this->timer);
    }
}
