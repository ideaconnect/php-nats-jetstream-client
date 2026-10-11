<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Future;
use IDCT\NATS\Transport\TransportInterface;
use Revolt\EventLoop;

use function Amp\async;

/**
 * Holds a writer's completion after the transport it wraps has taken the write (#192): the server has the bytes and
 * has answered them, and the writes made after it go on behind them, but the writer is not resumed until the hold
 * ends. For a test that keeps an operation suspended in a write while its bytes have already reached the server -
 * a delay after acceptance, not backpressure, which would hold the later writes too.
 *
 * The wrapped transport keeps its own write order: it chains on its own futures, not on the ones returned here.
 * Everything but write() is passed through; close() also lets the held writers go.
 */
final class CompletionHoldingTransport implements TransportInterface
{
    /** @var array<string, float> One-shot holds by needle: how long the matching write's completion is held at most. */
    private array $holds = [];

    /** @var array<string, DeferredFuture<null>> Completions held right now, by their timer. */
    private array $held = [];

    public function __construct(private readonly TransportInterface $inner) {}

    /**
     * Makes the next write containing $needle complete for its writer only $seconds after the wrapped transport
     * completed it, or when the test calls {@see releaseHeldCompletions()}, whichever comes first. One-shot.
     */
    public function holdCompletionOfNextWriteContaining(string $needle, float $seconds): void
    {
        $this->holds[$needle] = $seconds;
    }

    /** How many writers are held right now. */
    public function completionsHeld(): int
    {
        return count($this->held);
    }

    /** Lets every held writer go now. */
    public function releaseHeldCompletions(): void
    {
        foreach ($this->held as $timer => $released) {
            EventLoop::cancel($timer);
            if (!$released->isComplete()) {
                $released->complete();
            }
        }
    }

    public function connect(string $dsn, int $timeoutMs): Future
    {
        return $this->inner->connect($dsn, $timeoutMs);
    }

    public function upgradeTls(): Future
    {
        return $this->inner->upgradeTls();
    }

    public function write(string $bytes): Future
    {
        $written = $this->inner->write($bytes);

        $holdFor = null;
        foreach ($this->holds as $needle => $seconds) {
            if (str_contains($bytes, $needle)) {
                unset($this->holds[$needle]);
                $holdFor = $seconds;

                break;
            }
        }

        if ($holdFor === null) {
            return $written;
        }

        return async(function () use ($written, $holdFor): void {
            $written->await();
            $this->hold($holdFor);
        });
    }

    public function readLine(?Cancellation $cancellation = null): Future
    {
        return $this->inner->readLine($cancellation);
    }

    public function close(): Future
    {
        $this->releaseHeldCompletions();

        return $this->inner->close();
    }

    /** Holds the calling writer for $seconds, or until released; the timer is referenced, like a pending write's watcher. */
    private function hold(float $seconds): void
    {
        /** @var DeferredFuture<null> $released */
        $released = new DeferredFuture();
        $timer = EventLoop::delay($seconds, static function () use ($released): void {
            if (!$released->isComplete()) {
                $released->complete();
            }
        });
        $this->held[$timer] = $released;

        try {
            $released->getFuture()->await();
        } finally {
            EventLoop::cancel($timer);
            unset($this->held[$timer]);
        }
    }
}
