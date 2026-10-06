<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Cancellation;
use Amp\Future;
use IDCT\NATS\Transport\TlsAwareTransportInterface;
use Revolt\EventLoop;

/**
 * A transport decorator that counts the chunks read after the event loop ran a tick since the chunk before,
 * so a test can tell whether a wait loop paused between two reads without timing it (#176).
 *
 * A wait loop that drains a frame arriving in many chunks without pausing (#119) reads them back to back:
 * each read of an already buffered chunk is resolved through Revolt's microtask queue, which the loop
 * empties before it starts another tick, so no tick runs between those reads. A wait loop that pauses
 * after a read, with a 1 ms idle delay() say, waits for a timer, and the loop runs a tick before the next
 * read. After each chunk this decorator defers a callback, which runs in the loop's next tick and marks
 * it; a chunk read once the mark is set counts as read after a tick. The count follows from the order in
 * which the loop runs callbacks, not from how fast the machine is.
 *
 * The deferred callback is unreferenced, so it never keeps the loop running, and a test that counts the
 * referenced callbacks left behind does not see one that is still pending when a test ends.
 *
 * Empty reads ('') are passed through and not counted: they are the idle reads a wait loop pauses after.
 */
final class LoopTickCountingTransport implements TlsAwareTransportInterface
{
    /** Chunks read since the last {@see reset()} after the loop ran a tick since the chunk before. */
    public int $chunksReadAfterALoopTick = 0;

    /** Non-empty chunks read since the last {@see reset()}. */
    public int $chunksRead = 0;

    /** Whether the loop ran a tick since the last chunk was read. */
    private bool $tickedSinceTheLastChunk = false;

    /** The deferred callback that marks the next tick after the last chunk, until it runs. */
    private ?string $tickMark = null;

    public function __construct(private readonly TlsAwareTransportInterface $inner) {}

    /**
     * Starts counting afresh: the next chunk read is the first one, which nothing before it can count.
     */
    public function reset(): void
    {
        $this->chunksReadAfterALoopTick = 0;
        $this->chunksRead = 0;
        $this->tickedSinceTheLastChunk = false;
    }

    public function connect(string $dsn, int $timeoutMs): Future
    {
        return $this->inner->connect($dsn, $timeoutMs);
    }

    public function upgradeTls(): Future
    {
        return $this->inner->upgradeTls();
    }

    public function tlsActive(): bool
    {
        return $this->inner->tlsActive();
    }

    public function write(string $bytes): Future
    {
        return $this->inner->write($bytes);
    }

    public function readLine(?Cancellation $cancellation = null): Future
    {
        return $this->inner->readLine($cancellation)->map(function (string $chunk): string {
            if ($chunk === '') {
                return $chunk;
            }

            if ($this->chunksRead > 0 && $this->tickedSinceTheLastChunk) {
                $this->chunksReadAfterALoopTick++;
            }

            $this->chunksRead++;
            $this->tickedSinceTheLastChunk = false;
            // A fresh mark for this chunk. A mark still pending from the chunk before means no tick has run
            // since; cancelling it keeps one pending at most, and is a no-op if something else cancelled it.
            if ($this->tickMark !== null) {
                EventLoop::cancel($this->tickMark);
            }
            $this->tickMark = EventLoop::unreference(EventLoop::defer(function (): void {
                $this->tickMark = null;
                $this->tickedSinceTheLastChunk = true;
            }));

            return $chunk;
        });
    }

    public function close(): Future
    {
        return $this->inner->close();
    }
}
