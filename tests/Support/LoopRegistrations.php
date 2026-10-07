<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Revolt\EventLoop;

/**
 * Counts the callbacks the event loop registers - timers, deferred callbacks, stream and signal watchers - so that a
 * test can tell a fiber that waits for a future from one that polls on a timer, wherever in the code it polls, without
 * timing anything. A fiber parked on a future registers nothing while it waits; one that pauses a millisecond between
 * two looks registers a timer per look, hundreds in a second. Microtasks (EventLoop::queue(), which async() and the
 * callbacks of a completed future go through) are not registered callbacks and do not count.
 *
 * Revolt numbers its callbacks with one string increment per registration (a, b, ..., z, aa, ab, ...), in every
 * driver, so the identifier of a fresh callback says how many were registered before it. Each count registers and
 * cancels a deferred callback of its own to read it, and leaves its own probes out of the count. A driver that
 * numbered callbacks differently would break the decode, and the counter says so rather than count wrongly.
 *
 * Sibling of {@see CountingBuffer} and {@see CountingCancellation}, which count the looks of one operation.
 */
final class LoopRegistrations
{
    /** Probes this counter made so far, each a registration of its own. */
    private int $probes = 0;

    /** How many callbacks the event loop has registered so far, this counter's own probes left out. */
    public function soFar(): int
    {
        $id = EventLoop::defer(static function (): void {});
        EventLoop::cancel($id);

        if (preg_match('/^[a-z]+$/', $id) !== 1) {
            throw new \LogicException(sprintf('Revolt numbers its callbacks differently now ("%s"): LoopRegistrations cannot count them', $id));
        }

        // Bijective base 26: "a" is the first registration, "z" the 26th, "aa" the 27th.
        $ordinal = 0;
        foreach (str_split($id) as $letter) {
            $ordinal = $ordinal * 26 + (ord($letter) - ord('a') + 1);
        }

        return $ordinal - 1 - $this->probes++;
    }
}
