<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Cancellation;
use Amp\Future;
use IDCT\NATS\Transport\TransportInterface;

use function Amp\async;
use function Amp\delay;

/**
 * A pass-through decorator of a transport that counts the reads of the socket, and pauses every read past a cap for
 * 1 ms before it reads.
 *
 * A wait loop that reads again at once after a read that consumed nothing spins on a transport whose idle read returns
 * '' at once ({@see FakeTransport} in its default mode, a custom {@see TransportInterface}): such a read resolves without
 * the event loop reaching its timers, so no timer fires any more, the loop's own deadline included, and a test of it
 * would hang. Past the cap each read hands the event loop control, so the loop ends at its deadline instead, and the
 * count tells the spin from a loop that pauses on an idle read: a pausing loop stays far below the cap.
 */
final class SpinBreakingTransport implements TransportInterface
{
    /** Reads of the socket so far. */
    public int $reads = 0;

    public function __construct(private readonly TransportInterface $inner, private readonly int $readsBeforePausing) {}

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
        return $this->inner->write($bytes);
    }

    public function readLine(?Cancellation $cancellation = null): Future
    {
        $this->reads++;
        if ($this->reads <= $this->readsBeforePausing) {
            return $this->inner->readLine($cancellation);
        }

        return async(function () use ($cancellation): string {
            delay(0.001, cancellation: $cancellation);

            return $this->inner->readLine($cancellation)->await();
        });
    }

    public function close(): Future
    {
        return $this->inner->close();
    }
}
