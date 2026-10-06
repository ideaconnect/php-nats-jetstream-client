<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\Future;
use IDCT\NATS\Transport\CancellableDialTransportInterface;

/**
 * A pass-through decorator of a transport that counts the reads of the socket and calls {@see $onRead} as each one
 * starts. A read reaches the transport only once it holds the connection's read slot, so the call tells a test that a
 * read now waits on the socket, and the count how many reads did, without timing anything.
 */
final class WatchedTransport implements CancellableDialTransportInterface
{
    /** Reads of the socket so far. */
    public int $reads = 0;

    /** Reads of the socket so far that their cancellation ended, with nothing read. */
    public int $cancelledReads = 0;

    /** Reads of the socket under way right now. */
    public int $readsUnderWay = 0;

    /** @var (\Closure(): void)|null Called as each read of the socket starts, before the transport reads. */
    public ?\Closure $onRead = null;

    public function __construct(private readonly CancellableDialTransportInterface $inner) {}

    public function connect(string $dsn, int $timeoutMs, ?Cancellation $cancellation = null): Future
    {
        return $this->inner->connect($dsn, $timeoutMs, $cancellation);
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
        $this->readsUnderWay++;
        if ($this->onRead !== null) {
            ($this->onRead)();
        }

        return $this->inner->readLine($cancellation)->catch(function (\Throwable $failure): never {
            if ($failure instanceof CancelledException) {
                $this->cancelledReads++;
            }

            throw $failure;
        })->finally(function (): void {
            $this->readsUnderWay--;
        });
    }

    public function close(): Future
    {
        return $this->inner->close();
    }
}
