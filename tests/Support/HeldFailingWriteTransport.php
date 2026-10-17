<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Future;
use IDCT\NATS\Transport\CancellableDialTransportInterface;

/**
 * Holds back the next write containing a needle, for a test that has to act in the very tick the write fails: the
 * write never reaches the transport this wraps, and its writer resumes, failing with the error the test gives, only
 * when the test calls {@see failHeldWrite()}. The writer's resumption is queued by that call, so what the test
 * starts right after it - a disconnect(), a queued callback - runs once the writer's catch has run and suspended,
 * and before what that catch queued (#244).
 *
 * Everything else is passed through.
 */
final class HeldFailingWriteTransport implements CancellableDialTransportInterface
{
    private ?string $needle = null;

    /** @var ?DeferredFuture<null> */
    private ?DeferredFuture $held = null;

    public function __construct(private readonly CancellableDialTransportInterface $inner) {}

    /** Holds back the next write containing $needle until {@see failHeldWrite()}. One-shot. */
    public function holdNextWriteContaining(string $needle): void
    {
        $this->needle = $needle;
    }

    /** Whether a write is held back right now. */
    public function writeHeld(): bool
    {
        return $this->held !== null;
    }

    /** Fails the write held back with $error. */
    public function failHeldWrite(\Throwable $error): void
    {
        $held = $this->held;
        $this->held = null;
        $held?->error($error);
    }

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
        if ($this->needle === null || !str_contains($bytes, $this->needle)) {
            return $this->inner->write($bytes);
        }

        $this->needle = null;
        /** @var DeferredFuture<null> $held */
        $held = new DeferredFuture();
        $this->held = $held;

        return $held->getFuture();
    }

    public function readLine(?Cancellation $cancellation = null): Future
    {
        return $this->inner->readLine($cancellation);
    }

    public function close(): Future
    {
        return $this->inner->close();
    }
}
