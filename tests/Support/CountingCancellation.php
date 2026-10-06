<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Cancellation;
use Amp\DeferredCancellation;

/** A caller's cancellation that counts how often it is asked whether it was requested: how often the caller looked. */
final class CountingCancellation implements Cancellation
{
    public int $isRequestedCalls = 0;

    private readonly DeferredCancellation $inner;

    public function __construct()
    {
        $this->inner = new DeferredCancellation();
    }

    public function subscribe(\Closure $callback): string
    {
        return $this->inner->getCancellation()->subscribe($callback);
    }

    public function unsubscribe(string $id): void
    {
        $this->inner->getCancellation()->unsubscribe($id);
    }

    public function isRequested(): bool
    {
        $this->isRequestedCalls++;

        return $this->inner->isCancelled();
    }

    public function throwIfRequested(): void
    {
        $this->inner->getCancellation()->throwIfRequested();
    }
}
