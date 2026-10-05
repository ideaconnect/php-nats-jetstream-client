<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Revolt\EventLoop;

/**
 * A referenced timer that keeps the event loop running while a test runs, standing in for the rest of an
 * application - its sockets and timers. Without one, an await that never resumes ends the test with Revolt's
 * "Event loop terminated without resuming the current suspension" instead of reaching the timeout the test
 * set, and the assertion that says what went wrong.
 *
 * The test case calls {@see keepEventLoopAlive()} from its setUp() and {@see letEventLoopEnd()} from its
 * tearDown().
 */
trait KeepsEventLoopAlive
{
    private ?string $keepAliveTimer = null;

    private function keepEventLoopAlive(float $seconds = 30.0): void
    {
        $this->keepAliveTimer = EventLoop::delay($seconds, static function (): void {});
    }

    private function letEventLoopEnd(): void
    {
        if ($this->keepAliveTimer !== null) {
            EventLoop::cancel($this->keepAliveTimer);
            $this->keepAliveTimer = null;
        }
    }
}
