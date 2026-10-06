<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Cancellation;
use Amp\DeferredCancellation;
use Amp\Socket\ConnectContext;
use Amp\Socket\ConnectException;
use Amp\Socket\DnsSocketConnector;
use Amp\Socket\RetrySocketConnector;
use Amp\Socket\Socket;
use Amp\Socket\SocketAddress;
use Amp\Socket\SocketConnector;
use Revolt\EventLoop;

/**
 * A socket connector that dials through Amp's {@see DnsSocketConnector} and stops the dial once an attempt fails,
 * so a test can place the stop inside the pause that Amp's {@see RetrySocketConnector} takes between attempts
 * without a timer (#176).
 *
 * Install it as the retry connector's delegate, with the {@see DeferredCancellation} whose cancellation the dial
 * runs under. When an attempt fails with a {@see ConnectException} (a refused port, say), it queues the stop as a
 * microtask and rethrows. The retry connector catches the exception and starts its pause before the microtask can
 * run: nothing suspends in between, and Revolt runs every queued microtask before the next timer or I/O callback.
 * So the stop lands at the start of the pause, which lasts 2 s after the first attempt, however late timers fire
 * and however long the attempt took to fail. A dial still held by the pause ends about 2 s or more after the stop;
 * a dial that honours the stop ends in the same event-loop turn.
 */
final class StopsDialOnRefusalConnector implements SocketConnector
{
    /** When the stop fired, as {@see hrtime()} nanoseconds, or null while it has not. */
    public int|float|null $stoppedAt = null;

    public function __construct(private readonly DeferredCancellation $stop) {}

    public function connect(SocketAddress|string $uri, ?ConnectContext $context = null, ?Cancellation $cancellation = null): Socket
    {
        try {
            return (new DnsSocketConnector())->connect($uri, $context, $cancellation);
        } catch (ConnectException $failed) {
            EventLoop::queue(function (): void {
                $this->stoppedAt ??= hrtime(true);
                $this->stop->cancel();
            });

            throw $failed;
        }
    }
}
