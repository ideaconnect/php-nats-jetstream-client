<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Future;
use IDCT\NATS\Transport\TransportClosedException;
use IDCT\NATS\Transport\TransportInterface;
use Revolt\EventLoop;

use function Amp\async;

/**
 * {@see TransportInterface} double whose writes SUCCEED for the first N calls (handshake and setup)
 * and then SUSPEND forever - mirroring a peer that stalls with a full send buffer, where Amp's
 * socket write parks the calling fiber until the socket is closed. close() fails the parked writes
 * out with a {@see TransportClosedException}, exactly like a real socket teardown. Used to pin the
 * bounded-wait contracts of drain()/flush() against write-side backpressure.
 */
final class WedgedWriteTransport implements TransportInterface
{
    /** @var list<string> */
    public array $writes = [];

    private int $readIndex = 0;
    private bool $closed = false;

    /** @var list<DeferredFuture<never>> */
    private array $pendingWrites = [];

    /**
     * Bounded referenced keep-alive while writes are parked, so unreferenced timers (every
     * TimeoutCancellation) still fire; self-expires so a failing test cannot wedge the PHPUnit
     * process at exit (Revolt drives referenced callbacks in its shutdown handler).
     */
    private ?string $keepAliveId = null;

    /**
     * @param list<string> $readChunks One chunk per readLine() call, then '' (idle).
     * @param int $wedgeAfterWrites Writes beyond this count suspend until close().
     */
    public function __construct(
        private readonly array $readChunks,
        private readonly int $wedgeAfterWrites,
    ) {}

    #[\Override]
    public function connect(string $dsn, int $timeoutMs): Future
    {
        return async(static function (): void {});
    }

    #[\Override]
    public function write(string $bytes): Future
    {
        if ($this->closed) {
            return Future::error(new TransportClosedException('Transport is closed'));
        }

        $this->writes[] = $bytes;

        if (count($this->writes) <= $this->wedgeAfterWrites) {
            return Future::complete();
        }

        // Backpressure wedge: park the caller until close() fails the write out, exactly like a
        // real Amp socket write on a zero-window peer. Runs inline in the caller's fiber (the
        // real transports' write() contract), so the suspension happens HERE, not in a wrapper.
        $this->keepAliveId ??= EventLoop::delay(10.0, static function (): void {});

        /** @var DeferredFuture<never> $pending */
        $pending = new DeferredFuture();
        $this->pendingWrites[] = $pending;

        try {
            $pending->getFuture()->await();
        } catch (\Throwable $e) {
            // Failures surface through the returned future, never a synchronous throw - the same
            // contract as the real transports. The deferred can only ever error (close() fails it),
            // so this catch is the sole exit.
            return Future::error($e);
        }
    }

    #[\Override]
    public function upgradeTls(): Future
    {
        return async(static function (): void {});
    }

    #[\Override]
    public function readLine(?Cancellation $cancellation = null): Future
    {
        $cancellation?->throwIfRequested();

        if ($this->closed) {
            return Future::error(new TransportClosedException('Transport is closed'));
        }

        if ($this->readIndex < count($this->readChunks)) {
            return Future::complete($this->readChunks[$this->readIndex++]);
        }

        return Future::complete('');
    }

    #[\Override]
    public function close(): Future
    {
        $this->closed = true;

        if ($this->keepAliveId !== null) {
            EventLoop::cancel($this->keepAliveId);
            $this->keepAliveId = null;
        }

        $pending = $this->pendingWrites;
        $this->pendingWrites = [];
        foreach ($pending as $deferred) {
            $deferred->error(new TransportClosedException('The transport was closed before writing completed'));
        }

        return async(static function (): void {});
    }
}
