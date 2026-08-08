<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\ByteStream\ClosedException;
use Amp\ByteStream\ReadableStreamIteratorAggregate;
use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Socket\InternetAddress;
use Amp\Socket\Socket;
use Amp\Socket\SocketAddress;
use Amp\Socket\TlsInfo;
use Amp\Socket\TlsState;
use Revolt\EventLoop;

/**
 * In-memory {@see Socket} double whose write() SUSPENDS the calling fiber until close(), mirroring
 * Amp's WritableResourceStream backpressure semantics exactly: a write on a full buffer parks the
 * fiber via a suspension (it does not throw), and only free()/close() wakes pending writes - with a
 * {@see ClosedException}. Used to pin that transport close() paths never deadlock on a
 * backpressure-wedged courtesy write (the WS Close frame) before reaching the socket close.
 *
 * @implements \IteratorAggregate<int, string>
 */
final class WedgedWriteSocket implements Socket, \IteratorAggregate
{
    use ReadableStreamIteratorAggregate;

    private bool $closed = false;

    private int $readIndex = 0;

    /**
     * @param list<string> $chunks Optional scripted read chunks (one per read(), then EOF), so a
     *                             test can deliver inbound frames whose ANSWER write wedges.
     */
    public function __construct(private readonly array $chunks = []) {}

    /** Number of write() calls that were attempted (each suspends until close()). */
    private int $writeAttempts = 0;

    /** @var list<DeferredFuture<never>> Writes currently suspended, failed out by close(). */
    private array $pendingWrites = [];

    /**
     * Referenced keep-alive watcher active while writes are suspended. A real Amp socket with queued
     * writes keeps its referenced writable watcher enabled - which is what lets UNREFERENCED timers
     * (every TimeoutCancellation) still fire while a write is parked. Without mirroring that, the
     * event loop would terminate the moment every fiber is suspended and wrongly report a deadlock.
     * A bounded delay (not an eternal repeat) on purpose: if a test fails before close() runs, a
     * leaked eternal referenced watcher would wedge the whole PHPUnit process at exit (Revolt drives
     * referenced callbacks in its shutdown handler); the delay self-expires instead.
     */
    private ?string $keepAliveId = null;

    /** Upper bound (seconds) any single test may keep a write suspended; leaked watchers self-expire. */
    private const KEEP_ALIVE_SECONDS = 10.0;

    /** @var list<\Closure(): void> */
    private array $onClose = [];

    #[\Override]
    public function read(?Cancellation $cancellation = null, ?int $limit = null): ?string
    {
        $cancellation?->throwIfRequested();

        if ($this->closed || $this->readIndex >= count($this->chunks)) {
            return null;
        }

        return $this->chunks[$this->readIndex++];
    }

    #[\Override]
    public function write(string $bytes): void
    {
        if ($this->closed) {
            throw new ClosedException('The wedged socket is closed');
        }

        $this->writeAttempts++;

        $this->keepAliveId ??= EventLoop::delay(self::KEEP_ALIVE_SECONDS, static function (): void {});

        /** @var DeferredFuture<never> $pending */
        $pending = new DeferredFuture();
        $this->pendingWrites[] = $pending;

        // Suspends this fiber until close() errors the deferred - exactly like a real Amp socket
        // write parked on a zero-window peer, which only free()/close() ever wakes.
        $pending->getFuture()->await();
    }

    /** How many write() calls were attempted (and suspended). */
    public function writeAttempts(): int
    {
        return $this->writeAttempts;
    }

    #[\Override]
    public function end(): void
    {
        $this->close();
    }

    #[\Override]
    public function isReadable(): bool
    {
        return !$this->closed;
    }

    #[\Override]
    public function isWritable(): bool
    {
        return !$this->closed;
    }

    #[\Override]
    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;

        if ($this->keepAliveId !== null) {
            EventLoop::cancel($this->keepAliveId);
            $this->keepAliveId = null;
        }

        // Fail every suspended write out, mirroring WritableResourceStream::free().
        $pending = $this->pendingWrites;
        $this->pendingWrites = [];
        foreach ($pending as $deferred) {
            $deferred->error(new ClosedException('The socket was closed before writing completed'));
        }

        foreach ($this->onClose as $onClose) {
            $onClose();
        }
    }

    #[\Override]
    public function isClosed(): bool
    {
        return $this->closed;
    }

    #[\Override]
    public function onClose(\Closure $onClose): void
    {
        if ($this->closed) {
            $onClose();

            return;
        }

        $this->onClose[] = $onClose;
    }

    #[\Override]
    public function getLocalAddress(): SocketAddress
    {
        return new InternetAddress('127.0.0.1', 0);
    }

    #[\Override]
    public function getRemoteAddress(): SocketAddress
    {
        return new InternetAddress('127.0.0.1', 0);
    }

    #[\Override]
    public function setupTls(?Cancellation $cancellation = null): void
    {
        throw new \RuntimeException('WedgedWriteSocket does not support TLS');
    }

    #[\Override]
    public function shutdownTls(?Cancellation $cancellation = null): void
    {
        throw new \RuntimeException('WedgedWriteSocket does not support TLS');
    }

    #[\Override]
    public function isTlsConfigurationAvailable(): bool
    {
        return false;
    }

    #[\Override]
    public function getTlsState(): TlsState
    {
        return TlsState::Disabled;
    }

    #[\Override]
    public function getTlsInfo(): ?TlsInfo
    {
        return null;
    }
}
