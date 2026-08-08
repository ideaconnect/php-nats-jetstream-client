<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\ByteStream\ClosedException;
use Amp\ByteStream\ReadableStreamIteratorAggregate;
use Amp\Cancellation;
use Amp\Socket\InternetAddress;
use Amp\Socket\Socket;
use Amp\Socket\SocketAddress;
use Amp\Socket\TlsInfo;
use Amp\Socket\TlsState;

/**
 * In-memory {@see Socket} double that hands out exactly one scripted chunk per {@see read()} call, in
 * order, then reports EOF. Unlike a loopback TCP socket - whose kernel buffering coalesces small writes
 * into arbitrary read sizes - this guarantees the transport observes the exact read boundaries under
 * test, so read-boundary torture pins (#164) can force a header or mask key to span multiple reads.
 *
 * @implements \IteratorAggregate<int, string>
 */
final class ScriptedChunkSocket implements Socket, \IteratorAggregate
{
    use ReadableStreamIteratorAggregate;

    private int $index = 0;
    private bool $closed = false;

    /** Bytes the transport wrote back (e.g. a PONG); retained only so writes do not throw. */
    private string $written = '';

    /** @var list<\Closure(): void> */
    private array $onClose = [];

    /**
     * @param list<string> $chunks One chunk returned per read(), in order; read() returns null (EOF)
     *                             once they are exhausted.
     * @param bool $failWrites When true every write() throws ClosedException, emulating a peer that
     *                         died between delivering its last chunk and the client's answer write.
     */
    public function __construct(
        private readonly array $chunks,
        private readonly bool $failWrites = false,
    ) {}

    #[\Override]
    public function read(?Cancellation $cancellation = null, ?int $limit = null): ?string
    {
        $cancellation?->throwIfRequested();

        if ($this->closed || $this->index >= count($this->chunks)) {
            return null;
        }

        return $this->chunks[$this->index++];
    }

    #[\Override]
    public function write(string $bytes): void
    {
        if ($this->closed) {
            throw new ClosedException('The scripted socket is closed');
        }

        if ($this->failWrites) {
            throw new ClosedException('The scripted socket rejects writes');
        }

        $this->written .= $bytes;
    }

    /** Bytes the transport wrote back over this socket (masked PONGs, close frames). */
    public function writtenBytes(): string
    {
        return $this->written;
    }

    #[\Override]
    public function end(): void
    {
        $this->close();
    }

    #[\Override]
    public function isReadable(): bool
    {
        return !$this->closed && $this->index < count($this->chunks);
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
        throw new \RuntimeException('ScriptedChunkSocket does not support TLS');
    }

    #[\Override]
    public function shutdownTls(?Cancellation $cancellation = null): void
    {
        throw new \RuntimeException('ScriptedChunkSocket does not support TLS');
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
