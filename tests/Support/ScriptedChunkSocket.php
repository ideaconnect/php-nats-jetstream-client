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

    /** Whether the responseFromWritten hook already produced its (single) synthesized first read. */
    private bool $responded = false;

    /** Bytes the transport wrote back (e.g. a PONG); retained only so writes do not throw. */
    private string $written = '';

    /**
     * Counted BEFORE the closed/failWrites checks, so a caller can pin "no write was even attempted"
     * - writtenBytes() alone cannot: a post-close write throws before recording, so its bytes never
     * change whether or not the caller regressed into re-attempting the write.
     */
    private int $writeAttempts = 0;

    /** @var list<\Closure(): void> */
    private array $onClose = [];

    /**
     * @param list<string> $chunks One chunk returned per read(), in order; read() returns null (EOF)
     *                             once they are exhausted.
     * @param bool $failWrites When true every write() throws ClosedException, emulating a peer that
     *                         died between delivering its last chunk and the client's answer write.
     * @param null|\Closure(string): string $responseFromWritten Optional request->response probe: when
     *        set, the FIRST read() returns this closure's value computed from the bytes written so far
     *        (then the scripted $chunks follow). Lets a handshake test answer deterministically to a
     *        request containing a randomly generated Sec-WebSocket-Key, with exact read boundaries a
     *        loopback TCP socket cannot guarantee.
     */
    public function __construct(
        private readonly array $chunks,
        private readonly bool $failWrites = false,
        private readonly ?\Closure $responseFromWritten = null,
    ) {}

    #[\Override]
    public function read(?Cancellation $cancellation = null, ?int $limit = null): ?string
    {
        $cancellation?->throwIfRequested();

        if ($this->closed) {
            return null;
        }

        if ($this->responseFromWritten !== null && !$this->responded) {
            $this->responded = true;

            return ($this->responseFromWritten)($this->written);
        }

        if ($this->index >= count($this->chunks)) {
            return null;
        }

        return $this->chunks[$this->index++];
    }

    #[\Override]
    public function write(string $bytes): void
    {
        $this->writeAttempts++;

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

    /** How many write() calls were attempted, including those rejected as closed/failing. */
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
