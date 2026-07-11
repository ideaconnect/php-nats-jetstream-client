<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Future;
use IDCT\NATS\Transport\TlsAwareTransportInterface;
use IDCT\NATS\Transport\TransportClosedException;

use function Amp\async;
use function Amp\delay;

final class FakeTransport implements TlsAwareTransportInterface
{
    /** Queue sentinel: a readLine() that dequeues this value throws TransportClosedException (EOF). */
    public const EOF = '__EOF__';

    /** When true, upgradeTls() marks TLS active (models a transport with TLS materials configured). */
    public bool $canUpgrade = false;

    /** Whether a TLS handshake has "completed" (set by upgradeTls() when $canUpgrade). */
    public bool $tlsActive = false;

    /** When true, connect() marks TLS active - models a handshake-first transport with TLS materials. */
    public bool $tlsActiveOnConnect = false;

    /** @var list<string> */
    public array $connectCalls = [];

    /** @var list<string> */
    public array $writes = [];

    /** When set, write() throws a TransportClosedException for bytes containing this substring. */
    public ?string $throwOnWriteContaining = null;

    /**
     * Chunks appended to the read queue when write() sees bytes containing the key (each key fires
     * at most once). Lets a test hold a reply back until the request that consumes it is actually
     * written - e.g. so an earlier request's timeout wait cannot drain it from a static queue.
     *
     * @var array<string, list<string>>
     */
    public array $enqueueOnWriteContaining = [];

    /** When > 0, close() suspends this many seconds before completing (see close()). */
    public float $closeDelay = 0.0;

    public bool $closed = false;

    public int $upgradeTlsCalls = 0;

    // Diagnostics for blocking-mode reads (see $blockWhenEmpty).
    public int $startedReads = 0;
    public int $resolvedReads = 0;
    public ?bool $lastReadHadCancellation = null;

    /**
     * Creates an in-memory fake transport with pre-seeded read chunks.
     *
     * @param list<string> $readQueue
     * @param bool $blockWhenEmpty When true, readLine() on an exhausted queue mirrors a real socket:
     *                             it suspends until the supplied cancellation fires (or forever when
     *                             the cancellation is null) instead of returning '' immediately.
     * @param string|null $holdChunkContaining When set, readLine() delays $holdSeconds (ignoring the
     *                             cancellation) before returning a chunk containing this substring -
     *                             used to reproduce "reply delivered as the deadline fires" races.
     */
    public function __construct(
        private array $readQueue = [],
        private bool $blockWhenEmpty = false,
        private ?string $holdChunkContaining = null,
        private float $holdSeconds = 0.0,
    ) {}

    /**
     * Records connect calls for assertions.
     */
    public function connect(string $dsn, int $timeoutMs): Future
    {
        return async(function () use ($dsn, $timeoutMs): void {
            $this->connectCalls[] = $dsn . '|' . $timeoutMs;

            if ($this->tlsActiveOnConnect) {
                // Handshake-first transports negotiate TLS during connect(), before INFO is read.
                $this->tlsActive = true;
            }
        });
    }

    /**
     * Records TLS upgrade requests for assertions.
     */
    public function upgradeTls(): Future
    {
        return async(function (): void {
            $this->upgradeTlsCalls++;
            $this->tlsActive = $this->canUpgrade;
        });
    }

    public function tlsActive(): bool
    {
        return $this->tlsActive;
    }

    /**
     * Records writes for assertions.
     */
    public function write(string $bytes): Future
    {
        return async(function () use ($bytes): void {
            if ($this->throwOnWriteContaining !== null && str_contains($bytes, $this->throwOnWriteContaining)) {
                throw new TransportClosedException('Simulated write failure');
            }

            $this->writes[] = $bytes;

            foreach ($this->enqueueOnWriteContaining as $needle => $chunks) {
                if (str_contains($bytes, $needle)) {
                    array_push($this->readQueue, ...$chunks);
                    unset($this->enqueueOnWriteContaining[$needle]);
                }
            }
        });
    }

    /**
     * Returns the next queued read chunk.
     */
    public function readLine(?Cancellation $cancellation = null): Future
    {
        return async(function () use ($cancellation): string {
            if ($this->readQueue !== []) {
                $chunk = (string) array_shift($this->readQueue);
                if ($chunk === self::EOF) {
                    throw new TransportClosedException('Socket closed by peer (EOF)');
                }

                if ($this->holdChunkContaining !== null && $this->holdSeconds > 0.0 && str_contains($chunk, $this->holdChunkContaining)) {
                    // Hold this chunk past a request deadline (ignoring the cancellation) so the
                    // caller observes the reply arriving in the same tick the deadline expires.
                    delay($this->holdSeconds);
                }

                return $chunk;
            }

            if (!$this->blockWhenEmpty) {
                return '';
            }

            // Faithfully model a live but idle socket: suspend until cancelled (or forever if no
            // cancellation was supplied, mirroring AmpSocketTransport::readLine(null)).
            $this->startedReads++;
            $this->lastReadHadCancellation = $cancellation !== null;

            try {
                if ($cancellation === null) {
                    (new DeferredFuture())->getFuture()->await();
                }

                delay(PHP_INT_MAX, cancellation: $cancellation);
            } finally {
                $this->resolvedReads++;
            }

            return '';
        });
    }

    /**
     * Marks fake transport as closed.
     */
    public function close(): Future
    {
        return async(function (): void {
            if ($this->closeDelay > 0) {
                // Models a slow socket teardown so tests can interleave fibers inside a terminal
                // path's close await (the window probed by #146's publish-race regression test).
                delay($this->closeDelay);
            }

            $this->closed = true;
        });
    }
}
