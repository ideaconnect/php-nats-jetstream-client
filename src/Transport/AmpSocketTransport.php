<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

use Amp\Cancellation;
use Amp\Future;
use Amp\Socket\ConnectContext;
use Amp\Socket\ResourceSocket;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;

use function Amp\async;
use function Amp\Socket\connect;

/**
 * Amp-based socket transport implementation for NATS connections.
 */
final class AmpSocketTransport implements TlsAwareTransportInterface
{
    use AssemblesClientTlsContext;

    private ?Socket $socket = null;

    /** Whether the connect context carries a TLS context (TLS is required for this connection). */
    private bool $tlsContextConfigured = false;

    /** Whether the TLS handshake has already been performed on the current socket. */
    private bool $tlsEstablished = false;

    /** Last connect timeout in milliseconds, reused to bound a deferred TLS handshake. */
    private int $lastConnectTimeoutMs = 5_000;

    /**
     * @param NatsOptions $options Client connection options controlling TLS and socket behavior.
     */
    public function __construct(private readonly NatsOptions $options = new NatsOptions()) {}

    /**
     * Connects to a server DSN using Amp socket transport.
     *
     * When TLS is required the TLS context is always configured on the socket, but the handshake is
     * performed immediately only for handshake-first connections. Otherwise the handshake is
     * deferred to {@see upgradeTls()} so the standard "read INFO, then upgrade" flow is supported.
     */
    public function connect(string $dsn, int $timeoutMs): Future
    {
        return async(function () use ($dsn, $timeoutMs): void {
            // Amp expects timeout in seconds, while options use milliseconds.
            $this->lastConnectTimeoutMs = max(1, $timeoutMs);
            $this->tlsEstablished = false;

            // Disable Nagle's algorithm: NATS is a small-message request/reply protocol, and Nagle
            // combined with delayed ACKs adds ~40ms of latency per round trip otherwise.
            $context = (new ConnectContext())
                ->withConnectTimeout($this->lastConnectTimeoutMs / 1000)
                ->withTcpNoDelay();
            $context = $this->withTlsContext($context, $dsn);
            $this->tlsContextConfigured = $context->getTlsContext() !== null;

            $this->socket = connect($this->normalizeSocketUri($dsn), $context);
            $this->applyReadChunkSize();

            if ($this->tlsContextConfigured && $this->options->tlsHandshakeFirst) {
                $this->establishTls();
            }
        });
    }

    /**
     * Raises the socket read chunk size to the configured value (default 128 KiB, up from Amp's 8 KiB
     * default) so a large inbound payload arrives in far fewer chunks, dividing the per-chunk syscall +
     * fiber-spawn + parser-push overhead 8-32x (#119). A no-op for a Socket implementation that is not
     * a ResourceSocket, since setChunkSize is not on the Socket interface. This raises only the MAXIMUM
     * a read may return; the socket still returns just what is available, so small messages are
     * unaffected.
     */
    private function applyReadChunkSize(): void
    {
        $chunkSize = $this->options->readChunkSizeBytes;
        // setChunkSize only exists on ResourceSocket (not the Socket interface), hence the type check.
        // The `> 0` also narrows int to int<1, max> for setChunkSize()'s signature; NatsOptions already
        // rejects a non-positive readChunkSizeBytes, so the branch is never false in practice.
        if ($this->socket instanceof ResourceSocket && $chunkSize > 0) {
            $this->socket->setChunkSize($chunkSize);
        }
    }

    /**
     * Performs the deferred TLS handshake (standard post-INFO upgrade). No-op when TLS is not
     * configured or was already negotiated handshake-first.
     */
    public function upgradeTls(): Future
    {
        return async(function (): void {
            $this->establishTls();
        });
    }

    /**
     * Reports whether a TLS handshake has completed on the current socket.
     */
    public function tlsActive(): bool
    {
        return $this->tlsEstablished;
    }

    /**
     * Runs the TLS handshake on the current socket exactly once.
     */
    private function establishTls(): void
    {
        // No socket (standalone pre-connect call) or already negotiated: nothing to do.
        if ($this->socket === null || $this->tlsEstablished) {
            return;
        }

        if (!$this->tlsContextConfigured) {
            // The connection layer asked to upgrade (the server/options require TLS) but no TLS
            // materials were configured at connect time. Fail fast instead of leaving the socket
            // plaintext - otherwise the subsequent CONNECT would send credentials in the clear.
            throw new TlsRequiredException(
                'TLS upgrade requested but no TLS context was configured at connect time; '
                . 'set NatsOptions tlsRequired (or use a tls:// server and provide CA/cert materials) '
                . 'when the server requires TLS',
            );
        }

        $this->socket->setupTls(new TimeoutCancellation($this->lastConnectTimeoutMs / 1000));
        $this->tlsEstablished = true;
    }

    /**
     * Writes protocol bytes to the active socket.
     *
     * Runs inline in the caller's fiber and returns an already-resolved future (#136): every
     * caller awaits immediately, so an async() wrapper only added a Future allocation plus a fiber
     * suspend/resume per write. Amp's WritableResourceStream::write() may suspend this fiber on
     * backpressure, which the immediate-await contract makes safe. Failures surface through the
     * returned future - never a synchronous throw - so callers observe them exactly as before.
     */
    public function write(string $bytes): Future
    {
        $socket = $this->socket;
        if ($socket === null) {
            // A silent no-op here would confirm publishes/ACKs that never reached any socket
            // (#124); erroring lets callers buffer, join the in-flight recovery, or fail loudly.
            return Future::error(new TransportClosedException('Transport is not connected'));
        }

        try {
            $socket->write($bytes);
        } catch (\Throwable $e) {
            return Future::error($e);
        }

        return Future::complete();
    }

    /**
     * Reads the next available chunk from the active socket.
     *
     * Distinguishes three outcomes: a non-empty chunk of bytes; an empty string when there is no
     * socket yet (so handshake/idle polling can continue); and a {@see TransportClosedException}
     * when the peer closes a live socket (EOF), so the connection layer can reconnect from the read
     * path. A read timeout surfaces as an Amp CancelledException from the underlying read, never as
     * EOF, so a bounded read is not mistaken for a peer close.
     *
     * Runs inline in the caller's fiber and returns an already-resolved future (#163, mirroring the
     * write() path #136): every caller awaits the result immediately, so wrapping the read in async()
     * only added a second fiber (plus a Future allocation) per inbound chunk on top of the caller's
     * own read fiber. Amp's read() suspends this fiber until bytes, EOF, or cancellation - the
     * immediate-await contract makes that safe, exactly as it is for write()'s backpressure suspend.
     * All outcomes still surface through the returned future - never a synchronous throw - so the
     * cancellation/EOF/empty semantics callers depend on are byte-for-byte unchanged: a passed
     * Cancellation still reaches the underlying read (its CancelledException flows back through the
     * future), EOF still becomes TransportClosedException, and a missing socket still yields ''.
     */
    public function readLine(?Cancellation $cancellation = null): Future
    {
        $socket = $this->socket;
        if ($socket === null) {
            return Future::complete('');
        }

        try {
            $chunk = $socket->read($cancellation);
        } catch (\Throwable $e) {
            // Surface read failures (including a read timeout's CancelledException) through the
            // future so callers observe them via await(), never as a synchronous throw.
            return Future::error($e);
        }

        if ($chunk === null) {
            return Future::error(new TransportClosedException('Socket closed by peer (EOF)'));
        }

        return Future::complete($chunk);
    }

    /**
     * Closes the socket and clears transport state.
     */
    public function close(): Future
    {
        return async(function (): void {
            $this->socket?->close();
            $this->socket = null;
        });
    }

    private function withTlsContext(ConnectContext $context, string $dsn): ConnectContext
    {
        // Escape hatch: a caller-supplied TLS context is used verbatim (and implies TLS-required), so
        // advanced setups (in-memory PEM, ALPN, custom verification) bypass option-driven assembly.
        if ($this->options->tlsContext !== null) {
            return $context->withTlsContext($this->options->tlsContext);
        }

        $dsnScheme = strtolower((string) (parse_url($dsn, PHP_URL_SCHEME) ?? ''));
        $requiresTls = $this->options->tlsRequired || $dsnScheme === 'tls';

        if (!$requiresTls) {
            return $context;
        }

        $peerHost = (string) (parse_url($dsn, PHP_URL_HOST) ?? '');

        return $context->withTlsContext($this->clientTlsContextFromOptions($this->options, $peerHost));
    }

    /**
     * Rewrites supported NATS URI schemes into socket schemes accepted by Amp.
     */
    private function normalizeSocketUri(string $dsn): string
    {
        if (str_starts_with($dsn, 'tls://')) {
            return 'tcp://' . substr($dsn, strlen('tls://'));
        }

        // Accept the NATS scheme directly so the transport is usable standalone (the connection
        // layer normalizes nats:// before calling connect(), but a direct caller may not).
        if (str_starts_with($dsn, 'nats://')) {
            return 'tcp://' . substr($dsn, strlen('nats://'));
        }

        return $dsn;
    }
}
