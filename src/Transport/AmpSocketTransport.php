<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

use Amp\Cancellation;
use Amp\Future;
use Amp\Socket\Certificate;
use Amp\Socket\ClientTlsContext;
use Amp\Socket\ConnectContext;
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

            if ($this->tlsContextConfigured && $this->options->tlsHandshakeFirst) {
                $this->establishTls();
            }
        });
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
     */
    public function write(string $bytes): Future
    {
        return async(function () use ($bytes): void {
            $socket = $this->socket;
            if ($socket === null) {
                // A silent no-op here would confirm publishes/ACKs that never reached any socket
                // (#124); throwing lets callers buffer, join the in-flight recovery, or fail loudly.
                throw new TransportClosedException('Transport is not connected');
            }

            $socket->write($bytes);
        });
    }

    /**
     * Reads the next available chunk from the active socket.
     *
     * Distinguishes three outcomes: a non-empty chunk of bytes; an empty string when there is no
     * socket yet (so handshake/idle polling can continue); and a {@see TransportClosedException}
     * when the peer closes a live socket (EOF), so the connection layer can reconnect from the read
     * path. A read timeout surfaces as an Amp CancelledException from the underlying read, never as
     * EOF, so a bounded read is not mistaken for a peer close.
     */
    public function readLine(?Cancellation $cancellation = null): Future
    {
        return async(function () use ($cancellation): string {
            if ($this->socket === null) {
                return '';
            }

            $chunk = $this->socket->read($cancellation);
            if ($chunk === null) {
                throw new TransportClosedException('Socket closed by peer (EOF)');
            }

            return $chunk;
        });
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

        $peerName = $this->options->tlsPeerName;
        if ($peerName === null || $peerName === '') {
            $peerName = (string) (parse_url($dsn, PHP_URL_HOST) ?? '');
        }

        $tlsContext = new ClientTlsContext($peerName);

        if (!$this->options->tlsVerifyPeer) {
            $tlsContext = $tlsContext->withoutPeerVerification();
        }

        if ($this->options->tlsCaFile !== null && $this->options->tlsCaFile !== '') {
            $tlsContext = $tlsContext->withCaFile($this->options->tlsCaFile);
        }

        if ($this->options->tlsCertFile !== null && $this->options->tlsCertFile !== '') {
            $tlsContext = $tlsContext->withCertificate(new Certificate(
                $this->options->tlsCertFile,
                $this->options->tlsKeyFile,
                $this->options->tlsKeyPassphrase,
            ));
        }

        return $context->withTlsContext($tlsContext);
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
