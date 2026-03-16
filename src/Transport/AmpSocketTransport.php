<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

use Amp\Cancellation;
use Amp\Future;
use Amp\TimeoutCancellation;
use Amp\Socket\Certificate;
use Amp\Socket\ClientTlsContext;
use Amp\Socket\ConnectContext;
use Amp\Socket\Socket;
use IDCT\NATS\Connection\NatsOptions;
use function Amp\async;
use function Amp\Socket\connect;

/**
 * Amp-based socket transport implementation for NATS connections.
 */
final class AmpSocketTransport implements TransportInterface
{
    private ?Socket $socket = null;

    /**
     * @param NatsOptions $options Client connection options controlling TLS and socket behavior.
     */
    public function __construct(private readonly NatsOptions $options = new NatsOptions())
    {
    }

    /**
     * Connects to a server DSN using Amp socket transport.
     */
    public function connect(string $dsn, int $timeoutMs): Future
    {
        return async(function () use ($dsn, $timeoutMs): void {
            // Amp expects timeout in seconds, while options use milliseconds.
            $context = (new ConnectContext())->withConnectTimeout(max(1, $timeoutMs) / 1000);
            $context = $this->withTlsContext($context, $dsn);
            $this->socket = connect($this->normalizeSocketUri($dsn), $context);

            if ($context->getTlsContext() !== null) {
                $this->socket->setupTls(new TimeoutCancellation(max(1, $timeoutMs) / 1000));
            }
        });
    }

    /**
     * Writes protocol bytes to the active socket.
     */
    public function write(string $bytes): Future
    {
        return async(function () use ($bytes): void {
            $this->socket?->write($bytes);
        });
    }

    /**
     * Reads the next available chunk from the active socket.
     */
    public function readLine(?Cancellation $cancellation = null): Future
    {
        return async(function () use ($cancellation): string {
            $chunk = $this->socket?->read($cancellation);
            return $chunk ?? '';
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

        return $dsn;
    }
}
