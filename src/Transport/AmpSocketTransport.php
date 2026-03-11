<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

use Amp\Future;
use Amp\Socket\Certificate;
use Amp\Socket\ClientTlsContext;
use Amp\Socket\ConnectContext;
use Amp\Socket\Socket;
use IDCT\NATS\Connection\NatsOptions;
use function Amp\async;
use function Amp\Socket\connect;

final class AmpSocketTransport implements TransportInterface
{
    private ?Socket $socket = null;

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
            $context = (new ConnectContext())->withConnectTimeout($timeoutMs / 1000);
            $context = $this->withTlsContext($context, $dsn);
            $this->socket = connect($dsn, $context);

            if ($this->options->tlsHandshakeFirst) {
                $this->socket->setupTls();
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
    public function readLine(): Future
    {
        return async(function (): string {
            $chunk = $this->socket?->read();
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
}
