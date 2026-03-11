<?php

declare(strict_types=1);

namespace Idct\Nats\Transport;

use Amp\Future;
use Amp\Socket\ConnectContext;
use Amp\Socket\Socket;
use function Amp\async;
use function Amp\Socket\connect;

final class AmpSocketTransport implements TransportInterface
{
    private ?Socket $socket = null;

    /**
     * Connects to a server DSN using Amp socket transport.
     */
    public function connect(string $dsn, int $timeoutMs): Future
    {
        return async(function () use ($dsn, $timeoutMs): void {
            // Amp expects timeout in seconds, while options use milliseconds.
            $context = (new ConnectContext())->withConnectTimeout($timeoutMs / 1000);
            $this->socket = connect($dsn, $context);
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
}
