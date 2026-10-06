<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Future;
use Amp\Socket\ServerSocket;
use Amp\Socket\Socket;

use function Amp\async;
use function Amp\Socket\listen;

/**
 * Just enough of a NATS server on a real loopback TCP socket for a client to connect through the built-in
 * AmpSocketTransport: it greets the first client with INFO, answers its PINGs, records the sid of each SUB, and sends
 * whatever bytes the test gives it, when the test gives them. For a test that has to show what a real socket read does,
 * which a scripted transport only models.
 */
final class LoopbackNatsServer
{
    public const INFO = 'INFO {"server_id":"L1","server_name":"loopback","version":"2.12.0","max_payload":1048576,"headers":true}' . "\r\n";

    /** @var array<string, int> Subject => sid of the SUBs the client wrote. */
    private array $sids = [];

    private string $inbound = '';

    /** The client's socket, once it connected. */
    private ?Socket $peer = null;

    /** @var Future<void> */
    private Future $serving;

    private function __construct(private readonly ServerSocket $server)
    {
        $this->serving = async($this->serve(...));
        $this->serving->ignore();
    }

    public static function start(): self
    {
        return new self(listen('tcp://127.0.0.1:0'));
    }

    /** The URL a client connects to. */
    public function url(): string
    {
        return 'nats://' . $this->server->getAddress()->toString();
    }

    /** The sid the client subscribed for $subject, once the server has read its SUB. */
    public function sidFor(string $subject): ?int
    {
        return $this->sids[$subject] ?? null;
    }

    /**
     * Sends raw bytes to the connected client in one write, which returns at once unless the socket is backed up: a
     * caller can send from inside the client's own code without that code suspending.
     */
    public function send(string $bytes): void
    {
        if ($this->peer === null) {
            throw new \LogicException('No client is connected');
        }

        $this->peer->write($bytes);
    }

    public function close(): void
    {
        $this->server->close();
        $this->peer?->close();
    }

    private function serve(): void
    {
        $peer = $this->server->accept();
        if ($peer === null) {
            return;
        }

        $this->peer = $peer;
        $peer->write(self::INFO);

        while (($chunk = $peer->read()) !== null) {
            $this->inbound .= $chunk;
            $this->answer($peer);
        }
    }

    /** Answers every complete line the client has sent: a PONG for each PING, and a record of each SUB. */
    private function answer(Socket $peer): void
    {
        while (($end = strpos($this->inbound, "\r\n")) !== false) {
            $line = substr($this->inbound, 0, $end);
            $parts = explode(' ', $line);
            $op = strtoupper($parts[0]);
            $skip = $end + 2;

            if ($op === 'PUB' || $op === 'HPUB') {
                // The payload follows the line: wait for all of it before going on.
                $size = (int) $parts[count($parts) - 1];
                if (strlen($this->inbound) < $skip + $size + 2) {
                    return;
                }

                $skip += $size + 2;
            }

            $this->inbound = substr($this->inbound, $skip);

            if ($op === 'PING') {
                $peer->write("PONG\r\n");
            } elseif ($op === 'SUB') {
                $this->sids[$parts[1]] = (int) $parts[count($parts) - 1];
            }
        }
    }
}
