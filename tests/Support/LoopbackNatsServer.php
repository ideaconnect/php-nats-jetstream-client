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
 * AmpSocketTransport: it greets each client connection with INFO, answers its PINGs, records the sid of each SUB and
 * the control line of each PUB/HPUB, and sends whatever bytes the test gives it, when the test gives them. For a test
 * that has to show what a real socket read or write does, which a scripted transport only models.
 *
 * It accepts one connection after another, a reconnect's included; {@see send()} and {@see closeClient()} act on the
 * latest.
 */
final class LoopbackNatsServer
{
    public const INFO = 'INFO {"server_id":"L1","server_name":"loopback","version":"2.12.0","max_payload":1048576,"headers":true}' . "\r\n";

    /** @var array<string, int> Subject => sid of the SUBs the client wrote. */
    private array $sids = [];

    /** @var list<array{int, string}> The connection (counted from 0) and control line of each PUB/HPUB received. */
    private array $published = [];

    /** Client connections accepted so far. */
    private int $connections = 0;

    /** The client's latest socket, once it connected. */
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

    /** How many client connections the server has accepted. */
    public function connections(): int
    {
        return $this->connections;
    }

    /**
     * The control line of each PUB/HPUB received, with the connection (counted from 0) it came on.
     *
     * @return list<array{int, string}>
     */
    public function published(): array
    {
        return $this->published;
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

    /** Closes the latest client connection from the server's side, as a server that stops does; keeps listening. */
    public function closeClient(): void
    {
        $this->peer?->close();
    }

    public function close(): void
    {
        $this->server->close();
        $this->peer?->close();
    }

    private function serve(): void
    {
        while (($peer = $this->server->accept()) !== null) {
            $this->peer = $peer;
            $connection = $this->connections++;
            async(fn() => $this->session($peer, $connection))->ignore();
        }
    }

    private function session(Socket $peer, int $connection): void
    {
        $peer->write(self::INFO);

        $inbound = '';
        while (($chunk = $peer->read()) !== null) {
            $inbound .= $chunk;
            $this->answer($peer, $inbound, $connection);
        }
    }

    /** Answers every complete line the client has sent: a PONG for each PING, and a record of each SUB and PUB. */
    private function answer(Socket $peer, string &$inbound, int $connection): void
    {
        while (($end = strpos($inbound, "\r\n")) !== false) {
            $line = substr($inbound, 0, $end);
            $parts = explode(' ', $line);
            $op = strtoupper($parts[0]);
            $skip = $end + 2;

            if ($op === 'PUB' || $op === 'HPUB') {
                // The payload follows the line: wait for all of it before going on.
                $size = (int) $parts[count($parts) - 1];
                if (strlen($inbound) < $skip + $size + 2) {
                    return;
                }

                $skip += $size + 2;
                $this->published[] = [$connection, $line];
            }

            $inbound = substr($inbound, $skip);

            if ($op === 'PING') {
                $peer->write("PONG\r\n");
            } elseif ($op === 'SUB') {
                $this->sids[$parts[1]] = (int) $parts[count($parts) - 1];
            }
        }
    }
}
