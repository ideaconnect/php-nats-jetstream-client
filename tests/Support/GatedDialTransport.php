<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Cancellation;
use Amp\Future;
use IDCT\NATS\Transport\TlsAwareTransportInterface;
use IDCT\NATS\Transport\TransportClosedException;

/**
 * A transport decorator whose dials can be refused, modelling a server that is down: while refused,
 * connect() fails like a refused TCP connection instead of reaching the inner transport, so a client
 * recovery stays in its backoff loop. Composes with {@see SeveringTransport} in live tests - sever the
 * socket, refuse dials, and let the dials through again when the "server" is back.
 */
final class GatedDialTransport implements TlsAwareTransportInterface
{
    /** Dials refused so far. */
    public int $refusedDials = 0;

    private bool $refusing = false;

    public function __construct(private readonly TlsAwareTransportInterface $inner) {}

    public function refuseDials(): void
    {
        $this->refusing = true;
    }

    public function acceptDials(): void
    {
        $this->refusing = false;
    }

    /** Force-closes the live socket when the inner transport can (see {@see SeveringTransport::sever()}). */
    public function sever(): void
    {
        if (!$this->inner instanceof SeveringTransport) {
            throw new \LogicException('sever() requires an inner SeveringTransport');
        }

        $this->inner->sever();
    }

    public function connect(string $dsn, int $timeoutMs): Future
    {
        if ($this->refusing) {
            $this->refusedDials++;

            return Future::error(new TransportClosedException('Connection refused (dials are gated)'));
        }

        return $this->inner->connect($dsn, $timeoutMs);
    }

    public function upgradeTls(): Future
    {
        return $this->inner->upgradeTls();
    }

    public function tlsActive(): bool
    {
        return $this->inner->tlsActive();
    }

    public function write(string $bytes): Future
    {
        return $this->inner->write($bytes);
    }

    public function readLine(?Cancellation $cancellation = null): Future
    {
        return $this->inner->readLine($cancellation);
    }

    public function close(): Future
    {
        return $this->inner->close();
    }
}
