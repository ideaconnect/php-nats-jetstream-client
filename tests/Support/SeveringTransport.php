<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Cancellation;
use Amp\Future;
use Amp\Socket\Socket;
use IDCT\NATS\Transport\AmpSocketTransport;
use IDCT\NATS\Transport\TlsAwareTransportInterface;

/**
 * A transport decorator that passes everything through to a real AmpSocketTransport but can
 * force-close ("sever") the live TCP socket mid-session, simulating a server that dies or a
 * connection reset against a live server. Sibling of {@see DroppingTransport}, which can only drop
 * complete frames and never kills the socket (#141).
 *
 * sever() closes the underlying Amp socket directly while leaving the inner transport's socket
 * reference in place, so the client's NEXT read goes through the production readLine() path against
 * a genuinely dead TCP connection and observes a real EOF (translated by AmpSocketTransport into
 * TransportClosedException) - exactly what a mid-session server close produces. The decorator stays
 * usable afterwards: connect() delegates to the inner transport, which dials a fresh socket, so the
 * client's reconnect path works through this same instance.
 */
final class SeveringTransport implements TlsAwareTransportInterface
{
    public function __construct(private readonly AmpSocketTransport $inner) {}

    /**
     * Force-closes the inner transport's live socket. The inner transport keeps referencing the
     * (now dead) socket, so its next readLine() observes the real EOF instead of the "no socket"
     * empty read that inner close() would produce. A pending read is woken with the same EOF.
     */
    public function sever(): void
    {
        $property = new \ReflectionProperty(AmpSocketTransport::class, 'socket');
        $socket = $property->getValue($this->inner);
        if (!$socket instanceof Socket) {
            throw new \LogicException('sever() requires a connected inner transport with a live socket');
        }

        $socket->close();
    }

    public function connect(string $dsn, int $timeoutMs): Future
    {
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
