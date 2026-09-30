<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Cancellation;
use Amp\Future;
use IDCT\NATS\Transport\TransportInterface;

/**
 * A custom transport that cannot stop its dial: it implements only {@see TransportInterface}, over a
 * {@see ReconnectingTransport} that the test drives. The connection cannot cancel its dial, so a close has
 * to wait for the dial to end.
 */
final class UncancellableDialTransport implements TransportInterface
{
    public function __construct(public readonly ReconnectingTransport $inner) {}

    public function connect(string $dsn, int $timeoutMs): Future
    {
        return $this->inner->connect($dsn, $timeoutMs);
    }

    public function upgradeTls(): Future
    {
        return $this->inner->upgradeTls();
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
