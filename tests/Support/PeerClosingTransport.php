<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Cancellation;
use Amp\Future;
use IDCT\NATS\Transport\CancellableDialTransportInterface;
use IDCT\NATS\Transport\PeerCloseAwareTransportInterface;

/**
 * A {@see ReconnectingTransport} whose server can close a session without the client reading the close, the way a
 * server that stops (`docker stop`, a restart, a failover) closes a socket nobody reads: {@see closeUnread()}.
 *
 * From then on, until the next dial, the session is half gone, as a real socket in CLOSE-WAIT is:
 *   - {@see peerClosed()} answers true, as the built-in transports' peek does;
 *   - a write is still taken, and reaches no server: its bytes go to {@see $lost}, not to the server's record;
 *   - a read finds the close (EOF), and ends the session.
 * A read already waiting when the close comes does not see it; the tests that use this make none.
 */
final class PeerClosingTransport implements CancellableDialTransportInterface, PeerCloseAwareTransportInterface
{
    /** @var list<string> What the client wrote into the closed session: taken by the socket, received by no server. */
    public array $lost = [];

    /** How many times the connection asked {@see peerClosed()}. */
    public int $peerChecks = 0;

    /** The epoch of the session the server closed unread, or null. */
    private ?int $closedEpoch = null;

    public function __construct(public readonly ReconnectingTransport $inner) {}

    /** The server closes the live session; the client has not read the close. */
    public function closeUnread(): void
    {
        $this->closedEpoch = $this->inner->epoch();
    }

    public function peerClosed(): bool
    {
        $this->peerChecks++;

        return $this->closedUnread();
    }

    public function connect(string $dsn, int $timeoutMs, ?Cancellation $cancellation = null): Future
    {
        return $this->inner->connect($dsn, $timeoutMs, $cancellation);
    }

    public function upgradeTls(): Future
    {
        return $this->inner->upgradeTls();
    }

    public function write(string $bytes): Future
    {
        if ($this->closedUnread()) {
            $this->lost[] = $bytes;

            return Future::complete();
        }

        return $this->inner->write($bytes);
    }

    public function readLine(?Cancellation $cancellation = null): Future
    {
        if ($this->closedUnread()) {
            $this->inner->dropConnection();
        }

        return $this->inner->readLine($cancellation);
    }

    public function close(): Future
    {
        return $this->inner->close();
    }

    private function closedUnread(): bool
    {
        return $this->closedEpoch === $this->inner->epoch() && $this->inner->sessionLive();
    }
}
