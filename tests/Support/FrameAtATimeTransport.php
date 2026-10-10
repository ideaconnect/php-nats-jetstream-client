<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Cancellation;
use Amp\Future;
use IDCT\NATS\Transport\TlsAwareTransportInterface;

use function Amp\async;

/**
 * A transport decorator over a real inner transport that hands the client one complete inbound protocol frame per read
 * (#211): what the server sends in one TCP segment still reaches the client one frame at a time, so that a test against
 * a live server gets a deliberate read boundary between two frames, rather than a timing that a sleep only makes likely.
 * The pull consumer engine then reads a terminal status in a read of its own, whatever came behind it on the socket.
 *
 * readLine() returns the next complete frame it holds (a control line, or a MSG/HMSG with its declared payload) without
 * reading, or reads the inner transport until one is complete; an incomplete trailing frame is kept for the next read.
 * Peer EOF (TransportClosedException) and cancellation propagate unchanged. Writes, TLS and close pass through.
 */
final class FrameAtATimeTransport implements TlsAwareTransportInterface
{
    private string $buffer = '';

    public function __construct(private readonly TlsAwareTransportInterface $inner) {}

    public function connect(string $dsn, int $timeoutMs): Future
    {
        $this->buffer = '';

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

    public function close(): Future
    {
        return $this->inner->close();
    }

    public function readLine(?Cancellation $cancellation = null): Future
    {
        return async(function () use ($cancellation): string {
            while (($length = self::frameLength($this->buffer)) === null) {
                $chunk = $this->inner->readLine($cancellation)->await();
                if ($chunk === '') {
                    return '';
                }

                $this->buffer .= $chunk;
            }

            $frame = substr($this->buffer, 0, $length);
            $this->buffer = substr($this->buffer, $length);

            return $frame;
        });
    }

    /** The length of the complete frame $buffer starts with, or null while it holds no complete frame. */
    private static function frameLength(string $buffer): ?int
    {
        $eol = strpos($buffer, "\r\n");
        if ($eol === false) {
            return null;
        }

        $line = substr($buffer, 0, $eol);
        $headerLength = $eol + 2;
        $space = strpos($line, ' ');
        $verb = strtoupper($space === false ? $line : substr($line, 0, $space));
        if ($verb !== 'MSG' && $verb !== 'HMSG') {
            return $headerLength;
        }

        $tokens = preg_split('/\s+/', trim($line));
        $tokens = $tokens === false ? [] : $tokens;
        $total = $headerLength + (int) ($tokens[count($tokens) - 1] ?? 0) + 2;

        return strlen($buffer) >= $total ? $total : null;
    }
}
