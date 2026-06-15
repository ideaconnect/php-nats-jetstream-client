<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Cancellation;
use Amp\Future;
use IDCT\NATS\Transport\TlsAwareTransportInterface;

use function Amp\async;

/**
 * A transport decorator that passes everything through to a real inner transport but can drop
 * individual inbound protocol frames, simulating a genuine on-the-wire delivery loss against a live
 * server. Used to force a JetStream ordered-consumer sequence gap so the automatic recreate +
 * server-side replay path can be validated end-to-end (#86/#87).
 *
 * readLine() re-frames the raw byte stream: it accumulates bytes, splits out each COMPLETE NATS frame
 * (control lines, and MSG/HMSG with their declared payloads) at exact byte boundaries, invokes the
 * drop predicate per frame, and returns the surviving frames verbatim. An incomplete trailing frame is
 * retained for the next read. Peer EOF (TransportClosedException) and cancellation propagate unchanged.
 */
final class DroppingTransport implements TlsAwareTransportInterface
{
    private string $buffer = '';

    /**
     * @param \Closure(string):bool $shouldDrop Receives each complete inbound frame's raw bytes;
     *                                          returning true drops that frame.
     */
    public function __construct(
        private readonly TlsAwareTransportInterface $inner,
        private readonly \Closure $shouldDrop,
    ) {}

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

    public function close(): Future
    {
        return $this->inner->close();
    }

    public function readLine(?Cancellation $cancellation = null): Future
    {
        return async(function () use ($cancellation): string {
            $chunk = $this->inner->readLine($cancellation)->await();
            if ($chunk === '') {
                return '';
            }

            $this->buffer .= $chunk;

            $out = '';
            while (($length = $this->frameLength($this->buffer)) !== null) {
                $frame = substr($this->buffer, 0, $length);
                $this->buffer = substr($this->buffer, $length);

                if (($this->shouldDrop)($frame)) {
                    continue;
                }

                $out .= $frame;
            }

            // '' here means every complete frame in this read was dropped (or none completed yet) -
            // a valid "no bytes available" result per the transport contract.
            return $out;
        });
    }

    /**
     * Returns the byte length of the first complete NATS frame in $buffer, or null if it is not yet
     * fully buffered. Control frames end at the first CRLF; MSG/HMSG additionally carry a declared
     * payload (the last control-line token) followed by a trailing CRLF.
     */
    private function frameLength(string $buffer): ?int
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
        $payloadBytes = (int) ($tokens[count($tokens) - 1] ?? 0);
        $total = $headerLength + $payloadBytes + 2;

        return strlen($buffer) >= $total ? $total : null;
    }
}
