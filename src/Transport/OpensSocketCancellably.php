<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\Socket\ConnectContext;
use Amp\Socket\Socket;

use function Amp\async;
use function Amp\Socket\connect;

/**
 * Opens a transport's socket through Amp's connector so that a cancellation stops the dial at once
 * ({@see CancellableDialTransportInterface}).
 */
trait OpensSocketCancellably
{
    /**
     * Opens a socket to $uri, ending as soon as $cancellation fires. Amp hands the cancellation to each TCP
     * attempt, but not to the pause its retry connector takes between attempts (2 s, then 4 s), so the dial
     * is awaited here instead; a socket Amp still opens once that pause is over is closed rather than kept,
     * since a newer dial may own the transport by then.
     */
    private function openSocket(string $uri, ConnectContext $context, ?Cancellation $cancellation): Socket
    {
        if ($cancellation === null) {
            return connect($uri, $context);
        }

        $dial = async(static fn(): Socket => connect($uri, $context, $cancellation));
        try {
            return $dial->await($cancellation);
        } catch (CancelledException $stopped) {
            $dial->map(static function (Socket $socket): void {
                $socket->close();
            })->ignore();

            throw $stopped;
        }
    }
}
