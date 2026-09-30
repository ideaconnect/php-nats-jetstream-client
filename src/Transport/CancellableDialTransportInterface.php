<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

use Amp\Cancellation;
use Amp\Future;

/**
 * A transport whose dial can be stopped. The connection hands it a cancellation that fires when the
 * application closes the connection - disconnect() or drain() - while a connect() or a reconnect is
 * dialling, so that the close is not left waiting for the dial, or the dial left running after the close.
 *
 * Both built-in transports implement it. A custom transport that implements only {@see TransportInterface}
 * is dialled as before, and a close waits for its dial to end, up to the connect timeout.
 */
interface CancellableDialTransportInterface extends TransportInterface
{
    /**
     * Establishes a transport connection to the target DSN, like {@see TransportInterface::connect()}.
     *
     * @param Cancellation|null $cancellation Once it fires, the dial must end promptly, failing, and must
     *        leave no connection behind: a connection it opens anyway is closed, not kept.
     *
     * @return Future<void>
     */
    public function connect(string $dsn, int $timeoutMs, ?Cancellation $cancellation = null): Future;
}
