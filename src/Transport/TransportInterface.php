<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

use Amp\Cancellation;
use Amp\Future;

/**
 * Abstraction for asynchronous socket operations used by the NATS connection.
 */
interface TransportInterface
{
    /**
     * Establishes a transport connection to the target DSN.
     *
     * @return Future<void>
     */
    public function connect(string $dsn, int $timeoutMs): Future;

    /**
     * Writes raw protocol bytes to the transport.
     *
     * @return Future<void>
     */
    public function write(string $bytes): Future;

    /**
     * Reads a raw chunk from the transport.
     *
     * @return Future<string>
     */
    public function readLine(?Cancellation $cancellation = null): Future;

    /**
     * Closes the transport and underlying resources.
     *
     * @return Future<void>
     */
    public function close(): Future;
}
