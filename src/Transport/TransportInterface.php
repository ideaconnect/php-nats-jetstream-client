<?php

declare(strict_types=1);

namespace Idct\Nats\Transport;

use Amp\Future;

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
    public function readLine(): Future;

    /**
     * Closes the transport and underlying resources.
     *
     * @return Future<void>
     */
    public function close(): Future;
}
