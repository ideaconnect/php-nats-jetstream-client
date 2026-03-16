<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Cancellation;
use Amp\Future;
use IDCT\NATS\Transport\TransportInterface;
use function Amp\async;

final class FakeTransport implements TransportInterface
{
    /** @var list<string> */
    public array $connectCalls = [];

    /** @var list<string> */
    public array $writes = [];

    public bool $closed = false;

    /**
     * Creates an in-memory fake transport with pre-seeded read chunks.
     *
     * @param list<string> $readQueue
     */
    public function __construct(private array $readQueue = [])
    {
    }

    /**
     * Records connect calls for assertions.
     */
    public function connect(string $dsn, int $timeoutMs): Future
    {
        return async(function () use ($dsn, $timeoutMs): void {
            $this->connectCalls[] = $dsn . '|' . $timeoutMs;
        });
    }

    /**
     * Records writes for assertions.
     */
    public function write(string $bytes): Future
    {
        return async(function () use ($bytes): void {
            $this->writes[] = $bytes;
        });
    }

    /**
     * Returns the next queued read chunk.
     */
    public function readLine(?Cancellation $cancellation = null): Future
    {
        return async(function (): string {
            return array_shift($this->readQueue) ?? '';
        });
    }

    /**
     * Marks fake transport as closed.
     */
    public function close(): Future
    {
        return async(function (): void {
            $this->closed = true;
        });
    }
}
