<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Cancellation;
use Amp\Future;
use IDCT\NATS\Transport\TransportInterface;
use RuntimeException;
use function Amp\async;

final class FlakyTransport implements TransportInterface
{
    /** @var list<string> */
    public array $connectCalls = [];

    /** @var list<string> */
    public array $writes = [];

    /** @var list<list<string>> */
    private array $readQueuesByConnection;

    private int $successfulConnects = 0;
    private int $connectAttempts = 0;
    private int $remainingConnectFailures;
    private int $remainingReadFailures;

    /**
     * Creates a transport that can fail selected connect/read operations for reconnect tests.
     *
     * @param list<list<string>> $readQueuesByConnection
     */
    public function __construct(
        array $readQueuesByConnection,
        int $connectFailures = 0,
        int $readFailures = 0,
    ) {
        $this->readQueuesByConnection = $readQueuesByConnection;
        $this->remainingConnectFailures = $connectFailures;
        $this->remainingReadFailures = $readFailures;
    }

    /**
     * Connects or throws according to configured failure counters.
     */
    public function connect(string $dsn, int $timeoutMs): Future
    {
        return async(function () use ($dsn, $timeoutMs): void {
            $this->connectAttempts++;
            $this->connectCalls[] = $dsn . '|' . $timeoutMs;

            if ($this->remainingConnectFailures > 0) {
                $this->remainingConnectFailures--;
                throw new RuntimeException('connect failed');
            }

            $this->successfulConnects++;
        });
    }

    /**
     * Records outgoing protocol writes.
     */
    public function write(string $bytes): Future
    {
        return async(function () use ($bytes): void {
            $this->writes[] = $bytes;
        });
    }

    /**
     * Reads from the queue assigned to the current successful connection.
     */
    public function readLine(?Cancellation $cancellation = null): Future
    {
        return async(function (): string {
            if ($this->remainingReadFailures > 0) {
                $this->remainingReadFailures--;
                throw new RuntimeException('read failed');
            }

            $index = max(0, $this->successfulConnects - 1);
            if (!isset($this->readQueuesByConnection[$index])) {
                return '';
            }

            $next = array_shift($this->readQueuesByConnection[$index]) ?? '';
            if ($next === '__THROW__') {
                throw new RuntimeException('read failed');
            }

            return $next;
        });
    }

    /**
     * No-op close for test transport.
     */
    public function close(): Future
    {
        return async(function (): void {
        });
    }
}
