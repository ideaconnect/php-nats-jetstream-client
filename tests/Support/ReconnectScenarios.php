<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;

/**
 * Helpers for unit tests that take a connection over a {@see ReconnectingTransport} through outages.
 * The test case calls {@see closeOpenedConnections()} from its tearDown().
 */
trait ReconnectScenarios
{
    /** @var list<NatsConnection|NatsClient> */
    private array $opened = [];

    private function closeOpenedConnections(): void
    {
        foreach ($this->opened as $connection) {
            try {
                $connection->disconnect()->await(new TimeoutCancellation(1));
            } catch (\Throwable) {
                // Already closed, or closing races a recovery: nothing to tear down.
            }
        }

        $this->opened = [];
        // Let a recovery that was backing off observe the close before the next test runs.
        delay(0.05);
    }

    private function connect(
        ReconnectingTransport $transport,
        bool $waitForReconnect = true,
        int $requestTimeoutMs = 2_000,
        int $maxReconnectAttempts = 1_000,
        int|float $pingIntervalSeconds = 0,
        int $maxPingsOut = 2,
        ?\Closure $connectionListener = null,
        int $reconnectDelayMs = 5,
        int $reconnectMaxDelayMs = 20,
        ?\Closure $errorListener = null,
    ): NatsConnection {
        $connection = new NatsConnection(
            $this->options(
                $waitForReconnect,
                $requestTimeoutMs,
                $maxReconnectAttempts,
                $pingIntervalSeconds,
                $maxPingsOut,
                $connectionListener,
                $reconnectDelayMs,
                $reconnectMaxDelayMs,
                $errorListener,
            ),
            $transport,
        );
        $this->opened[] = $connection;
        $connection->connect()->await();

        return $connection;
    }

    private function connectClient(
        ReconnectingTransport $transport,
        int|float $pingIntervalSeconds = 0,
        int $maxPingsOut = 2,
    ): NatsClient {
        $client = new NatsClient($this->options(true, 2_000, 1_000, $pingIntervalSeconds, $maxPingsOut, null, 5, 20, null), $transport);
        $this->opened[] = $client;
        $client->connect()->await();

        return $client;
    }

    private function options(
        bool $waitForReconnect,
        int $requestTimeoutMs,
        int $maxReconnectAttempts,
        int|float $pingIntervalSeconds,
        int $maxPingsOut,
        ?\Closure $connectionListener,
        int $reconnectDelayMs,
        int $reconnectMaxDelayMs,
        ?\Closure $errorListener,
    ): NatsOptions {
        return new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: $requestTimeoutMs,
            reconnectEnabled: true,
            maxReconnectAttempts: $maxReconnectAttempts,
            reconnectDelayMs: $reconnectDelayMs,
            reconnectMaxDelayMs: $reconnectMaxDelayMs,
            reconnectJitterMs: 0,
            pingIntervalSeconds: $pingIntervalSeconds,
            maxPingsOut: $maxPingsOut,
            connectionListener: $connectionListener,
            errorListener: $errorListener,
            waitForReconnect: $waitForReconnect,
        );
    }

    /**
     * Kills the live session with dials refused and lets a background reader take the failure, so a
     * recovery owned by ANOTHER fiber is in flight - backing off between refused dials - when the test
     * issues its operation. Returns that reader; it completes when the recovery does.
     *
     * @return Future<int>
     */
    private function startRecoveryInBackground(NatsConnection|NatsClient $connection, ReconnectingTransport $transport): Future
    {
        $transport->refuseDials();

        return $this->loseConnection($connection, $transport);
    }

    /**
     * Like {@see startRecoveryInBackground()}, but the recovery's first dial is held (see
     * {@see ReconnectingTransport::holdNextDial()}): the recovery stays in flight, mid-dial, until the
     * test releases it.
     *
     * @return Future<int>
     */
    private function startRecoveryHeldMidDial(NatsConnection|NatsClient $connection, ReconnectingTransport $transport): Future
    {
        $transport->acceptDials();
        $transport->holdNextDial();

        return $this->loseConnection($connection, $transport);
    }

    /** @return Future<int> */
    private function loseConnection(NatsConnection|NatsClient $connection, ReconnectingTransport $transport): Future
    {
        $transport->dropConnection();

        $reader = async(static fn(): int => $connection->processIncoming()->await());
        $reader->ignore();
        $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Connecting);

        return $reader;
    }

    /** Lets dials through from a timer - which fires only while something hands the loop control. */
    private function acceptDialsAfter(ReconnectingTransport $transport, float $seconds): void
    {
        EventLoop::delay($seconds, static function () use ($transport): void {
            $transport->acceptDials();
        });
    }

    /** @param \Closure(): bool $condition */
    private function waitUntil(\Closure $condition, float $seconds = 2.0): void
    {
        $deadline = hrtime(true) + (int) ($seconds * 1e9);
        while (!$condition()) {
            if (hrtime(true) > $deadline) {
                self::fail('condition not reached within ' . $seconds . ' s');
            }

            delay(0.001);
        }
    }

    /** Lets the event loop run until a recovery the test does not own has reopened the connection. */
    private function waitUntilOpen(NatsConnection|NatsClient $connection): void
    {
        $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Open);
    }

    private function secondsSince(int $start): float
    {
        return (hrtime(true) - $start) / 1e9;
    }
}
