<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Support;

use Amp\Future;
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
 *
 * What a test opens belongs to its {@see TestResourceScope} (#183, {@see OwnsTestResources}): the helpers below register
 * each connection they make, and a connection the test adds to $opened is registered at tearDown. The shutdown closes
 * each, joins its close, releases the fixture gates registered with the scope and checks that nothing is left running,
 * under one deadline, where it used to disconnect each within a second, swallow whatever that threw, and wait 50 ms.
 */
trait ReconnectScenarios
{
    use OwnsTestResources;

    /** @var list<NatsConnection|NatsClient> */
    private array $opened = [];

    private function closeOpenedConnections(): void
    {
        foreach ($this->opened as $connection) {
            $this->own($connection);
        }
        $this->opened = [];

        $this->releaseOwnedResources();
    }

    private function connect(
        ReconnectingTransport|UncancellableDialTransport $transport,
        bool $waitForReconnect = true,
        int $requestTimeoutMs = 2_000,
        int $maxReconnectAttempts = 1_000,
        int|float $pingIntervalSeconds = 0,
        int $maxPingsOut = 2,
        ?\Closure $connectionListener = null,
        int $reconnectDelayMs = 5,
        int $reconnectMaxDelayMs = 20,
        ?\Closure $errorListener = null,
        int $connectTimeoutMs = 500,
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
                $connectTimeoutMs,
            ),
            $transport,
        );
        $this->opened[] = $this->own($connection);
        $connection->connect()->await();

        return $connection;
    }

    private function connectClient(
        ReconnectingTransport $transport,
        int|float $pingIntervalSeconds = 0,
        int $maxPingsOut = 2,
    ): NatsClient {
        $client = new NatsClient($this->options(true, 2_000, 1_000, $pingIntervalSeconds, $maxPingsOut, null, 5, 20, null), $transport);
        $this->opened[] = $this->own($client);
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
        int $connectTimeoutMs = 500,
    ): NatsOptions {
        return new NatsOptions(
            connectTimeoutMs: $connectTimeoutMs,
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
     * issues its operation. Returns that reader; it completes when the recovery does. The reader is an
     * operation's read without a subscription of its own ({@see loseConnection()}).
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

    /**
     * Kills the live session and lets a background reader notice it, returning that reader (its frame count) once the
     * connection is Connecting. The reader is an operation's read without a subscription of its own: it runs the
     * reconnect inline and waits for all of it, like your own read, but continues no remainder before it reads and,
     * with the default options, reports a handler's failure rather than throwing it. A test that left the rest of a
     * subscription queued behind a handler that threw, to have a backlog when the reconnect or a drain starts, keeps
     * it queued for them: your own read would continue it before it read (#186).
     *
     * @return Future<int>
     */
    private function loseConnection(NatsConnection|NatsClient $connection, ReconnectingTransport $transport): Future
    {
        $transport->dropConnection();

        $reader = async(static fn(): int => $connection->readIncomingForOperation()->await()->frames);
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
