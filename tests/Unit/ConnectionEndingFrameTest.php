<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\SlowConsumerException;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Transport\TransportClosedException;
use PHPUnit\Framework\TestCase;

use function Amp\delay;

/**
 * A frame that says the connection is finished - a fatal -ERR, which the server sends right before it closes
 * the socket, or a PONG the socket would not take - is a failed connection, like a failed read: the
 * connection recovers from it, or closes for good with reconnect off, and the operation that read the frame
 * is the only one to fail (#171).
 *
 * It used to stay Open. A server drops a client that stops answering its pings with -ERR 'Stale Connection',
 * which a synchronous process that sat idle for a few minutes meets on its next call: that call failed with
 * the -ERR, and the next one wrote into the socket the server had closed and failed as well - with reconnect
 * on, only after waiting out its whole timeout.
 */
final class ConnectionEndingFrameTest extends TestCase
{
    use ReconnectScenarios;

    private const STALE = "-ERR 'Stale Connection'\r\n";

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /**
     * With reconnect off the -ERR closes the connection for good, once, and releases the socket. The read
     * that met it fails with the server's error, which says why the connection ended, rather than with
     * "Reconnect is disabled".
     */
    public function testFatalErrWithReconnectOffClosesTheConnection(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connectWithReconnectOff($transport, $recorder);
        $transport->pushFrame(self::STALE);

        try {
            $connection->processIncoming()->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame("Server sent error frame: 'Stale Connection'", $e->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Closed], $recorder->events, 'closed once');
        self::assertFalse($transport->sessionLive(), 'the socket is released');
    }

    /**
     * The operation after it fails at once, on the Closed connection, instead of writing into the socket the
     * server has closed.
     */
    public function testOperationAfterTheFatalErrWithReconnectOffFailsWithoutWritingToTheClosedSocket(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connectWithReconnectOff($transport, new LifecycleRecorder());
        $transport->pushFrame(self::STALE);
        try {
            $connection->processIncoming()->await();
        } catch (ConnectionException) {
            // The -ERR itself: the previous test covers it.
        }
        $writesBefore = count($transport->writes);

        try {
            $connection->publish('orders', 'o1')->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException) {
            // Refused by the Closed connection.
        }

        self::assertCount($writesBefore, $transport->writes, 'nothing was written into the old socket');
    }

    /**
     * With reconnect on the -ERR starts the reconnect, and the request that met it waits for it within its
     * own timeout before failing with the server's error: the next request then runs on the new connection.
     * Before, it was written into the old one, which the server no longer answered, and timed out.
     */
    public function testFatalErrWithReconnectOnReconnectsSoTheNextRequestSucceeds(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 1_000);
        $this->answerFirstRequestWithStaleConnection($transport);

        try {
            $connection->request('svc', 'one')->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame("Server sent error frame: 'Stale Connection'", $e->getMessage());
        }

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $transport->epoch(), 'the connection was re-established');
        self::assertSame('re: two', $connection->request('svc', 'two')->await()->payload);
    }

    /**
     * The request waits for that reconnect no longer than its own timeout allows, and fails with the
     * server's error even when the timeout ends the wait; the reconnect carries on without it.
     */
    public function testRequestThatMeetsAFatalErrWaitsForTheReconnectOnlyWithinItsTimeout(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 300);
        $this->answerFirstRequestWithStaleConnection($transport);
        $transport->holdNextDial();

        $start = hrtime(true);
        try {
            $connection->request('svc', 'one')->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame("Server sent error frame: 'Stale Connection'", $e->getMessage());
        }
        $elapsed = $this->secondsSince($start);

        self::assertGreaterThan(0.2, $elapsed, 'it waited for the reconnect');
        self::assertLessThan(1.0, $elapsed, 'but not past its own timeout');
        self::assertSame(ConnectionState::Connecting, $connection->state(), 'the reconnect carries on');

        $this->releaseTheHeldDial($transport);
        $this->waitUntilOpen($connection);
        self::assertSame('re: two', $connection->request('svc', 'two')->await()->payload);
    }

    /**
     * A request that may not wait for a reconnect does not wait for this one either: it fails at once, and
     * the reconnect has started by then, so the next operation does not reach the old socket.
     */
    public function testRequestThatMeetsAFatalErrWithWaitingDisabledFailsAtOnce(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, waitForReconnect: false, requestTimeoutMs: 2_000);
        $this->answerFirstRequestWithStaleConnection($transport);
        $transport->holdNextDial();

        $start = hrtime(true);
        try {
            $connection->request('svc', 'one')->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame("Server sent error frame: 'Stale Connection'", $e->getMessage());
        }

        self::assertLessThan(0.2, $this->secondsSince($start));
        self::assertSame(ConnectionState::Connecting, $connection->state());

        $this->releaseTheHeldDial($transport);
        $this->waitUntilOpen($connection);
        self::assertSame('re: two', $connection->request('svc', 'two')->await()->payload);
    }

    /**
     * With waiting disabled nothing waits for the reconnect the -ERR starts. When that reconnect gives up,
     * its failure stays on its own fiber: it must not escape to the event loop as an unhandled error, which
     * would stop the application's loop.
     */
    public function testReconnectThatNothingWaitsForGivesUpWithoutAnUnhandledError(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, waitForReconnect: false, maxReconnectAttempts: 1);
        $this->answerFirstRequestWithStaleConnection($transport);
        $transport->refuseDials();

        try {
            $connection->request('svc', 'one')->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame("Server sent error frame: 'Stale Connection'", $e->getMessage());
        }

        $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Closed);
        // The reconnect's future is collected once its fiber is done: an unhandled error would surface here.
        gc_collect_cycles();
        delay(0.05);
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * A fatal -ERR the heartbeat reads closes the connection the same way: left Open, it failed whichever
     * operation came next. It is still reported through the error listener.
     */
    public function testFatalErrReadByTheHeartbeatClosesTheConnectionWithReconnectOff(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = new NatsConnection(
            new NatsOptions(
                connectTimeoutMs: 500,
                reconnectEnabled: false,
                pingIntervalSeconds: 0.05,
                maxPingsOut: 3,
                connectionListener: $recorder->connectionListener(),
                errorListener: $recorder->errorListener(),
            ),
            $transport,
        );
        $this->opened[] = $connection;
        $connection->connect()->await();
        $transport->pushFrame(self::STALE);

        $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Closed);

        self::assertContains(ConnectionEvent::Closed, $recorder->events);
        self::assertNotSame([], $recorder->errorsContaining('Stale Connection'), 'the -ERR is still reported');
    }

    /**
     * A PONG the socket will not take ends the connection too. The read fails with the socket's error, as
     * before, but the connection no longer stays Open on that socket.
     */
    public function testServerPingWhosePongWriteFailsClosesTheConnectionWithReconnectOff(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connectWithReconnectOff($transport, new LifecycleRecorder());
        $transport->failNextWriteContaining('PONG');
        $transport->pushFrame("PING\r\n");

        try {
            $connection->processIncoming()->await();
            self::fail('expected TransportClosedException');
        } catch (TransportClosedException $e) {
            self::assertSame('The connection is gone', $e->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * A full subscription queue says that one subscriber fell behind, not that the connection is finished:
     * the read still fails with it, and the connection stays Open.
     */
    public function testFullSubscriptionQueueDoesNotEndTheConnection(): void
    {
        $transport = new ReconnectingTransport();
        $connection = new NatsConnection(
            new NatsOptions(
                connectTimeoutMs: 500,
                reconnectEnabled: false,
                pingIntervalSeconds: 0,
                maxPendingMessagesPerSubscription: 1,
                slowConsumerPolicy: SlowConsumerPolicy::Error,
            ),
            $transport,
        );
        $this->opened[] = $connection;
        $connection->connect()->await();
        $sid = $connection->subscribe('updates', static function (): void {})->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('updates', $sid, 'first') . ReconnectingTransport::msgFrame('updates', $sid, 'second'),
        );

        try {
            $connection->processIncoming()->await();
            self::fail('expected SlowConsumerException');
        } catch (SlowConsumerException $e) {
            self::assertSame($sid, $e->sid);
        }

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertTrue($transport->sessionLive());
    }

    /**
     * During drain() a fatal -ERR does not reconnect, which would subscribe again what the drain has just
     * unsubscribed: drain() reports it and closes, as it does when the socket fails.
     */
    public function testFatalErrDuringDrainDoesNotReconnect(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, errorListener: $recorder->errorListener());
        $connection->subscribe('orders', static function (): void {})->await();
        // The drain's flush PING is answered by the -ERR instead of a PONG.
        $transport->answerPings = false;
        $transport->afterWrite = static function (string $bytes) use ($transport): void {
            if (str_contains($bytes, "PING\r\n")) {
                $transport->afterWrite = null;
                $transport->pushFrame(self::STALE);
            }
        };

        $connection->drain()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertCount(1, $transport->connectCalls, 'no reconnect dial');
        self::assertNotSame([], $recorder->errorsContaining('Stale Connection'), 'drain() reported the -ERR');
    }

    /** Releases the reconnect's held dial once it is parked: released earlier, the release would be lost. */
    private function releaseTheHeldDial(ReconnectingTransport $transport): void
    {
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
        $transport->releaseDial();
    }

    private function connectWithReconnectOff(ReconnectingTransport $transport, LifecycleRecorder $recorder): NatsConnection
    {
        $connection = new NatsConnection(
            new NatsOptions(
                connectTimeoutMs: 500,
                reconnectEnabled: false,
                pingIntervalSeconds: 0,
                connectionListener: $recorder->connectionListener(),
            ),
            $transport,
        );
        $this->opened[] = $connection;
        $connection->connect()->await();

        return $connection;
    }

    /**
     * The server answers the first request by dropping the client as stale: it says why, then answers
     * nothing more on that connection. A new connection is answered again.
     */
    private function answerFirstRequestWithStaleConnection(ReconnectingTransport $transport): void
    {
        $requests = 0;
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport, &$requests): array {
            if (++$requests === 1) {
                $transport->silence();

                return [self::STALE];
            }

            return $transport->replyFrame((string) $replyTo, 're: ' . $payload);
        };
    }
}
