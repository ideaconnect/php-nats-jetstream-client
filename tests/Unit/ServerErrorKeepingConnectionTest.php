<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Only a frame that says the connection is finished ends it (#171): a fatal -ERR, which the server sends
 * right before it closes the connection, and a PONG the socket would not take. 2.10.1 ended the connection
 * on every -ERR that failed a read, including the few the server sends while keeping the connection open:
 * a SUB beyond the maximum subscriptions, a permissions violation for a publish with a reply subject, an
 * invalid publish subject. A healthy connection then closed, and with reconnect on every attempt replayed the
 * rejected SUB and failed again until the reconnect gave up. Such an -ERR fails the read, as it did before
 * 2.10.1, and leaves the connection open.
 */
final class ServerErrorKeepingConnectionTest extends TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    #[DataProvider('errorsTheServerKeepsTheConnectionOpenFor')]
    public function testAnErrTheServerKeepsTheConnectionOpenForFailsTheReadButLeavesItOpen(string $error, bool $reconnect): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n", "-ERR '{$error}'\r\n"]);
        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: $reconnect, pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        try {
            $connection->processIncoming()->await();
            self::fail('expected the read to fail with the server error');
        } catch (ConnectionException $e) {
            self::assertSame("Server sent error frame: '{$error}'", $e->getMessage());
        }

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertFalse($transport->closed, 'the socket stays open');
        self::assertCount(1, $transport->connectCalls, 'nothing reconnects');
        $connection->publish('orders', 'still open')->await();
        self::assertStringContainsString("PUB orders 10\r\nstill open", implode('', $transport->writes));
    }

    /**
     * @return iterable<string, array{string, bool}>
     */
    public static function errorsTheServerKeepsTheConnectionOpenFor(): iterable
    {
        foreach (['reconnect off' => false, 'reconnect on' => true] as $mode => $reconnect) {
            yield "maximum subscriptions, {$mode}" => ['maximum subscriptions exceeded', $reconnect];
            yield "reserved reply subject, {$mode}" => ['Permissions Violation for Publish with Reply of "_INBOX.reserved"', $reconnect];
            yield "invalid publish subject, {$mode}" => ['Invalid Publish Subject', $reconnect];
            yield "bare permissions violation, {$mode}" => ['Permissions Violation', $reconnect];
        }
    }

    /**
     * Guard: an -ERR the server closes the connection after still ends it (#171).
     */
    #[DataProvider('errorsTheServerClosesTheConnectionAfter')]
    public function testAnErrTheServerClosesTheConnectionAfterStillEndsIt(string $error): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n", "-ERR '{$error}'\r\n"]);
        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false, pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        try {
            $connection->processIncoming()->await();
            self::fail('expected the read to fail with the server error');
        } catch (ConnectionException $e) {
            self::assertSame("Server sent error frame: '{$error}'", $e->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertTrue($transport->closed);
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function errorsTheServerClosesTheConnectionAfter(): iterable
    {
        yield 'stale connection' => ['Stale Connection'];
        yield 'authorization violation' => ['Authorization Violation'];
        yield 'maximum payload' => ['Maximum Payload Violation'];
    }

    /**
     * An INFO that is valid JSON but no object is reported like broken JSON, without failing the read or
     * touching the connection or what it knows of the server. A number used to reach the INFO parser and fail
     * the read with a TypeError, which 2.10.1 then took for a connection failure; an array replaced the
     * server info with defaults, after which a publish with headers was refused.
     */
    #[DataProvider('infoPayloadsThatAreNoObject')]
    public function testAnInfoThatIsNotAJsonObjectIsReportedWithoutFailingTheRead(string $payload): void
    {
        $recorder = new LifecycleRecorder();
        $transport = new FakeTransport([self::INFO, "PONG\r\n", "INFO {$payload}\r\n"]);
        $connection = new NatsConnection(new NatsOptions(
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
        ), $transport);
        $connection->connect()->await();

        $connection->processIncoming()->await();

        self::assertNotSame([], $recorder->errorsContaining('Discarding malformed async INFO frame: INFO payload is not a JSON object'));
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertFalse($transport->closed);
        self::assertSame('S1', $connection->serverInfo()?->serverId);
        self::assertTrue($connection->serverInfo()->headersSupported);
    }

    /** @return iterable<string, array{string}> */
    public static function infoPayloadsThatAreNoObject(): iterable
    {
        yield 'a number' => ['1'];
        yield 'an array' => ['[1,2]'];
        yield 'an empty array' => ['[]'];
    }

    /**
     * At connect such an INFO fails the connect as broken JSON does, with an error that says what is wrong
     * instead of one that wraps the TypeError.
     */
    #[DataProvider('infoPayloadsThatAreNoObject')]
    public function testAnInitialInfoThatIsNotAJsonObjectFailsTheConnectAsBrokenJsonDoes(string $payload): void
    {
        $connection = new NatsConnection(
            new NatsOptions(connectTimeoutMs: 500, reconnectEnabled: false, pingIntervalSeconds: 0),
            new FakeTransport(["INFO {$payload}\r\n", "PONG\r\n"]),
        );

        try {
            $connection->connect()->await();
            self::fail('expected the connect to fail');
        } catch (ConnectionException $e) {
            self::assertSame('INFO payload is not a JSON object', $e->getMessage());
            self::assertInstanceOf(\JsonException::class, $e->getPrevious());
        }
    }

    /**
     * The server's answer to a rejected SUB can sit unread until the next read, which then also meets the
     * fatal -ERR the server sent before it closed the connection, say 'Stale Connection' after a quiet
     * period. The fatal one decides: it ends the connection, and the read fails with it, while the
     * rejection is reported.
     */
    public function testAFatalErrAfterANonClosingOneInTheSameChunkEndsTheConnection(): void
    {
        $recorder = new LifecycleRecorder();
        $transport = new FakeTransport([
            self::INFO, "PONG\r\n",
            "-ERR 'maximum subscriptions exceeded'\r\n-ERR 'Stale Connection'\r\n",
        ]);
        $connection = new NatsConnection(new NatsOptions(
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
        ), $transport);
        $connection->connect()->await();

        try {
            $connection->processIncoming()->await();
            self::fail('expected the read to fail with the fatal error');
        } catch (ConnectionException $e) {
            self::assertSame("Server sent error frame: 'Stale Connection'", $e->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertNotSame([], $recorder->errorsContaining('maximum subscriptions exceeded'), 'the rejection is reported');
    }

    /**
     * A non-closing -ERR in the same chunk as a full subscription queue is what the read fails with, whichever
     * of the two came first, and the overflow is reported: the -ERR answers something the application sent,
     * while the overflow only says that a subscriber fell behind.
     */
    #[DataProvider('overflowOrders')]
    public function testANonClosingErrOutranksAnOverflowInTheSameChunk(bool $overflowFirst): void
    {
        $recorder = new LifecycleRecorder();
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = new NatsConnection(new NatsOptions(
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            maxPendingMessagesPerSubscription: 1,
            slowConsumerPolicy: SlowConsumerPolicy::Error,
            errorListener: $recorder->errorListener(),
        ), $transport);
        $connection->connect()->await();
        $sid = $connection->subscribe('updates', static function (): void {})->await();
        $overflow = "MSG updates {$sid} 5\r\nfirst\r\nMSG updates {$sid} 6\r\nsecond\r\n";
        $error = "-ERR 'maximum subscriptions exceeded'\r\n";
        $transport->pushReadChunk($overflowFirst ? $overflow . $error : $error . $overflow);

        try {
            $connection->processIncoming()->await();
            self::fail('expected the read to fail with the -ERR');
        } catch (ConnectionException $e) {
            self::assertSame("Server sent error frame: 'maximum subscriptions exceeded'", $e->getMessage());
        }

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(["Subscription queue overflow for sid {$sid}"], $recorder->errorsContaining('overflow'), 'the overflow is reported');
    }

    /** @return iterable<string, array{bool}> */
    public static function overflowOrders(): iterable
    {
        yield 'the overflow first' => [true];
        yield 'the -ERR first' => [false];
    }

    /**
     * A replay whose answers carry a fatal -ERR after a rejected SUB fails the attempt, as a fatal -ERR alone
     * does: the rejection does not hide it.
     */
    public function testAReconnectWhoseReplayMeetsAFatalErrAfterARejectedSubFails(): void
    {
        $transport = new FakeTransport([
            self::INFO, "PONG\r\n",
            "-ERR 'maximum subscriptions exceeded'\r\n",                                  // the second SUB, rejected
            FakeTransport::EOF,                                                             // the connection drops
            self::INFO, "PONG\r\n",                                                        // the reconnect's handshake
            "-ERR 'maximum subscriptions exceeded'\r\n-ERR 'Authorization Violation'\r\n", // the replay's answers
        ]);
        $connection = new NatsConnection(new NatsOptions(
            reconnectEnabled: true,
            maxReconnectAttempts: 1,
            reconnectDelayMs: 1,
            reconnectMaxDelayMs: 1,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
        ), $transport);
        $connection->connect()->await();
        $connection->subscribe('first', static function (): void {})->await();
        $connection->subscribe('second', static function (): void {})->await();

        try {
            $connection->processIncoming()->await();
        } catch (ConnectionException) {
            // The first rejection; the connection stays open.
        }
        try {
            $connection->processIncoming()->await();
        } catch (\Throwable) {
            // The read that met the EOF; what matters is how the reconnect ends.
        }

        self::assertSame(ConnectionState::Closed, $connection->state(), 'the one attempt failed on the fatal -ERR');
    }

    /**
     * The heartbeat's own read follows the same rule: an -ERR the server keeps the connection open for leaves
     * it open, and a fatal one after it ends it.
     */
    #[DataProvider('heartbeatReads')]
    public function testTheHeartbeatsReadEndsTheConnectionOnlyOnAFatalErr(string $chunk, ConnectionState $state): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false, pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();
        $transport->pushReadChunk($chunk);

        (new \ReflectionMethod($connection, 'pingTimerTick'))->invoke($connection);

        self::assertSame($state, $connection->state());
    }

    /**
     * @return iterable<string, array{string, ConnectionState}>
     */
    public static function heartbeatReads(): iterable
    {
        yield 'a rejected SUB' => ["-ERR 'maximum subscriptions exceeded'\r\n", ConnectionState::Open];
        yield 'a rejected SUB, then a fatal -ERR' => ["-ERR 'maximum subscriptions exceeded'\r\n-ERR 'Stale Connection'\r\n", ConnectionState::Closed];
    }

    /**
     * The read of a serving loop, which would only swallow what it throws (readIncomingForOperation() with
     * alwaysReport), reports such an -ERR and returns what it read, so the loop reads on.
     */
    public function testAReadForAServingLoopReportsAnErrTheServerKeepsTheConnectionOpenFor(): void
    {
        $recorder = new LifecycleRecorder();
        $transport = new FakeTransport([self::INFO, "PONG\r\n", "-ERR 'maximum subscriptions exceeded'\r\n"]);
        $connection = new NatsConnection(new NatsOptions(
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
        ), $transport);
        $connection->connect()->await();

        $read = $connection->readIncomingForOperation(alwaysReport: true)->await();

        self::assertSame(1, $read->frames);
        self::assertTrue($read->consumedBytes);
        self::assertSame(["Server sent error frame: 'maximum subscriptions exceeded'"], $recorder->errors);
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * That read still throws an -ERR that ends the connection, once the connection has ended.
     */
    public function testAReadForAServingLoopStillThrowsAnErrThatEndsTheConnection(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n", "-ERR 'Stale Connection'\r\n"]);
        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false, pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        try {
            $connection->readIncomingForOperation(alwaysReport: true)->await();
            self::fail('expected the read to fail with the fatal error');
        } catch (ConnectionException $e) {
            self::assertSame("Server sent error frame: 'Stale Connection'", $e->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * A reconnect replays every subscription, including one the server rejected for exceeding the maximum
     * subscriptions. The server rejects it again, and that used to fail the attempt, and every one after it,
     * until the reconnect gave up and closed the connection. The rejection is reported instead and the
     * reconnect completes.
     */
    public function testAReconnectWhoseReplayedSubTheServerRejectsCompletes(): void
    {
        $recorder = new LifecycleRecorder();
        $transport = new FakeTransport([
            self::INFO, "PONG\r\n",
            "-ERR 'maximum subscriptions exceeded'\r\n", // the second SUB, rejected
            FakeTransport::EOF,                           // the connection drops
            self::INFO, "PONG\r\n",                       // the reconnect's handshake
            "-ERR 'maximum subscriptions exceeded'\r\n", // the replayed second SUB, rejected again
        ]);
        $connection = new NatsConnection(new NatsOptions(
            reconnectEnabled: true,
            maxReconnectAttempts: 2,
            reconnectDelayMs: 1,
            reconnectMaxDelayMs: 1,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            connectionListener: $recorder->connectionListener(),
            errorListener: $recorder->errorListener(),
        ), $transport);
        $connection->connect()->await();
        $connection->subscribe('first', static function (): void {})->await();
        $connection->subscribe('second', static function (): void {})->await();

        try {
            $connection->processIncoming()->await();
        } catch (ConnectionException) {
            // The first rejection fails this read, as it always did; the connection stays open.
        }
        try {
            $connection->processIncoming()->await();
        } catch (\Throwable) {
            // The read that met the EOF; the reconnect is what matters here.
        }

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->connectCalls, 'one reconnect, which completed');
        self::assertNotSame([], $recorder->errorsContaining('maximum subscriptions exceeded'), 'the replay rejection is reported');
    }

    /**
     * With reconnect off, a connection the heartbeat gives up on closes with nobody waiting for the recovery.
     * The Closed event now carries the reason, so that the connection listener and the log learn it (#172).
     */
    #[DataProvider('maxPingsOut')]
    public function testTheClosedEventOfAHeartbeatThatGaveUpCarriesTheReason(int $maxPingsOut, string $reason): void
    {
        $recorder = new LifecycleRecorder();
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = new NatsConnection(new NatsOptions(
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            maxPingsOut: $maxPingsOut,
            connectionListener: $recorder->connectionListener(),
        ), $transport);
        $connection->connect()->await();
        (new \ReflectionProperty(NatsConnection::class, 'outstandingPings'))->setValue($connection, $maxPingsOut);

        (new \ReflectionMethod($connection, 'pingTimerTick'))->invoke($connection);

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(1, $recorder->closedEvents());
        self::assertSame([$reason], $recorder->closedErrors, 'the Closed event carries the reason');
    }

    /**
     * @return iterable<string, array{int, string}>
     */
    public static function maxPingsOut(): iterable
    {
        yield 'no PING' => [0, 'The heartbeat allows no unanswered PING (maxPingsOut is 0)'];
        yield 'one PING' => [1, 'The server did not answer the last PING'];
        yield 'two PINGs' => [2, 'The server did not answer the last 2 PINGs'];
    }
}
