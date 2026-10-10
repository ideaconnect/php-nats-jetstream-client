<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use IDCT\NATS\Transport\TransportClosedException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\delay;

/**
 * With reconnect off, a lost connection fails with "Reconnect is disabled", which reads like a configuration
 * problem. It used to carry no cause, so the error that actually ended the connection (a socket error, a
 * server -ERR such as 'Stale Connection') was lost (#172). It is now chained as the previous exception,
 * for the operation that failed and for any operation that joins the recovery. The message is unchanged,
 * for code that matches it.
 */
final class ReconnectDisabledCauseTest extends TestCase
{
    use OwnsTestResources;

    protected function tearDown(): void
    {
        // Every client and connection a test makes is registered as it is constructed (#183), weakly: the shutdown
        // closes what is left and checks that nothing goes on running into the next test.
        $this->releaseOwnedResources();
    }

    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    public function testAReadThatFailsSaysWhyTheConnectionEnded(): void
    {
        $connection = $this->connect(new FakeTransport([self::INFO, "PONG\r\n", FakeTransport::EOF]));

        $error = $this->reconnectDisabledError(static fn() => $connection->processIncoming()->await());

        self::assertInstanceOf(TransportClosedException::class, $error->getPrevious());
        self::assertSame('Socket closed by peer (EOF)', $error->getPrevious()->getMessage());
    }

    public function testAPublishWhoseWriteFailsSaysWhyTheConnectionEnded(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = $this->connect($transport);
        $transport->throwOnWriteContaining = 'PUB ';

        $error = $this->reconnectDisabledError(static fn() => $connection->publish('updates', 'payload')->await());

        self::assertInstanceOf(TransportClosedException::class, $error->getPrevious());
        self::assertSame('Simulated write failure', $error->getPrevious()->getMessage());
    }

    /**
     * A control write - here a SUB - recovers on its own fiber, which the operation waits for.
     */
    public function testAControlWriteThatFailsSaysWhyTheConnectionEnded(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = $this->connect($transport);
        $transport->throwOnWriteContaining = 'SUB ';

        $error = $this->reconnectDisabledError(static fn() => $connection->subscribe('updates', static function (): void {})->await());

        self::assertInstanceOf(TransportClosedException::class, $error->getPrevious());
        self::assertSame('Simulated write failure', $error->getPrevious()->getMessage());
    }

    public function testAStreamThatCannotBeParsedSaysWhyTheConnectionEnded(): void
    {
        $connection = $this->connect(new FakeTransport([self::INFO, "PONG\r\n", "BOGUS\r\n"]));

        $error = $this->reconnectDisabledError(static fn() => $connection->processIncoming()->await());

        self::assertInstanceOf(ProtocolException::class, $error->getPrevious());
        self::assertSame('Unsupported control frame: BOGUS', $error->getPrevious()->getMessage());
    }

    /**
     * A recovery the heartbeat starts fails with nobody waiting for it but the operations that join it while
     * it closes the socket: they learn why the heartbeat gave up on the connection.
     *
     * @param \Closure(NatsConnection, FakeTransport): void $arrange
     * @param class-string<\Throwable>                       $previousClass
     */
    #[DataProvider('heartbeatFailures')]
    public function testAnOperationJoiningARecoveryTheHeartbeatStartedLearnsWhyTheConnectionEnded(
        \Closure $arrange,
        string $previousClass,
        string $previousMessage,
    ): void {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = $this->connect($transport);
        $arrange($connection, $transport);
        // The recovery closes the socket last; holding the close keeps the recovery running for the join.
        $transport->closeDelay = 0.5;

        $heartbeat = async(fn() => $this->invokePrivate($connection, 'pingTimerTick'));
        delay(0.05);

        $error = $this->reconnectDisabledError(fn() => $this->invokePrivate($connection, 'recoverConnection'));
        $heartbeat->await();

        self::assertInstanceOf($previousClass, $error->getPrevious());
        self::assertSame($previousMessage, $error->getPrevious()->getMessage());
    }

    /**
     * @return iterable<string, array{\Closure(NatsConnection, FakeTransport): void, class-string<\Throwable>, string}>
     */
    public static function heartbeatFailures(): iterable
    {
        yield 'unanswered PINGs' => [
            static function (NatsConnection $connection): void {
                (new \ReflectionProperty(NatsConnection::class, 'outstandingPings'))->setValue($connection, 2);
            },
            ConnectionException::class,
            'The server did not answer the last 2 PINGs',
        ];
        yield 'PING write failure' => [
            static function (NatsConnection $connection, FakeTransport $transport): void {
                $transport->throwOnWriteContaining = 'PING';
            },
            TransportClosedException::class,
            'Simulated write failure',
        ];
        yield 'socket closed during the PONG read' => [
            static function (NatsConnection $connection, FakeTransport $transport): void {
                $transport->pushReadChunk(FakeTransport::EOF);
            },
            TransportClosedException::class,
            'Socket closed by peer (EOF)',
        ];
        yield 'protocol violation during the PONG read' => [
            static function (NatsConnection $connection, FakeTransport $transport): void {
                $transport->throwOnNextRead = new ProtocolException('Corrupt frame');
            },
            ProtocolException::class,
            'Corrupt frame',
        ];
        yield 'fatal -ERR during the PONG read' => [
            static function (NatsConnection $connection, FakeTransport $transport): void {
                $transport->pushReadChunk("-ERR 'Stale Connection'\r\n");
            },
            ConnectionException::class,
            "Server sent error frame: 'Stale Connection'",
        ];
    }

    /**
     * A read that meets a fatal -ERR fails with the server's error itself (#171); an operation that joins the
     * recovery the -ERR started learns it too.
     */
    public function testAnOperationJoiningTheRecoveryAFatalErrStartedLearnsTheServersError(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = $this->connect($transport);
        $transport->pushReadChunk("-ERR 'Stale Connection'\r\n");
        $transport->closeDelay = 0.5;

        $read = async(static fn() => $connection->processIncoming()->await());
        delay(0.05);

        $error = $this->reconnectDisabledError(fn() => $this->invokePrivate($connection, 'recoverConnection'));

        self::assertInstanceOf(ConnectionException::class, $error->getPrevious());
        self::assertSame("Server sent error frame: 'Stale Connection'", $error->getPrevious()->getMessage());
        try {
            $read->await();
            self::fail('expected the read to fail with the server error');
        } catch (ConnectionException $readError) {
            self::assertSame("Server sent error frame: 'Stale Connection'", $readError->getMessage());
        }
    }

    private function connect(FakeTransport $transport): NatsConnection
    {
        $connection = $this->own(new NatsConnection(new NatsOptions(reconnectEnabled: false, pingIntervalSeconds: 0), $transport));
        $connection->connect()->await();

        return $connection;
    }

    /**
     * Runs the operation and returns the "Reconnect is disabled" error it fails with, whose message and code
     * are the ones it always had.
     */
    private function reconnectDisabledError(\Closure $operation): ConnectionException
    {
        try {
            $operation();
        } catch (ConnectionException $e) {
            self::assertSame('Reconnect is disabled', $e->getMessage());
            self::assertSame(0, $e->getCode());

            return $e;
        }

        self::fail('expected ConnectionException: Reconnect is disabled');
    }

    private function invokePrivate(object $object, string $method, mixed ...$args): mixed
    {
        return (new \ReflectionMethod($object, $method))->invoke($object, ...$args);
    }
}
