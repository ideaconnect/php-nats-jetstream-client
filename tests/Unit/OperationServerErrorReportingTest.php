<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\SlowConsumerException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\SubscriptionLimitServer;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;

/** Nonfatal server-error reporting is opt-in and preserves the operation's other policies (#209). */
final class OperationServerErrorReportingTest extends TestCase
{
    use OwnsTestResources;

    private const ERROR = "Server sent error frame: 'Invalid Publish Subject'";
    private const FRAME = "-ERR 'Invalid Publish Subject'\r\n";

    protected function tearDown(): void
    {
        $this->releaseOwnedResources();
    }

    /** @return iterable<string, array{string}> */
    public static function readers(): iterable
    {
        yield 'application read' => ['application'];
        yield 'ordinary operation' => ['operation'];
        yield 'operation opting in' => ['reporting'];
    }

    /**
     * The reporting is opt-in: your own read and an operation's read throw the -ERR, an operation's read with
     * reportServerErrors reports it and returns; each delivers the chunk's message.
     */
    #[DataProvider('readers')]
    public function testReportingIsOptInAndStillDeliversTheChunksMessages(string $reader): void
    {
        [$transport, $client, $recorder] = $this->fixture();
        $seen = self::log();
        $sid = $client->subscribe('events', static function (NatsMessage $message) use ($seen): void {
            $seen[] = $message->payload;
        })->await();
        $transport->pushReadChunk(self::FRAME . ReconnectingTransport::msgFrame('events', $sid, 'first'));
        $failure = null;
        try {
            $read = match ($reader) {
                'application' => $client->readIncoming(),
                'operation' => $client->readIncomingForOperation(ownSid: $sid),
                default => $client->readIncomingForOperation(ownSid: $sid, reportServerErrors: true),
            };
            $read->await(new TimeoutCancellation(2));
        } catch (\Throwable $error) {
            $failure = $error;
        }

        if ($reader === 'reporting') {
            self::assertNull($failure);
            self::assertSame([self::ERROR], $recorder->errors);
        } else {
            self::assertInstanceOf(ConnectionException::class, $failure);
            self::assertSame(self::ERROR, $failure->getMessage());
            self::assertSame([], $recorder->errors);
        }
        self::assertSame(['first'], $seen->getArrayCopy());
        self::assertSame(ConnectionState::Open, $client->state());
    }

    /** @return iterable<string, array{string, bool, bool}> */
    public static function policies(): iterable
    {
        foreach (['handler', 'overflow'] as $kind) {
            foreach ([false, true] as $failOperations) {
                foreach ([false, true] as $ownSid) {
                    yield $kind . '/' . ($failOperations ? 'fail' : 'report') . '/' . ($ownSid ? 'own' : 'other')
                        => [$kind, $failOperations, $ownSid];
                }
            }
        }
    }

    /**
     * An operation's read with reportServerErrors keeps the other rules: a handler failure or an overflow fails it
     * when the options say so or when it is the operation's own subscription's, and is reported otherwise, the -ERR
     * reported either way. A handler throwing a ConnectionException with the -ERR's very text is still a handler failure.
     */
    #[DataProvider('policies')]
    public function testServerReportingPreservesFailureOptionsAndOwnSubscriptionProtection(string $kind, bool $failOperations, bool $ownSid): void
    {
        [$transport, $client, $recorder] = $this->fixture($failOperations);
        // A handler may throw this exact exception type and text too: that does not make it a server frame.
        $handlerFailure = new ConnectionException(self::ERROR);
        $sid = $client->subscribe('events', static function () use ($kind, $handlerFailure): void {
            if ($kind === 'handler') {
                throw $handlerFailure;
            }
        })->await();
        $operationSid = $ownSid ? $sid : $client->subscribe('quiet', static function (): void {})->await();
        $frames = self::FRAME . ReconnectingTransport::msgFrame('events', $sid, 'one');
        if ($kind === 'overflow') {
            $frames .= ReconnectingTransport::msgFrame('events', $sid, 'two');
        }
        $transport->pushReadChunk($frames);
        $failure = null;
        try {
            $client->readIncomingForOperation(ownSid: $operationSid, reportServerErrors: true)->await(new TimeoutCancellation(2));
        } catch (\Throwable $error) {
            $failure = $error;
        }

        if ($failOperations || $ownSid) {
            if ($kind === 'handler') {
                self::assertSame($handlerFailure, $failure);
            } else {
                self::assertInstanceOf(SlowConsumerException::class, $failure);
                self::assertSame($sid, $failure->sid);
            }
        } else {
            self::assertNull($failure);
            self::assertCount($kind === 'handler' ? 2 : 1, $recorder->errorsContaining(self::ERROR));
            if ($kind === 'overflow') {
                self::assertCount(1, $recorder->errorsContaining('overflow'));
            }
        }
        self::assertContains(self::ERROR, $recorder->errors);
        self::assertSame(ConnectionState::Open, $client->state());
    }

    /**
     * The report goes out once the whole chunk is queued: an error listener that reads finds this chunk's message
     * delivered before the next chunk's.
     */
    public function testAListenerThatReadsFindsThisChunksMessagesBeforeTheNextChunksMessages(): void
    {
        $seen = self::log();
        $clientInListener = null;
        $listener = static function () use (&$clientInListener): void {
            self::assertInstanceOf(NatsClient::class, $clientInListener);
            $clientInListener->readIncoming()->await(new TimeoutCancellation(2));
        };
        [$transport, $client, $recorder] = $this->fixture(listener: $listener, pendingLimit: 2);
        $clientInListener = $client;
        $sid = $client->subscribe('events', static function (NatsMessage $message) use ($seen): void {
            $seen[] = $message->payload;
        })->await();
        $transport->pushReadChunk(self::FRAME . ReconnectingTransport::msgFrame('events', $sid, 'first'));
        $transport->pushReadChunk(ReconnectingTransport::msgFrame('events', $sid, 'second'));

        $client->readIncomingForOperation(ownSid: $sid, reportServerErrors: true)->await(new TimeoutCancellation(2));

        self::assertSame(['first', 'second'], $seen->getArrayCopy());
        self::assertSame([self::ERROR], $recorder->errors);
    }

    /** @return iterable<string, array{bool, bool}> */
    public static function throwingObservers(): iterable
    {
        yield 'listener throws' => [true, false];
        yield 'logger throws' => [false, true];
        yield 'both throw' => [true, true];
    }

    /**
     * Two -ERRs in one chunk are each reported once, to the listener and to the logger at error level, and a throwing
     * listener or logger does not stop the chunk's delivery.
     */
    #[DataProvider('throwingObservers')]
    public function testEachErrorIsReportedOnceAndObserverFailuresDoNotStopDelivery(bool $listenerThrows, bool $loggerThrows): void
    {
        $logged = self::log();
        $levels = self::log();
        $logger = $this->createMock(LoggerInterface::class);
        $logger->method('log')->willReturnCallback(static function ($level, string|\Stringable $message) use ($logged, $levels, $loggerThrows): void {
            if (str_starts_with((string) $message, 'NATS connection error: ')) {
                $logged[] = (string) $message;
                $levels[] = (string) $level;
                if ($loggerThrows) {
                    throw new \RuntimeException('logger failed');
                }
            }
        });
        $listener = static function () use ($listenerThrows): void {
            if ($listenerThrows) {
                throw new \RuntimeException('listener failed');
            }
        };
        [$transport, $client, $recorder] = $this->fixture(listener: $listener, logger: $logger);
        $seen = self::log();
        $sid = $client->subscribe('events', static function (NatsMessage $message) use ($seen): void {
            $seen[] = $message->payload;
        })->await();
        $transport->pushReadChunk(self::FRAME . "-ERR 'maximum subscriptions exceeded'\r\n"
            . ReconnectingTransport::msgFrame('events', $sid, 'first'));

        $client->readIncomingForOperation(ownSid: $sid, reportServerErrors: true)->await(new TimeoutCancellation(2));

        self::assertSame([self::ERROR, "Server sent error frame: 'maximum subscriptions exceeded'"], $recorder->errors);
        self::assertSame(array_map(static fn(string $error): string => 'NATS connection error: ' . $error, $recorder->errors), $logged->getArrayCopy());
        self::assertSame([LogLevel::ERROR, LogLevel::ERROR], $levels->getArrayCopy(), 'logged at the level of the other server errors');
        self::assertSame(['first'], $seen->getArrayCopy());
    }

    /** @return iterable<string, array{bool}> */
    public static function fatalOrders(): iterable
    {
        yield 'nonfatal first' => [true];
        yield 'fatal first' => [false];
    }

    /** A fatal -ERR in the same chunk still fails the read and closes the connection, in either order. */
    #[DataProvider('fatalOrders')]
    public function testAFatalErrorWinsInEitherOrder(bool $nonfatalFirst): void
    {
        [$transport, $client, $recorder] = $this->fixture();
        $sid = $client->subscribe('quiet', static function (): void {})->await();
        $fatal = "-ERR 'Stale Connection'\r\n";
        $transport->pushReadChunk($nonfatalFirst ? self::FRAME . $fatal : $fatal . self::FRAME);
        $failure = null;
        try {
            $client->readIncomingForOperation(ownSid: $sid, reportServerErrors: true)->await(new TimeoutCancellation(2));
        } catch (\Throwable $error) {
            $failure = $error;
        }
        self::assertInstanceOf(ConnectionException::class, $failure);
        self::assertSame("Server sent error frame: 'Stale Connection'", $failure->getMessage());
        self::assertSame(ConnectionState::Closed, $client->state());
        self::assertSame([self::ERROR], $recorder->errorsContaining(self::ERROR));
    }

    /** @return array{FakeTransport, NatsClient, LifecycleRecorder} */
    private function fixture(bool $failOperations = false, ?\Closure $listener = null, ?LoggerInterface $logger = null, int $pendingLimit = 1): array
    {
        $transport = new FakeTransport([SubscriptionLimitServer::INFO, "PONG\r\n"]);
        $recorder = new LifecycleRecorder();
        $client = $this->own(new NatsClient(new NatsOptions(
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            maxPendingMessagesPerSubscription: $pendingLimit,
            slowConsumerPolicy: SlowConsumerPolicy::Error,
            handlerErrorsFailOperations: $failOperations,
            slowConsumerErrorsFailOperations: $failOperations,
            errorListener: static function (\Throwable $error) use ($recorder, $listener): void {
                $recorder->errors[] = $error->getMessage();
                $listener?->__invoke();
            },
            logger: $logger,
        ), $transport));
        $client->connect()->await();

        return [$transport, $client, $recorder];
    }

    /** @return \ArrayObject<int, string> */
    private static function log(): \ArrayObject
    {
        return new \ArrayObject();
    }
}
