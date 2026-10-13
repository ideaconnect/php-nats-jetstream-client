<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\AuthenticationException;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;

use function Amp\delay;

/**
 * A lame-duck failover whose reconnect is refused authentication reports the refusal once through the error listener
 * and the error log (#201). performRecovery() reports the refusal before it rethrows it, also to a recovery's joiners;
 * up to 2.26.5 both failover catches - inline in handleServerInfoUpdate() for your own read, and in the fiber
 * startLameDuckFailover() starts for an operation's read (#191) - reported it a second time. Over the scripted
 * two-server ReconnectingTransport: after a warm-up request, the lame-duck INFO names a second server, so the pool grows
 * to two; every later handshake is answered with -ERR 'Authorization Violation', or every later dial refused. Each data
 * set runs the inline path (your processIncoming() reads the INFO) and the own-fiber path (a request()'s read brings it,
 * the request then failing with "Connection is not open"). Every connection, read and held dial is owned by the test.
 */
final class LameDuckAuthenticationReportingTest extends TestCase
{
    use OwnsTestResources;

    private const LAME_DUCK = 'INFO {"server_id":"S1","version":"2.12.0","max_payload":1048576,"ldm":true,"connect_urls":["10.0.0.2:4222"]}' . "\r\n";

    protected function tearDown(): void
    {
        $this->releaseOwnedResources();
    }

    /** @return iterable<string, array{bool}> */
    public static function paths(): iterable
    {
        yield 'inline' => [false];
        yield 'own fiber' => [true];
    }

    /** @return iterable<string, array{bool, bool, bool}> */
    public static function reportingPaths(): iterable
    {
        foreach (self::paths() as $path => [$operation]) {
            foreach (['ordinary' => [false, false], 'throwing listener' => [true, false], 'throwing logger' => [false, true], 'both throwing' => [true, true]] as $reporting => [$listenerThrows, $loggerThrows]) {
                yield $path . ', ' . $reporting => [$operation, $listenerThrows, $loggerThrows];
            }
        }
    }

    /**
     * The refusal is reported once to the error listener and once to the error log - the same object the Closed event
     * carries - with normal callbacks, a throwing listener, a throwing logger or both; the connection ends Closed
     * after two dials (the refusal stops the retries), with one Closed event.
     */
    #[DataProvider('reportingPaths')]
    public function testAuthenticationRefusalIsReportedOnce(bool $operation, bool $listenerThrows, bool $loggerThrows): void
    {
        $transport = new ReconnectingTransport();
        $reports = new LameDuckReportLog($listenerThrows, $loggerThrows);
        $connection = $this->connection($transport, $reports);
        $transport->rejectAuthentication = true;

        $this->finishFailover($operation, $connection, $transport);

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertCount(2, $transport->connectCalls, 'authentication refusal stops retries immediately');
        self::assertSame('tcp://10.0.0.2:4222|2000', $transport->connectCalls[1]);
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::DiscoveredServers, ConnectionEvent::LameDuck, ConnectionEvent::Disconnected, ConnectionEvent::Closed], $reports->events);
        self::assertCount(1, $reports->closedErrors);
        self::assertInstanceOf(AuthenticationException::class, $reports->closedErrors[0]);
        self::assertSame("Server rejected authentication during connect: 'Authorization Violation'", $reports->closedErrors[0]->getMessage());
        foreach ([...$reports->listenerErrors, ...$reports->loggedErrors] as $reported) {
            self::assertSame($reports->closedErrors[0], $reported, 'every report carries the original refusal object');
        }
        self::assertCount(1, $reports->listenerErrors, 'authentication listener report must occur once');
        self::assertCount(1, $reports->loggedErrors, 'authentication error log must occur once');
        self::assertSame($reports->closedErrors, $reports->listenerErrors, 'Closed and listener receive the same object');
        self::assertSame($reports->listenerErrors, $reports->loggedErrors);
    }

    /**
     * The failover joins a heartbeat recovery whose dial is held while the LameDuck listener waits: once the dial is
     * released and refused, the joined failover reports nothing more - one report, one log, no extra dial.
     */
    #[DataProvider('paths')]
    public function testAuthenticationRefusalInJoinedHeartbeatRecoveryIsReportedOnce(bool $operation): void
    {
        $transport = new ReconnectingTransport();
        $reports = new LameDuckReportLog();
        $listenerReachedDial = new DeferredFuture();
        $letListenerReturn = new DeferredFuture();
        $listenerReturned = new DeferredFuture();
        $this->resources()->onRelease(static function () use ($letListenerReturn): void {
            if (!$letListenerReturn->isComplete()) {
                $letListenerReturn->complete();
            }
        }, 'held LameDuck listener');
        $connection = $this->connection($transport, $reports, 0.01, function () use ($transport, $listenerReachedDial, $letListenerReturn, $listenerReturned): void {
            $transport->silence();
            $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
            $listenerReachedDial->complete();
            $letListenerReturn->getFuture()->await(new TimeoutCancellation(5));
            $listenerReturned->complete();
        });
        $transport->rejectAuthentication = true;
        $transport->holdNextDial();
        $this->resources()->onRelease($transport->releaseDial(...), 'held heartbeat recovery dial');

        $read = $this->beginFailover($operation, $connection, $transport);
        $listenerReachedDial->getFuture()->await(new TimeoutCancellation(5));
        self::assertSame(ConnectionState::Connecting, $connection->state());
        self::assertSame(0, $reports->listenerReportCount(), 'authentication has not run while the heartbeat dial is held');
        $letListenerReturn->complete();
        $listenerReturned->getFuture()->await(new TimeoutCancellation(5));
        // Let the dispatch and its own-fiber failover enter their waits before releasing recovery.
        delay(0);
        self::assertFalse($read->isComplete());
        $transport->releaseDial();
        $this->awaitFailover($operation, $read);

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertCount(2, $transport->connectCalls, 'the failover joins the heartbeat, without another dial');
        self::assertCount(1, $reports->closedErrors);
        self::assertInstanceOf(AuthenticationException::class, $reports->closedErrors[0]);
        self::assertCount(1, $reports->listenerErrors, 'joined authentication listener report must occur once');
        self::assertCount(1, $reports->loggedErrors, 'joined authentication error log must occur once');
        self::assertSame($reports->closedErrors, $reports->listenerErrors);
        self::assertSame($reports->listenerErrors, $reports->loggedErrors);
    }

    /**
     * Guard: "Reconnect attempts exhausted" is not reported by performRecovery(), so the failover's catch stays its one
     * report; the Closed event keeps the last dial's failure as that exception's previous.
     */
    #[DataProvider('paths')]
    public function testExhaustedFailoverStillReportsItsFailure(bool $operation): void
    {
        $transport = new ReconnectingTransport();
        $reports = new LameDuckReportLog();
        $connection = $this->connection($transport, $reports, maxReconnectAttempts: 1);
        $transport->refuseDials();

        $this->finishFailover($operation, $connection, $transport);

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertCount(2, $transport->connectCalls);
        self::assertCount(1, $reports->closedErrors);
        self::assertCount(1, $reports->listenerErrors, 'exhaustion must remain reported once');
        self::assertSame('Reconnect attempts exhausted', $reports->listenerErrors[0]->getMessage());
        self::assertSame($reports->listenerErrors, $reports->loggedErrors);
        self::assertSame($reports->closedErrors[0], $reports->listenerErrors[0]->getPrevious(), 'Closed keeps the last dial failure');
    }

    /**
     * Guard: a refusal in the recovery of a failed read with no lame duck - your processIncoming() or an operation's
     * read - is still reported once by performRecovery() and thrown to the read's caller.
     */
    #[DataProvider('paths')]
    public function testNonLameDuckRecoveryStillReportsAuthenticationOnce(bool $operation): void
    {
        $transport = new ReconnectingTransport();
        $reports = new LameDuckReportLog();
        $connection = $this->connection($transport, $reports);
        $transport->rejectAuthentication = true;
        $transport->dropConnection();
        $cancellation = new DeferredCancellation();
        $read = $this->resources()->ownOperation(
            $operation ? $connection->readIncomingForOperation($cancellation->getCancellation()) : $connection->processIncoming($cancellation->getCancellation()),
            $cancellation->cancel(...),
            'failed socket read',
        );
        $thrown = null;
        try {
            $read->await(new TimeoutCancellation(5));
        } catch (\Throwable $error) {
            $thrown = $error;
        }

        self::assertInstanceOf(AuthenticationException::class, $thrown);
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertCount(2, $transport->connectCalls);
        $authListener = array_values(array_filter($reports->listenerErrors, static fn(\Throwable $error): bool => $error instanceof AuthenticationException));
        $authLog = array_values(array_filter($reports->loggedErrors, static fn(\Throwable $error): bool => $error instanceof AuthenticationException));
        self::assertSame([$thrown], $authListener);
        self::assertSame([$thrown], $authLog);
        self::assertSame([$thrown], $reports->closedErrors);
    }

    /**
     * Two separate refusals on the same connection, reopened with connect() between them, are reported once each,
     * although their messages are identical: nothing deduplicates by message or across occurrences.
     */
    #[DataProvider('paths')]
    public function testSeparateRefusalsWithIdenticalMessagesAreBothReported(bool $operation): void
    {
        $transport = new ReconnectingTransport();
        $reports = new LameDuckReportLog();
        $connection = $this->connection($transport, $reports);
        for ($attempt = 0; $attempt < 2; $attempt++) {
            $transport->rejectAuthentication = true;
            $this->finishFailover($operation, $connection, $transport);
            if ($attempt === 0) {
                $transport->rejectAuthentication = false;
                $connection->connect()->await(new TimeoutCancellation(5));
                $connection->request('svc.warm', '', 1_000)->await();
            }
        }

        self::assertCount(2, $reports->listenerErrors, 'each independent authentication failure is reported once');
        self::assertCount(2, $reports->loggedErrors);
        self::assertCount(2, $reports->closedErrors);
        self::assertSame($reports->closedErrors, $reports->listenerErrors);
        self::assertSame($reports->listenerErrors, $reports->loggedErrors);
        self::assertNotSame($reports->listenerErrors[0], $reports->listenerErrors[1]);
        self::assertSame($reports->listenerErrors[0]->getMessage(), $reports->listenerErrors[1]->getMessage());
        self::assertCount(4, $transport->connectCalls);
    }

    /** @param (\Closure(): void)|null $lameDuckListener */
    private function connection(ReconnectingTransport $transport, LameDuckReportLog $reports, float $pingInterval = 0, ?\Closure $lameDuckListener = null, int $maxReconnectAttempts = 3): NatsConnection
    {
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport): array {
            if ($replyTo === null) {
                return [];
            }

            return $subject === 'svc.warm' ? $transport->replyFrame($replyTo, 'ok') : [self::LAME_DUCK];
        };
        $connection = $this->own(new NatsConnection(new NatsOptions(
            connectTimeoutMs: 2_000,
            reconnectEnabled: true,
            maxReconnectAttempts: $maxReconnectAttempts,
            reconnectDelayMs: 1,
            reconnectJitterMs: 0,
            pingIntervalSeconds: $pingInterval,
            maxPingsOut: 1,
            connectionListener: static function (ConnectionEvent $event, ?\Throwable $error) use ($reports, $lameDuckListener): void {
                $reports->events[] = $event;
                if ($event === ConnectionEvent::Closed) {
                    $reports->closedErrors[] = $error;
                }
                if ($event === ConnectionEvent::LameDuck) {
                    $lameDuckListener?->__invoke();
                }
            },
            errorListener: $reports->errorListener(),
            logger: $reports,
        ), $transport));
        $connection->connect()->await(new TimeoutCancellation(5));
        self::assertSame('ok', $connection->request('svc.warm', '', 1_000)->await()->payload);

        return $connection;
    }

    /** @return Future<mixed> */
    private function beginFailover(bool $operation, NatsConnection $connection, ReconnectingTransport $transport): Future
    {
        $cancellation = new DeferredCancellation();
        if (!$operation) {
            $transport->pushFrame(self::LAME_DUCK);
        }

        return $this->resources()->ownOperation(
            $operation ? $connection->request('svc.failover', '', 4_000, $cancellation->getCancellation()) : $connection->processIncoming($cancellation->getCancellation()),
            $cancellation->cancel(...),
            'lame-duck triggering read',
        );
    }

    private function finishFailover(bool $operation, NatsConnection $connection, ReconnectingTransport $transport): void
    {
        $this->awaitFailover($operation, $this->beginFailover($operation, $connection, $transport));
    }

    /** @param Future<mixed> $read */
    private function awaitFailover(bool $operation, Future $read): void
    {
        $result = null;
        $thrown = null;
        try {
            $result = $read->await(new TimeoutCancellation(5));
        } catch (\Throwable $error) {
            $thrown = $error;
        }
        if ($operation) {
            self::assertInstanceOf(ConnectionException::class, $thrown);
            self::assertSame('Connection is not open', $thrown->getMessage());
        } else {
            self::assertNull($thrown, 'inline failover remains an asynchronous report');
            self::assertSame(1, $result);
        }
    }

    /** @param \Closure(): bool $condition */
    private function waitUntil(\Closure $condition): void
    {
        $deadline = hrtime(true) + 3_000_000_000;
        while (!$condition()) {
            if (hrtime(true) >= $deadline) {
                self::fail('condition not reached within the hang guard');
            }
            delay(0.001);
        }
    }
}

/** Counts asynchronous error logs separately from the Closed lifecycle log. */
final class LameDuckReportLog extends AbstractLogger
{
    /** @var list<\Throwable> */
    public array $loggedErrors = [];
    /** @var list<\Throwable> */
    public array $listenerErrors = [];
    /** @var list<ConnectionEvent> */
    public array $events = [];
    /** @var list<\Throwable|null> */
    public array $closedErrors = [];

    public function __construct(private readonly bool $listenerThrows = false, private readonly bool $loggerThrows = false) {}

    /** @phpstan-impure The listener can append a report while the test yields to the event loop. */
    public function listenerReportCount(): int
    {
        return count($this->listenerErrors);
    }

    /** @return \Closure(\Throwable): void */
    public function errorListener(): \Closure
    {
        return function (\Throwable $error): void {
            $this->listenerErrors[] = $error;
            if ($this->listenerThrows) {
                throw new \RuntimeException('listener failed');
            }
        };
    }

    /** @param array<mixed> $context */
    public function log($level, string|\Stringable $message, array $context = []): void
    {
        if (str_starts_with((string) $message, 'NATS connection error: ') && ($context['exception'] ?? null) instanceof \Throwable) {
            $this->loggedErrors[] = $context['exception'];
            if ($this->loggerThrows) {
                throw new \RuntimeException('logger failed');
            }
        }
    }
}
