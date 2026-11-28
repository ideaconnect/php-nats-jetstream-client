<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredCancellation;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\AuthenticationException;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;

/**
 * A reconnect that is refused authentication reports the refusal once through the error listener and the error log
 * (#201): performRecovery() reports the refusal before it rethrows it, also to a recovery's joiners. Up to 2.35.0 a
 * lame-duck notice failed the connection over, and this class pinned that its failover reported the refusal only once
 * as well, inline and in a fiber of its own; those tests went with the failover (#265). What is left is the guard of
 * the recovery of a failed read. Over the scripted ReconnectingTransport: after a warm-up request, the connection
 * drops and every later handshake is answered with -ERR 'Authorization Violation'. Every connection and read is owned
 * by the test.
 */
final class LameDuckAuthenticationReportingTest extends TestCase
{
    use OwnsTestResources;

    protected function tearDown(): void
    {
        $this->releaseOwnedResources();
    }

    /** @return iterable<string, array{bool}> */
    public static function paths(): iterable
    {
        yield 'your read' => [false];
        yield 'an operation\'s read' => [true];
    }

    /**
     * Guard: a refusal in the recovery of a failed read - your processIncoming() or an operation's read - is reported
     * once by performRecovery() and thrown to the read's caller.
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

    private function connection(ReconnectingTransport $transport, LameDuckReportLog $reports): NatsConnection
    {
        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $replyTo !== null && $subject === 'svc.warm' ? $transport->replyFrame($replyTo, 'ok') : [];
        $connection = $this->own(new NatsConnection(new NatsOptions(
            connectTimeoutMs: 2_000,
            reconnectEnabled: true,
            maxReconnectAttempts: 3,
            reconnectDelayMs: 1,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            maxPingsOut: 1,
            connectionListener: static function (ConnectionEvent $event, ?\Throwable $error) use ($reports): void {
                $reports->events[] = $event;
                if ($event === ConnectionEvent::Closed) {
                    $reports->closedErrors[] = $error;
                }
            },
            errorListener: $reports->errorListener(),
            logger: $reports,
        ), $transport));
        $connection->connect()->await(new TimeoutCancellation(5));
        self::assertSame('ok', $connection->request('svc.warm', '', 1_000)->await()->payload);

        return $connection;
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

    /** @return \Closure(\Throwable): void */
    public function errorListener(): \Closure
    {
        return function (\Throwable $error): void {
            $this->listenerErrors[] = $error;
        };
    }

    /** @param array<mixed> $context */
    public function log($level, string|\Stringable $message, array $context = []): void
    {
        if (str_starts_with((string) $message, 'NATS connection error: ') && ($context['exception'] ?? null) instanceof \Throwable) {
            $this->loggedErrors[] = $context['exception'];
        }
    }
}
