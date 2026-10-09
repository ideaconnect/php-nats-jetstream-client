<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Transport\TransportClosedException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\delay;

/** A failed request PUB/HPUB must keep the request's budget and never enter the reconnect buffer. */
final class FailedRequestWriteTest extends TestCase
{
    use ReconnectScenarios;

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{string}> */
    public static function variants(): iterable
    {
        yield 'request PUB' => ['request'];
        yield 'request HPUB' => ['headers'];
        yield 'requestMany PUB' => ['many'];
        yield 'requestMany HPUB' => ['manyHeaders'];
    }

    #[DataProvider('variants')]
    public function testTimeoutEndsTheRequestBeforeRecoveryAndDoesNotResendItLater(string $variant): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, connectTimeoutMs: 2_000);
        $transport->holdNextDial();
        $transport->failNextWriteContaining('PUB svc');
        $request = self::issue($connection, $variant, 80);

        try {
            $request->await(new TimeoutCancellation(0.5));
            self::fail('expected the request timeout before the held recovery completes');
        } catch (TimeoutException $error) {
            self::assertStringContainsString('Request timed out for subject svc', $error->getMessage());
            self::assertSame(ConnectionState::Connecting, $connection->state());
        } finally {
            $transport->releaseDial();
        }

        $this->waitUntilOpen($connection);
        delay(0.01);
        self::assertSame([], $transport->controlLinesStartingWith('PUB svc', 1));
        self::assertSame([], $transport->controlLinesStartingWith('HPUB svc', 1));
        self::assertSame(0, $connection->statistics()->outMsgs);
        self::assertNoMuxWaiters($connection);
    }

    #[DataProvider('variants')]
    public function testCancellationEndsTheRequestBeforeRecoveryAndDoesNotResendItLater(string $variant): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, connectTimeoutMs: 2_000);
        $transport->holdNextDial();
        $transport->failNextWriteContaining('PUB svc');
        $cancellation = new DeferredCancellation();
        $request = self::issue($connection, $variant, 2_000, $cancellation->getCancellation());
        $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Connecting);
        $cancellation->cancel();

        try {
            $request->await(new TimeoutCancellation(0.5));
            self::fail('expected caller cancellation');
        } catch (CancelledException) {
            self::assertTrue($request->isComplete(), 'the caller cancellation completed the request, not the outer test guard');
            self::assertSame(ConnectionState::Connecting, $connection->state());
        } finally {
            $transport->releaseDial();
        }

        $this->waitUntilOpen($connection);
        delay(0.01);
        self::assertSame([], $transport->controlLinesStartingWith('PUB svc', 1));
        self::assertSame([], $transport->controlLinesStartingWith('HPUB svc', 1));
        self::assertNoMuxWaiters($connection);
    }

    #[DataProvider('variants')]
    public function testWaitingDisabledFailsBeforeRecoveryWithoutBufferingTheRequest(string $variant): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, waitForReconnect: false, connectTimeoutMs: 2_000);
        $transport->holdNextDial();
        $transport->failNextWriteContaining('PUB svc');

        try {
            self::issue($connection, $variant, 2_000)->await(new TimeoutCancellation(0.5));
            self::fail('expected ConnectionException without waiting for recovery');
        } catch (ConnectionException $error) {
            self::assertSame('Connection is not open', $error->getMessage());
            self::assertInstanceOf(TransportClosedException::class, $error->getPrevious());
            // The fail-fast request can finish before the recovery fiber reaches its dial.
            $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
        } finally {
            $transport->releaseDial();
        }

        $this->waitUntilOpen($connection);
        delay(0.01);
        self::assertSame([], $transport->controlLinesStartingWith('PUB svc', 1));
        self::assertSame([], $transport->controlLinesStartingWith('HPUB svc', 1));
        self::assertNoMuxWaiters($connection);
    }

    #[DataProvider('variants')]
    public function testRecoveryWithinBudgetReplaysTheInboxAndRetriesTheRequestOnce(string $variant): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $replyTo !== null
            ? $transport->replyFrame($replyTo, 'reply:' . $payload)
            : [];
        $transport->holdNextDial();
        $transport->failNextWriteContaining('PUB svc');
        $request = self::issue($connection, $variant, 2_000);
        $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Connecting);
        $transport->releaseDial();

        $reply = $request->await(new TimeoutCancellation(3));
        self::assertSame('reply:payload', is_array($reply) ? $reply[0]->payload : $reply->payload);
        self::assertSame(1, $transport->epoch());
        self::assertCount(1, $transport->controlLinesStartingWith(self::publishPrefix($variant), 1));
        self::assertSame(1, $connection->statistics()->outMsgs);
        self::assertSame(strlen('payload'), $connection->statistics()->outBytes);
        self::assertNoMuxWaiters($connection);
    }

    #[DataProvider('variants')]
    public function testReconnectDisabledClosesTheConnectionInsteadOfRetrying(string $variant): void
    {
        $transport = new ReconnectingTransport();
        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false, pingIntervalSeconds: 0), $transport);
        $this->opened[] = $connection;
        $connection->connect()->await();
        $transport->failNextWriteContaining('PUB svc');

        try {
            self::issue($connection, $variant, 2_000)->await();
            self::fail('expected reconnect-disabled failure');
        } catch (ConnectionException $error) {
            self::assertSame('Reconnect is disabled', $error->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertNoMuxWaiters($connection);
    }

    #[DataProvider('variants')]
    public function testCancellationInTheReconnectedListenerPreventsTheRetry(string $variant): void
    {
        $transport = new ReconnectingTransport();
        $cancellation = new DeferredCancellation();
        $connection = $this->connect($transport, connectionListener: static function (ConnectionEvent $event) use ($cancellation): void {
            if ($event === ConnectionEvent::Reconnected) {
                $cancellation->cancel();
            }
        });
        $transport->failNextWriteContaining('PUB svc');

        try {
            self::issue($connection, $variant, 2_000, $cancellation->getCancellation())->await();
            self::fail('expected cancellation before the retry');
        } catch (CancelledException) {
            self::assertSame(ConnectionState::Open, $connection->state());
        }

        delay(0.01);
        self::assertSame([], $transport->controlLinesStartingWith(self::publishPrefix($variant), 1));
        self::assertNoMuxWaiters($connection);
    }

    #[DataProvider('variants')]
    public function testCancellationBeforeTheScheduledRetryRunsPreventsTheWrite(string $variant): void
    {
        $transport = new ReconnectingTransport();
        $cancellation = new DeferredCancellation();
        $connection = $this->connect($transport, connectionListener: static function (ConnectionEvent $event) use ($cancellation): void {
            if ($event === ConnectionEvent::Reconnected) {
                // Run after the recovery completes and wakes the request, before its scheduled
                // writer starts. Checking only in the request's fiber would allow this late PUB.
                EventLoop::queue(static function () use ($cancellation): void {
                    EventLoop::queue(static function () use ($cancellation): void {
                        EventLoop::queue($cancellation->cancel(...));
                    });
                });
            }
        });
        $transport->failNextWriteContaining('PUB svc');

        try {
            self::issue($connection, $variant, 2_000, $cancellation->getCancellation())->await();
            self::fail('expected cancellation before the scheduled retry');
        } catch (CancelledException) {
            self::assertSame(ConnectionState::Open, $connection->state());
        }

        delay(0.01);
        self::assertSame([], $transport->controlLinesStartingWith(self::publishPrefix($variant), 1));
        self::assertNoMuxWaiters($connection);
    }

    /** @return iterable<string, array{string, bool}> */
    public static function invalidInboxes(): iterable
    {
        foreach (self::variants() as $name => [$variant]) {
            yield $name . ' / permission rejection' => [$variant, true];
            yield $name . ' / subscription limit drop' => [$variant, false];
        }
    }

    #[DataProvider('invalidInboxes')]
    public function testAnInvalidatedReplayedInboxPreventsTheRetry(string $variant, bool $permissions): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $transport->afterWrite = static function (string $bytes) use ($transport, $permissions): void {
            if (!$permissions && $transport->epoch() === 1 && str_starts_with($bytes, 'CONNECT ')) {
                // Inject the limit error ahead of the replay fence PONG. afterWrite runs after
                // processing the coalesced SUB+PING, so hold that PONG back before the replay.
                $transport->answerPings = false;
            }

            if ($transport->epoch() === 1 && str_starts_with($bytes, 'SUB ')) {
                $wildcard = explode(' ', $bytes)[1];
                $error = $permissions
                    ? 'Permissions Violation for Subscription to "' . $wildcard . '"'
                    : 'maximum subscriptions exceeded';
                $transport->pushFrame("-ERR '" . $error . "'\r\n");
            }
        };
        $transport->failNextWriteContaining('PUB svc');

        try {
            self::issue($connection, $variant, 2_000)->await();
            self::fail('expected the replayed inbox rejection');
        } catch (ConnectionException $error) {
            self::assertStringContainsString('shared reply-inbox subscription', $error->getMessage());
            self::assertStringContainsString($permissions ? 'permissions violation' : 'maximum subscriptions exceeded', $error->getMessage());
        }

        self::assertSame([], $transport->controlLinesStartingWith(self::publishPrefix($variant), 1));
        self::assertNoMuxWaiters($connection);
    }

    #[DataProvider('variants')]
    public function testASecondFailedWriteDoesNotRetryTheRequestAgain(string $variant): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, connectionListener: static function (ConnectionEvent $event) use ($transport): void {
            if ($event === ConnectionEvent::Reconnected) {
                $transport->failNextWriteContaining('PUB svc');
            }
        });
        $transport->failNextWriteContaining('PUB svc');

        try {
            self::issue($connection, $variant, 2_000)->await();
            self::fail('expected the retry write failure');
        } catch (TransportClosedException) {
            self::assertSame(1, $transport->epoch(), 'only one recovery and one retry');
        }

        self::assertSame(0, $connection->statistics()->outMsgs);
        self::assertNoMuxWaiters($connection);
    }

    #[DataProvider('variants')]
    public function testInlineWriteBackpressureIsBoundedByTheRequestTimeout(string $variant): void
    {
        $transport = new FakeTransport([ReconnectingTransport::INFO, "PONG\r\n"]);
        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $this->opened[] = $connection;
        $connection->connect()->await();
        $transport->wedgeOnWriteContaining = 'PUB svc';

        try {
            self::issue($connection, $variant, 80)->await(new TimeoutCancellation(0.5));
            self::fail('expected request timeout during inline write backpressure');
        } catch (TimeoutException $error) {
            self::assertStringContainsString('Request timed out for subject svc', $error->getMessage());
        }

        self::assertNoMuxWaiters($connection);
        // The transport cannot cancel an already-started write. Teardown releases it; it must never retry.
        $connection->disconnect()->await();
        delay(0.01);
        self::assertCount(1, array_filter($transport->writes, static fn(string $bytes): bool => str_starts_with($bytes, self::publishPrefix($variant))));
    }

    #[DataProvider('variants')]
    public function testInlineWriteBackpressureIsBoundedByCallerCancellation(string $variant): void
    {
        $transport = new FakeTransport([ReconnectingTransport::INFO, "PONG\r\n"]);
        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $this->opened[] = $connection;
        $connection->connect()->await();
        $transport->wedgeOnWriteContaining = 'PUB svc';
        $cancellation = new DeferredCancellation();
        $request = self::issue($connection, $variant, 2_000, $cancellation->getCancellation());
        $this->waitUntil(static fn(): bool => count($transport->writes) === 4);
        $cancellation->cancel();

        try {
            $request->await(new TimeoutCancellation(0.5));
            self::fail('expected cancellation during inline write backpressure');
        } catch (CancelledException) {
            self::assertTrue($request->isComplete(), 'the request ended before the outer test guard');
        }

        self::assertNoMuxWaiters($connection);
        $connection->disconnect()->await();
        delay(0.01);
        self::assertCount(1, array_filter($transport->writes, static fn(string $bytes): bool => str_starts_with($bytes, self::publishPrefix($variant))));
    }

    #[DataProvider('variants')]
    public function testAnAbandonedWriteThatLaterFailsRepairsTheConnectionWithoutResending(string $variant): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $transport->stallNextWriteContaining('PUB svc', 5.0);

        try {
            self::issue($connection, $variant, 80)->await(new TimeoutCancellation(0.5));
            self::fail('expected request timeout before the pending write finishes');
        } catch (TimeoutException) {
            self::assertSame(ConnectionState::Open, $connection->state());
        }

        $transport->dropConnection();
        $this->waitUntil(static fn(): bool => $transport->epoch() === 1);
        $this->waitUntilOpen($connection);
        self::assertSame([], $transport->controlLinesStartingWith(self::publishPrefix($variant), 1));
        self::assertNoMuxWaiters($connection);
    }

    /** @return iterable<string, array{array<string,string>|null, bool}> */
    public static function collections(): iterable
    {
        yield 'PUB without replies' => [null, false];
        yield 'HPUB without replies' => [['X-Test' => 'value'], false];
        yield 'PUB with a reply' => [null, true];
        yield 'HPUB with a reply' => [['X-Test' => 'value'], true];
    }

    /** @param array<string,string>|null $headers */
    #[DataProvider('collections')]
    public function testCollectionTimeoutAfterASuccessfulRetryReturnsTheCollectedReplies(?array $headers, bool $answer): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        if ($answer) {
            $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $replyTo !== null
                ? $transport->replyFrame($replyTo, 'reply')
                : [];
        }
        $transport->failNextWriteContaining('PUB svc');

        $replies = $connection->requestMany('svc', 'payload', headers: $headers, totalTimeoutMs: 80)->await(new TimeoutCancellation(0.5));

        self::assertSame($answer ? ['reply'] : [], array_map(static fn(NatsMessage $message): string => $message->payload, $replies));
        self::assertSame(1, $transport->epoch());
        self::assertCount(1, $transport->controlLinesStartingWith($headers === null ? 'PUB svc' : 'HPUB svc', 1));
        self::assertNoMuxWaiters($connection);
    }

    /** @return Future<NatsMessage|list<NatsMessage>> */
    private static function issue(NatsConnection $connection, string $variant, int $timeoutMs, ?Cancellation $cancellation = null): Future
    {
        return match ($variant) {
            'request' => $connection->request('svc', 'payload', $timeoutMs, $cancellation),
            'headers' => $connection->requestWithHeaders('svc', 'payload', ['X-Test' => 'value'], $timeoutMs, $cancellation),
            'many' => $connection->requestMany('svc', 'payload', maxResponses: 1, totalTimeoutMs: $timeoutMs, cancellation: $cancellation),
            'manyHeaders' => $connection->requestMany('svc', 'payload', headers: ['X-Test' => 'value'], maxResponses: 1, totalTimeoutMs: $timeoutMs, cancellation: $cancellation),
            default => throw new \LogicException('Unknown request variant'),
        };
    }

    private static function publishPrefix(string $variant): string
    {
        return $variant === 'headers' || $variant === 'manyHeaders' ? 'HPUB svc' : 'PUB svc';
    }

    private static function assertNoMuxWaiters(NatsConnection $connection): void
    {
        self::assertSame([], (new \ReflectionProperty(NatsConnection::class, 'muxWaiters'))->getValue($connection));
        self::assertSame([], (new \ReflectionProperty(NatsConnection::class, 'muxWakes'))->getValue($connection));
    }
}
