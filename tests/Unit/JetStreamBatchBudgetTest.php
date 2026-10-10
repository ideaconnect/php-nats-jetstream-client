<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WedgedWriteTransport;
use IDCT\NATS\Transport\TransportClosedException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;

final class JetStreamBatchBudgetTest extends TestCase
{
    use ReconnectScenarios;

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{string,string}> */
    public static function outageCases(): iterable
    {
        foreach (['fetchBatch', 'fetchNext', 'directGetBatch', 'directGetLastForSubjects'] as $operation) {
            foreach (['existing', 'SUB', 'PUB'] as $trigger) {
                yield $operation . '/' . $trigger => [$operation, $trigger];
            }
        }
    }

    #[DataProvider('outageCases')]
    public function testSetupEndsWithinItsOwnBudgetAndCannotSendAfterRecovery(string $operation, string $trigger): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport);
        $transport->holdNextDial();
        if ($trigger === 'existing') {
            $transport->dropConnection();
            async(static fn() => $client->processIncoming()->await())->ignore();
            $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Connecting);
        } else {
            $transport->failNextWriteContaining($trigger === 'SUB' ? 'SUB _INBOX.JS.' : 'PUB $JS.API.');
        }
        $pending = self::issue($client, $operation);
        try {
            $pending->await(new TimeoutCancellation(1.5));
            self::fail('expected the batch budget to expire before the held reconnect');
        } catch (JetStreamException $error) {
            self::assertBatchTimeout($operation, $error);
            self::assertSame(ConnectionState::Connecting, $client->state());
            self::assertFalse($client->isSubscriptionActive(1));
        } finally {
            $transport->releaseDial();
        }
        $this->waitUntilOpen($client);
        self::assertSame([], $transport->controlLinesStartingWith('PUB $JS.API.', 1));
        self::assertSame([], $transport->controlLinesStartingWith('SUB _INBOX.JS.', 1));
    }

    /** @return iterable<string, array{string,string,bool}> */
    public static function immediateFailures(): iterable
    {
        foreach (['fetchBatch', 'directGetBatch'] as $operation) {
            foreach (['SUB', 'PUB'] as $trigger) {
                foreach ([true, false] as $reconnect) {
                    yield $operation . '/' . $trigger . '/' . (int) $reconnect => [$operation, $trigger, $reconnect];
                }
            }
        }
    }

    #[DataProvider('immediateFailures')]
    public function testFailedWritesRespectWaitingDisabledAndReconnectDisabled(string $operation, string $trigger, bool $reconnect): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, wait: $reconnect ? false : true, reconnect: $reconnect);
        $transport->holdNextDial();
        $transport->failNextWriteContaining($trigger === 'SUB' ? 'SUB _INBOX.JS.' : 'PUB $JS.API.');
        try {
            self::issue($client, $operation)->await(new TimeoutCancellation(0.5));
            self::fail('expected an immediate connection error');
        } catch (ConnectionException $error) {
            self::assertSame($reconnect ? 'Connection is not open' : 'Reconnect is disabled', $error->getMessage());
            if ($reconnect) {
                $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
            }
        } finally {
            $transport->releaseDial();
        }
        if ($reconnect) {
            $this->waitUntilOpen($client);
        } else {
            self::assertSame(ConnectionState::Closed, $client->state());
        }
        self::assertSame([], $transport->controlLinesStartingWith('PUB $JS.API.', 1));
    }

    /** @return iterable<string, array{string,int}> */
    public static function inlineWrites(): iterable
    {
        foreach (['fetchBatch', 'directGetBatch'] as $operation) {
            foreach (['SUB' => 2, 'PUB' => 3, 'UNSUB' => 4] as $stage => $successfulWrites) {
                yield $operation . '/' . $stage => [$operation, $successfulWrites];
            }
        }
    }

    #[DataProvider('inlineWrites')]
    public function testInlineWriteBackpressureCannotHoldTheOperationPastItsBudget(string $operation, int $successfulWrites): void
    {
        $transport = new WedgedWriteTransport([ReconnectingTransport::INFO, "PONG\r\n"], $successfulWrites);
        $client = $this->own(new NatsClient(new NatsOptions(pingIntervalSeconds: 0), $transport));
        $this->opened[] = $client;
        $client->connect()->await();
        $pending = self::issue($client, $operation);
        try {
            $pending->await(new TimeoutCancellation(1.5));
            self::fail('expected the batch timeout');
        } catch (JetStreamException $error) {
            self::assertBatchTimeout($operation, $error);
        }
        self::assertTrue($pending->isComplete(), 'the operation, rather than the test guard, ended');
        self::assertFalse($client->isSubscriptionActive(1));
        $client->disconnect()->await();
    }

    public function testAnExpiryLongerThanTheGlobalRequestTimeoutCanWaitForRecovery(): void
    {
        $transport = new ReconnectingTransport();
        self::answer($transport);
        $client = $this->client($transport, requestTimeoutMs: 50);
        $transport->holdNextDial();
        $transport->dropConnection();
        async(static fn() => $client->processIncoming()->await())->ignore();
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Connecting);
        $pending = self::issue($client, 'fetchBatch', 500);
        delay(0.15);
        self::assertFalse($pending->isComplete(), 'the global request timeout does not cap the fetch');
        $transport->releaseDial();
        self::assertSame(['message'], $pending->await(new TimeoutCancellation(1)));
    }

    /** @return iterable<string,array{float,string}> */
    public static function shortenedPulls(): iterable
    {
        yield 'before old cutoff' => [0.99, 'existing'];
        yield 'after old cutoff' => [1.01, 'existing'];
        yield 'failed PUB retry recomputes payload' => [1.01, 'PUB'];
    }

    #[DataProvider('shortenedPulls')]
    public function testTheWireExpiryFitsTheRemainingBudgetWithoutTheOldCutoff(float $wait, string $trigger): void
    {
        $transport = new ReconnectingTransport();
        self::answer($transport);
        $bodies = [];
        $transport->afterWrite = static function (string $bytes) use (&$bodies): void {
            if (str_starts_with($bytes, 'PUB $JS.API.')) {
                $bodies[] = json_decode(explode("\r\n", $bytes)[1], true, flags: JSON_THROW_ON_ERROR);
            }
        };
        $client = $this->client($transport);
        $transport->holdNextDial();
        if ($trigger === 'existing') {
            $transport->dropConnection();
            async(static fn() => $client->processIncoming()->await())->ignore();
            $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Connecting);
        } else {
            $transport->failNextWriteContaining('PUB $JS.API.');
        }
        $gate = EventLoop::delay($wait, $transport->releaseDial(...));
        try {
            self::assertSame(['message'], self::issue($client, 'fetchBatch', 1000)->await(new TimeoutCancellation(2.5)));
        } finally {
            EventLoop::cancel($gate);
        }
        self::assertCount(1, $bodies);
        self::assertGreaterThan(0, $bodies[0]['expires']);
        self::assertLessThan(1_000_000_000, $bodies[0]['expires']);
    }

    /** @return iterable<string,array{bool}> */
    public static function omittedHeartbeats(): iterable
    {
        yield 'delayed send' => [false];
        yield 'failed shortened send is retried' => [true];
    }

    #[DataProvider('omittedHeartbeats')]
    public function testAnOmittedWireHeartbeatAlsoDisablesTheLocalMissTimer(bool $retry): void
    {
        $transport = new ReconnectingTransport();
        self::answer($transport);
        $transport->responseDelay = 0.22;
        $body = null;
        $transport->afterWrite = static function (string $bytes) use (&$body): void {
            if (str_starts_with($bytes, 'PUB $JS.API.')) {
                $body = json_decode(explode("\r\n", $bytes)[1], true, flags: JSON_THROW_ON_ERROR);
            }
        };
        $client = $this->client($transport);
        $transport->holdNextDial();
        $transport->dropConnection();
        async(static fn() => $client->processIncoming()->await())->ignore();
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Connecting);
        if ($retry) {
            $transport->failNextWriteContaining('PUB $JS.API.');
        }
        $gate = EventLoop::delay(1.73, $transport->releaseDial(...));
        try {
            $messages = $client->jetStream()->fetchBatch('S', 'C', 1, 1000, ['idle_heartbeat' => 100_000_000])->await(new TimeoutCancellation(2.5));
            self::assertSame('message', $messages[0]->payload);
        } catch (JetStreamException $error) {
            // The answer lands 0.22 s after the shortened send, about 50 ms before the fetch's deadline: the 100 ms
            // response margin is all there is once the answer has to come after two heartbeats. A runner slowed down by
            // coverage can spend those 50 ms on the reconnect, and the fetch then ends with its 408. An armed local miss
            // timer ends it with its own error instead, as it does whenever the reconnect is quick.
            self::assertSame(408, $error->getCode(), $error->getMessage());
        } finally {
            EventLoop::cancel($gate);
        }
        self::assertIsArray($body);
        self::assertArrayNotHasKey('idle_heartbeat', $body);
        self::assertLessThan(200_000_000, $body['expires']);
    }

    public function testLateSubCompletionIsReleasedInOrderAndLeavesPongCorrelationIntact(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport);
        $transport->stallNextWriteContaining('SUB _INBOX.JS.', 5);
        try {
            self::issue($client, 'fetchBatch')->await(new TimeoutCancellation(1.5));
            self::fail('expected SUB timeout');
        } catch (JetStreamException $error) {
            self::assertSame(408, $error->getCode());
        }
        self::assertFalse($client->isSubscriptionActive(1));
        $transport->releaseStalledWrites();
        $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith('UNSUB ') !== []);
        self::assertCount(1, $transport->controlLinesStartingWith('SUB _INBOX.JS.'));
        self::assertSame(['UNSUB 1'], $transport->controlLinesStartingWith('UNSUB '));
        $client->flush()->await(new TimeoutCancellation(0.5));
    }

    public function testTimeoutDuringAReconnectedListenerReleasesTheReplayedInbox(): void
    {
        $transport = new ReconnectingTransport();
        $listener = new DeferredFuture();
        $client = $this->client($transport, listener: static function (ConnectionEvent $event) use ($listener): void {
            if ($event === ConnectionEvent::Reconnected) {
                $listener->getFuture()->await();
            }
        });
        $transport->failNextWriteContaining('SUB _INBOX.JS.');
        $keepAlive = EventLoop::delay(2, static function (): void {});
        try {
            self::issue($client, 'fetchBatch')->await(new TimeoutCancellation(1.5));
            self::fail('expected operation timeout during listener');
        } catch (JetStreamException $error) {
            self::assertSame(408, $error->getCode());
            self::assertSame(ConnectionState::Open, $client->state());
            self::assertFalse($client->isSubscriptionActive(1));
            $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith('UNSUB ', 1) !== []);
            self::assertSame(['UNSUB 1'], $transport->controlLinesStartingWith('UNSUB ', 1));
        } finally {
            EventLoop::cancel($keepAlive);
            $listener->complete();
        }
    }

    /** @return iterable<string,array{string,bool}> */
    public static function rejectedInboxes(): iterable
    {
        foreach (['fetchBatch', 'directGetBatch'] as $operation) {
            yield $operation . '/permissions' => [$operation, true];
            yield $operation . '/limit' => [$operation, false];
        }
    }

    #[DataProvider('rejectedInboxes')]
    public function testAnInboxRejectedDuringPubRecoveryCannotReceiveTheRetry(string $operation, bool $permissions): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport);
        $transport->afterWrite = static function (string $bytes) use ($transport, $permissions): void {
            if (!$permissions && $transport->epoch() === 1 && str_starts_with($bytes, 'CONNECT ')) {
                $transport->answerPings = false;
            }
            if ($transport->epoch() === 1 && str_starts_with($bytes, 'SUB ')) {
                $inbox = explode(' ', $bytes)[1];
                $error = $permissions ? 'Permissions Violation for Subscription to "' . $inbox . '"' : 'maximum subscriptions exceeded';
                $transport->pushFrame("-ERR '" . $error . "'\r\n");
            }
        };
        $transport->failNextWriteContaining('PUB $JS.API.');
        try {
            self::issue($client, $operation)->await(new TimeoutCancellation(0.5));
            self::fail('expected inbox rejection');
        } catch (ConnectionException $error) {
            self::assertStringContainsString($permissions ? 'Permissions Violation' : 'maximum subscriptions exceeded', $error->getMessage());
        }
        self::assertSame([], $transport->controlLinesStartingWith('PUB $JS.API.', 1));
    }

    /** @return iterable<string,array{bool}> */
    public static function progressingBatches(): iterable
    {
        yield 'end marker settles pending publication' => [true];
        yield 'actual silence eventually stalls' => [false];
    }

    #[DataProvider('progressingBatches')]
    public function testDirectGetProgressRenewsAPendingPubAndNeverRetriesAfterCompletion(bool $complete): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport);
        $write = new DeferredFuture();
        $timers = [];
        $transport->responder = static function (string $subject, ?string $reply) use ($transport, $complete, &$timers): array {
            if ($reply === null) {
                return [];
            }
            foreach ([[0.4, 'one'], [0.8, 'two']] as [$when, $payload]) {
                $timers[] = EventLoop::delay($when, static function () use ($transport, $reply, $payload): void {
                    foreach ($transport->replyFrame($reply, $payload) as $frame) {
                        $transport->pushFrame($frame);
                    }
                });
            }
            if ($complete) {
                $timers[] = EventLoop::delay(1.2, static fn() => $transport->pushFrame(self::endFrame($reply, 1)));
            }

            return [];
        };
        $transport->afterWrite = static function (string $bytes) use ($write): void {
            if (str_starts_with($bytes, 'PUB $JS.API.')) {
                $write->getFuture()->await();
                throw new TransportClosedException('late failed PUB');
            }
        };
        $stop = false;
        $pump = self::pump($client, static function () use (&$stop): bool {
            return $stop;
        });
        $pending = self::issue($client, 'directGetBatch');
        try {
            delay(1.1);
            self::assertFalse($pending->isComplete(), 'recent replies keep the pending publication alive beyond its initial interval');
            if ($complete) {
                self::assertSame(['one', 'two'], $pending->await(new TimeoutCancellation(0.5)));
            } else {
                try {
                    $pending->await(new TimeoutCancellation(1));
                    self::fail('expected a real no-progress stall');
                } catch (JetStreamException $error) {
                    self::assertStringContainsString('received 2 message(s), no end-of-batch marker', $error->getMessage());
                }
            }
        } finally {
            $stop = true;
            $pump->await(new TimeoutCancellation(0.5));
            foreach ($timers as $timer) {
                EventLoop::cancel($timer);
            }
            $write->complete();
        }
        $this->waitUntil(static fn(): bool => $transport->epoch() === 1 && $client->state() === ConnectionState::Open);
        self::assertSame([], $transport->controlLinesStartingWith('PUB $JS.API.', 1));
    }

    private function client(ReconnectingTransport $transport, bool $wait = true, bool $reconnect = true, int $requestTimeoutMs = 2000, ?\Closure $listener = null): NatsClient
    {
        $client = $this->own(new NatsClient(new NatsOptions(
            connectTimeoutMs: 5000,
            requestTimeoutMs: $requestTimeoutMs,
            reconnectEnabled: $reconnect,
            waitForReconnect: $wait,
            reconnectDelayMs: 5,
            reconnectMaxDelayMs: 20,
            reconnectJitterMs: 0,
            maxReconnectAttempts: 1000,
            pingIntervalSeconds: 0,
            connectionListener: $listener,
        ), $transport));
        $this->opened[] = $client;
        $client->connect()->await();

        return $client;
    }

    /** @return Future<list<string>> */
    private static function issue(NatsClient $client, string $operation, int $expiresMs = 1): Future
    {
        $js = $client->jetStream();
        $future = match ($operation) {
            'fetchNext' => $js->fetchNext('S', 'C', $expiresMs)->map(static fn(NatsMessage $message): array => [$message]),
            'directGetBatch' => $js->directGetBatch('S', ['batch' => 2], $expiresMs),
            'directGetLastForSubjects' => $js->directGetLastForSubjects('S', ['evt.s'], $expiresMs),
            default => $js->fetchBatch('S', 'C', 1, $expiresMs),
        };

        return $future->map(static fn(array $messages): array => array_map(static fn(NatsMessage $message): string => $message->payload, $messages));
    }

    private static function assertBatchTimeout(string $operation, JetStreamException $error): void
    {
        if (str_starts_with($operation, 'fetch')) {
            self::assertSame(408, $error->getCode());
            self::assertSame('No messages received within timeout', $error->getMessage());
        } else {
            self::assertStringContainsString('stalled: no progress for 1001 ms', $error->getMessage());
        }
    }

    private static function answer(ReconnectingTransport $transport): void
    {
        $transport->responder = static function (string $subject, ?string $reply) use ($transport): array {
            if ($reply === null) {
                return [];
            }
            $frames = $transport->replyFrame($reply, 'message');
            if (str_starts_with($subject, '$JS.API.DIRECT.GET.')) {
                $frames[] = self::endFrame($reply, $transport->sidFor($reply) ?? 1);
            }

            return $frames;
        };
    }

    private static function endFrame(string $reply, int $sid): string
    {
        $headers = "NATS/1.0 204\r\n\r\n";

        return sprintf("HMSG %s %d %d %d\r\n", $reply, $sid, strlen($headers), strlen($headers)) . $headers . "\r\n";
    }

    /** @param \Closure():bool $stopped
     * @return Future<void>
     */
    private static function pump(NatsClient $client, \Closure $stopped): Future
    {
        return async(static function () use ($client, $stopped): void {
            while (!$stopped()) {
                try {
                    $client->processIncoming(new TimeoutCancellation(0.1))->await();
                } catch (CancelledException) {
                }
            }
        });
    }
}
