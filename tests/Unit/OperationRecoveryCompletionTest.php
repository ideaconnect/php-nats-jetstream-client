<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredFuture;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Core\SubscriptionQueue;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;

/** Recovery completion for initiating operations (#202). All suspension gates belong to the test. */
final class OperationRecoveryCompletionTest extends TestCase
{
    use ReconnectScenarios;

    private const LAME_DUCK = 'INFO {"server_id":"S1","ldm":true,"connect_urls":["127.0.0.1:4223"]}' . "\r\n";

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{string, string}> */
    public static function pollCases(): iterable
    {
        foreach (['lost read', 'lame duck', 'joiner'] as $origin) {
            foreach (['listener', 'lower sid'] as $tail) {
                yield $origin . ', held ' . $tail => [$origin, $tail];
            }
        }
    }

    /**
     * A poll on `jobs` whose read meets the lost connection, or brings the lame-duck INFO, takes the `job` the new server
     * sends during the replay while the recovery's tail - the Reconnected listener, or the handler of `low`, a lower
     * sid delivered first - is still held. Up to 2.25.10 such a poll waited for the whole tail and timed out with its
     * message queued. The joiner rows are guards: a poll that joined another read's reconnect already returned.
     */
    #[DataProvider('pollCases')]
    public function testAPollTakesItsReplayMessageWhileTheRecoveryTailIsHeld(string $origin, string $tail): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        [$entered, $release, $returned] = $this->gate();
        $events = [];
        $client = $this->client($watched, static function (ConnectionEvent $event) use (&$events, $tail, $entered, $release, $returned): void {
            $events[] = $event;
            if ($event === ConnectionEvent::Reconnected && $tail === 'listener') {
                self::hold($entered, $release, $returned);
            }
        });
        $low = $client->subscribe('low', static function () use ($tail, $entered, $release, $returned): void {
            if ($tail === 'lower sid') {
                self::hold($entered, $release, $returned);
            }
        })->await();
        $queue = $client->subscribeQueue('jobs')->await();
        $transport->afterWrite = static function (string $bytes) use ($transport, $low, $queue): void {
            if ($transport->epoch() === 1 && str_contains($bytes, 'SUB jobs')) {
                $transport->afterWrite = null;
                $transport->pushFrame(ReconnectingTransport::msgFrame('low', $low, 'l') . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job'));
            }
        };

        if ($origin === 'joiner') {
            $transport->holdNextDial();
            $this->resources()->onRelease($transport->releaseDial(...), 'the held dial');
            $reader = $this->resources()->ownOperation($client->processIncoming(), label: 'the initiating application read');
            $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
            $transport->dropConnection();
            $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
        }
        $poll = $this->resources()->ownOperation(async(static fn(): ?NatsMessage => $queue->setTimeout(30.0)->next()), label: 'the poll');
        if ($origin === 'joiner') {
            $checkpoint = new DeferredFuture();
            EventLoop::queue(static fn() => $checkpoint->complete());
            $checkpoint->getFuture()->await(new TimeoutCancellation(5));
            self::assertSame(ConnectionState::Connecting, $client->state());
            $transport->releaseDial();
        } else {
            $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
            self::trigger($transport, $origin);
        }
        $entered->getFuture()->await(new TimeoutCancellation(5));
        self::assertSame(ConnectionState::Open, $client->state());

        $message = null;
        $error = null;
        try {
            // A watchdog bounds a broken probe; the operation has a long budget. Release is only after the assertion.
            $message = $poll->await(new TimeoutCancellation(1));
        } catch (\Throwable $failure) {
            $error = $failure;
        }
        self::assertNull($error, 'the poll must complete with recovery delivery still held: ' . ($error?->getMessage() ?? ''));
        self::assertSame('job', $message?->payload);
        self::assertFalse($returned->isComplete());
        $release->complete();
        $returned->getFuture()->await(new TimeoutCancellation(5));
        if (isset($reader)) {
            $reader->await(new TimeoutCancellation(5));
        }
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected], array_values(array_filter($events, static fn(ConnectionEvent $event): bool => $event !== ConnectionEvent::LameDuck && $event !== ConnectionEvent::DiscoveredServers)));
    }

    /** @return iterable<string, array{string}> */
    public static function origins(): iterable
    {
        yield 'lost read' => ['lost read'];
        yield 'lame duck' => ['lame duck'];
    }

    /**
     * A request whose read started the recovery reads its reply, which the Reconnected listener puts on the new socket
     * before it waits, while that listener is still held. Up to 2.25.10 nothing read the socket meanwhile and the
     * request timed out, its reply lost.
     */
    #[DataProvider('origins')]
    public function testARequestReadsItsReplyWhileTheReconnectedListenerIsHeld(string $origin): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        [$entered, $release, $returned] = $this->gate();
        $reply = null;
        $reconnected = 0;
        $client = $this->client($watched, static function (ConnectionEvent $event) use ($transport, &$reply, &$reconnected, $entered, $release, $returned): void {
            if ($event === ConnectionEvent::Reconnected) {
                $reconnected++;
                $transport->pushFrame($transport->replyFrame((string) $reply, 'reply')[0]);
                self::hold($entered, $release, $returned);
            }
        });
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport, &$reply): array {
            if ($subject === 'svc.warm' && $replyTo !== null) {
                return $transport->replyFrame($replyTo, 'warm');
            }
            $reply = $replyTo;

            return [];
        };
        $client->request('svc.warm', '', 5_000)->await();
        $request = $this->resources()->ownOperation($client->request('svc.echo', '', 30_000), label: 'the request');
        $this->waitUntil(static function () use (&$reply, $watched): bool {
            return $reply !== null && $watched->readsUnderWay === 1;
        });
        self::trigger($transport, $origin);
        $entered->getFuture()->await(new TimeoutCancellation(5));
        $message = null;
        $error = null;
        try {
            $message = $request->await(new TimeoutCancellation(1));
        } catch (\Throwable $failure) {
            $error = $failure;
        }
        self::assertNull($error, 'the request must read while the listener is held: ' . ($error?->getMessage() ?? ''));
        self::assertSame('reply', $message?->payload);
        self::assertFalse($returned->isComplete());
        $release->complete();
        $returned->getFuture()->await(new TimeoutCancellation(5));
        self::assertSame(1, $reconnected);
        self::assertCount(2, $transport->connectCalls);
    }

    /** Guard: an application processIncoming() that started the recovery still waits for the Reconnected listener. */
    #[DataProvider('origins')]
    public function testAnApplicationReadStillWaitsForTheReconnectedListener(string $origin): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        [$entered, $release, $returned] = $this->gate();
        $client = $this->client($watched, static function (ConnectionEvent $event) use ($entered, $release, $returned): void {
            if ($event === ConnectionEvent::Reconnected) {
                self::hold($entered, $release, $returned);
            }
        });
        $read = $this->resources()->ownOperation($client->processIncoming(), label: 'the application read');
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        self::trigger($transport, $origin);
        $entered->getFuture()->await(new TimeoutCancellation(5));
        $checkpoint = new DeferredFuture();
        EventLoop::queue(static fn() => $checkpoint->complete());
        $checkpoint->getFuture()->await(new TimeoutCancellation(5));
        self::assertFalse($read->isComplete());
        $release->complete();
        $read->await(new TimeoutCancellation(5));
        self::assertTrue($returned->isComplete());
    }

    /**
     * The initiating poll returns while the Reconnected listener is held; let go, the listener's own poll loses the
     * next connection and recovers it in a fiber of its own, and gets its message. Finishing the initiating operation
     * does not abandon the recovery fiber still running the listener.
     */
    #[DataProvider('origins')]
    public function testTheListenerCanRecoverAgainAfterTheInitiatingPollReturned(string $origin): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        [$entered, $release, $returned] = $this->gate();
        $outcome = new class {
            public ?SubscriptionQueue $queue = null;
            public ?NatsMessage $message = null;
            public ?\Throwable $error = null;
            public int $announcements = 0;
        };
        $client = $this->client($watched, static function (ConnectionEvent $event) use ($outcome, $transport, $watched, $entered, $release, $returned): void {
            if ($event !== ConnectionEvent::Reconnected || ++$outcome->announcements !== 1) {
                return;
            }

            $entered->complete();
            $release->getFuture()->await();
            // The outer poll has finished before this read starts, so this is the listener's own failed read.
            $watched->onRead = static function () use ($transport, $watched): void {
                $watched->onRead = null;
                $transport->dropConnection();
            };
            try {
                $outcome->message = $outcome->queue?->setTimeout(30.0)->next();
            } catch (\Throwable $error) {
                $outcome->error = $error;
            } finally {
                $returned->complete();
            }
        });
        $outer = $client->subscribeQueue('outer.jobs')->await();
        $inner = $client->subscribeQueue('listener.jobs')->await();
        $outcome->queue = $inner;
        $transport->afterWrite = static function (string $bytes) use ($transport, $outer, $inner): void {
            if (!str_contains($bytes, 'SUB listener.jobs')) {
                return;
            }
            if ($transport->epoch() === 1) {
                $transport->pushFrame(ReconnectingTransport::msgFrame('outer.jobs', $outer->sid, 'outer'));
            } elseif ($transport->epoch() === 2) {
                $transport->afterWrite = null;
                $transport->pushFrame(ReconnectingTransport::msgFrame('listener.jobs', $inner->sid, 'inner'));
            }
        };
        $poll = $this->resources()->ownOperation(async(static fn(): ?NatsMessage => $outer->setTimeout(30.0)->next()), label: 'the initiating poll');
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        self::trigger($transport, $origin);
        $entered->getFuture()->await(new TimeoutCancellation(5));
        $message = null;
        $error = null;
        try {
            $message = $poll->await(new TimeoutCancellation(1));
        } catch (\Throwable $failure) {
            $error = $failure;
        }
        self::assertNull($error, 'the initiating poll must return while the listener is held: ' . ($error?->getMessage() ?? ''));
        self::assertSame('outer', $message?->payload);
        self::assertFalse($returned->isComplete());
        self::assertSame(1, $transport->epoch());
        $release->complete();
        $returned->getFuture()->await(new TimeoutCancellation(5));
        $this->waitUntil(static fn(): bool => $outcome->announcements === 2);
        self::assertNull($outcome->error);
        self::assertSame('inner', $outcome->message?->payload);
        self::assertSame(2, $client->statistics()->reconnects);
        self::assertCount(3, $transport->connectCalls);
    }

    /**
     * The request's read fails and its error listener's publish recovers the connection inline, so the read's own
     * recovery, for a generation already replaced, does nothing. Its wrapper must still let the read go, and the
     * request then reads the reply the replacement connection brings. Completing only from the recovery's own success
     * would leave the request parked until its deadline.
     */
    public function testASkippedStaleRecoveryCompletesTheReadAndLetsTheRequestReadItsReply(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $holder = new class {
            public ?NatsClient $client = null;
            public ?string $replyTo = null;
            public bool $recovering = false;
        };
        $client = $this->client($watched, static function (): void {}, static function () use ($holder): void {
            if ($holder->client === null || $holder->recovering) {
                return;
            }
            $holder->recovering = true;
            // This failed publish recovers the connection inline while the original read reports its error.
            $holder->client->publish('svc.recover', '')->await();
        });
        $holder->client = $client;
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport, $holder): array {
            if ($subject === 'svc.warm' && $replyTo !== null) {
                return $transport->replyFrame($replyTo, 'warm');
            }
            if ($subject === 'svc.recover' && $holder->replyTo !== null) {
                return $transport->replyFrame($holder->replyTo, 'recovered');
            }
            $holder->replyTo = $replyTo;

            return [];
        };
        $this->resources()->ownCallback(EventLoop::delay(45.0, static function (): void {}), 'the fallback probe keep-alive');
        $client->request('svc.warm', '', 5_000)->await();
        $request = $this->resources()->ownOperation($client->request('svc.echo', '', 30_000), label: 'the stale read request');
        $this->waitUntil(static fn(): bool => $holder->replyTo !== null && $watched->readsUnderWay === 1);
        $transport->dropConnection();
        $this->waitUntil(static fn(): bool => $transport->epoch() === 1 && $client->state() === ConnectionState::Open);
        $message = null;
        $error = null;
        try {
            $message = $request->await(new TimeoutCancellation(1));
        } catch (\Throwable $failure) {
            $error = $failure;
        }
        self::assertNull($error, 'the skipped stale recovery must settle before the request deadline: ' . ($error?->getMessage() ?? ''));
        self::assertSame('recovered', $message?->payload);
        self::assertSame(1, $client->statistics()->reconnects);
        self::assertCount(2, $transport->connectCalls);
    }

    /**
     * @param \Closure(ConnectionEvent): void $listener
     * @param (\Closure(\Throwable): void)|null $errorListener
     */
    private function client(WatchedTransport $transport, \Closure $listener, ?\Closure $errorListener = null): NatsClient
    {
        $client = $this->own(new NatsClient(new NatsOptions(
            pingIntervalSeconds: 0,
            reconnectDelayMs: 1,
            reconnectMaxDelayMs: 1,
            reconnectJitterMs: 0,
            connectionListener: $listener,
            errorListener: $errorListener,
        ), $transport));
        $client->connect()->await();

        return $client;
    }

    /** @return array{DeferredFuture<null>, DeferredFuture<null>, DeferredFuture<null>} */
    private function gate(): array
    {
        $entered = new DeferredFuture();
        $release = new DeferredFuture();
        $returned = new DeferredFuture();
        $this->resources()->onRelease(static function () use ($release): void {
            if (!$release->isComplete()) {
                $release->complete();
            }
        }, 'the held recovery tail');
        $this->resources()->ownCallback(EventLoop::delay(45.0, static function (): void {}), 'the tail keep-alive');

        return [$entered, $release, $returned];
    }

    /**
     * @param DeferredFuture<null> $entered
     * @param DeferredFuture<null> $release
     * @param DeferredFuture<null> $returned
     */
    private static function hold(DeferredFuture $entered, DeferredFuture $release, DeferredFuture $returned): void
    {
        $entered->complete();
        $release->getFuture()->await();
        $returned->complete();
    }

    private static function trigger(ReconnectingTransport $transport, string $origin): void
    {
        if ($origin === 'lost read') {
            $transport->dropConnection();
        } else {
            $transport->pushFrame(self::LAME_DUCK);
        }
    }
}
