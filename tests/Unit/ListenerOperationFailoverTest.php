<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;

/**
 * What a DiscoveredServers or LameDuck listener starts and does not await waits for the failover the lame-duck INFO
 * starts, and runs on the new server, whichever read brought the INFO (#203). Over ReconnectingTransport, the INFO naming
 * port 4223, brought by your own processIncoming() (an inline failover in the listener's fiber) or by a request's read (a
 * failover in a fiber of its own); the failover's dial is held until the case releases it, so the side operation is
 * looked at while the failover is under way. Every connection, operation and held dial belongs to the test.
 *
 * Up to 2.26.3 the side operation failed at once with "Connection is not open" on the inline path (its caller fiber
 * was taken for the recovery fiber), and a side read on the other path read the leaving socket, the client's own close
 * of it reported as "Socket closed by peer (EOF)".
 */
final class ListenerOperationFailoverTest extends TestCase
{
    use ReconnectScenarios;

    private const INFO = 'INFO {"server_id":"S1","ldm":true,"connect_urls":["127.0.0.1:4223"]}' . "\r\n";

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{ConnectionEvent, bool, string, bool}> */
    public static function listenerCases(): iterable
    {
        foreach ([ConnectionEvent::DiscoveredServers, ConnectionEvent::LameDuck] as $hook) {
            foreach ([false, true] as $operationOrigin) {
                foreach (['request', 'read'] as $side) {
                    foreach ([false, true] as $wrapped) {
                        yield $hook->name . ', ' . ($operationOrigin ? 'operation read' : 'application read') . ', ' . $side . ($wrapped ? ', wrapped' : ', direct') => [$hook, $operationOrigin, $side, $wrapped];
                    }
                }
            }
        }
    }

    /**
     * From either listener, on either path, a side request or read, called directly or wrapped in async(), is still
     * pending while the dial is held, without having read the leaving socket; released, the request gets `side-reply`
     * on the second server, the read returns (0 on the operation path when called directly), nothing is reported, and
     * there is one failover with the usual events.
     */
    #[DataProvider('listenerCases')]
    public function testASideOperationWaitsForFailoverWithoutReadingTheLeavingSocket(ConnectionEvent $hook, bool $operationOrigin, string $operation, bool $wrapped): void
    {
        [$connection, $transport, $side, $trigger, $errors, $events, $leavingReads] = $this->scenario($hook, $operationOrigin, $operation, $wrapped);
        self::assertFalse($side->isComplete(), 'side operation must wait while the failover dial is held');
        $leavingReadsWhileHeld = $leavingReads();
        $transport->releaseDial();
        $result = $side->await(new TimeoutCancellation(5));
        $trigger->await(new TimeoutCancellation(5));
        $this->waitUntilOpen($connection);
        if ($operation === 'request') {
            self::assertInstanceOf(NatsMessage::class, $result);
            self::assertSame('side-reply', $result->payload);
            self::assertCount(0, $transport->controlLinesStartingWith('PUB svc.side ', 0));
            self::assertCount(1, $transport->controlLinesStartingWith('PUB svc.side ', 1));
        } else {
            self::assertIsInt($result);
            if ($operationOrigin && !$wrapped) {
                self::assertSame(0, $result, 'the pending-failover read returns without consuming either socket');
            }
        }
        self::assertSame([], $errors());
        self::assertSame(0, $leavingReadsWhileHeld, 'the side read must not take the leaving socket');
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::DiscoveredServers, ConnectionEvent::LameDuck, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected], $events());
        self::assertSame(1, $connection->statistics()->reconnects);
        self::assertCount(2, $transport->connectCalls);
    }

    /** @return iterable<string, array{ConnectionEvent, string}> */
    public static function otherInlineOperations(): iterable
    {
        foreach ([ConnectionEvent::DiscoveredServers, ConnectionEvent::LameDuck] as $hook) {
            foreach (['requestHeaders', 'requestMany', 'subscribe', 'subscribeGuarded', 'drainSubscription', 'flush', 'flushReporting', 'rtt', 'connect', 'drain'] as $operation) {
                yield $hook->name . ', ' . $operation => [$hook, $operation];
            }
        }
    }

    /**
     * The other entry points started from a listener on the inline path - request with headers, requestMany(),
     * subscribe(), a guarded subscribe, drainSubscription(), both flushes, rtt(), connect() and drain() - wait for the
     * failover instead of being refused: still pending while the dial is held, then complete without errors, one
     * reconnect (drain() ends Closed).
     */
    #[DataProvider('otherInlineOperations')]
    public function testEveryEntryPointCapturesRecoveryProvenanceBeforeItsFiberRuns(ConnectionEvent $hook, string $operation): void
    {
        [$connection, $transport, $side, $trigger, $errors] = $this->scenario($hook, false, $operation, false);
        self::assertFalse($side->isComplete(), 'side operation must wait while the failover dial is held');
        self::assertSame(ConnectionState::Connecting, $connection->state());
        $transport->releaseDial();
        $result = $side->await(new TimeoutCancellation(5));
        $trigger->await(new TimeoutCancellation(5));
        if ($operation === 'requestHeaders') {
            self::assertInstanceOf(NatsMessage::class, $result);
            self::assertSame('side-reply', $result->payload);
        } elseif ($operation === 'requestMany') {
            self::assertIsArray($result);
            self::assertSame('side-reply', $result[0]->payload);
        }
        self::assertSame([], $errors());
        self::assertSame($operation === 'drain' ? ConnectionState::Closed : ConnectionState::Open, $connection->state());
        self::assertSame(1, $connection->statistics()->reconnects);
        self::assertCount(2, $transport->connectCalls);
    }

    /** @return iterable<string, array{ConnectionEvent, bool, string, bool}> */
    public static function waitingDisabledCases(): iterable
    {
        foreach (self::listenerCases() as $name => $case) {
            if (!$case[3]) {
                yield $name => $case;
            }
        }
    }

    /**
     * With waiting for a reconnect disabled the side operation still fails at once with "Connection is not open",
     * without reading the leaving socket or reporting an EOF.
     */
    #[DataProvider('waitingDisabledCases')]
    public function testWaitingDisabledStillRefusesTheSideOperation(ConnectionEvent $hook, bool $operationOrigin, string $operation, bool $wrapped): void
    {
        [$connection, $transport, $side, $trigger, $errors, , $leavingReads] = $this->scenario($hook, $operationOrigin, $operation, $wrapped, false);
        $error = self::failureOf($side);
        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertSame('Connection is not open', $error->getMessage());
        self::assertSame([], $errors(), 'a policy refusal must not produce an EOF report');
        self::assertSame(0, $leavingReads(), 'a policy refusal must not read the leaving socket');
        $transport->releaseDial();
        $trigger->await(new TimeoutCancellation(5));
        $this->waitUntilOpen($connection);
    }

    /** @return iterable<string, array{ConnectionEvent, bool}> */
    public static function boundedCases(): iterable
    {
        foreach ([ConnectionEvent::DiscoveredServers, ConnectionEvent::LameDuck] as $hook) {
            foreach ([false, true] as $cancelled) {
                yield $hook->name . ($cancelled ? ', cancellation' : ', deadline') => [$hook, $cancelled];
            }
        }
    }

    /**
     * A side request waiting for the inline failover ends at its caller's cancellation or its own deadline, unsent,
     * while the failover carries on.
     */
    #[DataProvider('boundedCases')]
    public function testTheSideRequestWaitKeepsItsOwnCancellationAndDeadline(ConnectionEvent $hook, bool $cancelled): void
    {
        $cancellation = new DeferredCancellation();
        [$connection, $transport, $side, $trigger] = $this->scenario($hook, false, $cancelled ? 'cancelledRequest' : 'shortRequest', false, true, $cancellation);
        self::assertFalse($side->isComplete(), 'side operation must wait while the failover dial is held');
        if ($cancelled) {
            $cancellation->cancel();
        }
        $error = self::failureOf($side);
        self::assertInstanceOf($cancelled ? CancelledException::class : TimeoutException::class, $error);
        self::assertSame(ConnectionState::Connecting, $connection->state(), 'ending the side wait does not end recovery');
        self::assertCount(0, $transport->controlLinesStartingWith('PUB svc.side '));
        $transport->releaseDial();
        $trigger->await(new TimeoutCancellation(5));
        $this->waitUntilOpen($connection);
        self::assertSame(1, $connection->statistics()->reconnects);
    }

    /**
     * A side read waiting at the gate for a failover in a fiber of its own ends at its caller's cancellation or deadline
     * (it has no operation wake-up), the read itself settled, nothing reported and the leaving socket unread.
     */
    #[DataProvider('boundedCases')]
    public function testTheSideReadWaitKeepsItsOwnCancellationAndDeadline(ConnectionEvent $hook, bool $cancelled): void
    {
        $cancellation = new DeferredCancellation();
        [$connection, $transport, $side, $trigger, $errors, , $leavingReads] = $this->scenario($hook, true, $cancelled ? 'cancelledRead' : 'shortRead', false, true, $cancellation);
        self::assertFalse($side->isComplete());
        if ($cancelled) {
            $cancellation->cancel();
        }
        self::assertInstanceOf(CancelledException::class, self::failureOf($side));
        self::assertTrue($side->isComplete(), 'the read itself ended, not just the test watchdog');
        self::assertSame(ConnectionState::Connecting, $connection->state());
        self::assertSame([], $errors(), 'a cancelled side read must not report an intentional-close EOF');
        self::assertSame(0, $leavingReads());
        $transport->releaseDial();
        $trigger->await(new TimeoutCancellation(5));
        $this->waitUntilOpen($connection);
        self::assertSame(1, $connection->statistics()->reconnects);
    }

    /**
     * The gate waits only until the failover is over (#202's boundary), not for the Reconnected listener: the side read
     * returns 0 while that listener is still held.
     */
    public function testTheSideReadReturnsWhileTheReconnectedListenerIsStillHeld(): void
    {
        $this->resources()->ownCallback(EventLoop::delay(30.0, static function (): void {}), 'the held listener keep-alive');
        /** @var DeferredFuture<void> $entered */
        $entered = new DeferredFuture();
        /** @var DeferredFuture<void> $release */
        $release = new DeferredFuture();
        $this->resources()->onRelease(static function () use ($release): void {
            if (!$release->isComplete()) {
                $release->complete();
            }
        }, 'the Reconnected listener');
        [$connection, $transport, $side, $trigger, $errors] = $this->scenario(
            ConnectionEvent::LameDuck,
            true,
            'read',
            false,
            onReconnected: static function () use ($entered, $release): void {
                $entered->complete();
                $release->getFuture()->await();
            },
        );
        $transport->releaseDial();
        $entered->getFuture()->await(new TimeoutCancellation(5));
        $failure = null;
        $result = null;
        try {
            $result = $side->await(new TimeoutCancellation(1));
        } catch (\Throwable $error) {
            $failure = $error;
        }
        self::assertNull($failure, 'the side read must finish while the Reconnected listener is held');
        self::assertSame(0, $result);
        self::assertSame([], $errors());
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertFalse($release->isComplete());
        $release->complete();
        $trigger->await(new TimeoutCancellation(5));
    }


    /**
     * @param (\Closure(): void)|null $onReconnected
     * @return array{NatsConnection, ReconnectingTransport, Future<mixed>, Future<mixed>, \Closure(): list<string>, \Closure(): list<ConnectionEvent>, \Closure(): int}
     */
    private function scenario(ConnectionEvent $hook, bool $operationOrigin, string $operation, bool $wrapped, bool $wait = true, ?DeferredCancellation $cancellation = null, ?\Closure $onReconnected = null): array
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $holder = new class {
            public ?NatsConnection $connection = null;
            /** @var Future<mixed>|null */
            public ?Future $side = null;
            public int $sid = 0;
            public int $leavingReads = 0;
            public bool $recordReads = false;
            /** @var list<string> */
            public array $errors = [];
            /** @var list<ConnectionEvent> */
            public array $events = [];
        };
        $watched->onRead = static function () use ($transport, $holder): void {
            if ($holder->recordReads && $transport->epoch() === 0) {
                $holder->leavingReads++;
            }
        };
        $listener = function (ConnectionEvent $event) use ($hook, $operation, $wrapped, $cancellation, $transport, $holder, $onReconnected): void {
            $holder->events[] = $event;
            if ($event === ConnectionEvent::Reconnected && $onReconnected !== null) {
                $onReconnected();
            }
            if ($event === ConnectionEvent::Reconnected && $operation === 'read') {
                $transport->pushFrame("PING\r\n");
            }
            $connection = $holder->connection;
            if ($event !== $hook || $holder->side !== null || $connection === null) {
                return;
            }
            $holder->recordReads = true;
            $start = static fn(): Future => match ($operation) {
                'request' => $connection->request('svc.side', '', 10_000),
                'requestHeaders' => $connection->requestWithHeaders('svc.side', '', ['X-Test' => '203'], 10_000),
                'requestMany' => $connection->requestMany('svc.side', '', maxResponses: 1, totalTimeoutMs: 10_000),
                'read' => $connection->processIncoming(new TimeoutCancellation(10)),
                'subscribe' => $connection->subscribe('side', static function (): void {}),
                'subscribeGuarded' => $connection->subscribeGuarded('side', static function (): void {}, static function (): void {}),
                'drainSubscription' => $connection->drainSubscription($holder->sid),
                'flush' => $connection->flush(),
                'flushReporting' => $connection->flushReportingFailures(),
                'rtt' => $connection->rtt(),
                'connect' => $connection->connect(),
                'drain' => $connection->drain(),
                'shortRequest' => $connection->request('svc.side', '', 300),
                'cancelledRequest' => $connection->request('svc.side', '', 10_000, $cancellation?->getCancellation()),
                'shortRead' => $connection->processIncoming(new TimeoutCancellation(0.3)),
                'cancelledRead' => $connection->processIncoming($cancellation?->getCancellation()),
                default => throw new \LogicException('Unknown side operation'),
            };
            $holder->side = $this->resources()->ownOperation($wrapped ? async(static fn(): mixed => $start()->await()) : $start(), label: 'the listener side operation');
        };
        $connection = $this->own(new NatsConnection($this->options(
            $wait,
            10_000,
            10,
            0,
            2,
            $listener,
            5,
            20,
            static function (\Throwable $error) use ($holder): void {
                $holder->errors[] = $error->getMessage();
            },
            10_000,
        ), $watched));
        $holder->connection = $connection;
        $connection->connect()->await();
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport): array {
            if ($replyTo === null) {
                return [];
            }

            return $subject === 'svc.trigger'
                ? [self::INFO . implode('', $transport->replyFrame($replyTo, 'trigger-reply'))]
                : $transport->replyFrame($replyTo, 'side-reply');
        };
        $connection->request('svc.warm', '', 5_000)->await();
        $holder->sid = $connection->subscribe('old', static function (): void {})->await();
        $transport->holdNextDial();
        $this->resources()->onRelease($transport->releaseDial(...), 'the failover dial');
        if ($operationOrigin) {
            $trigger = $this->resources()->ownOperation($connection->request('svc.trigger', '', 10_000), label: 'the initiating request');
        } else {
            $transport->pushFrame(self::INFO);
            $trigger = $this->resources()->ownOperation($connection->processIncoming(new TimeoutCancellation(10)), label: 'the initiating application read');
        }
        $this->waitUntil(static function () use ($transport, $operation, $holder): bool {
            // The unfixed drain closes recovery before its first dial: expose that operation outcome.
            return count($transport->connectCalls) === 2 || ($operation === 'drain' && ($holder->side?->isComplete() ?? false));
        });
        self::assertNotNull($holder->side);

        return [$connection, $transport, $holder->side, $trigger,
            static fn(): array => $holder->errors,
            static fn(): array => $holder->events,
            static fn(): int => $holder->leavingReads];
    }

    /** @param Future<mixed> $future */
    private static function failureOf(Future $future): ?\Throwable
    {
        try {
            $future->await(new TimeoutCancellation(2));

            return null;
        } catch (\Throwable $error) {
            return $error;
        }
    }
}
