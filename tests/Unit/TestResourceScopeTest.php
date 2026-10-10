<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\TestResourceCleanupFailure;
use IDCT\NATS\Tests\Support\TestResourceScope;
use IDCT\NATS\Tests\Support\UncancellableDialTransport;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;

/**
 * The test-resource scope of #183 (tests/Support/TestResourceScope.php), driven directly: each case makes its own scope,
 * leaves something running the way a test would, closes the scope, and checks what the shutdown did. A case that holds
 * something on purpose without a release path releases it itself after the check, so that nothing outlives it.
 */
final class TestResourceScopeTest extends TestCase
{
    /** @var list<TestResourceScope> */
    private array $scopes = [];

    /**
     * A case that fails before it closes its scope still has it closed here; a scope a case closed already does
     * nothing, and what a case holds on purpose it releases itself, so a report here belongs to a failed case.
     */
    protected function tearDown(): void
    {
        $scopes = $this->scopes;
        $this->scopes = [];
        foreach ($scopes as $scope) {
            try {
                $scope->close(static::class . '::' . $this->name());
            } catch (TestResourceCleanupFailure $e) {
                fwrite(STDERR, PHP_EOL . $e->getMessage() . PHP_EOL);
            }
        }
    }

    /** A connection left Open, its heartbeat timer referenced, is Closed by the shutdown, which reports nothing. */
    public function testAConnectionLeftOpenIsClosed(): void
    {
        $scope = $this->scope();
        $connection = $scope->own(new NatsConnection(self::options(pingIntervalSeconds: 30), new ReconnectingTransport()));
        $connection->connect()->await();

        $scope->close();

        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * A recovery backing off (1 s between dials that are refused) is cut short: the shutdown ends well within the
     * backoff, the connection is Closed, and no dial follows it.
     */
    public function testARecoveryBackingOffIsStoppedAndNoDialFollows(): void
    {
        $scope = $this->scope();
        $transport = new ReconnectingTransport();
        $connection = $scope->own(new NatsConnection(self::options(reconnectDelayMs: 1_000, reconnectMaxDelayMs: 1_000), $transport));
        $connection->connect()->await();
        $transport->refuseDials();
        $transport->dropConnection();
        $scope->ownOperation(async(static fn() => $connection->readIncomingForOperation()->await()), label: 'the read that met the drop');
        self::waitUntil(static fn(): bool => $connection->state() === ConnectionState::Connecting);
        $dials = count($transport->connectCalls);

        $started = hrtime(true);
        $scope->close();
        $elapsed = self::secondsSince($started);
        delay(0.05);

        self::assertLessThan(0.5, $elapsed);
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertCount($dials, $transport->connectCalls, 'no dial after the shutdown');
    }

    /**
     * A dial its transport cannot stop, held mid-recovery: the disconnect waits for it, so the release hook lets it go
     * once the close intent has run, before the disconnect is joined. The shutdown ends well before the 5 s connect
     * timeout that bounds the disconnect's own wait, and the reader that started the recovery is settled.
     */
    public function testAHeldUncancellableDialIsReleasedBeforeItsDisconnectIsJoined(): void
    {
        $scope = $this->scope();
        $inner = new ReconnectingTransport();
        $connection = $scope->own(new NatsConnection(self::options(connectTimeoutMs: 5_000), new UncancellableDialTransport($inner)));
        $connection->connect()->await();
        $scope->onRelease($inner->releaseDial(...), 'the held dial');
        $inner->holdNextDial();
        $inner->dropConnection();
        $reader = $scope->ownOperation(async(static fn() => $connection->readIncomingForOperation()->await()), label: 'the reader');
        self::waitUntil(static fn(): bool => $connection->state() === ConnectionState::Connecting);

        $started = hrtime(true);
        $scope->close();

        self::assertLessThan(2.0, self::secondsSince($started));
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertTrue($reader->isComplete());
    }

    /**
     * A read parked on an idle socket is not ended by the close of a transport that does not wake it: its own
     * cancellation source, the operation's end control, ends it once the close intent has run.
     */
    public function testAParkedReadEndsThroughItsOwnCancellation(): void
    {
        $scope = $this->scope();
        $transport = new FakeTransport(self::greeting(), blockWhenEmpty: true);
        $connection = $scope->own(new NatsConnection(self::options(), $transport));
        $connection->connect()->await();
        $stop = new DeferredCancellation();
        $read = $scope->ownOperation(
            async(static fn() => $connection->processIncoming($stop->getCancellation())->await()),
            $stop->cancel(...),
            'the parked read',
        );
        self::waitUntil(static fn(): bool => $transport->startedReads > 0);

        $scope->close();

        self::assertTrue($read->isComplete());
        self::assertSame($transport->startedReads, $transport->resolvedReads);
    }

    /**
     * A flush whose caller stopped waiting after 10 ms goes on waiting for its PONG; owned, it is joined, and the
     * disconnect ends it.
     */
    public function testAnAbandonedFlushIsJoined(): void
    {
        $scope = $this->scope();
        $transport = new ReconnectingTransport();
        $connection = $scope->own(new NatsConnection(self::options(requestTimeoutMs: 30_000), $transport));
        $connection->connect()->await();
        $transport->answerPings = false;
        $flush = $scope->ownOperation($connection->flush(), label: 'the flush');
        try {
            $flush->await(new TimeoutCancellation(0.01));
            self::fail('the PONG never comes');
        } catch (\Amp\CancelledException) {
            // The caller gave up; the flush did not.
        }
        self::assertFalse($flush->isComplete());

        $scope->close();

        self::assertTrue($flush->isComplete());
    }

    /**
     * A handler held on a gate is released only once the connection's close intent has run: the release hook sees
     * the connection discarding what it received, so the handler cannot carry on as if nothing closed.
     */
    public function testAHeldHandlerIsReleasedOnlyAfterTheCloseIntent(): void
    {
        $scope = $this->scope();
        $transport = new ReconnectingTransport();
        $connection = $scope->own(new NatsConnection(self::options(), $transport));
        $connection->connect()->await();
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        $sid = $connection->subscribe('orders', static function () use ($gate): void {
            $gate->getFuture()->await();
        })->await();
        /** @var \ArrayObject<int, bool> $discardingAtRelease */
        $discardingAtRelease = new \ArrayObject();
        $scope->onRelease(static function () use ($gate, $connection, $discardingAtRelease): void {
            $discardingAtRelease[] = $connection->isDiscardingUndelivered();
            $gate->complete();
        }, 'the held handler');
        $transport->pushFrame(ReconnectingTransport::msgFrame('orders', $sid, 'o1'));
        $read = $scope->ownOperation(async(static fn() => $connection->processIncoming()->await()), label: 'the read in the handler');
        delay(0.02);
        self::assertFalse($read->isComplete(), 'the handler holds the read');

        $scope->close();

        self::assertSame([true], $discardingAtRelease->getArrayCopy());
        self::assertTrue($read->isComplete());
    }

    /**
     * The shutdown has one deadline, a referenced timer, shared by every resource: two connections whose disconnects
     * each wait for a held dial nobody releases (5 s connect timeouts) are reported together at the 0.3 s budget, not
     * at twice it, and with nothing else on the event loop the deadline still fires.
     */
    public function testTheDeadlineIsSharedAndFiresWithNothingElseOnTheLoop(): void
    {
        $scope = $this->scope(0.3);
        $held = [];
        foreach (['a', 'b'] as $name) {
            $inner = new ReconnectingTransport();
            $connection = $scope->own(new NatsConnection(self::options(connectTimeoutMs: 5_000), new UncancellableDialTransport($inner)), 'connection ' . $name);
            $connection->connect()->await();
            $inner->holdNextDial();
            $inner->dropConnection();
            $held[] = [$inner, $connection, async(static fn() => $connection->readIncomingForOperation()->await())];
        }
        self::waitUntil(static fn(): bool => $held[0][1]->state() === ConnectionState::Connecting && $held[1][1]->state() === ConnectionState::Connecting);

        $started = hrtime(true);
        $failure = null;
        try {
            $scope->close('this case');
        } catch (TestResourceCleanupFailure $e) {
            $failure = $e;
        }
        $elapsed = self::secondsSince($started);

        self::assertNotNull($failure);
        self::assertStringContainsString('Test resources of this case did not shut down cleanly', $failure->getMessage());
        self::assertStringContainsString('connection a: disconnect() did not complete in time', $failure->getMessage());
        self::assertStringContainsString('connection b: disconnect() did not complete in time', $failure->getMessage());
        self::assertGreaterThanOrEqual(0.28, $elapsed);
        self::assertLessThan(0.55, $elapsed, 'one deadline for both');

        foreach ($held as [$inner, $connection, $reader]) {
            $inner->releaseDial();
            $connection->disconnect()->await(new TimeoutCancellation(5));
            $reader->await(new TimeoutCancellation(5));
        }
    }

    /**
     * A connection whose close sticks does not hold up another's after-close hook: connection b is closed and its hook
     * runs while connection a still waits for its held dial; only a is reported.
     */
    public function testAStuckConnectionDoesNotHoldAnothersAfterCloseHook(): void
    {
        $scope = $this->scope(0.3);
        $inner = new ReconnectingTransport();
        $stuck = $scope->own(new NatsConnection(self::options(connectTimeoutMs: 5_000), new UncancellableDialTransport($inner)), 'connection a');
        $stuck->connect()->await();
        $inner->holdNextDial();
        $inner->dropConnection();
        $reader = async(static fn() => $stuck->readIncomingForOperation()->await());
        self::waitUntil(static fn(): bool => $stuck->state() === ConnectionState::Connecting);
        $fine = $scope->own(new NatsConnection(self::options(), new ReconnectingTransport()), 'connection b');
        $fine->connect()->await();
        /** @var \ArrayObject<int, ConnectionState> $seen */
        $seen = new \ArrayObject();
        $weakFine = \WeakReference::create($fine);
        $scope->afterClose($fine, static function () use ($weakFine, $seen): void {
            $seen[] = $weakFine->get()?->state() ?? ConnectionState::Idle;
        }, 'its watch cleanup');

        $failure = null;
        try {
            $scope->close();
        } catch (TestResourceCleanupFailure $e) {
            $failure = $e;
        }

        self::assertSame([ConnectionState::Closed], $seen->getArrayCopy());
        self::assertNotNull($failure);
        self::assertStringContainsString('connection a', $failure->getMessage());
        self::assertStringNotContainsString('connection b', $failure->getMessage());

        $inner->releaseDial();
        $stuck->disconnect()->await(new TimeoutCancellation(5));
        $reader->await(new TimeoutCancellation(5));
    }

    /** A loop callback the scope does not own survives the shutdown; one it owns is cancelled. */
    public function testOnlyTheCallbacksItOwnsAreCancelled(): void
    {
        $scope = $this->scope();
        $unrelated = EventLoop::repeat(10, static function (): void {});
        $owned = $scope->ownCallback(EventLoop::repeat(10, static function (): void {}), 'a test timer');

        try {
            $scope->close();

            self::assertContains($unrelated, EventLoop::getIdentifiers());
            self::assertNotContains($owned, EventLoop::getIdentifiers());
        } finally {
            EventLoop::cancel($unrelated);
        }
    }

    /**
     * Owned weakly: a connection the test drops is collected (its destructor runs, as the lifetime tests need), and the
     * shutdown has nothing left to do for it.
     */
    public function testAConnectionTheTestDropsIsStillCollected(): void
    {
        $scope = $this->scope();
        $connection = $scope->own(new NatsConnection(self::options(), new FakeTransport()));
        $weak = \WeakReference::create($connection);
        unset($connection);
        gc_collect_cycles();

        self::assertNull($weak->get());
        $scope->close();
    }

    /**
     * A connection listener that connects again on Closed would restart the connection during the shutdown: the
     * check after the shutdown reports it, and a stop hook that disables the listener first keeps it Closed.
     */
    public function testAClosedListenerCannotRestartTheConnectionUnnoticed(): void
    {
        foreach ([false, true] as $disabled) {
            $scope = $this->scope(1.0);
            $supervisor = new class {
                public ?NatsConnection $connection = null;
                public bool $restarts = true;
            };
            $connection = $scope->own(new NatsConnection(self::options(connectionListener: static function (ConnectionEvent $event) use ($supervisor): void {
                if ($event === ConnectionEvent::Closed && $supervisor->restarts && $supervisor->connection !== null) {
                    $supervisor->connection->connect()->ignore();
                }
            }), new ReconnectingTransport()), 'the restarting connection');
            $supervisor->connection = $connection;
            $connection->connect()->await();
            if ($disabled) {
                $scope->onStop(static function () use ($supervisor): void {
                    $supervisor->restarts = false;
                }, 'the restarting listener');
            }

            $failure = null;
            try {
                $scope->close();
            } catch (TestResourceCleanupFailure $e) {
                $failure = $e;
            }
            delay(0.05);

            if ($disabled) {
                self::assertNull($failure, (string) $failure?->getMessage());
                self::assertSame(ConnectionState::Closed, $connection->state());
            } else {
                self::assertNotNull($failure, 'the restart is reported');
                self::assertMatchesRegularExpression('/the restarting connection is still (open|connecting)/', $failure->getMessage());
                $supervisor->restarts = false;
                $connection->disconnect()->await(new TimeoutCancellation(5));
            }
            $supervisor->connection = null;
        }
    }

    /**
     * A cleanup step that throws is reported with its label, and the steps after it still run: the second release
     * hook releases its gate, and the operation that gate held is settled.
     */
    public function testACleanupStepThatThrowsIsReportedAndTheOthersStillRun(): void
    {
        $scope = $this->scope(1.0);
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        $scope->onRelease(static function (): void {
            throw new \RuntimeException('this release broke');
        }, 'a broken release');
        $scope->onRelease($gate->complete(...), 'the gate');
        $held = $scope->ownOperation(async(static fn() => $gate->getFuture()->await()), label: 'the gated operation');

        $failure = null;
        try {
            $scope->close();
        } catch (TestResourceCleanupFailure $e) {
            $failure = $e;
        }

        self::assertNotNull($failure);
        self::assertStringContainsString('release a broken release threw RuntimeException: this release broke', $failure->getMessage());
        self::assertStringNotContainsString('the gated operation', $failure->getMessage());
        self::assertTrue($held->isComplete());
    }

    /**
     * An owned operation that failed, as the test expected, is settled and not reported; one that never settles is
     * reported as pending at the deadline.
     */
    public function testAFailedOperationIsSettledAndAPendingOneIsReported(): void
    {
        $scope = $this->scope(0.2);
        $failed = $scope->ownOperation(async(static function (): never {
            throw new TimeoutException('the test asserted this');
        }), label: 'the failed operation');
        try {
            $failed->await();
        } catch (TimeoutException) {
            // What the test expected.
        }
        /** @var DeferredFuture<null> $never */
        $never = new DeferredFuture();
        $scope->ownOperation($never->getFuture(), label: 'the stuck operation');

        $failure = null;
        try {
            $scope->close();
        } catch (TestResourceCleanupFailure $e) {
            $failure = $e;
        }

        self::assertNotNull($failure);
        self::assertStringContainsString('the stuck operation is still pending', $failure->getMessage());
        self::assertStringNotContainsString('the failed operation', $failure->getMessage());
        $never->complete();
    }

    /**
     * A request that ended at its own timeout while its write is still stalled (a stall that outlives its session) is
     * complete, and the connection Closed, yet the write still runs: the fixture's idle check reports it unless the
     * stall's release is registered.
     */
    public function testAWriteThatOutlivesTheCloseIsCheckedAndReleased(): void
    {
        foreach ([false, true] as $released) {
            $scope = $this->scope(0.5);
            $transport = new ReconnectingTransport();
            $connection = $scope->own(new NatsConnection(self::options(requestTimeoutMs: 50), $transport));
            $connection->connect()->await();
            $scope->expectIdle(static fn(): bool => $transport->writesStalled() === 0, 'the transport\'s writes');
            if ($released) {
                $scope->onRelease($transport->releaseStalledWrites(...), 'the stalled write');
            }
            $transport->stallNextWriteContaining('PUB svc.echo', 30, outlivesSession: true);
            $request = $connection->request('svc.echo', 'ping');
            try {
                $request->await(new TimeoutCancellation(2));
            } catch (\Throwable) {
                // Its own timeout.
            }
            self::assertTrue($request->isComplete());
            self::assertSame(1, $transport->writesStalled());

            $failure = null;
            try {
                $scope->close();
            } catch (TestResourceCleanupFailure $e) {
                $failure = $e;
            }

            if ($released) {
                self::assertNull($failure, (string) $failure?->getMessage());
            } else {
                self::assertNotNull($failure);
                self::assertStringContainsString('the transport\'s writes is not idle', $failure->getMessage());
                $transport->releaseStalledWrites();
                self::waitUntil(static fn(): bool => $transport->writesStalled() === 0);
            }
            self::assertSame(0, $transport->writesStalled());
        }
    }

    /** Registering one connection twice keeps one registration; a closed scope takes no more. */
    public function testOneRegistrationPerConnectionAndNoneAfterTheClose(): void
    {
        $scope = $this->scope();
        $client = new NatsClient(self::options(), new FakeTransport());
        self::assertSame($client, $scope->own($client));
        self::assertSame($client, $scope->own($client, 'another label'));
        self::assertTrue($scope->owns($client));

        $scope->close();

        self::assertSame(ConnectionState::Closed, $client->state());
        $this->expectException(\LogicException::class);
        $scope->own($client);
    }

    private function scope(float $budgetSeconds = 6.0): TestResourceScope
    {
        return $this->scopes[] = new TestResourceScope($budgetSeconds);
    }

    private static function options(
        int $connectTimeoutMs = 500,
        int $requestTimeoutMs = 2_000,
        int|float $pingIntervalSeconds = 0,
        int $reconnectDelayMs = 5,
        int $reconnectMaxDelayMs = 20,
        ?\Closure $connectionListener = null,
    ): NatsOptions {
        return new NatsOptions(
            connectTimeoutMs: $connectTimeoutMs,
            requestTimeoutMs: $requestTimeoutMs,
            reconnectEnabled: true,
            maxReconnectAttempts: 1_000,
            reconnectDelayMs: $reconnectDelayMs,
            reconnectMaxDelayMs: $reconnectMaxDelayMs,
            reconnectJitterMs: 0,
            pingIntervalSeconds: $pingIntervalSeconds,
            connectionListener: $connectionListener,
        );
    }

    /** @return list<string> */
    private static function greeting(): array
    {
        return [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
    }

    private static function waitUntil(\Closure $condition, float $within = 2.0): void
    {
        $deadline = hrtime(true) + (int) ($within * 1e9);
        while (!$condition()) {
            if (hrtime(true) > $deadline) {
                self::fail(sprintf('condition not reached within %g s', $within));
            }
            delay(0.001);
        }
    }

    private static function secondsSince(int $started): float
    {
        return (hrtime(true) - $started) / 1e9;
    }
}
