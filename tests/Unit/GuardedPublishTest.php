<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ListenerAction;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;

/**
 * The guarded publish the pull consumer engine writes its pulls with (#206, NatsConnection::publishGuarded()): an
 * ordinary publish, reconnect buffering and the retry after a failed write's recovery included, whose guard is called
 * before each attempt to send the frame, in the step that makes it, and refuses the send by throwing. The engine's
 * guard throws once the server has rejected the run's inbox, which another fiber's read can record while the pull's
 * write waits: for the socket, for the recovery a failed write runs (whose replay of the inbox the new server can
 * reject), for a sealed reconnect flush (#165), or for the writer of a publish made while the connection drains, which
 * runs in a fiber of its own. A pull written then on the dead inbox could take a batch nobody receives.
 *
 * Each test drives one branch of the write, with a guard that counts its calls and refuses from a given call on, and a
 * control in which it lets the publish go: refused, the publish fails with the guard's exception, nothing written and
 * no recovery run for the refusal. Over tests/Support/ReconnectingTransport.php, and tests/Support/FakeTransport.php for
 * the drain; a sealed flush is set up through the connection's state, as the reconnect holds it while it flushes.
 */
final class GuardedPublishTest extends TestCase
{
    use ReconnectScenarios;

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{bool}> */
    public static function refusedOrLetGo(): iterable
    {
        yield 'the guard refusing' => [true];
        yield 'the guard letting it go (control)' => [false];
    }

    /**
     * On an Open connection the guard is called once, before the write: a guard that lets the publish go leaves an
     * ordinary publish, its PUB on the wire with its reply subject and counted in the statistics; one that refuses fails
     * it with its exception, nothing written, the connection Open and no reconnect.
     */
    #[DataProvider('refusedOrLetGo')]
    public function testOnAnOpenConnectionTheGuardIsCalledOnceBeforeTheWrite(bool $refused): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        [$guard, $calls] = self::guard($refused ? 1 : null);

        $error = self::failureOf($connection->publishGuarded('orders', 'payload', '_INBOX.reply.1', $guard));

        self::assertSame(1, $calls->count);
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(0, $connection->statistics()->reconnects);
        if ($refused) {
            self::assertSame('refused at call 1', $error?->getMessage());
            self::assertSame([], $transport->controlLinesStartingWith('PUB'));
            self::assertSame(0, $connection->statistics()->outMsgs);

            return;
        }
        self::assertNull($error, sprintf('the publish failed with %s', $error?->getMessage()));
        self::assertSame(['PUB orders _INBOX.reply.1 7'], $transport->controlLinesStartingWith('PUB'));
        self::assertSame(1, $connection->statistics()->outMsgs);
    }

    /**
     * A write that fails runs the recovery, and the guard is called again before the retry on the new connection:
     * refused there, the publish fails with the guard's exception, nothing written on the new connection, the recovery
     * run once, for the write's failure, and the connection Open. Let go, the retry goes out on the new connection. The
     * retry used to go out whatever the recovery's replay had recorded meanwhile.
     */
    #[DataProvider('refusedOrLetGo')]
    public function testTheGuardIsCalledAgainBeforeTheRetryAfterAFailedWritesRecovery(bool $refused): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        [$guard, $calls] = self::guard($refused ? 2 : null);

        $transport->failNextWriteContaining('PUB orders');
        $error = self::failureOf($connection->publishGuarded('orders', 'payload', '_INBOX.reply.1', $guard));

        self::assertSame(2, $calls->count, 'before the write, and before the retry');
        self::assertSame(1, $transport->epoch());
        self::assertSame(1, $connection->statistics()->reconnects);
        self::assertSame(ConnectionState::Open, $connection->state());
        if ($refused) {
            self::assertSame('refused at call 2', $error?->getMessage());
            self::assertSame([], $transport->controlLinesStartingWith('PUB', 1));

            return;
        }
        self::assertNull($error, sprintf('the publish failed with %s', $error?->getMessage()));
        self::assertSame(['PUB orders _INBOX.reply.1 7'], $transport->controlLinesStartingWith('PUB', 1));
    }

    /**
     * The guard goes with the frame when the recovery leaves the connection in another state: the Reconnected listener
     * returns with the next reconnect backing off between refused dials, so the frame takes the path of a publish made
     * now, the reconnect buffer. Refused there, the publish fails and nothing is buffered, so the next connection gets
     * no PUB; let go, the frame is buffered and sent on the next connection.
     */
    #[DataProvider('refusedOrLetGo')]
    public function testTheGuardGoesWithTheFrameWhenTheRecoveryLeavesAnotherReconnectInFlight(bool $refused): void
    {
        $transport = new ReconnectingTransport();
        $action = new ListenerAction(new LifecycleRecorder(), function (NatsConnection $connection) use ($transport): ConnectionState {
            // The new connection dies with dials refused: a reader starts the next reconnect, which backs off.
            $transport->refuseDials();
            $transport->dropConnection();
            async(static fn(): int => $connection->processIncoming(new TimeoutCancellation(3))->await())->ignore();
            $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Connecting);

            return $connection->state();
        });
        $connection = $this->connect($transport, connectionListener: $action->listener());
        $action->connection = $connection;
        [$guard, $calls] = self::guard($refused ? 2 : null);

        $transport->failNextWriteContaining('PUB orders');
        $error = self::failureOf($connection->publishGuarded('orders', 'payload', '_INBOX.reply.1', $guard));
        $transport->acceptDials();
        $this->waitUntilOpen($connection);

        self::assertNull($action->failure);
        self::assertSame(ConnectionState::Connecting, $action->result, 'the listener returned with the next reconnect in flight');
        self::assertSame(2, $calls->count, 'before the write, and on the path of a publish made after the recovery');
        self::assertSame(2, $transport->epoch());
        if ($refused) {
            self::assertSame('refused at call 2', $error?->getMessage());
            self::assertSame([], $transport->controlLinesStartingWith('PUB'));

            return;
        }
        self::assertNull($error, sprintf('the publish failed with %s', $error?->getMessage()));
        self::assertSame(['PUB orders _INBOX.reply.1 7'], $transport->controlLinesStartingWith('PUB', 2));
    }

    /**
     * A publish parked on a sealed reconnect flush (#165) calls the guard again once the flush lets it go on: the guard
     * lets it park, and the gate then opens with the connection Open. Refused there, the publish fails with nothing
     * written; let go, it is written.
     */
    #[DataProvider('refusedOrLetGo')]
    public function testTheGuardIsCalledAgainOnceASealedReconnectFlushLetsThePublishGoOn(bool $refused): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $state = new \ReflectionProperty(NatsConnection::class, 'state');
        $flushGate = new \ReflectionProperty(NatsConnection::class, 'reconnectFlushGate');
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        // As a reconnect holds the connection while it flushes a sealed buffer.
        $state->setValue($connection, ConnectionState::Connecting);
        $flushGate->setValue($connection, $gate);
        [$guard, $calls] = self::guard($refused ? 2 : null);

        $publish = $connection->publishGuarded('orders', 'payload', '_INBOX.reply.1', $guard);
        $publish->ignore();
        $this->waitUntil(static fn(): bool => $calls->count === 1);
        delay(0.01);
        self::assertFalse($publish->isComplete(), 'the publish is parked on the sealed flush');
        $state->setValue($connection, ConnectionState::Open);
        $flushGate->setValue($connection, null);
        $gate->complete();
        $error = self::failureOf($publish);

        self::assertSame(2, $calls->count, 'before parking, and once the flush let it go on');
        if ($refused) {
            self::assertSame('refused at call 2', $error?->getMessage());
            self::assertSame([], $transport->controlLinesStartingWith('PUB'));

            return;
        }
        self::assertNull($error, sprintf('the publish failed with %s', $error?->getMessage()));
        self::assertSame(['PUB orders _INBOX.reply.1 7'], $transport->controlLinesStartingWith('PUB'));
    }

    /**
     * While the connection drains, the writer that hands the frame to the transport, a fiber of its own, calls the guard
     * again right before it writes: a handler the drain runs publishes, its guard lets the publish go and records a
     * refusal right after, in a queued callback, as another fiber's read records a rejection. The writer's guard refuses:
     * the publish fails with its exception, nothing written, and the drain closes the connection as usual. Let go, the
     * frame is written to the connection being drained.
     */
    #[DataProvider('refusedOrLetGo')]
    public function testWhileTheConnectionDrainsTheWriterCallsTheGuardRightBeforeItWrites(bool $refused): void
    {
        $transport = new FakeTransport([ReconnectingTransport::INFO, "PONG\r\n"]);
        // The backlog MSG, and the flush's PONG behind it, arrive once the drain's UNSUB is written: the handler runs
        // while the connection is Draining.
        $transport->enqueueOnWriteContaining = ['UNSUB' => ["MSG updates 1 5\r\nhello\r\n", "PONG\r\n"]];
        $client = $this->own(new NatsClient(new NatsOptions(requestTimeoutMs: 2_000, pingIntervalSeconds: 0), $transport));
        $client->connect()->await();
        $publishing = new class {
            public int $calls = 0;
            public bool $refusing = false;
            public ?ConnectionState $state = null;
            public ?\Throwable $failure = null;
        };
        $guard = static function () use ($publishing, $refused): void {
            ++$publishing->calls;
            if ($publishing->refusing) {
                throw new \RuntimeException('refused in the writer');
            }
            if ($refused) {
                EventLoop::queue(static function () use ($publishing): void {
                    $publishing->refusing = true;
                });
            }
        };
        $client->subscribe('updates', static function () use ($client, $guard, $publishing): void {
            $publishing->state = $client->state();
            try {
                $client->publishGuarded('updates.reply', 'ok', '_INBOX.reply.1', $guard)->await();
            } catch (\Throwable $failure) {
                $publishing->failure = $failure;
            }
        })->await();

        $client->drain()->await(new TimeoutCancellation(5));

        self::assertSame(ConnectionState::Draining, $publishing->state, 'the handler published while the connection drained');
        self::assertSame(2, $publishing->calls, 'at the publish, and in its writer');
        self::assertSame(ConnectionState::Closed, $client->state());
        if ($refused) {
            self::assertSame('refused in the writer', $publishing->failure?->getMessage());
            self::assertStringNotContainsString('PUB updates.reply', implode('', $transport->writes));

            return;
        }
        self::assertNull($publishing->failure, sprintf('the publish failed with %s', $publishing->failure?->getMessage()));
        self::assertStringContainsString("PUB updates.reply _INBOX.reply.1 2\r\nok\r\n", implode('', $transport->writes));
    }

    /**
     * A guard that counts its calls and, from call $refuseFrom on (never when null), refuses the send with an exception
     * naming the call, as the pull consumer's guard does once the inbox's rejection is recorded.
     *
     * @return array{\Closure(): void, object{count: int}}
     */
    private static function guard(?int $refuseFrom): array
    {
        $calls = new class {
            public int $count = 0;
        };

        return [static function () use ($calls, $refuseFrom): void {
            ++$calls->count;
            if ($refuseFrom !== null && $calls->count >= $refuseFrom) {
                throw new \RuntimeException('refused at call ' . $calls->count);
            }
        }, $calls];
    }

    /**
     * What $publish failed with, within 3 s; null when it succeeded.
     *
     * @param Future<void> $publish
     */
    private static function failureOf(Future $publish): ?\Throwable
    {
        try {
            $publish->await(new TimeoutCancellation(3));

            return null;
        } catch (\Throwable $failure) {
            return $failure;
        }
    }
}
