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
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\UncancellableDialTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;

/**
 * Operations issued while a reconnect is in flight wait for it within their own single budget instead
 * of failing at once ({@see NatsOptions::$waitForReconnect}).
 *
 * Failing on the spot starved synchronous callers: a reconnect only advances while something waits on
 * the event loop, so a process whose every operation failed immediately left a reconnect the heartbeat
 * started in the background parked for good. Most tests keep a recovery owned by ANOTHER fiber in
 * flight (dials refused, backing off) when the operation is issued, and let the dials through from a
 * timer - which only fires if the waiting operation really hands the event loop control.
 */
final class WaitForReconnectTest extends TestCase
{
    use ReconnectScenarios;

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    public function testRequestDuringReconnectWaitsForItAndSucceeds(): void
    {
        $transport = new ReconnectingTransport();
        $this->echoResponder($transport);
        $connection = $this->connect($transport);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $this->acceptDialsAfter($transport, 0.05);
        $reply = $connection->request('svc.echo', 'hi', 2_000)->await();

        self::assertSame('echo:hi', $reply->payload);
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $connection->statistics()->reconnects);
        self::assertSame([], $transport->controlLinesStartingWith('PUB svc.echo', 0), 'nothing may be sent on the dead connection');
        self::assertCount(1, $transport->controlLinesStartingWith('PUB svc.echo', $transport->epoch()));
        self::assertSame(0, $reader->await());
    }

    public function testRequestWithHeadersDuringReconnectWaitsForItAndSucceeds(): void
    {
        $transport = new ReconnectingTransport();
        $this->echoResponder($transport);
        $connection = $this->connect($transport);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $this->acceptDialsAfter($transport, 0.05);
        $reply = $connection->requestWithHeaders('svc.echo', 'hi', ['X-Trace' => 't1'], 2_000)->await();

        self::assertSame('echo:hi', $reply->payload);
        self::assertCount(1, $transport->controlLinesStartingWith('HPUB svc.echo', $transport->epoch()));
        $reader->await();
    }

    public function testRequestTimesOutWhenTheReconnectOutlastsItsTimeout(): void
    {
        $transport = new ReconnectingTransport();
        $this->echoResponder($transport);
        $connection = $this->connect($transport);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $start = hrtime(true);
        try {
            $connection->request('svc.echo', 'hi', 150)->await();
            self::fail('expected the request to time out while the reconnect is still failing');
        } catch (TimeoutException $e) {
            self::assertSame('Request timed out for subject svc.echo while waiting for the connection to be re-established', $e->getMessage());
        }

        $elapsed = $this->secondsSince($start);
        self::assertGreaterThanOrEqual(0.14, $elapsed, 'the request waited out its timeout');
        self::assertLessThan(1.2, $elapsed, 'its own timeout, not the connection\'s 2 s one');
        self::assertSame(ConnectionState::Connecting, $connection->state());
        self::assertSame([], $transport->controlLinesStartingWith('PUB svc.echo'), 'a request that never got a connection is never sent');

        $transport->acceptDials();
        $reader->await();
        self::assertSame([], $transport->controlLinesStartingWith('PUB svc.echo'), 'nor sent later, after the reconnect');
    }

    /**
     * One budget covers the whole request: the reconnect ends ~200 ms into a 1.2 s budget and the server
     * answers 1.1 s after the publish, so the request must time out at ~1.2 s. With a fresh budget after
     * the wait it would have received the reply at ~1.3 s. The outcome, unlike an elapsed time, does not
     * depend on how late the runner fires its timers, and the reconnect may end up to ~1 s late without
     * changing it.
     */
    public function testRequestSharesOneTimeoutBetweenTheReconnectWaitAndTheReply(): void
    {
        $transport = new ReconnectingTransport();
        $this->echoResponder($transport);
        $transport->responseDelay = 1.1;
        $connection = $this->connect($transport);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $this->acceptDialsAfter($transport, 0.2);
        try {
            $connection->request('svc.echo', 'hi', 1_200)->await();
            self::fail('expected the request to run out of its single budget');
        } catch (TimeoutException $e) {
            self::assertSame('Request timed out for subject svc.echo', $e->getMessage());
        }

        self::assertCount(1, $transport->controlLinesStartingWith('PUB svc.echo', $transport->epoch()), 'the request was sent once reconnected');
        $reader->await();
    }

    /**
     * The same single budget bounds a request that joins another request's still-running reply-inbox
     * set-up (its SUB write held up by backpressure): the joiner gives up at its own deadline, while that
     * SUB write is still stalled, and the first request, with budget to spare, completes. The joiner's
     * deadline is due before the stall ends, so which of the two comes first does not depend on how late
     * the runner fires its timers, unlike an elapsed time.
     */
    public function testRequestJoiningASlowReplyInboxSetUpGivesUpAtItsOwnDeadline(): void
    {
        $transport = new ReconnectingTransport();
        $this->echoResponder($transport);
        $connection = $this->connect($transport);
        $transport->stallNextWriteContaining('SUB _INBOX.', 0.3);

        $first = $connection->request('svc.echo', 'first', 2_000);
        $start = hrtime(true);
        try {
            $connection->request('svc.echo', 'second', 100)->await();
            self::fail('expected the second request to give up while the reply inbox is still being set up');
        } catch (TimeoutException $e) {
            self::assertSame('Request timed out for subject svc.echo while waiting for the reply inbox to be set up', $e->getMessage());
        }

        self::assertGreaterThanOrEqual(0.09, $this->secondsSince($start), 'it used its whole budget');
        self::assertSame(1, $transport->writesStalled(), 'it gave up while the SUB of the reply inbox was still being written');
        self::assertSame('echo:first', $first->await()->payload);
        self::assertSame(['PUB svc.echo'], array_map(
            static fn(string $line): string => implode(' ', array_slice(explode(' ', $line), 0, 2)),
            $transport->controlLinesStartingWith('PUB svc.echo'),
        ), 'only the first request was sent');
    }

    public function testRequestSurfacesTheReconnectFailureWhenAttemptsAreExhausted(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, maxReconnectAttempts: 3);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $start = hrtime(true);
        try {
            $connection->request('svc.echo', 'hi', 2_000)->await();
            self::fail('expected the reconnect failure');
        } catch (ConnectionException $e) {
            self::assertSame('Reconnect attempts exhausted', $e->getMessage());
        }

        self::assertLessThan(1.0, $this->secondsSince($start), 'the failure surfaces when the reconnect gives up, not at the request timeout');
        self::assertSame(ConnectionState::Closed, $connection->state());

        try {
            $reader->await();
            self::fail('the reader that ran the recovery shares its failure');
        } catch (ConnectionException) {
            // Expected.
        }

        // Nothing is in flight any more, so the next request fails at once.
        $start = hrtime(true);
        try {
            $connection->request('svc.echo', 'hi', 2_000)->await();
            self::fail('expected ConnectionException on a closed connection');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }
        self::assertLessThan(1.0, $this->secondsSince($start), 'at once, not at its 2 s timeout');
    }

    public function testRequestFailsFastDuringReconnectWhenWaitingIsDisabled(): void
    {
        $transport = new ReconnectingTransport();
        $this->echoResponder($transport);
        $connection = $this->connect($transport, waitForReconnect: false);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $start = hrtime(true);
        try {
            $connection->request('svc.echo', 'hi', 2_000)->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }

        self::assertLessThan(1.0, $this->secondsSince($start), 'at once, not at its 2 s timeout');
        $transport->acceptDials();
        $reader->await();
    }

    public function testRequestHonoursExternalCancellationWhileWaitingForReconnect(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $cancellation = new DeferredCancellation();
        EventLoop::delay(0.05, static function () use ($cancellation): void {
            $cancellation->cancel();
        });

        $start = hrtime(true);
        try {
            $connection->request('svc.echo', 'hi', 2_000, $cancellation->getCancellation())->await();
            self::fail('expected CancelledException');
        } catch (CancelledException) {
            // Expected: the caller's own cancellation, not a timeout.
        }

        self::assertLessThan(1.0, $this->secondsSince($start), 'when the cancellation fired, not at its 2 s timeout');
        $transport->acceptDials();
        $reader->await();
    }

    public function testDisconnectEndsAWaitForReconnect(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        EventLoop::delay(0.05, static function () use ($connection): void {
            $connection->disconnect()->await();
        });

        $start = hrtime(true);
        try {
            $connection->request('svc.echo', 'hi', 2_000)->await();
            self::fail('expected ConnectionException once the user closed the connection');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }

        self::assertLessThan(1.0, $this->secondsSince($start), 'when the disconnect came, not at its 2 s timeout');
        self::assertSame(ConnectionState::Closed, $connection->state());

        try {
            $reader->await();
        } catch (\Throwable) {
            // The aborted recovery's own outcome is not what this test is about.
        }
    }

    /**
     * disconnect() waits for the reconnect it stops only as long as the connect timeout: one whose dial the
     * transport cannot stop is still in flight when disconnect() returns. An operation issued then must fail
     * at once rather than wait for it, since nothing will reopen a connection the user closed.
     */
    public function testOperationAfterDisconnectDuringAReconnectFailsAtOnce(): void
    {
        $inner = new ReconnectingTransport();
        $connection = $this->connect(new UncancellableDialTransport($inner));
        $reader = $this->startRecoveryHeldMidDial($connection, $inner);

        // Returns once the connect timeout is over, with the reconnect still dialling.
        $connection->disconnect()->await(new TimeoutCancellation(5));
        self::assertFalse($reader->isComplete(), 'the reconnect is still in flight');

        $start = hrtime(true);
        try {
            $connection->request('svc.echo', 'hi', 2_000)->await();
            self::fail('expected ConnectionException on a connection the user closed');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }

        self::assertLessThan(1.0, $this->secondsSince($start), 'no waiting for the reconnect that is winding down');

        $inner->releaseDial();
        try {
            $reader->await();
        } catch (\Throwable) {
            // The aborted recovery's own outcome is not what this test is about.
        }
    }

    /**
     * A connection/error listener runs inside the recovery fiber: an operation it awaits must be
     * refused at once rather than wait on the very recovery that is suspended in the listener (a
     * deadlock until the operation's timeout), and the recovery itself must be unaffected (#145).
     */
    public function testOperationsFromAListenerInsideTheRecoveryFailFastInsteadOfDeadlocking(): void
    {
        $transport = new ReconnectingTransport();
        /** @var array<string, \Closure(NatsConnection): mixed> $operations */
        $operations = [
            'request' => static fn(NatsConnection $c): mixed => $c->request('svc.echo', 'x', 2_000)->await(),
            'requestMany' => static fn(NatsConnection $c): mixed => $c->requestMany('svc.echo', 'x', null, 1, 2_000)->await(),
            'subscribe' => static fn(NatsConnection $c): mixed => $c->subscribe('updates', static function (): void {})->await(),
            'flush' => static function (NatsConnection $c): mixed {
                $c->flush()->await();

                return null;
            },
            'rtt' => static fn(NatsConnection $c): mixed => $c->rtt()->await(),
            'processIncoming' => static fn(NatsConnection $c): mixed => $c->processIncoming(new TimeoutCancellation(2))->await(),
        ];
        /** @var array<string, array{\Throwable|null, float}> $outcomes */
        $outcomes = [];
        // Filled in once connected: the listener also fires for the initial connect.
        $holder = new class {
            public ?NatsConnection $connection = null;
        };
        $listener = static function (ConnectionEvent $event) use ($holder, &$outcomes, $operations): void {
            $connection = $holder->connection;
            if ($event !== ConnectionEvent::Disconnected || $connection === null) {
                return;
            }

            foreach ($operations as $name => $operation) {
                $start = hrtime(true);
                $error = null;
                try {
                    $operation($connection);
                } catch (\Throwable $e) {
                    $error = $e;
                }
                $outcomes[$name] = [$error, (hrtime(true) - $start) / 1e9];
            }
        };
        $connection = $this->connect($transport, connectionListener: $listener);
        $holder->connection = $connection;

        // The reader that notices the dead session runs the recovery - and fires Disconnected - in its own fiber.
        $transport->dropConnection();
        $connection->processIncoming()->await(new TimeoutCancellation(5));

        self::assertSame(ConnectionState::Open, $connection->state(), 'the refused operations must not affect the recovery itself');
        self::assertSame(1, $connection->statistics()->reconnects);
        self::assertSame(array_keys($operations), array_keys($outcomes));
        foreach ($outcomes as $name => [$error, $elapsed]) {
            if (!$error instanceof ConnectionException) {
                self::fail($name . ' must be refused with ConnectionException, got ' . ($error === null ? 'success' : $error::class));
            }
            self::assertSame('Connection is not open', $error->getMessage(), $name);
            self::assertLessThan(1.0, $elapsed, $name . ' must be refused at once, not wait on the recovery it runs inside until its 2 s timeout');
        }
    }

    public function testRequestManyDuringReconnectWaitsForItAndCollectsReplies(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport): array {
            if ($subject !== 'svc.scan' || $replyTo === null) {
                return [];
            }

            return [...$transport->replyFrame($replyTo, 'a'), ...$transport->replyFrame($replyTo, 'b')];
        };
        $connection = $this->connect($transport);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $this->acceptDialsAfter($transport, 0.05);
        $replies = $connection->requestMany('svc.scan', 'q', null, 2, 2_000)->await();

        self::assertSame(['a', 'b'], array_map(static fn(NatsMessage $message): string => $message->payload, $replies));
        self::assertSame([], $transport->controlLinesStartingWith('PUB svc.scan', 0));
        $reader->await();
    }

    public function testRequestManyTimesOutInsteadOfReturningNothingWhenTheReconnectOutlastsIt(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        try {
            $connection->requestMany('svc.scan', 'q', null, 2, 150)->await();
            self::fail('expected a timeout rather than an empty collection');
        } catch (TimeoutException $e) {
            self::assertSame('Request timed out for subject svc.scan while waiting for the connection to be re-established', $e->getMessage());
        }

        self::assertSame([], $transport->controlLinesStartingWith('PUB svc.scan'));
        $transport->acceptDials();
        $reader->await();
    }

    public function testSubscribeDuringReconnectWaitsAndSubscribesOnceOnTheNewConnection(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $this->acceptDialsAfter($transport, 0.05);
        $received = [];
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame([], $transport->controlLinesStartingWith('SUB updates', 0));
        self::assertSame(['SUB updates ' . $sid], $transport->controlLinesStartingWith('SUB updates', $transport->epoch()));

        $transport->pushFrame(ReconnectingTransport::msgFrame('updates', $sid, 'hello'));
        $connection->processIncoming(new TimeoutCancellation(2))->await();
        self::assertSame(['hello'], $received);
        $reader->await();
    }

    public function testSubscribeTimesOutWhenTheReconnectOutlastsTheRequestTimeout(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 150);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $start = hrtime(true);
        try {
            $connection->subscribe('updates', static function (): void {})->await();
            self::fail('expected the subscribe to time out');
        } catch (TimeoutException $e) {
            self::assertSame('Subscribe to "updates" timed out waiting for the connection to be re-established', $e->getMessage());
        }

        $elapsed = $this->secondsSince($start);
        self::assertGreaterThanOrEqual(0.14, $elapsed);
        self::assertLessThan(1.2, $elapsed, 'the subscribe gave up at the request timeout');

        // The failed subscribe left nothing behind: not even the replay writes its SUB.
        $transport->acceptDials();
        $reader->await();
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame([], $transport->controlLinesStartingWith('SUB updates'));
    }

    public function testFlushDuringReconnectWaitsForTheNewConnectionsPong(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $this->acceptDialsAfter($transport, 0.05);
        $connection->flush()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        // The handshake's PING plus the flush's own, both on the new connection.
        self::assertCount(2, $transport->controlLinesStartingWith('PING', $transport->epoch()));
        $reader->await();
    }

    public function testFlushTimesOutWhenTheReconnectOutlastsTheRequestTimeout(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 150);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $start = hrtime(true);
        try {
            $connection->flush()->await();
            self::fail('expected flush() to time out');
        } catch (TimeoutException $e) {
            self::assertSame('Flush timed out waiting for the connection to be re-established', $e->getMessage());
        }

        $elapsed = $this->secondsSince($start);
        self::assertGreaterThanOrEqual(0.14, $elapsed);
        self::assertLessThan(1.2, $elapsed, 'the flush gave up at the request timeout');
        $transport->acceptDials();
        $reader->await();
    }

    /**
     * The reconnect ends ~200 ms into flush's 1.2 s budget and the server answers the flush's PING 1.1 s
     * after it: one budget means the flush gives up at ~1.2 s, before that PONG (~1.3 s), where a fresh
     * budget after the reconnect would last until ~1.4 s and receive it. The outcome, unlike an elapsed time,
     * does not depend on how late the runner fires its timers, and the reconnect may end up to ~1 s late
     * without changing it.
     */
    public function testFlushSharesOneTimeoutBetweenTheReconnectWaitAndThePong(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 1_200);
        $transport->pongDelay = 1.1;
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $this->acceptDialsAfter($transport, 0.2);
        try {
            $connection->flush()->await();
            self::fail('expected the flush to time out before the PONG came');
        } catch (TimeoutException $e) {
            self::assertSame('Flush timed out waiting for server PONG', $e->getMessage());
        }

        self::assertSame(ConnectionState::Open, $connection->state());
        $reader->await();
    }

    /**
     * Without any reconnect, flush()'s PING write (slowed by backpressure) and its PONG wait share the
     * one request-timeout budget it documents: 200 ms of write leave 100 ms for the PONG, which the server
     * sends 200 ms after the write, so the flush times out first. With a full 300 ms for each phase, as
     * each phase used to get, it would receive the PONG. Unlike an elapsed time, which of the two happens
     * does not depend on how late the runner fires its timers.
     */
    public function testFlushWriteAndPongPhasesShareOneTimeout(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 300);
        $transport->stallNextWriteContaining("PING\r\n", 0.2);
        $transport->pongDelay = 0.2;

        $start = hrtime(true);
        try {
            $connection->flush()->await();
            self::fail('expected the flush to time out before the PONG came');
        } catch (TimeoutException $e) {
            self::assertSame('Flush timed out waiting for server PONG', $e->getMessage());
        }

        self::assertGreaterThanOrEqual(0.28, $this->secondsSince($start), 'the flush used its whole budget');
    }

    public function testRttDuringReconnectExcludesTheWaitFromTheMeasurement(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $this->acceptDialsAfter($transport, 0.1);
        $start = hrtime(true);
        $rtt = $connection->rtt()->await();
        $elapsed = $this->secondsSince($start);

        self::assertGreaterThanOrEqual(0.09, $elapsed, 'rtt() waited for the reconnect');
        // Compared with the elapsed time rather than a fixed bound, so that a slow runner cannot fail it: the round
        // trip it measured leaves out the 0.1 s or more it waited for the reconnect.
        self::assertLessThan($elapsed - 0.09, $rtt, 'the wait is not part of the measured round trip');
        $reader->await();
    }

    public function testRttTimesOutWhenTheReconnectOutlastsTheRequestTimeout(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 150);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        try {
            $connection->rtt()->await();
            self::fail('expected rtt() to time out');
        } catch (TimeoutException $e) {
            self::assertSame('RTT measurement timed out waiting for the connection to be re-established', $e->getMessage());
        }

        $transport->acceptDials();
        $reader->await();
    }

    public function testProcessIncomingDuringReconnectWaitsThenReadsFromTheNewConnection(): void
    {
        $transport = new ReconnectingTransport();
        $subscription = new class {
            public ?int $sid = null;
        };
        // The server sends a message on the new connection once the reconnect is over, however long that takes.
        $connection = $this->connect($transport, connectionListener: static function (ConnectionEvent $event) use ($transport, $subscription): void {
            if ($event === ConnectionEvent::Reconnected && $subscription->sid !== null) {
                $transport->pushFrame(ReconnectingTransport::msgFrame('updates', $subscription->sid, 'after'));
            }
        });
        $received = [];
        $subscription->sid = $connection->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $this->acceptDialsAfter($transport, 0.05);
        $frames = $connection->processIncoming(new TimeoutCancellation(5))->await();

        self::assertSame(1, $frames);
        self::assertSame(['after'], $received, 'the replayed subscription delivers on the new connection');
        $reader->await();
    }

    public function testProcessIncomingWaitForReconnectIsBoundedByItsCancellation(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $start = hrtime(true);
        try {
            $connection->processIncoming(new TimeoutCancellation(0.1))->await();
            self::fail('expected the read to be cancelled while the reconnect is still failing');
        } catch (CancelledException) {
            // Expected.
        }

        $elapsed = $this->secondsSince($start);
        self::assertGreaterThanOrEqual(0.09, $elapsed);
        self::assertLessThan(1.0, $elapsed, 'the read gave up when its cancellation fired');
        self::assertSame(ConnectionState::Connecting, $connection->state());
        $transport->acceptDials();
        $reader->await();
    }

    /**
     * Readers waiting for a reconnect must not touch the new socket before the recovery flips it Open:
     * one stolen INFO or PONG would fail the handshake and cost another dial (#148).
     */
    public function testReadersWaitingForReconnectLeaveTheHandshakeToTheRecovery(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        /** @var list<Future<int>> $waiters */
        $waiters = [];
        for ($i = 0; $i < 3; $i++) {
            $waiters[] = async(static fn(): int => $connection->processIncoming(new TimeoutCancellation(2))->await());
        }
        delay(0.02);

        $dialsBefore = count($transport->connectCalls);
        $transport->acceptDials();
        $reader->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $connection->statistics()->reconnects);
        self::assertCount($dialsBefore + 1, $transport->connectCalls, 'the first accepted dial completes the reconnect');

        $transport->pushFrame("PING\r\n");
        $frames = array_map(static fn(Future $waiter): int => $waiter->await(), $waiters);
        self::assertSame(1, array_sum($frames), 'the waiters read from the new connection once it is open');
    }

    /**
     * A serving loop's read delivers what an earlier read left queued before it reads. When a handler it runs
     * awaits while the connection drops and another fiber's read reconnects, it waits for that reconnect before
     * it reads, like any reader (#148). Reading at once would take the PONG of the reconnect's handshake: the
     * attempt would fail, and with one attempt allowed the connection would close.
     */
    public function testAServingReadDeliveringWhatAnEarlierReadLeftQueuedLeavesTheHandshakeToTheRecovery(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect(
            $transport,
            maxReconnectAttempts: 1,
            connectionListener: $recorder->connectionListener(),
            reconnectDelayMs: 1,
            reconnectMaxDelayMs: 1,
            errorListener: $recorder->errorListener(),
        );
        $seen = [];
        $sidA = $connection->subscribe('a', static function (): void {
            throw new \RuntimeException('handler a');
        })->await();
        $sidB = $connection->subscribe('b', static function (NatsMessage $message) use (&$seen): void {
            delay(0.05);
            $seen[] = $message->payload;
        })->await();
        // A read stops at a's throwing handler and leaves b's message queued.
        $transport->pushFrame(ReconnectingTransport::msgFrame('a', $sidA, 'x') . ReconnectingTransport::msgFrame('b', $sidB, 'y'));
        try {
            $connection->processIncoming()->await();
            self::fail('expected the handler failure');
        } catch (\RuntimeException $e) {
            self::assertSame('handler a', $e->getMessage());
        }

        // The serving read delivers that message, and b's handler awaits. Meanwhile the connection drops and
        // another fiber's read reconnects at once; the new connection's CONNECT write takes 100 ms, so its
        // handshake is still under way when the handler returns.
        $serving = $connection->readIncomingForOperation(alwaysReport: true);
        // Should an assertion below fail first, tearDown() fails this read: that is no error of the next test.
        $serving->ignore();
        delay(0.01);
        $transport->stallNextWriteContaining('CONNECT', 0.1);
        $transport->dropConnection();
        $reader = async(static fn(): int => $connection->processIncoming()->await());

        self::assertSame(0, $reader->await(), 'the read that met the drop ran the reconnect');
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected], $recorder->events);
        self::assertCount(2, $transport->connectCalls, 'the first reconnect attempt succeeded');
        self::assertSame(['Socket closed by peer (EOF)'], $recorder->errors);

        // The serving read reads the new connection.
        $transport->pushFrame(ReconnectingTransport::msgFrame('b', $sidB, 'z'));
        self::assertTrue($serving->await(new TimeoutCancellation(1))->consumedBytes);
        self::assertSame(['y', 'z'], $seen);
    }

    /**
     * A request whose read fails because another fiber (here the heartbeat) started the recovery must
     * give up at its own deadline instead of waiting for the whole recovery. The heartbeat notices the hang
     * within ~0.05 s, well inside the request's 1.2 s budget, so that the request is still reading when the
     * recovery closes the socket even on a runner that fires its timers late.
     */
    public function testInFlightRequestWhoseReadFailsReturnsAtItsOwnDeadline(): void
    {
        $transport = new ReconnectingTransport();
        $this->echoResponder($transport);
        $connection = $this->connect($transport, pingIntervalSeconds: 0.02, maxPingsOut: 1);
        $transport->refuseDials();
        $transport->silence();

        $start = hrtime(true);
        try {
            $connection->request('svc.echo', 'hi', 1_200)->await(new TimeoutCancellation(5));
            self::fail('expected the request to time out');
        } catch (TimeoutException $e) {
            self::assertSame('Request timed out for subject svc.echo', $e->getMessage());
        }

        self::assertLessThan(2.2, $this->secondsSince($start), 'the request gave up at its own deadline');
        self::assertSame(ConnectionState::Connecting, $connection->state(), 'the heartbeat\'s recovery is still backing off');
        $transport->acceptDials();
        $this->waitUntilOpen($connection);
    }

    /**
     * The same when the request's read gets a corrupt chunk instead of an error - here the last bytes of the
     * old socket, read while the heartbeat's recovery closes it: the request joins that recovery only until
     * its own deadline. It used to wait for the whole reconnect. As above, the request's 1.2 s budget is
     * well beyond the ~0.1 s the heartbeat takes to notice the hang.
     */
    public function testInFlightRequestWhoseReadHitsACorruptChunkReturnsAtItsOwnDeadline(): void
    {
        $transport = new ReconnectingTransport();
        $this->echoResponder($transport);
        $connection = $this->connect($transport, pingIntervalSeconds: 0.05, maxPingsOut: 1);
        // Sets up the reply inbox, so the request below only waits for its reply - which never comes.
        $connection->request('svc.echo', 'warm-up', 2_000)->await();
        $transport->responder = null;
        $transport->answerPings = false;
        $transport->closeDelay = 0.1;
        $transport->beforeClose = static function () use ($transport): void {
            $transport->beforeClose = null;
            $transport->refuseDials();
            $transport->pushFrame("BOGUS\r\n");
        };

        $start = hrtime(true);
        try {
            $connection->request('svc.echo', 'hi', 1_200)->await(new TimeoutCancellation(5));
            self::fail('expected the request to time out');
        } catch (TimeoutException $e) {
            self::assertSame('Request timed out for subject svc.echo', $e->getMessage());
        }

        self::assertLessThan(2.2, $this->secondsSince($start), 'the request gave up at its own deadline');
        self::assertSame(ConnectionState::Connecting, $connection->state(), 'the heartbeat\'s recovery is still backing off');
        $transport->acceptDials();
        $this->waitUntilOpen($connection);
    }

    /**
     * The reported stall: a hung server is noticed by the heartbeat, which starts the recovery in its
     * own fiber, and the process then behaves synchronously - request, and on failure sleep without
     * touching the event loop. Failing on the spot never let the loop run that recovery, so every
     * request failed for good; waiting for it lets the first request drive the reconnect and succeed.
     */
    public function testSynchronousCallerDrivesAHeartbeatStartedReconnect(): void
    {
        $transport = new ReconnectingTransport();
        $this->echoResponder($transport);
        $connection = $this->connect($transport, pingIntervalSeconds: 0.02, maxPingsOut: 1);
        $transport->refuseDials();
        $transport->silence();

        // Only the heartbeat, running while this fiber waits, can notice the hang.
        $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Connecting);
        $transport->acceptDials();

        $replies = [];
        $failures = 0;
        for ($attempt = 0; $attempt < 20 && $replies === []; $attempt++) {
            try {
                $replies[] = $connection->request('svc.echo', 'hi', 1_000)->await()->payload;
            } catch (ConnectionException) {
                $failures++;
                usleep(20_000);
            }
        }

        self::assertSame(['echo:hi'], $replies);
        self::assertSame(0, $failures, 'the first request waited for the reconnect instead of failing on the spot');
        self::assertSame(1, $connection->statistics()->reconnects);
    }

    /**
     * Buffered publishes return without suspending, so a publisher that only buffers never let the
     * loop run the reconnect that flushes its buffer. Each buffered publish now yields one tick.
     */
    public function testBufferedPublishesLetASynchronousPublisherDriveTheReconnect(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $reader = $this->startRecoveryInBackground($connection, $transport);
        $transport->acceptDials();

        $published = 0;
        while ($published < 50 && $connection->state() !== ConnectionState::Open) {
            $connection->publish('events', 'e' . $published)->await();
            $published++;
            usleep(5_000);
        }

        self::assertSame(ConnectionState::Open, $connection->state(), 'the publisher drove the reconnect');
        self::assertSame(
            array_map(static fn(int $i): string => 'PUB events ' . strlen('e' . $i), range(0, $published - 1)),
            $transport->controlLinesStartingWith('PUB events', $transport->epoch()),
            'every buffered publish was flushed on the new connection, in order',
        );
        $reader->await();
    }

    public function testBufferedPublishesDoNotYieldWhenWaitingIsDisabled(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, waitForReconnect: false);
        $reader = $this->startRecoveryInBackground($connection, $transport);
        $transport->acceptDials();

        for ($i = 0; $i < 30; $i++) {
            $connection->publish('events', 'e')->await();
            usleep(5_000);
        }

        self::assertSame(ConnectionState::Connecting, $connection->state(), 'nothing handed the event loop control');

        // Once something waits on the loop the reconnect completes and flushes the buffer.
        $reader->await();
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(30, $transport->controlLinesStartingWith('PUB events', $transport->epoch()));
    }

    /**
     * drain() is the lossless close, so while a reconnect is in flight it waits for it: the reconnect
     * flushes the publishes buffered during the outage, then drain unsubscribes and closes the new
     * connection.
     */
    public function testDrainDuringReconnectWaitsThenFlushesBufferedPublishesAndCloses(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $sid = $connection->subscribe('updates', static function (): void {})->await();
        $reader = $this->startRecoveryInBackground($connection, $transport);
        $connection->publish('events', 'buffered-1')->await();
        $connection->publish('events', 'buffered-2')->await();

        $this->acceptDialsAfter($transport, 0.05);
        $connection->drain()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(1, $connection->statistics()->reconnects);
        $epoch = $transport->epoch();
        self::assertSame(
            ['PUB events 10', 'PUB events 10'],
            $transport->controlLinesStartingWith('PUB events', $epoch),
            'the reconnect flushed the publishes buffered during the outage',
        );
        self::assertSame(['UNSUB ' . $sid], $transport->controlLinesStartingWith('UNSUB', $epoch), 'then drain unsubscribed on the new connection');
        $reader->await();
    }

    /**
     * The reconnect ends ~200 ms into drain's 1.2 s budget and the server answers the drain's PING 1.1 s
     * after it: the drain closes at ~1.2 s without that PONG (~1.3 s), its flush getting only what the wait
     * left of the one budget, where a fresh budget for the flush would last until ~1.4 s and wait for it.
     * Which comes first, unlike an elapsed time, does not depend on how late the runner fires its timers,
     * and the reconnect may end up to ~1 s late without changing it.
     */
    public function testDrainSharesOneTimeoutBetweenTheReconnectWaitAndItsFlush(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 1_200);
        $server = new class {
            public bool $pinged = false;
            public bool $answered = false;
        };
        $transport->answerPings = false;
        // The server answers the drain's PING, the one written while the connection drains, 1.1 s after it.
        $transport->afterWrite = static function (string $bytes) use ($connection, $transport, $server): void {
            if (str_ends_with($bytes, "PING\r\n") && $connection->state() === ConnectionState::Draining) {
                $server->pinged = true;
                EventLoop::delay(1.1, static function () use ($transport, $server): void {
                    $server->answered = true;
                    $transport->pushFrame("PONG\r\n");
                });
            }
        };
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $this->acceptDialsAfter($transport, 0.2);
        $start = hrtime(true);
        $connection->drain()->await();

        self::assertTrue($server->pinged, 'the drain flushed on the new connection');
        self::assertFalse($server->answered, 'the drain closed when its one budget ran out, before the server answered its PING');
        self::assertGreaterThanOrEqual(1.15, $this->secondsSince($start), 'the drain waited for its PONG until the budget ran out');
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(1, $connection->statistics()->reconnects);
        $reader->await();
    }

    /**
     * When the reconnect outlasts drain's budget the drain still ends Closed: it stops the reconnect,
     * runs its usual backlog pass - here a message queued behind a handler that is still busy, reported
     * as undelivered - and reports the buffered publishes it discards instead of dropping them silently.
     * The handler stays busy until the test lets it go, rather than for a fixed time, so that a runner
     * slow to start the drain cannot let it finish first and deliver the message.
     */
    public function testDrainThatRunsOutOfTimeWaitingForTheReconnectStillClosesAndReportsWhatItDiscards(): void
    {
        $transport = new ReconnectingTransport();
        /** @var list<string> $errors */
        $errors = [];
        $connection = $this->connect(
            $transport,
            requestTimeoutMs: 150,
            errorListener: static function (\Throwable $error) use (&$errors): void {
                $errors[] = $error->getMessage();
            },
        );
        $received = [];
        /** @var DeferredFuture<null> $handlerRelease */
        $handlerRelease = new DeferredFuture();
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use (&$received, $handlerRelease): void {
            $received[] = $message->payload;
            if ($message->payload === 'm1') {
                // Still busy when the drain gives up: released once the drain is over, or after 5 s.
                $handlerRelease->getFuture()->await(new TimeoutCancellation(5));
            }
        })->await();

        // Two messages arrive together; the first one's handler is still running when the connection
        // drops, so the second waits in the backlog.
        $transport->pushFrame(ReconnectingTransport::msgFrame('updates', $sid, 'm1') . ReconnectingTransport::msgFrame('updates', $sid, 'm2'));
        $dispatcher = async(static fn(): int => $connection->processIncoming()->await());
        $dispatcher->ignore();
        delay(0.02);
        self::assertSame(['m1'], $received);

        $reader = $this->startRecoveryInBackground($connection, $transport);
        $connection->publish('events', 'buffered')->await();

        $start = hrtime(true);
        $connection->drain()->await();
        $elapsed = $this->secondsSince($start);
        $handlerRelease->complete();

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertGreaterThanOrEqual(0.14, $elapsed);
        self::assertLessThan(1.2, $elapsed, 'the drain gave up when its budget ran out');
        self::assertContains(
            sprintf('Drain ran out of time waiting for the reconnect: %d bytes of buffered publishes were discarded', strlen("PUB events 8\r\nbuffered\r\n")),
            $errors,
        );
        self::assertContains('drain deadline exceeded: 1 buffered message(s) were not delivered before close', $errors);

        // The reconnect stopped: even with the server back it does not reopen the connection.
        $transport->acceptDials();
        delay(0.1);
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(0, $connection->statistics()->reconnects);

        try {
            $reader->await();
        } catch (\Throwable) {
            // The aborted recovery's own outcome is not what this test is about.
        }
    }

    /**
     * With waiting disabled, drain() during a reconnect does what nats.go's Drain() does: it closes the
     * connection and throws, so the reconnect cannot reopen a connection the application is shutting
     * down. The buffered publishes it discards are reported.
     */
    public function testDrainDuringReconnectClosesAndThrowsWhenWaitingIsDisabled(): void
    {
        $transport = new ReconnectingTransport();
        /** @var list<string> $errors */
        $errors = [];
        $connection = $this->connect(
            $transport,
            waitForReconnect: false,
            errorListener: static function (\Throwable $error) use (&$errors): void {
                $errors[] = $error->getMessage();
            },
        );
        $reader = $this->startRecoveryInBackground($connection, $transport);
        $connection->publish('events', 'buffered')->await();

        $start = hrtime(true);
        try {
            $connection->drain()->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Cannot drain while reconnecting: the connection was closed instead', $e->getMessage());
        }

        self::assertLessThan(1.0, $this->secondsSince($start), 'at once, not after waiting for the reconnect');
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertContains(
            sprintf('Drain could not wait for the reconnect: %d bytes of buffered publishes were discarded', strlen("PUB events 8\r\nbuffered\r\n")),
            $errors,
        );

        $transport->acceptDials();
        delay(0.1);
        self::assertSame(ConnectionState::Closed, $connection->state(), 'the reconnect did not reopen the connection');
        self::assertSame(0, $connection->statistics()->reconnects);

        try {
            $reader->await();
        } catch (\Throwable) {
            // The aborted recovery's own outcome is not what this test is about.
        }
    }

    /**
     * A drain() awaited from a Disconnected listener runs inside the reconnect and cannot wait for it
     * (it would wait on itself): it closes the connection at once and throws, and the reconnect - which
     * would otherwise succeed straight away - stops.
     */
    public function testDrainFromADisconnectedListenerClosesInsteadOfDeadlocking(): void
    {
        $transport = new ReconnectingTransport();
        $holder = new class {
            public ?NatsConnection $connection = null;

            /** @var array{\Throwable|null, float}|null */
            public ?array $outcome = null;
        };
        $listener = static function (ConnectionEvent $event) use ($holder): void {
            $connection = $holder->connection;
            if ($event !== ConnectionEvent::Disconnected || $connection === null) {
                return;
            }

            $start = hrtime(true);
            $error = null;
            try {
                $connection->drain()->await();
            } catch (\Throwable $e) {
                $error = $e;
            }
            $holder->outcome = [$error, (hrtime(true) - $start) / 1e9];
        };
        $connection = $this->connect($transport, connectionListener: $listener);
        $holder->connection = $connection;

        // Dials are accepted: without the drain the reconnect would succeed at once.
        $transport->dropConnection();
        $connection->processIncoming()->await(new TimeoutCancellation(5));

        self::assertNotNull($holder->outcome);
        [$error, $elapsed] = $holder->outcome;
        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertSame('Cannot drain while reconnecting: the connection was closed instead', $error->getMessage());
        self::assertLessThan(1.0, $elapsed, 'at once, not after waiting on itself until its 2 s budget ran out');
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(0, $connection->statistics()->reconnects);
        self::assertCount(1, $transport->connectCalls, 'the reconnect stopped before dialling');
    }

    /**
     * A reconnect that gives up while drain() waits leaves the connection Closed - drain's goal - so the
     * drain resolves; the reconnect's own failure path reported what it discarded.
     */
    public function testDrainWaitingForAReconnectThatGivesUpEndsClosed(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, maxReconnectAttempts: 3);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $start = hrtime(true);
        $connection->drain()->await();

        self::assertLessThan(1.0, $this->secondsSince($start));
        self::assertSame(ConnectionState::Closed, $connection->state());

        try {
            $reader->await();
            self::fail('the reader that ran the recovery gets its failure');
        } catch (ConnectionException $e) {
            self::assertSame('Reconnect attempts exhausted', $e->getMessage());
        }
    }

    /**
     * disconnect() stops a reconnect mid-backoff at once, so a drain waiting for that reconnect returns
     * straight away instead of when the 2 s backoff delay ends - and the close is announced once.
     */
    public function testDrainWaitingForAReconnectEndsWhenTheUserDisconnects(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect(
            $transport,
            requestTimeoutMs: 5_000,
            connectionListener: $recorder->connectionListener(),
            reconnectDelayMs: 2_000,
            reconnectMaxDelayMs: 2_000,
        );
        $reader = $this->startRecoveryInBackground($connection, $transport);

        EventLoop::delay(0.05, static function () use ($connection): void {
            $connection->disconnect()->await();
        });

        $start = hrtime(true);
        $connection->drain()->await();

        self::assertLessThan(1.0, $this->secondsSince($start), 'when the disconnect came, not when the 2 s backoff delay ended');
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Closed], $recorder->events);
        $reader->await();
    }

    /**
     * A drain() while another is already waiting for the reconnect fails at once, like any second drain
     * does; the first one drains the reopened connection.
     */
    public function testSecondDrainWhileTheFirstWaitsForAReconnectFailsAtOnce(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $reader = $this->startRecoveryInBackground($connection, $transport);

        $this->acceptDialsAfter($transport, 0.1);
        $first = $connection->drain();
        delay(0.02);

        try {
            $connection->drain()->await();
            self::fail('expected the second drain to fail');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }

        // An order of events rather than an elapsed time, so that a slow runner cannot fail it.
        self::assertSame(ConnectionState::Connecting, $connection->state(), 'it failed without waiting for the reconnect');
        $first->await();
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(1, $connection->statistics()->reconnects);
        $reader->await();
    }

    /**
     * A drain() after disconnect() has nothing to drain and throws at once, even while the reconnect the
     * disconnect stopped is still winding down: here one whose dial the transport cannot stop, which
     * disconnect() waits for only as long as the connect timeout.
     */
    public function testDrainAfterDisconnectDuringAReconnectThrowsWithNothingToDrain(): void
    {
        $inner = new ReconnectingTransport();
        $connection = $this->connect(new UncancellableDialTransport($inner));
        $reader = $this->startRecoveryHeldMidDial($connection, $inner);
        // Returns once the connect timeout is over, with the reconnect still dialling.
        $connection->disconnect()->await(new TimeoutCancellation(5));
        self::assertFalse($reader->isComplete(), 'the reconnect is still in flight');

        $start = hrtime(true);
        try {
            $connection->drain()->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }

        self::assertLessThan(1.0, $this->secondsSince($start), 'at once, not after waiting for the reconnect');
        self::assertSame(ConnectionState::Closed, $connection->state());

        $inner->releaseDial();
        try {
            $reader->await();
        } catch (\Throwable) {
            // The aborted recovery's own outcome is not what this test is about.
        }
    }

    public function testJetStreamPublishDuringReconnectWaitsForTheAck(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $subject === 'orders.created' && $replyTo !== null
            ? $transport->replyFrame($replyTo, '{"stream":"ORDERS","seq":7}')
            : [];
        $client = $this->connectClient($transport);
        $reader = $this->startRecoveryInBackground($client, $transport);

        $this->acceptDialsAfter($transport, 0.05);
        $ack = $client->jetStream()->publish('orders.created', 'order-1')->await();

        self::assertSame('ORDERS', $ack->stream);
        self::assertSame(7, $ack->seq);
        self::assertSame([], $transport->controlLinesStartingWith('PUB orders.created', 0));
        $reader->await();
    }

    public function testJetStreamFetchDuringReconnectWaitsThenFetches(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport): array {
            if ($subject !== '$JS.API.CONSUMER.MSG.NEXT.ORDERS.worker' || $replyTo === null) {
                return [];
            }

            $sid = $transport->sidFor($replyTo);

            return $sid === null ? [] : [ReconnectingTransport::msgFrame(
                'orders.created',
                $sid,
                'order-1',
                '$JS.ACK.ORDERS.worker.1.1.1.1700000000000000000.0',
            )];
        };
        $client = $this->connectClient($transport);
        $reader = $this->startRecoveryInBackground($client, $transport);

        $this->acceptDialsAfter($transport, 0.05);
        $messages = $client->jetStream()->fetchBatch('ORDERS', 'worker', 1, 2_000)->await();

        self::assertCount(1, $messages);
        self::assertSame('order-1', $messages[0]->payload);
        self::assertSame([], $transport->controlLinesStartingWith('PUB $JS.API.CONSUMER.MSG.NEXT', 0));
        $reader->await();
    }

    /**
     * A pull fetch whose connection hangs mid-fetch - the heartbeat recovers in its own fiber - must
     * end by its own deadline with the usual "no messages" result instead of waiting for the recovery.
     */
    public function testJetStreamFetchLosingItsConnectionMidFetchEndsByItsDeadline(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->connectClient($transport, pingIntervalSeconds: 0.02, maxPingsOut: 1);
        $transport->refuseDials();
        EventLoop::delay(0.05, static function () use ($transport): void {
            $transport->silence();
        });

        $start = hrtime(true);
        try {
            $client->jetStream()->fetchBatch('ORDERS', 'worker', 1, 300)->await(new TimeoutCancellation(4));
            self::fail('expected the fetch to end without messages');
        } catch (JetStreamException $e) {
            self::assertSame(408, $e->getCode());
        }

        // The fetch deadline is its expiry plus one second of slack.
        self::assertLessThan(2.3, $this->secondsSince($start), 'the fetch ended by its deadline');
        self::assertSame(ConnectionState::Connecting, $client->state());
        $transport->acceptDials();
        $this->waitUntilOpen($client);
    }

    /** Answers every request on "svc.echo" with "echo:<payload>". */
    /** @return iterable<string, array{string}> */
    public static function nonBlockingQueuePolls(): iterable
    {
        yield 'fetch()' => ['fetch'];
        yield 'next() without a timeout' => ['next'];
        yield 'fetchAll() without a timeout' => ['fetchAll'];
    }

    /**
     * A SubscriptionQueue polled without a timeout while a reconnect is in flight returns nothing once its
     * short non-blocking wait is over, as on an idle connection, instead of failing with "Connection is not
     * open".
     */
    #[DataProvider('nonBlockingQueuePolls')]
    public function testQueuePolledWithoutATimeoutDuringAReconnectReturnsNothing(string $poll): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->connectClient($transport);
        $queue = $client->subscribeQueue('jobs')->await();
        $reader = $this->startRecoveryInBackground($client, $transport);

        $start = hrtime(true);
        $result = match ($poll) {
            'fetch' => $queue->fetch(),
            'next' => $queue->next(),
            default => $queue->fetchAll(),
        };

        self::assertSame($poll === 'fetchAll' ? [] : null, $result);
        self::assertLessThan(1.0, $this->secondsSince($start), 'only its short non-blocking wait, not a wait for the reconnect');
        self::assertSame(ConnectionState::Connecting, $client->state(), 'the reconnect is still backing off');
        $transport->acceptDials();
        $this->waitUntilOpen($client);
        $reader->await();
    }

    /** @return iterable<string, array{string}> */
    public static function queuePollsWithATimeout(): iterable
    {
        yield 'next()' => ['next'];
        yield 'fetchAll()' => ['fetchAll'];
    }

    /**
     * With a timeout, next() and fetchAll() wait for the reconnect within it, then read on the new connection
     * and get the message the server sends there.
     */
    #[DataProvider('queuePollsWithATimeout')]
    public function testQueuePolledWithATimeoutDuringAReconnectWaitsForIt(string $poll): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->connectClient($transport);
        $queue = $client->subscribeQueue('jobs')->await();
        $reader = $this->startRecoveryInBackground($client, $transport);
        $queue->setTimeout(2.0);
        $polled = async(static fn(): array => $poll === 'next'
            ? [$queue->next()?->payload]
            : array_map(static fn(NatsMessage $message): string => $message->payload, $queue->fetchAll(1)));
        delay(0.05);
        self::assertFalse($polled->isComplete(), 'it waits for the reconnect');

        $transport->acceptDials();
        $this->waitUntilOpen($client);
        $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j1'));

        self::assertSame(['j1'], $polled->await());
        self::assertSame(1, $client->statistics()->reconnects);
        $reader->await();
    }

    /** A reconnect that gives up fails a queue poll waiting for it with the reconnect's own error. */
    public function testQueuePolledDuringAReconnectThatGivesUpFailsWithItsError(): void
    {
        $transport = new ReconnectingTransport();
        $client = new NatsClient($this->options(true, 2_000, 3, 0, 2, null, 5, 20, null), $transport);
        $this->opened[] = $client;
        $client->connect()->await();
        $queue = $client->subscribeQueue('jobs')->await();
        $reader = $this->startRecoveryInBackground($client, $transport);

        try {
            $queue->setTimeout(2.0)->next();
            self::fail('expected the reconnect failure');
        } catch (ConnectionException $e) {
            self::assertSame('Reconnect attempts exhausted', $e->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $client->state());
        try {
            $reader->await();
        } catch (ConnectionException) {
            // The reader that ran the recovery shares its failure.
        }
    }

    private function echoResponder(ReconnectingTransport $transport): void
    {
        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $subject === 'svc.echo' && $replyTo !== null
            ? $transport->replyFrame($replyTo, 'echo:' . $payload)
            : [];
    }
}
