<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\AuthenticationException;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\FlakyTransport;
use PHPUnit\Framework\TestCase;
use SplQueue;

use function Amp\async;

/**
 * Kills surviving Infection mutants in {@see NatsConnection}'s auto-unsubscribe teardown (#112),
 * the not-open unsubscribe path (#116), and the initial-connect-retry / recovery terminal paths
 * (#56/#123/#133). Each test pins the exact observable behavior (subscription registration, an
 * emitted frame, or an emitted lifecycle event / async error) that the mutation would change; none
 * relies on wall-clock timing.
 */
final class NatsConnection_9MutationTest extends TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    private function connect(FakeTransport $transport, ?NatsOptions $options = null): NatsConnection
    {
        $connection = new NatsConnection($options ?? new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        return $connection;
    }

    /**
     * @param object $target
     */
    private function setProp(object $target, string $property, mixed $value): void
    {
        (new \ReflectionProperty(NatsConnection::class, $property))->setValue($target, $value);
    }

    /**
     * unsubscribe($sid, $max) must complete (drop) the subscription the instant the server-side max
     * has ALREADY been reached at receive with the local backlog drained: nothing more will ever be
     * delivered, so the subscription must not linger (or a reconnect would re-arm and over-deliver).
     *
     * kills MethodCallRemoval @ line 1300: dropping completeAutoUnsubIfSatisfied() leaves the
     * satisfied subscription registered instead of tearing it down.
     */
    public function testAutoUnsubscribeCompletesWhenServerMaxAlreadyReached(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = $this->connect($transport);

        $sid = $connection->subscribe('updates', static function (NatsMessage $m): void {
        })->await();

        // The server already sent (and we counted at intake, #112) the full max with the local queue
        // empty; arming auto-unsub at exactly that max must satisfy immediately.
        $this->setProp($connection, 'receivedCounts', [$sid => 5]);

        $connection->unsubscribe($sid, 5)->await();

        // Real: completeAutoUnsubIfSatisfied() drops the subscription. Mutant: it stays registered.
        self::assertFalse(
            $connection->isSubscriptionActive($sid),
            'an auto-unsub armed at an already-reached max with an empty backlog must drop the subscription',
        );
    }

    /**
     * A plain unsubscribe (no max) on a connection that is NOT open releases local state WITHOUT
     * telling the server - the socket cannot carry the UNSUB. The early return after
     * dropSubscriptionState() is what prevents falling through into the open-path transport write.
     *
     * kills ReturnRemoval @ line 1310: without the return, the not-open branch falls through and
     * writes an UNSUB frame onto a connection that is not open.
     */
    public function testPlainUnsubscribeOnNonOpenConnectionWritesNoUnsub(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = $this->connect($transport);

        $sid = $connection->subscribe('updates', static function (NatsMessage $m): void {
        })->await();

        // Model an in-flight reconnect: subscriptionMeta is retained (to be replayed) while state is
        // no longer Open, so unsubscribe() takes the not-open branch with the sid still registered.
        $this->setProp($connection, 'state', ConnectionState::Connecting);

        $writesBefore = \count($transport->writes);
        $connection->unsubscribe($sid)->await();
        $newWrites = \array_slice($transport->writes, $writesBefore);

        // Real: no UNSUB reaches the (non-open) socket. Mutant: an "UNSUB <sid>" frame is written.
        self::assertSame(
            [],
            $newWrites,
            'a plain unsubscribe on a non-open connection must not write UNSUB',
        );
    }

    /**
     * retryInitialConnect() (#56) that hits an AuthenticationException on a retry attempt must emit a
     * Closed lifecycle event before rethrowing - the terminal close is observable, not silent.
     *
     * kills MethodCallRemoval @ line 2474: dropping emitEvent(Closed, $e) still rethrows the auth
     * error but never announces the Closed transition.
     */
    public function testRetryInitialConnectAuthFailureEmitsClosedEvent(): void
    {
        $events = [];
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                // connection 0: the initial connect handshake fails recoverably (retry is attempted).
                ['__THROW__'],
                // connection 1: the retry's handshake hits an auth violation -> fail fast.
                [self::INFO, "-ERR Authorization Violation\r\n"],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $connection = new NatsConnection(
            new NatsOptions(
                servers: ['nats://127.0.0.1:4222'],
                pingIntervalSeconds: 0,
                reconnectEnabled: false,
                retryOnFailedInitialConnect: true,
                maxReconnectAttempts: 5,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                connectionListener: static function (ConnectionEvent $e) use (&$events): void {
                    $events[] = $e;
                },
            ),
            $transport,
        );

        try {
            $connection->connect()->await();
            self::fail('expected AuthenticationException');
        } catch (AuthenticationException) {
            // expected: auth failure aborts the retry loop.
        }

        // Real: the auth-abort path emits Closed. Mutant: no Closed event is ever emitted.
        self::assertContains(
            ConnectionEvent::Closed,
            $events,
            'an auth failure during initial-connect retry must emit a Closed event',
        );
    }

    /**
     * retryInitialConnect() that EXHAUSTS every attempt must report failure (return false) so
     * performConnect() takes its terminal close path and emits Closed. A truthy return would make
     * performConnect() believe it connected and skip the Closed transition entirely.
     *
     * kills FalseValue @ line 2486: returning true after exhaustion suppresses the Closed event
     * (connect() then throws only the generic "aborted before open" guard, no Closed emitted).
     */
    public function testRetryInitialConnectExhaustionEmitsClosedEvent(): void
    {
        $events = [];
        // Every connect fails at the socket level, so both the initial dial and each retry fail.
        $transport = new FlakyTransport(
            readQueuesByConnection: [],
            connectFailures: 10,
            readFailures: 0,
        );

        $connection = new NatsConnection(
            new NatsOptions(
                servers: ['nats://127.0.0.1:4222'],
                pingIntervalSeconds: 0,
                reconnectEnabled: false,
                retryOnFailedInitialConnect: true,
                maxReconnectAttempts: 2,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                connectionListener: static function (ConnectionEvent $e) use (&$events): void {
                    $events[] = $e;
                },
            ),
            $transport,
        );

        try {
            $connection->connect()->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException) {
            // expected: retries exhausted.
        }

        // Real: exhaustion returns false, performConnect() closes and emits Closed. Mutant: returns
        // true, performConnect() returns as if connected, and no Closed event is emitted.
        self::assertContains(
            ConnectionEvent::Closed,
            $events,
            'exhausting initial-connect retries must emit a Closed event',
        );
    }

    /**
     * A recovery on a reconnect-DISABLED connection is a terminal close: any parsed-but-undelivered
     * inbound backlog it discards must be named through the error listener (mirrors the outbound
     * reconnect-buffer discard, #123/#158), never dropped silently.
     *
     * kills MethodCallRemoval @ line 2507: dropping reportDiscardedInboundBacklog() still closes and
     * throws, but the discarded inbound messages are never reported.
     */
    public function testRecoveryReconnectDisabledReportsDiscardedInboundBacklog(): void
    {
        $errors = [];
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = $this->connect(
            $transport,
            new NatsOptions(
                pingIntervalSeconds: 0,
                reconnectEnabled: false,
                errorListener: static function (\Throwable $e) use (&$errors): void {
                    $errors[] = $e->getMessage();
                },
            ),
        );

        // Two parsed inbound messages queued but not yet delivered (as enqueueMessage() would leave
        // them); the terminal recovery below must name them as discarded.
        $queue = new SplQueue();
        $queue->enqueue(new NatsMessage('updates', 7, null, 'a'));
        $queue->enqueue(new NatsMessage('updates', 7, null, 'b'));
        $this->setProp($connection, 'pendingMessages', [7 => $queue]);

        try {
            async(static function () use ($connection): void {
                (new \ReflectionMethod(NatsConnection::class, 'performRecovery'))->invoke($connection);
            })->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Reconnect is disabled', $e->getMessage());
        }

        // Real: the discard is reported naming the exact count. Mutant: no such error is emitted.
        self::assertContains(
            'Connection closed: 2 parsed inbound message(s) were discarded undelivered',
            $errors,
            'a reconnect-disabled terminal recovery must name the discarded inbound backlog',
        );
    }

    /**
     * resubscribeAll() (reconnect replay) must DROP a subscription whose auto-unsubscribe max has
     * already been reached instead of re-SUBbing it: a fresh SUB would reset the server counter and
     * over-deliver live messages past the intended max (#112).
     *
     * kills MethodCallRemoval @ line 2711: dropping dropSubscriptionState() leaves the satisfied
     * subscription registered (still active) instead of tearing it down during replay.
     */
    public function testResubscribeDropsSatisfiedSubscription(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = $this->connect($transport);

        // A subscription whose received count already met its auto-unsub max (remaining <= 0).
        $this->setProp($connection, 'subscriptions', [1 => static function (NatsMessage $m): void {
        }]);
        $this->setProp($connection, 'subscriptionMeta', [1 => ['subject' => 'updates', 'queue' => null]]);
        $this->setProp($connection, 'autoUnsubMax', [1 => 3]);
        $this->setProp($connection, 'receivedCounts', [1 => 3]);

        async(static function () use ($connection): void {
            (new \ReflectionMethod(NatsConnection::class, 'resubscribeAll'))->invoke($connection);
        })->await();

        // Real: the satisfied subscription is dropped. Mutant: it survives the replay.
        self::assertFalse(
            $connection->isSubscriptionActive(1),
            'resubscribeAll must drop a subscription whose auto-unsub max was already reached',
        );
    }

    /**
     * After dropping a satisfied subscription, resubscribeAll() must CONTINUE to the remaining
     * subscriptions and replay them - a satisfied sid must not abort the whole replay.
     *
     * kills Continue_ @ line 2713 (continue -> break): with break, a later live subscription is never
     * re-SUBbed after an earlier satisfied one is dropped.
     */
    public function testResubscribeContinuesPastSatisfiedSubscriptionToReplayLaterSubs(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = $this->connect($transport);

        // sid 1 is satisfied (dropped during replay); sid 2 is live and must still be re-SUBbed.
        $this->setProp($connection, 'subscriptions', [
            1 => static function (NatsMessage $m): void {
            },
            2 => static function (NatsMessage $m): void {
            },
        ]);
        $this->setProp($connection, 'subscriptionMeta', [
            1 => ['subject' => 'satisfied', 'queue' => null],
            2 => ['subject' => 'live', 'queue' => null],
        ]);
        $this->setProp($connection, 'autoUnsubMax', [1 => 3]);
        $this->setProp($connection, 'receivedCounts', [1 => 3]);

        $writesBefore = \count($transport->writes);
        async(static function () use ($connection): void {
            (new \ReflectionMethod(NatsConnection::class, 'resubscribeAll'))->invoke($connection);
        })->await();
        $replayed = \implode('', \array_slice($transport->writes, $writesBefore));

        // Real: the loop continues past the dropped sid 1 and replays sid 2's SUB. Mutant: break exits
        // at sid 1, so sid 2 is never replayed and no bytes are written.
        self::assertStringContainsString(
            'SUB live 2',
            $replayed,
            'resubscribeAll must replay a live subscription that follows a dropped satisfied one',
        );
    }
}
