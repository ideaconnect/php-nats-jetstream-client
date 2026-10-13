<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use IDCT\NATS\Tests\Support\SubscriptionLimitServer;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The UNSUB a dropped shared reply inbox (mux) owes belongs to the connection it was dropped on (#193). A mux dropped
 * for a maximum-subscriptions -ERR is queued for release, since the -ERR may have been another SUB's and the server
 * may still hold it; the next install on that connection writes its UNSUB ahead of the new SUB, or a drain does. Up to
 * 2.26.5 the queue outlived a reconnect: a mux dropped on a connection then lost was released again on the new one,
 * which never held it, and a replayed mux the new server rejected got a second UNSUB after the replay's own. Over
 * SubscriptionLimitServer, which counts the subscriptions it holds per connection (epoch) and rejects a SUB past its
 * limit; the four request cases run through request() and requestMany(). Connections are owned by the test.
 */
final class MuxReleaseAcrossReconnectTest extends TestCase
{
    use OwnsTestResources;

    private const LIMIT_ERROR = "Server sent error frame: 'maximum subscriptions exceeded'";

    protected function tearDown(): void
    {
        $this->releaseOwnedResources();
    }

    /** @return iterable<string, array{string}> */
    public static function operations(): iterable
    {
        yield 'request' => ['request'];
        yield 'requestMany' => ['requestMany'];
    }

    /**
     * A mux dropped at the limit, its connection then lost: it is not replayed, and the next install on the new
     * connection is SUB + PING only, with no UNSUB of its old sid (only the application subscription's).
     */
    #[DataProvider('operations')]
    public function testADroppedMuxIsNotReleasedOnTheConnectionThatReplacesItsOwn(string $operation): void
    {
        [$server, $connection] = $this->connect(limit: 1);
        $appSid = $this->subscribe($connection, 'app.one');
        $this->expectLimitFailure($connection, $operation);
        self::assertSame([], $server->controlLines('UNSUB', epoch: 0));

        $this->reconnect($server, $connection);
        self::assertSame(['SUB app.one 1'], $server->controlLines('SUB', epoch: 1), 'the dropped mux was not replayed');
        $connection->unsubscribe($appSid)->await();
        self::assertSame('echo:next', $this->request($connection, $operation, 'next'));

        self::assertSame(['UNSUB 1'], $server->controlLines('UNSUB', epoch: 1), 'only the application subscription needs releasing on this connection');
        self::assertMatchesRegularExpression('/^SUB _INBOX\.[0-9a-f]{24}\.\* 3\r\nPING\r\n$/', self::muxInstalls($server, 1)[0]);
    }

    /**
     * A working mux the new server rejects during the replay: the replay writes its one UNSUB, and the next install
     * carries none.
     */
    #[DataProvider('operations')]
    public function testARejectedReplayedMuxIsReleasedExactlyOnce(string $operation): void
    {
        [$server, $connection] = $this->connect(limit: 2);
        $appSid = $this->subscribe($connection, 'app.one');
        self::assertSame('echo:first', $this->request($connection, $operation, 'first'));

        $server->limit = 1;
        $this->reconnect($server, $connection);
        self::assertSame(['UNSUB 2'], $server->controlLines('UNSUB', epoch: 1), 'the replay releases its rejected mux');
        self::assertSame(['app.one'], array_values($server->heldSubscriptions()));
        $connection->unsubscribe($appSid)->await();
        self::assertSame('echo:next', $this->request($connection, $operation, 'next'));

        self::assertSame(['UNSUB 2', 'UNSUB 1'], $server->controlLines('UNSUB', epoch: 1), 'the replacement install must not release the old mux again');
        self::assertMatchesRegularExpression('/^SUB _INBOX\.[0-9a-f]{24}\.\* 3\r\nPING\r\n$/', self::muxInstalls($server, 1)[1]);
    }

    /**
     * The first mux SUB's write fails, the recovery replays it and the new server rejects it: the replay's UNSUB is the
     * only one, also after the failed set-up's cleanup has run.
     */
    #[DataProvider('operations')]
    public function testARejectedMuxReplayedAfterItsFailedWriteIsReleasedExactlyOnce(string $operation): void
    {
        [$server, $connection] = $this->connect(limit: 1);
        $appSid = $this->subscribe($connection, 'app.one');
        $server->dropConnection();

        try {
            $this->request($connection, $operation, 'first');
            self::fail('the failed SUB must recover and report its replay rejection');
        } catch (ConnectionException $error) {
            self::assertStringContainsString('the server may have rejected the shared reply-inbox subscription', $error->getMessage());
        }

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $server->epoch());
        self::assertSame(['UNSUB 2'], $server->controlLines('UNSUB', epoch: 1));
        $connection->unsubscribe($appSid)->await();
        self::assertSame('echo:next', $this->request($connection, $operation, 'next'));

        self::assertSame(['UNSUB 2', 'UNSUB 1'], $server->controlLines('UNSUB', epoch: 1), 'failed set-up cleanup must not requeue a release already written by replay');
        self::assertMatchesRegularExpression('/^SUB _INBOX\.[0-9a-f]{24}\.\* 3\r\nPING\r\n$/', self::muxInstalls($server, 1)[1]);
    }

    /**
     * Guard: an ambiguous -ERR (another SUB's) drops a mux the server holds; the next install on the same connection
     * still writes its UNSUB ahead of the new SUB, freeing the slot, and gets its reply.
     */
    #[DataProvider('operations')]
    public function testTheNextInstallOnTheSameConnectionStillFreesADroppedMuxThatTheServerHolds(string $operation): void
    {
        [$server, $connection] = $this->connect(limit: 2);
        $this->subscribe($connection, 'app.one');
        $appTwo = $this->subscribe($connection, 'app.two');
        $this->subscribe($connection, 'app.rejected');
        $connection->unsubscribe($appTwo)->await();
        $this->expectLimitFailure($connection, $operation);
        self::assertSame([1, 4], array_keys($server->heldSubscriptions()), 'the ambiguous error dropped a mux that actually occupies a server slot');

        self::assertSame('echo:next', $this->request($connection, $operation, 'next'));
        self::assertSame(['UNSUB 2', 'UNSUB 4'], $server->controlLines('UNSUB', epoch: 0));
        self::assertMatchesRegularExpression('/^UNSUB 4\r\nSUB _INBOX\.[0-9a-f]{24}\.\* 5\r\nPING\r\n$/', self::muxInstalls($server, 0)[1]);
        self::assertSame([1, 5], array_keys($server->heldSubscriptions()), 'cleanup freed the slot before the replacement SUB');
    }

    /** A drain after a reconnect does not release a mux dropped on the connection before. */
    public function testADrainDoesNotReleaseAMuxDroppedOnThePreviousConnection(): void
    {
        [$server, $connection] = $this->connect(limit: 1);
        $this->subscribe($connection, 'app.one');
        $this->expectLimitFailure($connection, 'request');
        $this->reconnect($server, $connection);

        $connection->drain()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(['UNSUB 1'], $server->controlLines('UNSUB', epoch: 1), 'the drain must not carry an old connection\'s mux release');
    }

    /** A drain after a replay that released its rejected mux does not release it again. */
    public function testADrainDoesNotRepeatTheReleaseOfARejectedReplayedMux(): void
    {
        [$server, $connection] = $this->connect(limit: 2);
        $this->subscribe($connection, 'app.one');
        self::assertSame('echo:first', $this->request($connection, 'request', 'first'));
        $server->limit = 1;
        $this->reconnect($server, $connection);
        self::assertSame(['UNSUB 2'], $server->controlLines('UNSUB', epoch: 1));

        $connection->drain()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(['UNSUB 2', 'UNSUB 1'], $server->controlLines('UNSUB', epoch: 1), 'the drain must not repeat the replay\'s mux release');
    }

    /** Guard: a drain on the connection a held mux was dropped on still releases it, once. */
    public function testADrainStillReleasesADroppedMuxHeldOnTheSameConnection(): void
    {
        [$server, $connection] = $this->connect(limit: 2);
        $this->subscribe($connection, 'app.one');
        $appTwo = $this->subscribe($connection, 'app.two');
        $this->subscribe($connection, 'app.rejected');
        $connection->unsubscribe($appTwo)->await();
        $this->expectLimitFailure($connection, 'request');
        self::assertSame([1, 4], array_keys($server->heldSubscriptions()));

        $connection->drain()->await();

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(['UNSUB 4'], array_values(array_filter(
            $server->controlLines('UNSUB', epoch: 0),
            static fn(string $line): bool => $line === 'UNSUB 4',
        )), 'the drain still releases the dropped mux once on its own connection');
    }

    /** @return array{SubscriptionLimitServer, NatsConnection} */
    private function connect(int $limit): array
    {
        $server = new SubscriptionLimitServer();
        $server->limit = $limit;
        $server->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $replyTo === null
            ? []
            : $server->replyFrame($replyTo, 'echo:' . $payload);
        $connection = $this->own(new NatsConnection(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: 1_000,
            reconnectEnabled: true,
            maxReconnectAttempts: 3,
            reconnectDelayMs: 1,
            reconnectMaxDelayMs: 1,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
        ), $server));
        $connection->connect()->await();

        return [$server, $connection];
    }

    private function subscribe(NatsConnection $connection, string $subject): int
    {
        return $connection->subscribe($subject, static function (): void {})->await();
    }

    private function reconnect(SubscriptionLimitServer $server, NatsConnection $connection): void
    {
        $server->dropConnection();
        $connection->processIncoming()->await();
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $server->epoch());
    }

    private function request(NatsConnection $connection, string $operation, string $payload): string
    {
        if ($operation === 'request') {
            return $connection->request('svc.echo', $payload, 1_000)->await()->payload;
        }

        $replies = $connection->requestMany('svc.echo', $payload, null, 1, 1_000)->await();
        self::assertCount(1, $replies);

        return $replies[0]->payload;
    }

    private function expectLimitFailure(NatsConnection $connection, string $operation): void
    {
        try {
            $this->request($connection, $operation, 'first');
            self::fail('the unread limit error must drop the unconfirmed mux');
        } catch (ConnectionException $error) {
            self::assertSame(self::LIMIT_ERROR, $error->getMessage());
        }
    }

    /** @return list<string> */
    private static function muxInstalls(SubscriptionLimitServer $server, int $epoch): array
    {
        return array_values(array_map(
            static fn(array $write): string => $write['bytes'],
            array_filter($server->writes, static fn(array $write): bool => $write['epoch'] === $epoch && str_contains($write['bytes'], 'SUB _INBOX.')),
        ));
    }
}
