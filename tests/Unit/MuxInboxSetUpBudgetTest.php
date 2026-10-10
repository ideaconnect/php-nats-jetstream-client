<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use IDCT\NATS\Tests\Support\SubscriptionLimitServer;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\delay;

/**
 * The set-up of the shared reply inbox within the budget of the request that sets it up (#194, #213). The request that
 * wrote the inbox's SUB used to do so with no budget at all: a SUB held up by backpressure held it past its own timeout
 * and its caller's cancellation, and it could not run while a drain() delivered. It now subscribes within its budget,
 * through the bounded writer of the guarded inboxes. That made a second rule necessary: the set-up is shared, and a
 * request that joined it must not fail because the request that made it stopped waiting. Such a set-up is released -
 * its SUB, once out, followed by an UNSUB - and the request that joined it sets the inbox up itself, within its own
 * budget; what fails every request alike, a rejection or a closed connection, still fails them all.
 *
 * Over tests/Support/SubscriptionLimitServer.php, whose writes reach it in the order they were made, as bytes on a
 * socket do, and which can hold a write up as backpressure would.
 */
final class MuxInboxSetUpBudgetTest extends TestCase
{
    use OwnsTestResources;

    private const LIMIT_ERROR = "Server sent error frame: 'maximum subscriptions exceeded'";

    /** @var list<NatsConnection> */
    private array $opened = [];

    protected function tearDown(): void
    {
        // #183: what the test registered is closed and checked first.
        $this->releaseOwnedResources();

        foreach ($this->opened as $connection) {
            try {
                $connection->disconnect()->await(new TimeoutCancellation(1));
            } catch (\Throwable) {
                // Already closed.
            }
        }

        $this->opened = [];
    }

    /** @return iterable<string, array{bool}> */
    public static function ownersThatStopWaiting(): iterable
    {
        yield 'its own timeout' => [false];
        yield 'its caller\'s cancellation' => [true];
    }

    /**
     * A request whose set-up of the reply inbox outlasts its own budget fails with its own outcome, at its deadline, and
     * leaves the inbox to a request that joined its set-up: the first SUB is held up, the request that wrote it gives up
     * - its timeout of 150 ms, or its caller's cancellation - and the request that joined it, with a budget of three
     * seconds, subscribes the inbox again and is answered once the held SUB is let through. The SUB given up on is
     * followed by an UNSUB, so the server ends up holding the one inbox in use, and the request that gave up is never
     * sent. On 2.24.3 that request waited for its SUB, whatever its timeout.
     */
    #[DataProvider('ownersThatStopWaiting')]
    public function testARequestThatStopsWaitingForItsSetUpLeavesTheInboxToTheRequestsThatJoinedIt(bool $cancelled): void
    {
        $server = $this->echoServer();
        $connection = $this->connect($server);
        $server->stallNextWriteContaining('SUB _INBOX.', 30.0);
        $cancellation = new DeferredCancellation();
        $owner = $connection->request('svc.echo', 'owner', $cancelled ? 10_000 : 150, $cancelled ? $cancellation->getCancellation() : null);
        $this->waitUntil(static fn(): bool => $server->writesStalled() === 1);
        $joiner = $connection->request('svc.echo', 'joiner', 3_000);
        delay(0.01);
        $start = hrtime(true);
        if ($cancelled) {
            $cancellation->cancel();
        }

        try {
            $owner->await(new TimeoutCancellation(1));
            self::fail('expected the request to stop waiting for its set-up');
        } catch (TimeoutException $e) {
            self::assertFalse($cancelled, 'a cancelled request reports its cancellation, not a timeout');
            self::assertSame('Request timed out for subject svc.echo while waiting for the reply inbox to be set up', $e->getMessage());
        } catch (CancelledException) {
            self::assertTrue($cancelled, 'a request that timed out reports its timeout, not a cancellation');
        }
        self::assertLessThan(0.5, self::secondsSince($start), 'it did not wait for the held SUB');
        self::assertFalse($joiner->isComplete(), 'the request that joined it waits on');

        $server->releaseStalledWrites();

        self::assertSame('echo:joiner', $joiner->await(new TimeoutCancellation(2))->payload);
        $installs = self::muxInstallWrites($server);
        self::assertCount(2, $installs, 'the joiner subscribed the inbox again');
        $this->waitUntil(static fn(): bool => count($server->controlLines('UNSUB ' . self::muxSidOf($installs[0]))) === 1);
        self::assertCount(1, array_filter($server->heldSubscriptions(), static fn(string $subject): bool => str_starts_with($subject, '_INBOX.')), 'the server holds the inbox in use alone');
        self::assertSame(['PUB svc.echo'], array_map(static fn(string $line): string => implode(' ', array_slice(explode(' ', $line), 0, 2)), $server->controlLines('PUB svc.echo')));
    }

    /**
     * Guard, passes on both: a request that joined another's set-up and is cancelled leaves the set-up to the request
     * that makes it, which is answered once its held SUB is let through; one inbox is subscribed.
     */
    public function testACancelledRequestThatJoinedASetUpLeavesItToTheRequestThatMakesIt(): void
    {
        $server = $this->echoServer();
        $connection = $this->connect($server);
        $server->stallNextWriteContaining('SUB _INBOX.', 30.0);
        $owner = $connection->request('svc.echo', 'owner', 3_000);
        $this->waitUntil(static fn(): bool => $server->writesStalled() === 1);
        $cancellation = new DeferredCancellation();
        $joiner = $connection->request('svc.echo', 'joiner', 3_000, $cancellation->getCancellation());
        delay(0.01);
        $cancellation->cancel();

        try {
            $joiner->await(new TimeoutCancellation(1));
            self::fail('expected the joiner to be cancelled');
        } catch (CancelledException) {
            // Its caller's cancellation.
        }
        $server->releaseStalledWrites();

        self::assertSame('echo:owner', $owner->await(new TimeoutCancellation(2))->payload);
        self::assertCount(1, self::muxInstallWrites($server));
    }

    /**
     * A set-up whose request is cancelled before its writer has run writes nothing - no SUB, no PING, no pong slot - and
     * leaves the UNSUB it owed for an inbox dropped before to the next set-up: the server rejects the first inbox at its
     * limit of one subscription, the slot is freed, and the next request is cancelled one event-loop hop after it was
     * made, as its writer is about to run. A request made after that subscribes the inbox in a write that first
     * unsubscribes the dropped one, and is answered. On 2.24.3 the set-up wrote its SUB at once, in the request's own
     * fiber, whatever the cancellation.
     */
    public function testASetUpCancelledBeforeItsWriterRunsWritesNothingAndLeavesItsOwedReleaseToTheNextSetUp(): void
    {
        $server = $this->echoServer(limit: 1);
        $connection = $this->connect($server);
        $appSid = $connection->subscribe('app.updates', static function (): void {})->await();
        try {
            $connection->request('svc.echo', 'one', 1_000)->await();
            self::fail('expected the request to fail: the server rejected its reply inbox');
        } catch (ConnectionException $e) {
            self::assertSame(self::LIMIT_ERROR, $e->getMessage());
        }
        $connection->unsubscribe($appSid)->await();
        $dropped = self::muxSidOf(self::muxInstallWrites($server)[0]);
        $pings = count($server->controlLines('PING'));

        $cancellation = new DeferredCancellation();
        $cancelled = $connection->request('svc.echo', 'two', 3_000, $cancellation->getCancellation());
        EventLoop::queue($cancellation->cancel(...));
        try {
            $cancelled->await(new TimeoutCancellation(1));
            self::fail('expected the request to be cancelled');
        } catch (CancelledException) {
            // Its caller's cancellation, before its writer ran.
        }
        delay(0.01);
        self::assertCount(1, self::muxInstallWrites($server), 'the cancelled set-up wrote no SUB');
        self::assertCount($pings, $server->controlLines('PING'), 'nor a PING');

        self::assertSame('echo:three', $connection->request('svc.echo', 'three', 1_000)->await()->payload);
        $installs = self::muxInstallWrites($server);
        self::assertCount(2, $installs);
        self::assertStringStartsWith('UNSUB ' . $dropped . "\r\nSUB _INBOX.", $installs[1], 'the owed UNSUB went out with the next set-up');
    }

    /** A server that echoes every request on its reply subject - when the server holds a subscription for it. */
    private function echoServer(int $limit = PHP_INT_MAX): SubscriptionLimitServer
    {
        $server = new SubscriptionLimitServer();
        $server->limit = $limit;
        $server->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $replyTo === null
            ? []
            : $server->replyFrame($replyTo, 'echo:' . $payload);

        return $server;
    }

    private function connect(SubscriptionLimitServer $server): NatsConnection
    {
        $connection = $this->own(new NatsConnection(new NatsOptions(connectTimeoutMs: 500, reconnectEnabled: false, pingIntervalSeconds: 0), $server));
        $this->opened[] = $connection;
        $connection->connect()->await();

        return $connection;
    }

    /** @param \Closure(): bool $condition */
    private function waitUntil(\Closure $condition): void
    {
        $deadline = hrtime(true) + 2_000_000_000;
        while (!$condition()) {
            if (hrtime(true) > $deadline) {
                self::fail('condition not reached within 2 s');
            }

            delay(0.001);
        }
    }

    /**
     * The writes that subscribed a reply inbox: each carries one wildcard SUB under the inbox prefix.
     *
     * @return list<string>
     */
    private static function muxInstallWrites(SubscriptionLimitServer $server): array
    {
        return array_values(array_map(
            static fn(array $write): string => $write['bytes'],
            array_filter($server->writes, static fn(array $write): bool => preg_match('/^SUB _INBOX\.\S+\.\* \d+\r$/m', $write['bytes']) === 1),
        ));
    }

    private static function muxSidOf(string $install): int
    {
        if (preg_match('/^SUB _INBOX\.\S+\.\* (\d+)\r$/m', $install, $match) !== 1) {
            self::fail('not a write that subscribed a reply inbox: ' . $install);
        }

        return (int) $match[1];
    }

    private static function secondsSince(int $start): float
    {
        return (hrtime(true) - $start) / 1e9;
    }
}
