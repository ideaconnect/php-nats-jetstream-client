<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * Behavior guards for the #162 per-chunk drain-scan fix. drainAllPending() now iterates a dirty set
 * of sids-with-backlog instead of scanning every live subscription, so these pin that the observable
 * delivery behavior (which handler fires, and in what order) is UNCHANGED, and that the dirty set is
 * maintained as claimed (only active sids, entry removed once a queue empties, empty queue retained
 * per #139). The delivery/ordering cases (1, 2) hold identically under the old full-map scan; the
 * dirty-set mechanics (3, 4) verify the new bookkeeping.
 */
final class DrainScanPendingDirtyTest extends TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /**
     * A message for ONE of many otherwise-idle subscriptions is still delivered - the dirty set must
     * route the active sid even though it is a tiny minority of the live subscriptions being scanned.
     */
    public function testMessageToOneOfManyIdleSubscriptionsIsDelivered(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $seen = [];
        $sids = [];
        for ($i = 0; $i < 50; $i++) {
            $subject = 'idle.' . $i;
            $sids[$subject] = $connection->subscribe(
                $subject,
                static function (NatsMessage $message) use (&$seen, $subject): void {
                    $seen[] = $subject . ':' . $message->payload;
                },
            )->await();
        }

        // Deliver to a single mid-list subscription only.
        $targetSubject = 'idle.37';
        $targetSid = $sids[$targetSubject];
        $transport->pushReadChunk("MSG {$targetSubject} {$targetSid} 5\r\nhello\r\n");
        $connection->processIncoming()->await();

        self::assertSame(['idle.37:hello'], $seen);
    }

    /**
     * Cross-sid delivery order is preserved as the registration (ascending-sid) order of the old
     * array_keys(pendingMessages) scan, and per-sid FIFO holds - even when the WIRE order within a
     * single chunk is neither. All frames arrive in one chunk so they are enqueued before the single
     * post-chunk drain; the expected sequence groups by ascending sid, FIFO within each sid.
     */
    public function testCrossSidDeliveryOrderMatchesRegistrationOrderNotWireOrder(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = new NatsConnection(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        $order = [];
        $sidA = $connection->subscribe('a', static function (NatsMessage $m) use (&$order): void {
            $order[] = 'a:' . $m->payload;
        })->await();
        $sidB = $connection->subscribe('b', static function (NatsMessage $m) use (&$order): void {
            $order[] = 'b:' . $m->payload;
        })->await();
        $sidC = $connection->subscribe('c', static function (NatsMessage $m) use (&$order): void {
            $order[] = 'c:' . $m->payload;
        })->await();

        // One chunk, wire order deliberately NOT ascending: C1, A1, B1, C2, A2.
        $chunk = "MSG c {$sidC} 2\r\nc1\r\n"
            . "MSG a {$sidA} 2\r\na1\r\n"
            . "MSG b {$sidB} 2\r\nb1\r\n"
            . "MSG c {$sidC} 2\r\nc2\r\n"
            . "MSG a {$sidA} 2\r\na2\r\n";
        $transport->pushReadChunk($chunk);
        $connection->processIncoming()->await();

        // Grouped by ascending sid (a, b, c), FIFO within each - not the wire order c1,a1,b1,c2,a2.
        self::assertSame(['a:a1', 'a:a2', 'b:b1', 'c:c1', 'c:c2'], $order);
    }

    /**
     * The dirty set tracks only the sid(s) with a queued message - not every live subscription - and
     * the entry is removed once the queue drains empty, while the empty SplQueue is retained for reuse
     * (#139). This is the core of the #162 fix: a drained sid is not re-scanned on the next chunk.
     */
    public function testDirtySetTracksOnlyActiveSidAndClearsWhenQueueEmpties(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $delivered = [];
        $handler = static function (NatsMessage $message) use (&$delivered): void {
            $delivered[] = $message->payload;
        };
        // Three idle subscriptions, each with a retained (empty) queue as subscribe() leaves them.
        foreach ([1, 2, 3] as $sid) {
            $this->setPrivate($connection, 'subscriptions', $this->withKey(
                $this->getPrivate($connection, 'subscriptions'),
                $sid,
                $handler,
            ));
            $this->setPrivate($connection, 'pendingMessages', $this->withKey(
                $this->getPrivate($connection, 'pendingMessages'),
                $sid,
                new \SplQueue(),
            ));
        }

        // Enqueue a message for sid 2 only, through the real intake path.
        $this->invokePrivate($connection, 'enqueueMessage', 2, new NatsMessage('two', 2, null, 'm'));

        // Dirty set holds ONLY sid 2 - not all three subscriptions.
        self::assertSame([2 => true], $this->getPrivate($connection, 'pendingDirty'));

        $this->invokePrivate($connection, 'drainAllPending');

        self::assertSame(['m'], $delivered);
        // Entry removed once the queue emptied, so it is not re-scanned next chunk (#162)...
        self::assertSame([], $this->getPrivate($connection, 'pendingDirty'));
        // ...but the empty queue object is retained for reuse (#139), still keyed by every sid.
        $pending = $this->getPrivate($connection, 'pendingMessages');
        self::assertSame([1, 2, 3], array_keys($pending));
        self::assertTrue($pending[2]->isEmpty());
    }

    /**
     * hasUndeliveredDrainBacklog() stays consistent with the dirty set: true while a sid has a queued
     * message, false once it drains - the invariant drain()'s roundup loop (#149/#150) depends on.
     */
    public function testHasUndeliveredDrainBacklogTracksDirtySet(): void
    {
        $connection = new NatsConnection(new NatsOptions(), new FakeTransport());

        $this->setPrivate($connection, 'subscriptions', [
            7 => static function (NatsMessage $message): void {},
        ]);
        $this->setPrivate($connection, 'pendingMessages', [7 => new \SplQueue()]);

        self::assertFalse($this->invokePrivate($connection, 'hasUndeliveredDrainBacklog'));

        $this->invokePrivate($connection, 'enqueueMessage', 7, new NatsMessage('s', 7, null, 'x'));
        self::assertTrue($this->invokePrivate($connection, 'hasUndeliveredDrainBacklog'));

        $this->invokePrivate($connection, 'drainAllPending');
        self::assertFalse($this->invokePrivate($connection, 'hasUndeliveredDrainBacklog'));
    }

    /**
     * @param array<int, mixed> $array
     * @return array<int, mixed>
     */
    private function withKey(array $array, int $key, mixed $value): array
    {
        $array[$key] = $value;

        return $array;
    }

    private function invokePrivate(object $object, string $method, mixed ...$args): mixed
    {
        return (new \ReflectionMethod($object, $method))->invoke($object, ...$args);
    }

    private function setPrivate(object $object, string $property, mixed $value): void
    {
        (new \ReflectionProperty($object, $property))->setValue($object, $value);
    }

    private function getPrivate(object $object, string $property): mixed
    {
        return (new \ReflectionProperty($object, $property))->getValue($object);
    }
}
