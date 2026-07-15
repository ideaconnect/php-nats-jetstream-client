<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * White-box tests for the #118 muxed request inbox correctness invariants: token routing, uniqueness,
 * the no-late-leak discard, the foreign-base guard, the slow-consumer exemption, and terminal reset.
 * These drive the private mux primitives directly (reflection), so they pin the invariants regardless
 * of the request()/requestMany() wrappers.
 */
final class MuxRequestInboxInternalsTest extends TestCase
{
    private function connection(?NatsOptions $options = null): NatsConnection
    {
        return new NatsConnection($options ?? new NatsOptions(), new FakeTransport());
    }

    private function message(string $subject): NatsMessage
    {
        return new NatsMessage(subject: $subject, sid: 1, replyTo: null, payload: 'x');
    }

    /** dispatchMuxReply routes a reply to ONLY the waiter whose token matches the reply subject. */
    public function testDispatchMuxReplyRoutesReplyToItsTokenWaiter(): void
    {
        $connection = $this->connection();
        $this->setPrivate($connection, 'muxBase', '_INBOX.base');

        $aCalls = 0;
        $bCalls = 0;
        $this->invokePrivate($connection, 'registerMuxWaiter', 'a', function (NatsMessage $m) use (&$aCalls): void { $aCalls++; });
        $this->invokePrivate($connection, 'registerMuxWaiter', 'b', function (NatsMessage $m) use (&$bCalls): void { $bCalls++; });

        $this->invokePrivate($connection, 'dispatchMuxReply', $this->message('_INBOX.base.a'));

        self::assertSame(1, $aCalls);
        self::assertSame(0, $bCalls);
    }

    /** A reply whose token has no live waiter (removed / never registered) is discarded, not misrouted. */
    public function testDispatchMuxReplyDropsReplyForRemovedOrUnknownToken(): void
    {
        $connection = $this->connection();
        $this->setPrivate($connection, 'muxBase', '_INBOX.base');

        $calls = 0;
        $this->invokePrivate($connection, 'registerMuxWaiter', 'a', function (NatsMessage $m) use (&$calls): void { $calls++; });
        $this->invokePrivate($connection, 'removeMuxWaiter', 'a');

        // Late/duplicate reply for the now-removed token 'a', and a reply for a never-registered token.
        $this->invokePrivate($connection, 'dispatchMuxReply', $this->message('_INBOX.base.a'));
        $this->invokePrivate($connection, 'dispatchMuxReply', $this->message('_INBOX.base.zzz'));

        self::assertSame(0, $calls);
        self::assertSame([], $this->getPrivate($connection, 'muxWaiters'));
    }

    /** A reply addressed to a DIFFERENT base (wrong prefix) never reaches a waiter (the strncmp guard). */
    public function testDispatchMuxReplyRejectsForeignBaseSubject(): void
    {
        $connection = $this->connection();
        $this->setPrivate($connection, 'muxBase', '_INBOX.mine');

        $calls = 0;
        $this->invokePrivate($connection, 'registerMuxWaiter', 'a', function (NatsMessage $m) use (&$calls): void { $calls++; });

        // Foreign base of the SAME length as muxBase, so the token-strip (strlen(base)+1) lines up on the
        // 'a' suffix: WITHOUT the prefix guard the reply would be mis-delivered to waiter 'a'; WITH it the
        // strncmp mismatch drops it. Asserting 0 calls thus pins the guard (and kills its return removal).
        $this->invokePrivate($connection, 'dispatchMuxReply', $this->message('_INBOX.evil.a'));

        self::assertSame(0, $calls);
    }

    /** newMuxToken() yields pairwise-distinct tokens within an epoch (uniqueness by construction). */
    public function testNewMuxTokenProducesDistinctTokens(): void
    {
        $connection = $this->connection();

        $tokens = [];
        for ($i = 0; $i < 5000; $i++) {
            $tokens[] = $this->invokePrivate($connection, 'newMuxToken');
        }

        self::assertCount(5000, array_unique($tokens));
    }

    /** The mux sid is exempt from the slow-consumer count bound: it never drops, even far past the limit. */
    public function testUnboundedSidBypassesSlowConsumerDrop(): void
    {
        $connection = $this->connection(new NatsOptions(maxPendingMessagesPerSubscription: 2, slowConsumerPolicy: SlowConsumerPolicy::DropOldest));

        $sid = 1;
        $this->setPrivate($connection, 'pendingMessages', [$sid => new \SplQueue()]);
        $this->setPrivate($connection, 'unboundedSids', [$sid => true]);

        for ($i = 0; $i < 5; $i++) {
            $this->invokePrivate($connection, 'enqueueMessage', $sid, $this->message('_INBOX.base.a'));
        }

        /** @var array<int, \SplQueue<NatsMessage>> $pending */
        $pending = $this->getPrivate($connection, 'pendingMessages');
        // All 5 retained despite the limit of 2 - nothing dropped.
        self::assertSame(5, $pending[$sid]->count());
    }

    /** A NON-exempt sid keeps the bound: Error policy throws once the pending queue is full. */
    public function testNonExemptSidStillDropsUnderSlowConsumerPolicy(): void
    {
        $connection = $this->connection(new NatsOptions(maxPendingMessagesPerSubscription: 2, slowConsumerPolicy: SlowConsumerPolicy::Error));

        $sid = 1;
        $queue = new \SplQueue();
        $queue->enqueue($this->message('a'));
        $queue->enqueue($this->message('b'));
        $this->setPrivate($connection, 'pendingMessages', [$sid => $queue]);
        // Not in unboundedSids -> the bound applies.

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Subscription queue overflow for sid ' . $sid);
        $this->invokePrivate($connection, 'enqueueMessage', $sid, $this->message('c'));
    }

    /** A terminal close resets all mux state so the next connect() starts a fresh epoch. */
    public function testReleaseRuntimeStateClearsMuxState(): void
    {
        $connection = $this->connection();
        $this->setPrivate($connection, 'muxBase', '_INBOX.old');
        $this->setPrivate($connection, 'muxSid', 7);
        $this->setPrivate($connection, 'muxWaiters', ['a' => static function (): void {}]);
        $this->setPrivate($connection, 'unboundedSids', [7 => true]);

        $this->invokePrivate($connection, 'releaseRuntimeState');

        self::assertNull($this->getPrivate($connection, 'muxBase'));
        self::assertNull($this->getPrivate($connection, 'muxSid'));
        self::assertSame([], $this->getPrivate($connection, 'muxWaiters'));
        self::assertSame([], $this->getPrivate($connection, 'unboundedSids'));
    }

    /** dropSubscriptionState clears the unbounded flag so it never outlives its sid. */
    public function testDropSubscriptionStateClearsUnboundedFlag(): void
    {
        $connection = $this->connection();
        $this->setPrivate($connection, 'unboundedSids', [5 => true]);

        $this->invokePrivate($connection, 'dropSubscriptionState', 5);

        self::assertSame([], $this->getPrivate($connection, 'unboundedSids'));
    }

    private function invokePrivate(object $object, string $method, mixed ...$args): mixed
    {
        $ref = new \ReflectionMethod($object, $method);

        return $ref->invoke($object, ...$args);
    }

    private function setPrivate(object $object, string $property, mixed $value): void
    {
        $ref = new \ReflectionProperty($object, $property);
        $ref->setValue($object, $value);
    }

    private function getPrivate(object $object, string $property): mixed
    {
        $ref = new \ReflectionProperty($object, $property);

        return $ref->getValue($object);
    }
}
