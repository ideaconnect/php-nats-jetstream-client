<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\Future;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\SubscriptionQueue;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

use function Amp\async;

/**
 * Targeted tests that pin behaviors broken by specific surviving Infection mutants
 * in src/Core/NatsClient.php. Each assertion group documents the mutant it kills.
 */
final class NatsClientMutationTest extends TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /**
     * The subscribeQueue() handler closure captures $subscriptionQueue by reference and is registered
     * with the connection BEFORE the queue object is constructed. If a message is dispatched to that
     * handler during the window where $subscriptionQueue is still null (between handler registration
     * and queue construction), the nullsafe `?->enqueue()` must silently no-op.
     *
     * The mutant drops the nullsafe (`$subscriptionQueue->enqueue($msg)`), which would call enqueue()
     * on null and throw an Error that propagates out of processIncoming().
     *
     * We force that exact window: start subscribeQueue() as a separate fiber (it registers the handler
     * and then suspends on its SUB write await, BEFORE assigning the queue), then drive processIncoming()
     * which dispatches a pre-seeded MSG for that sid into the still-null queue.
     */
    public function testSubscribeQueueHandlerNoOpsWhenQueueNotYetConstructed(): void
    {
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            // MSG for sid 1 (the sid subscribeQueue will obtain), delivered while the queue is null.
            "MSG updates 1 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // Start subscribeQueue() concurrently. Its body registers the subscription handler and then
        // suspends awaiting the SUB write - at that point $subscriptionQueue (captured by ref) is null.
        /** @var Future<SubscriptionQueue> $queueFuture */
        $queueFuture = $client->subscribeQueue('updates');

        // Pump the loop so the subscribeQueue fiber advances to (and parks at) its write await with the
        // handler already registered, then dispatch the pending MSG into the still-null queue.
        // kills NullSafeMethodCall @ 127 - on the mutant this call to enqueue() on null throws an Error.
        $delivered = async(static fn(): int => $client->processIncoming()->await())->await();

        // Real code: the message is dispatched to the handler, the nullsafe no-ops, one frame processed.
        self::assertSame(1, $delivered);

        // The queue resolves and, because the early message was dropped by the nullsafe guard, it is empty.
        $queue = $queueFuture->await();
        self::assertInstanceOf(SubscriptionQueue::class, $queue);
        self::assertNull($queue->fetch());
    }

    /**
     * flush() is part of the public facade API and must be callable from outside the class. The mutant
     * narrows its visibility to `protected`, which makes an external call throw an Error.
     *
     * We invoke $client->flush() directly from this (foreign) scope: on real code it writes a PING,
     * reads the seeded PONG, and resolves; on the mutant the protected-method call is a fatal Error.
     */
    public function testFlushIsPubliclyCallableAndRoundTripsPing(): void
    {
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n", // answers the connect handshake PING
            "PONG\r\n", // answers the flush() PING
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $writesBeforeFlush = count($transport->writes);

        // kills PublicVisibility @ 181 - calling a protected flush() from this scope would be an Error.
        // flush() resolves with Future<void>; the load-bearing kill is that this public call succeeds
        // (a protected method invoked from this foreign scope would be a fatal Error before await()).
        $client->flush()->await();

        // flush() must have written a PING control frame (proves the public method actually ran).
        self::assertGreaterThan($writesBeforeFlush, count($transport->writes));
        self::assertContains("PING\r\n", $transport->writes);
    }
}
