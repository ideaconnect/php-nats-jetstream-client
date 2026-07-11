<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Core\SubscriptionQueue;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Transport\TransportInterface;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\delay;

final class SubscriptionQueueTest extends TestCase
{
    private function makeConnectedClient(FakeTransport $transport): NatsClient
    {
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        return $client;
    }

    /** @return list<string> */
    private function infoAndPong(): array
    {
        return [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
    }

    public function testSubscribeQueueReturnsSidAndFetchesMessage(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG events 1 5\r\nhello\r\n",
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('events')->await();

        self::assertInstanceOf(SubscriptionQueue::class, $queue);
        self::assertSame(1, $queue->sid);

        $msg = $queue->fetch();
        self::assertNotNull($msg);
        self::assertSame('hello', $msg->payload);
        self::assertSame('events', $msg->subject);
    }

    /**
     * A message dispatched for the new sid while subscribe()'s SUB write is still suspended (the
     * handler is registered before the write, so the sid is already routable) must reach the
     * returned queue instead of being silently dropped in the pre-construction window (#129).
     */
    public function testMessageDeliveredDuringSubscribeAwaitReachesTheQueue(): void
    {
        $info = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";
        $release = new DeferredFuture();

        $transport = new class ($info, $release) implements TransportInterface {
            /** @var list<string> */
            public array $writes = [];
            /** @var list<string> */
            private array $reads;

            /** @param DeferredFuture<void> $release */
            public function __construct(string $info, private DeferredFuture $release)
            {
                $this->reads = [$info, "PONG\r\n", "MSG updates 1 5\r\nearly\r\n"];
            }

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(static fn(): null => null);
            }

            public function upgradeTls(): Future
            {
                return async(static fn(): null => null);
            }

            public function write(string $bytes): Future
            {
                return async(function () use ($bytes): void {
                    $this->writes[] = $bytes;
                    // Hold the SUB write open: the handler is registered before the write, so a
                    // concurrent read can deliver for the new sid while subscribeQueue() is still
                    // suspended here and its SubscriptionQueue object does not exist yet.
                    if (str_starts_with($bytes, 'SUB ')) {
                        $this->release->getFuture()->await();
                    }
                });
            }

            public function readLine(?Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    return array_shift($this->reads) ?? '';
                });
            }

            public function close(): Future
            {
                return async(static fn(): null => null);
            }
        };

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $queueFuture = $client->subscribeQueue('updates');
        delay(0.01); // subscribeQueue() is now suspended on the held SUB write

        // A concurrent read dispatches the message for sid 1; the queue object does not exist yet.
        $client->processIncoming()->await();

        $release->complete();
        $queue = $queueFuture->await();

        $message = $queue->fetch();
        self::assertNotNull($message, 'a delivery during the construction window must not be dropped');
        self::assertSame('early', $message->payload);
    }

    public function testFetchReturnsNullWhenNoMessages(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('events')->await();

        $msg = $queue->fetch();
        self::assertNull($msg);
    }

    public function testNextReturnsBufferedMessageImmediately(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG data 1 3\r\nabc\r\n",
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('data')->await();

        // Pre-load the message into the internal buffer via processIncoming.
        $client->processIncoming()->await();

        $msg = $queue->next();
        self::assertNotNull($msg);
        self::assertSame('abc', $msg->payload);
    }

    public function testNextReturnsNullOnTimeout(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('empty')->await();
        $queue->setTimeout(0.01);

        $msg = $queue->next();
        self::assertNull($msg);
    }

    public function testNextWithoutTimeoutRunsSingleCycleAndReturnsMessage(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG data 1 3\r\nxyz\r\n",
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('data')->await();

        // No timeout configured (default 0): a single processIncoming cycle should surface the message.
        $msg = $queue->next();
        self::assertNotNull($msg);
        self::assertSame('xyz', $msg->payload);
    }

    public function testNextWithoutTimeoutReturnsNullWhenEmpty(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('empty')->await();

        // Default timeout 0 must NOT block indefinitely; a single empty cycle returns null.
        $msg = $queue->next();
        self::assertNull($msg);
    }

    public function testFetchAllCollectsMultipleMessages(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG items 1 1\r\na\r\n",
            "MSG items 1 1\r\nb\r\n",
            "MSG items 1 1\r\nc\r\n",
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('items')->await();
        $queue->setTimeout(0.1);

        $messages = $queue->fetchAll();
        self::assertCount(3, $messages);
        self::assertSame('a', $messages[0]->payload);
        self::assertSame('b', $messages[1]->payload);
        self::assertSame('c', $messages[2]->payload);
    }

    public function testFetchAllRespectsLimit(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG items 1 1\r\na\r\n",
            "MSG items 1 1\r\nb\r\n",
            "MSG items 1 1\r\nc\r\n",
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('items')->await();
        $queue->setTimeout(0.1);

        $messages = $queue->fetchAll(2);
        self::assertCount(2, $messages);
        self::assertSame('a', $messages[0]->payload);
        self::assertSame('b', $messages[1]->payload);
    }

    public function testSubscribeQueueWithQueueGroup(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG work 1 4\r\njob1\r\n",
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('work', 'workers')->await();

        // Verify the SUB command includes the queue group.
        $allWrites = implode('||', $transport->writes);
        self::assertStringContainsString('SUB work workers 1', $allWrites);

        $msg = $queue->fetch();
        self::assertNotNull($msg);
        self::assertSame('job1', $msg->payload);
    }

    public function testSetTimeoutReturnsSelf(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('x')->await();

        $result = $queue->setTimeout(5.0);
        self::assertSame($queue, $result);
    }

    public function testFetchReturnsAlreadyBufferedMessage(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG data 1 2\r\nhi\r\n",
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('data')->await();

        // Buffer the message first so fetch() returns it directly without another read.
        $client->processIncoming()->await();

        $msg = $queue->fetch();
        self::assertNotNull($msg);
        self::assertSame('hi', $msg->payload);
    }

    public function testNextWithTimeoutReturnsMessageArrivingDuringWait(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG data 1 3\r\nabc\r\n",
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('data')->await();
        $queue->setTimeout(0.2);

        // Not pre-buffered: next() enters the bounded wait loop and breaks as soon as the message
        // is read, rather than running to the timeout.
        $msg = $queue->next();
        self::assertNotNull($msg);
        self::assertSame('abc', $msg->payload);
    }

    public function testFetchAllWithoutTimeoutCollectsBufferedMessages(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG items 1 1\r\na\r\n",
            "MSG items 1 1\r\nb\r\n",
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('items')->await();

        // No timeout configured exercises the unbounded (null cancellation) collection path.
        $messages = $queue->fetchAll();
        self::assertCount(2, $messages);
        self::assertSame('a', $messages[0]->payload);
        self::assertSame('b', $messages[1]->payload);
    }

    public function testFetchAllReturnsEarlyWhenBufferedMeetsLimit(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG items 1 1\r\na\r\n",
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('items')->await();

        // Pre-buffer one message, then request a limit of one: the buffered drain alone satisfies it.
        $client->processIncoming()->await();

        $messages = $queue->fetchAll(1);
        self::assertCount(1, $messages);
        self::assertSame('a', $messages[0]->payload);
    }

    public function testFetchDoesNotBlockOnIdleSubject(): void
    {
        // blockWhenEmpty mirrors a live but idle socket: readLine() suspends until cancelled. The
        // outer 2s bound fails the test if fetch() ever parks the caller (it must return ~instantly).
        $transport = new FakeTransport($this->infoAndPong(), blockWhenEmpty: true);
        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('idle.subject')->await();

        $result = \Amp\Future\await([async(static fn(): ?\IDCT\NATS\Core\NatsMessage => $queue->fetch())], new TimeoutCancellation(2.0))[0];

        self::assertNull($result);
        self::assertTrue($transport->lastReadHadCancellation, 'fetch() must bound the read with a cancellation');
    }

    public function testNextWithDefaultTimeoutDoesNotBlockOnIdleSubject(): void
    {
        $transport = new FakeTransport($this->infoAndPong(), blockWhenEmpty: true);
        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('idle.subject')->await();

        // Default timeout is 0 (the <= 0 branch); it must not block the calling fiber.
        $result = \Amp\Future\await([async(static fn(): ?\IDCT\NATS\Core\NatsMessage => $queue->next())], new TimeoutCancellation(2.0))[0];

        self::assertNull($result);
    }

    public function testNextWithNegativeTimeoutDoesNotBlockOnIdleSubject(): void
    {
        $transport = new FakeTransport($this->infoAndPong(), blockWhenEmpty: true);
        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('idle.subject')->await();
        $queue->setTimeout(-1.0);

        $result = \Amp\Future\await([async(static fn(): ?\IDCT\NATS\Core\NatsMessage => $queue->next())], new TimeoutCancellation(2.0))[0];

        self::assertNull($result);
    }

    public function testFetchAllWithDefaultTimeoutDoesNotBlockOnIdleSubject(): void
    {
        $transport = new FakeTransport($this->infoAndPong(), blockWhenEmpty: true);
        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('idle.subject')->await();

        // No setTimeout(): fetchAll() must still bound its read and return [] rather than parking.
        $result = \Amp\Future\await([async(static fn(): array => $queue->fetchAll())], new TimeoutCancellation(2.0))[0];

        self::assertSame([], $result);
    }

    public function testFetchAllDoesNotBailOnTransientEmptyReadWithinTimeout(): void
    {
        // A transient 0-frame read (e.g. the heartbeat self-read briefly owning the socket) between
        // two deliveries must NOT end fetchAll early while a timeout window remains.
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG events 1 4\r\nmsg1\r\n",
            '', // empty read -> processIncoming returns 0 frames mid-window
            "MSG events 1 4\r\nmsg2\r\n",
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('events')->await();
        $queue->setTimeout(0.5);

        $messages = $queue->fetchAll(2);

        self::assertCount(2, $messages);
        self::assertSame('msg1', $messages[0]->payload);
        self::assertSame('msg2', $messages[1]->payload);
    }

    public function testEnqueueBoundsBacklogWithDropOldest(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = $this->makeConnectedClient($transport);
        $queue = new SubscriptionQueue($client, 99, 2, SlowConsumerPolicy::DropOldest);

        $queue->enqueue(new NatsMessage('events', 99, null, 'a'));
        $queue->enqueue(new NatsMessage('events', 99, null, 'b'));
        $queue->enqueue(new NatsMessage('events', 99, null, 'c')); // over cap (2) -> drop oldest ('a')

        $messages = $queue->fetchAll();

        self::assertCount(2, $messages);
        self::assertSame('b', $messages[0]->payload);
        self::assertSame('c', $messages[1]->payload);
    }

    public function testEnqueueDropsNewestWhenPolicyIsDropNewest(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = $this->makeConnectedClient($transport);
        $queue = new SubscriptionQueue($client, 99, 2, SlowConsumerPolicy::DropNewest);

        $queue->enqueue(new NatsMessage('events', 99, null, 'a'));
        $queue->enqueue(new NatsMessage('events', 99, null, 'b'));
        $queue->enqueue(new NatsMessage('events', 99, null, 'c')); // over cap -> drop the newest ('c')

        $messages = $queue->fetchAll();

        self::assertCount(2, $messages);
        self::assertSame('a', $messages[0]->payload);
        self::assertSame('b', $messages[1]->payload);
    }

    /**
     * A DropOldest overflow must not be silent (#134): droppedCount() increments monotonically and
     * the client's errorListener receives a slow-consumer error, mirroring the connection layer.
     */
    public function testEnqueueDropOldestIncrementsDroppedCountAndNotifiesErrorListener(): void
    {
        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        });
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient($options, $transport);
        $client->connect()->await();
        $queue = new SubscriptionQueue($client, 99, 2, SlowConsumerPolicy::DropOldest);

        $queue->enqueue(new NatsMessage('events', 99, null, 'a'));
        $queue->enqueue(new NatsMessage('events', 99, null, 'b'));
        self::assertSame(0, $queue->droppedCount(), 'no drop below the cap');

        $queue->enqueue(new NatsMessage('events', 99, null, 'c')); // over cap (2) -> drops oldest ('a')
        self::assertSame(1, $queue->droppedCount());

        $queue->enqueue(new NatsMessage('events', 99, null, 'd')); // drops 'b' -> counter is monotonic
        self::assertSame(2, $queue->droppedCount());

        self::assertCount(2, $errors);
        self::assertStringContainsString('Slow consumer on sid 99', $errors[0]->getMessage());
        self::assertStringContainsString('dropped oldest message', $errors[0]->getMessage());
    }

    /**
     * A DropNewest overflow must not be silent (#134): the discarded (never-enqueued) message still
     * increments droppedCount() and reaches the client's errorListener.
     */
    public function testEnqueueDropNewestIncrementsDroppedCountAndNotifiesErrorListener(): void
    {
        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        });
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient($options, $transport);
        $client->connect()->await();
        $queue = new SubscriptionQueue($client, 99, 2, SlowConsumerPolicy::DropNewest);

        $queue->enqueue(new NatsMessage('events', 99, null, 'a'));
        $queue->enqueue(new NatsMessage('events', 99, null, 'b'));
        self::assertSame(0, $queue->droppedCount(), 'no drop below the cap');

        $queue->enqueue(new NatsMessage('events', 99, null, 'c')); // over cap (2) -> 'c' is discarded
        self::assertSame(1, $queue->droppedCount());

        self::assertCount(1, $errors);
        self::assertStringContainsString('Slow consumer on sid 99', $errors[0]->getMessage());
        self::assertStringContainsString('dropped newest message', $errors[0]->getMessage());
    }

    public function testUnsubscribeSendsUnsubForOwnSid(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('events')->await();

        $queue->unsubscribe()->await();

        self::assertStringContainsString("UNSUB {$queue->sid}\r\n", implode('', $transport->writes));
    }

    /**
     * Line 66: SlowConsumerPolicy::Error throws NatsException when the queue is full.
     */
    public function testEnqueueThrowsOnOverflowWhenPolicyIsError(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = $this->makeConnectedClient($transport);
        $queue = new SubscriptionQueue($client, 99, 2, SlowConsumerPolicy::Error);

        $queue->enqueue(new NatsMessage('events', 99, null, 'a'));
        $queue->enqueue(new NatsMessage('events', 99, null, 'b'));

        $this->expectException(\IDCT\NATS\Exception\NatsException::class);
        $this->expectExceptionMessage('Subscription queue overflow for sid 99');

        // Third enqueue exceeds the cap of 2 and must throw with the Error policy.
        $queue->enqueue(new NatsMessage('events', 99, null, 'c'));
    }

    /**
     * Line 92: close() is an alias of unsubscribe() and sends UNSUB for the queue's own sid.
     */
    public function testCloseSendsUnsubForOwnSid(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('events')->await();

        $queue->close()->await();

        self::assertStringContainsString("UNSUB {$queue->sid}\r\n", implode('', $transport->writes));
    }

    /**
     * Line 169: next() with a positive timeout times out (CancelledException caught) and returns null.
     *
     * The blocking transport means processIncoming() suspends the fiber until the TimeoutCancellation
     * fires, which exercises the catch(CancelledException) clause on line 169.
     */
    public function testNextWithTimeoutReturnsNullWhenNoMessageArrivesBeforeDeadline(): void
    {
        // blockWhenEmpty suspends processIncoming() so the TimeoutCancellation fires inside it,
        // propagates as CancelledException caught at line 169, and next() returns null.
        $transport = new FakeTransport($this->infoAndPong(), blockWhenEmpty: true);
        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('idle')->await();
        $queue->setTimeout(0.02);

        $msg = $queue->next();

        self::assertNull($msg);
    }

    /**
     * Line 231: the final-drain body in fetchAll() collects a message that was enqueued into the
     * queue by a concurrent fiber while fetchAll() was blocked inside processIncoming().
     *
     * Flow:
     *  1. fetchAll() blocks in processIncoming() (blockWhenEmpty transport).
     *  2. A parallel async fiber waits briefly then calls enqueue() directly.
     *  3. The TimeoutCancellation fires; processIncoming() throws CancelledException (line 225).
     *  4. The final drain loop (line 231) finds the concurrently-enqueued message and returns it.
     */
    public function testFetchAllFinalDrainCollectsConcurrentlyEnqueuedMessage(): void
    {
        $transport = new FakeTransport($this->infoAndPong(), blockWhenEmpty: true);
        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('events')->await();
        $queue->setTimeout(0.1);

        // Schedule a direct enqueue to fire after fetchAll() has suspended in processIncoming().
        async(static function () use ($queue): void {
            \Amp\delay(0.02);
            $queue->enqueue(new NatsMessage('events', 1, null, 'late'));
        });

        $messages = $queue->fetchAll();

        self::assertCount(1, $messages);
        self::assertSame('late', $messages[0]->payload);
    }
}
