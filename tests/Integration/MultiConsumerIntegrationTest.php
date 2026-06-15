<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Integration;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\JetStreamContext;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\delay;

/**
 * Hardening for multiple consumers over the same and separate streams: fan-out independence, shared-durable
 * load-balancing (no duplication), cross-stream isolation, concurrent consumption, and core queue-group
 * distribution. These exercise concurrency and routing behavior that single-consumer tests do not.
 */
final class MultiConsumerIntegrationTest extends TestCase
{
    use IntegrationTestBootstrap;

    protected function setUp(): void
    {
        $this->requireIntegrationEnabled();
    }

    public function testTwoDurableConsumersOnSameStreamEachReceiveAllMessages(): void
    {
        // Fan-out: two independent durable consumers on one stream each have their own cursor, so each
        // must receive the full message set - one consumer acking must not consume the other's copy.
        $stream = $this->streamName();
        $subject = $this->subjectFor($stream);
        $client = $this->connect();

        try {
            $js = $client->jetStream();
            $js->createStream($stream, [$subject])->await();

            $expected = [];
            for ($i = 0; $i < 10; $i++) {
                $expected[] = 'm' . $i;
                $js->publish($subject, 'm' . $i)->await();
            }

            $consumerA = 'CA' . strtoupper(bin2hex(random_bytes(2)));
            $consumerB = 'CB' . strtoupper(bin2hex(random_bytes(2)));
            $js->createConsumer($stream, $consumerA, $subject)->await();
            $js->createConsumer($stream, $consumerB, $subject)->await();

            $gotA = $this->drainPull($js, $stream, $consumerA, 10, 8.0);
            $gotB = $this->drainPull($js, $stream, $consumerB, 10, 8.0);

            self::assertSame($expected, $gotA, 'consumer A did not receive the full stream in order');
            self::assertSame($expected, $gotB, 'consumer B did not receive the full stream in order (fan-out leak)');
        } finally {
            $this->cleanupStream($client, $stream);
            $client->disconnect()->await();
        }
    }

    public function testSharedDurableConsumerLoadBalancesAcrossTwoConnectionsWithoutDuplication(): void
    {
        // Load-balance: two client connections pulling from the SAME durable consumer must split the
        // messages - every message is delivered to exactly one puller (union complete, zero duplicates).
        $stream = $this->streamName();
        $subject = $this->subjectFor($stream);
        $consumer = 'CS' . strtoupper(bin2hex(random_bytes(2)));

        $producer = $this->connect();
        $pullerA = $this->connect();
        $pullerB = $this->connect();

        try {
            $js = $producer->jetStream();
            $js->createStream($stream, [$subject])->await();
            $js->createConsumer($stream, $consumer, $subject)->await();

            $total = 20;
            for ($i = 0; $i < $total; $i++) {
                $js->publish($subject, 'm' . $i)->await();
            }

            // Alternate small pulls from the two connections until the union is complete.
            $seen = [];
            $deadline = $this->monotonic() + 10.0;
            while (count($seen) < $total && $this->monotonic() < $deadline) {
                foreach ([$pullerA, $pullerB] as $puller) {
                    foreach ($this->fetchTolerant($puller->jetStream(), $stream, $consumer, 3, 800) as $message) {
                        $seen[] = $message->payload;
                        $puller->jetStream()->ack($message)->await();
                    }
                }
            }

            sort($seen);
            $unique = array_values(array_unique($seen));
            self::assertCount($total, $unique, 'shared durable did not deliver every message exactly once');
            self::assertSame($unique, $seen, 'a message was delivered more than once across the two pullers');
        } finally {
            $this->cleanupStream($producer, $stream);
            $producer->disconnect()->await();
            $pullerA->disconnect()->await();
            $pullerB->disconnect()->await();
        }
    }

    public function testConsumersOnSeparateStreamsDoNotCrossTalk(): void
    {
        // Isolation: two streams with disjoint subjects and one consumer each. Each consumer must see only
        // its own stream's messages - no cross-stream delivery.
        $client = $this->connect();
        $streamA = $this->streamName();
        $streamB = $this->streamName();
        $subjectA = $this->subjectFor($streamA);
        $subjectB = $this->subjectFor($streamB);

        try {
            $js = $client->jetStream();
            $js->createStream($streamA, [$subjectA])->await();
            $js->createStream($streamB, [$subjectB])->await();

            for ($i = 0; $i < 5; $i++) {
                $js->publish($subjectA, 'a' . $i)->await();
                $js->publish($subjectB, 'b' . $i)->await();
            }

            $consumerA = 'IA' . strtoupper(bin2hex(random_bytes(2)));
            $consumerB = 'IB' . strtoupper(bin2hex(random_bytes(2)));
            $js->createConsumer($streamA, $consumerA, $subjectA)->await();
            $js->createConsumer($streamB, $consumerB, $subjectB)->await();

            $gotA = $this->drainPull($js, $streamA, $consumerA, 5, 6.0);
            $gotB = $this->drainPull($js, $streamB, $consumerB, 5, 6.0);

            self::assertSame(['a0', 'a1', 'a2', 'a3', 'a4'], $gotA);
            self::assertSame(['b0', 'b1', 'b2', 'b3', 'b4'], $gotB);
            foreach ($gotA as $payload) {
                self::assertStringStartsWith('a', $payload, 'stream B message leaked into consumer A');
            }
        } finally {
            $this->cleanupStream($client, $streamA);
            $this->cleanupStream($client, $streamB);
            $client->disconnect()->await();
        }
    }

    public function testConcurrentOrderedConsumersOnSeparateStreamsStayInOrder(): void
    {
        // Two ordered consumers, each on its own stream, each on its own connection, pumped concurrently.
        // Each must receive its stream's messages complete and in order with no cross-talk.
        $producer = $this->connect();
        $clientA = $this->connect();
        $clientB = $this->connect();

        $streamA = $this->streamName();
        $streamB = $this->streamName();
        $subjectA = $this->subjectFor($streamA);
        $subjectB = $this->subjectFor($streamB);

        try {
            $js = $producer->jetStream();
            $js->createStream($streamA, [$subjectA])->await();
            $js->createStream($streamB, [$subjectB])->await();

            $total = 15;
            for ($i = 0; $i < $total; $i++) {
                $js->publish($subjectA, 'a' . $i)->await();
                $js->publish($subjectB, 'b' . $i)->await();
            }

            /** @var list<string> $gotA */
            $gotA = [];
            /** @var list<string> $gotB */
            $gotB = [];
            $clientA->jetStream()->subscribeOrderedConsumer($streamA, static function (NatsMessage $m) use (&$gotA): void {
                $gotA[] = $m->payload;
            }, $subjectA)->await();
            $clientB->jetStream()->subscribeOrderedConsumer($streamB, static function (NatsMessage $m) use (&$gotB): void {
                $gotB[] = $m->payload;
            }, $subjectB)->await();

            $cancel = new DeferredCancellation();
            $pumps = [
                async(static fn(): null => self::pump($clientA, $cancel)),
                async(static fn(): null => self::pump($clientB, $cancel)),
            ];

            $deadline = $this->monotonic() + 10.0;
            while ((count($gotA) < $total || count($gotB) < $total) && $this->monotonic() < $deadline) {
                delay(0.05);
            }
            $cancel->cancel();
            foreach ($pumps as $pump) {
                $pump->await();
            }

            $expectedA = array_map(static fn(int $i): string => 'a' . $i, range(0, $total - 1));
            $expectedB = array_map(static fn(int $i): string => 'b' . $i, range(0, $total - 1));
            self::assertSame($expectedA, $gotA, 'stream A ordered consumer out of order / incomplete / cross-talk');
            self::assertSame($expectedB, $gotB, 'stream B ordered consumer out of order / incomplete / cross-talk');
        } finally {
            $this->cleanupStream($producer, $streamA);
            $this->cleanupStream($producer, $streamB);
            $producer->disconnect()->await();
            $clientA->disconnect()->await();
            $clientB->disconnect()->await();
        }
    }

    public function testCoreQueueGroupSubscribersLoadBalanceWithoutDuplication(): void
    {
        // Core NATS queue group: several subscribers on the same subject and queue group must split the
        // messages - every message delivered to exactly one group member, none duplicated.
        $client = $this->connect();
        $subject = 'q.' . strtolower(bin2hex(random_bytes(3)));

        try {
            $counts = [0, 0, 0];
            /** @var list<string> $seen */
            $seen = [];
            foreach ([0, 1, 2] as $idx) {
                $client->subscribe($subject, static function (NatsMessage $m) use (&$counts, &$seen, $idx): void {
                    $counts[$idx]++;
                    $seen[] = $m->payload;
                }, 'workers')->await();
            }

            $total = 30;
            for ($i = 0; $i < $total; $i++) {
                $client->publish($subject, 'm' . $i)->await();
            }

            $deadline = $this->monotonic() + 5.0;
            while (count($seen) < $total && $this->monotonic() < $deadline) {
                try {
                    $client->processIncoming(new TimeoutCancellation(0.5))->await();
                } catch (CancelledException) {
                    // keep polling until the deadline
                }
            }

            sort($seen);
            $unique = array_values(array_unique($seen));
            self::assertCount($total, $unique, 'queue group did not deliver every message exactly once');
            self::assertSame($unique, $seen, 'a message was delivered to more than one queue-group member');
            self::assertSame($total, array_sum($counts));
            $membersThatReceived = count(array_filter($counts, static fn(int $c): bool => $c > 0));
            self::assertGreaterThan(1, $membersThatReceived, 'load was not distributed across the queue group');
        } finally {
            $client->disconnect()->await();
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────────────────

    private function connect(): NatsClient
    {
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        return $client;
    }

    /**
     * Pulls from a consumer until $expected payloads are collected or the deadline passes; acks each.
     *
     * @return list<string>
     */
    private function drainPull(JetStreamContext $js, string $stream, string $consumer, int $expected, float $deadlineSeconds): array
    {
        $collected = [];
        $deadline = $this->monotonic() + $deadlineSeconds;
        while (count($collected) < $expected && $this->monotonic() < $deadline) {
            $batch = $this->fetchTolerant($js, $stream, $consumer, $expected - count($collected), 1000);
            foreach ($batch as $message) {
                $collected[] = $message->payload;
                $js->ack($message)->await();
            }
        }

        return $collected;
    }

    /**
     * fetchBatch throws JetStreamException(408) when a pull window expires with zero messages (a benign
     * "nothing available right now" outcome for a poll loop). Treat that as an empty batch; re-throw any
     * other terminal status (e.g. 409 consumer-deleted).
     *
     * @return list<NatsMessage>
     */
    private function fetchTolerant(JetStreamContext $js, string $stream, string $consumer, int $batch, int $expiresMs): array
    {
        try {
            return $js->fetchBatch($stream, $consumer, $batch, $expiresMs)->await();
        } catch (JetStreamException $e) {
            if ($e->getCode() === 408) {
                return [];
            }

            throw $e;
        }
    }

    private static function pump(NatsClient $client, DeferredCancellation $cancel): null
    {
        $cancellation = $cancel->getCancellation();
        while (!$cancellation->isRequested()) {
            try {
                $client->processIncoming($cancellation)->await();
            } catch (CancelledException) {
                break;
            }
        }

        return null;
    }

    private function streamName(): string
    {
        return 'MC_' . strtoupper(bin2hex(random_bytes(3)));
    }

    private function subjectFor(string $stream): string
    {
        return 'mc.' . strtolower($stream) . '.evt';
    }

    private function cleanupStream(NatsClient $client, string $stream): void
    {
        try {
            $client->jetStream()->deleteStream($stream)->await();
        } catch (\Throwable) {
            // best-effort teardown
        }
    }
}
