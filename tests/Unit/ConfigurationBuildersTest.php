<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\JetStream\Configuration\ConsumerConfiguration;
use IDCT\NATS\JetStream\Configuration\StreamConfiguration;
use IDCT\NATS\JetStream\Enum\AckPolicy;
use IDCT\NATS\JetStream\Enum\DeliverPolicy;
use IDCT\NATS\JetStream\Enum\DiscardPolicy;
use IDCT\NATS\JetStream\Enum\ReplayPolicy;
use IDCT\NATS\JetStream\Enum\RetentionPolicy;
use IDCT\NATS\JetStream\Enum\StorageBackend;
use PHPUnit\Framework\TestCase;

/**
 * Exercises every setter on the typed JetStream configuration builders, asserting the wire payload
 * (including the seconds/ms -> nanoseconds conversions the server expects).
 */
final class ConfigurationBuildersTest extends TestCase
{
    public function testStreamConfigurationMapsEverySetter(): void
    {
        $config = StreamConfiguration::create('ORDERS')
            ->subjects('orders.*', 'orders.created')
            ->retention(RetentionPolicy::WorkQueue)
            ->storage(StorageBackend::Memory)
            ->discard(DiscardPolicy::New)
            ->maxMessages(1000)
            ->maxBytes(1_048_576)
            ->maxAge(60)
            ->maxMsgSize(4096)
            ->maxMsgsPerSubject(10)
            ->maxConsumers(5)
            ->replicas(3)
            ->duplicateWindow(120)
            ->allowDirect()
            ->mirrorDirect()
            ->allowRollupHeaders()
            ->denyDelete()
            ->denyPurge()
            ->sealed(false)
            ->compression('s2')
            ->description('orders stream')
            ->metadata(['team' => 'core'])
            ->set('first_seq', 42);

        self::assertSame('ORDERS', $config->name());

        $array = $config->toArray();
        self::assertSame('ORDERS', $array['name']);
        self::assertSame(['orders.*', 'orders.created'], $array['subjects']);
        self::assertSame('workqueue', $array['retention']);
        self::assertSame('memory', $array['storage']);
        self::assertSame('new', $array['discard']);
        self::assertSame(1000, $array['max_msgs']);
        self::assertSame(1_048_576, $array['max_bytes']);
        self::assertSame(60_000_000_000, $array['max_age']); // seconds -> ns
        self::assertSame(4096, $array['max_msg_size']);
        self::assertSame(10, $array['max_msgs_per_subject']);
        self::assertSame(5, $array['max_consumers']);
        self::assertSame(3, $array['num_replicas']);
        self::assertSame(120_000_000_000, $array['duplicate_window']); // seconds -> ns
        self::assertTrue($array['allow_direct']);
        self::assertTrue($array['mirror_direct']);
        self::assertTrue($array['allow_rollup_hdrs']);
        self::assertTrue($array['deny_delete']);
        self::assertTrue($array['deny_purge']);
        self::assertFalse($array['sealed']);
        self::assertSame('s2', $array['compression']);
        self::assertSame('orders stream', $array['description']);
        self::assertSame(['team' => 'core'], $array['metadata']);
        self::assertSame(42, $array['first_seq']);
    }

    public function testStreamConfigurationDefaultsToEmptySubjects(): void
    {
        $array = StreamConfiguration::create('EMPTY')->toArray();

        self::assertSame('EMPTY', $array['name']);
        self::assertSame([], $array['subjects']);
    }

    public function testConsumerConfigurationMapsEverySetter(): void
    {
        $config = ConsumerConfiguration::create()
            ->durable('worker')
            ->ackPolicy(AckPolicy::Explicit)
            ->deliverPolicy(DeliverPolicy::ByStartSequence)
            ->replayPolicy(ReplayPolicy::Instant)
            ->ackWait(5000)
            ->maxDeliver(7)
            ->maxAckPending(100)
            ->filterSubject('orders.created')
            ->filterSubjects(['a.>', 'b.>'])
            ->deliverSubject('_INBOX.deliver')
            ->deliverGroup('q')
            ->optStartSequence(99)
            ->optStartTime('2026-01-01T00:00:00Z')
            ->headersOnly()
            ->memoryStorage()
            ->replicas(2)
            ->inactiveThreshold(30_000)
            ->backoff([1000, 2000])
            ->description('worker consumer')
            ->metadata(['env' => 'prod'])
            ->set('rate_limit_bps', 1024);

        self::assertSame('worker', $config->getName());

        $array = $config->toArray();
        self::assertSame('worker', $array['durable_name']);
        self::assertSame('explicit', $array['ack_policy']);
        self::assertSame('by_start_sequence', $array['deliver_policy']);
        self::assertSame('instant', $array['replay_policy']);
        self::assertSame(5_000_000_000, $array['ack_wait']); // ms -> ns
        self::assertSame(7, $array['max_deliver']);
        self::assertSame(100, $array['max_ack_pending']);
        self::assertSame('orders.created', $array['filter_subject']);
        self::assertSame(['a.>', 'b.>'], $array['filter_subjects']);
        self::assertSame('_INBOX.deliver', $array['deliver_subject']);
        self::assertSame('q', $array['deliver_group']);
        self::assertSame(99, $array['opt_start_seq']);
        self::assertSame('2026-01-01T00:00:00Z', $array['opt_start_time']);
        self::assertTrue($array['headers_only']);
        self::assertTrue($array['mem_storage']);
        self::assertSame(2, $array['num_replicas']);
        self::assertSame(30_000_000_000, $array['inactive_threshold']); // ms -> ns
        self::assertSame([1_000_000_000, 2_000_000_000], $array['backoff']); // ms -> ns each
        self::assertSame('worker consumer', $array['description']);
        self::assertSame(['env' => 'prod'], $array['metadata']);
        self::assertSame(1024, $array['rate_limit_bps']);
    }

    public function testConsumerConfigurationEphemeralHasNoDurableName(): void
    {
        $config = ConsumerConfiguration::create()->ackPolicy(AckPolicy::None);

        self::assertNull($config->getName());
        self::assertArrayNotHasKey('durable_name', $config->toArray());
        self::assertSame('none', $config->toArray()['ack_policy']);
    }

    public function testConsumerConfigurationCarriesTheFlowControlAckPolicy(): void
    {
        $array = ConsumerConfiguration::create()
            ->durable('SRC')
            ->deliverSubject('deliver.wq')
            ->ackPolicy(AckPolicy::FlowControl)
            ->toArray();

        self::assertSame('flow_control', $array['ack_policy']);
        self::assertSame(['durable_name' => 'SRC', 'deliver_subject' => 'deliver.wq', 'ack_policy' => 'flow_control'], $array);
    }
}
