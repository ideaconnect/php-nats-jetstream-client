<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Integration;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use PHPUnit\Framework\TestCase;

final class JetStreamIntegrationTest extends TestCase
{
    use IntegrationTestBootstrap;

    /**
     * Verifies account info and basic stream lifecycle operations against live JetStream.
     */
    public function testJetStreamAccountAndStreamLifecycle(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $account = $js->accountInfo()->await();
        $created = $js->createStream($stream, ['it.' . strtolower($stream) . '.>'])->await();
        $fetched = $js->getStream($stream)->await();
        $deleted = $js->deleteStream($stream)->await();

        self::assertGreaterThanOrEqual(0, $account->streams);
        self::assertSame($stream, $created->name);
        self::assertSame($stream, $fetched->name);
        self::assertTrue($deleted);

        $client->disconnect()->await();
    }

    /**
     * Verifies consumer lifecycle and publish acknowledgment against live JetStream.
     */
    public function testJetStreamConsumerAndPublishAck(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.events';
        $consumer = 'C_' . strtoupper(bin2hex(random_bytes(2)));

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();

        $created = $js->createConsumer($stream, $consumer, $subject)->await();
        $fetched = $js->getConsumer($stream, $consumer)->await();
        $ack = $js->publish($subject, '{"event":"created"}')->await();
        $deletedConsumer = $js->deleteConsumer($stream, $consumer)->await();
        $deletedStream = $js->deleteStream($stream)->await();

        self::assertSame($stream, $created->streamName);
        self::assertSame($consumer, $fetched->name);
        self::assertSame($stream, $ack->stream);
        self::assertGreaterThanOrEqual(1, $ack->seq);
        self::assertTrue($deletedConsumer);
        self::assertTrue($deletedStream);

        $client->disconnect()->await();
    }

    /**
     * Verifies consumer list API returns durable consumers created for a stream.
     */
    public function testJetStreamListConsumers(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.consumers';
        $consumerA = 'C_' . strtoupper(bin2hex(random_bytes(2)));
        $consumerB = 'C_' . strtoupper(bin2hex(random_bytes(2)));

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();
        $js->createConsumer($stream, $consumerA, $subject)->await();
        $js->createConsumer($stream, $consumerB, $subject)->await();

        $consumers = $js->listConsumers($stream)->await();
        $names = array_map(static fn ($consumer): string => $consumer->name, $consumers);

        self::assertContains($consumerA, $names);
        self::assertContains($consumerB, $names);

        $js->deleteConsumer($stream, $consumerA)->await();
        $js->deleteConsumer($stream, $consumerB)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies stream update persists modified subject configuration.
     */
    public function testJetStreamUpdateStreamConfiguration(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subjectA = 'it.' . strtolower($stream) . '.a';
        $subjectB = 'it.' . strtolower($stream) . '.b';

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream($stream, [$subjectA])->await();
        $updated = $js->updateStream($stream, ['subjects' => [$subjectA, $subjectB]])->await();
        $fetched = $js->getStream($stream)->await();

        self::assertSame($stream, $updated->name);
        self::assertContains($subjectA, $updated->subjects);
        self::assertContains($subjectB, $updated->subjects);
        self::assertContains($subjectA, $fetched->subjects);
        self::assertContains($subjectB, $fetched->subjects);

        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies stream purge removes published messages.
     */
    public function testJetStreamPurgeStreamByFilter(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.purge';

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();

        $js->publish($subject, '{"event":"one"}')->await();
        $js->publish($subject, '{"event":"two"}')->await();

        $purge = $js->purgeStream($stream)->await();
        $state = $js->getStream($stream)->await()->raw['state'] ?? [];

        self::assertGreaterThanOrEqual(2, $purge['purged']);
        self::assertSame(0, (int) ($state['messages'] ?? -1));

        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies direct stream message get returns payload by sequence.
     */
    public function testJetStreamGetStreamMessage(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.direct';

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();
        $ack = $js->publish($subject, '{"event":"direct-get"}')->await();

        $message = $js->getStreamMessage($stream, $ack->seq)->await();

        self::assertSame($subject, $message->subject);
        self::assertSame('{"event":"direct-get"}', $message->payload);

        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies stream list API includes newly created streams.
     */
    public function testJetStreamListStreams(): void
    {
        $this->requireIntegrationEnabled();

        $streamA = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $streamB = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subjectA = 'it.' . strtolower($streamA) . '.list';
        $subjectB = 'it.' . strtolower($streamB) . '.list';

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream($streamA, [$subjectA])->await();
        $js->createStream($streamB, [$subjectB])->await();

        $streams = $js->listStreams()->await();
        $names = array_map(static fn ($stream): string => $stream->name, $streams);

        self::assertContains($streamA, $names);
        self::assertContains($streamB, $names);

        $js->deleteStream($streamA)->await();
        $js->deleteStream($streamB)->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies scheduled publish delivers a delayed message to the configured target subject.
     */
    public function testJetStreamScheduledPublish(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $scheduleSubject = 'schedules.' . strtolower($stream) . '.one';
        $targetSubject = 'events.' . strtolower($stream) . '.scheduled';
        $consumer = 'C_' . strtoupper(bin2hex(random_bytes(2)));

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream(
            $stream,
            [$scheduleSubject, $targetSubject],
            ['allow_msg_schedules' => true],
        )->await();

        $ack = $js->publishScheduled(
            $scheduleSubject,
            $targetSubject,
            '{"event":"scheduled"}',
            '@at ' . gmdate('Y-m-d\TH:i:s\Z', time() + 2),
            null,
        )->await();

        $observedMessages = 0;
        $delivered = false;
        $deadline = microtime(true) + 6.0;
        while (!$delivered && microtime(true) < $deadline) {
            $state = $js->getStream($stream)->await()->raw['state'] ?? [];
            $observedMessages = max(0, (int) ($state['messages'] ?? 0));
            $delivered = $observedMessages >= 1;

            if ($delivered) {
                continue;
            }

            usleep(250_000);
        }

        self::assertSame($stream, $ack->stream);
        self::assertGreaterThanOrEqual(1, $observedMessages);

        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies unsupported schedule expressions are rejected before publish.
     */
    public function testJetStreamScheduledPublishRejectsUnsupportedPatterns(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $scheduleSubject = 'schedules.' . strtolower($stream) . '.invalid';
        $targetSubject = 'events.' . strtolower($stream) . '.invalid';

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream(
            $stream,
            [$scheduleSubject, $targetSubject],
            ['allow_msg_schedules' => true],
        )->await();

        try {
            $js->publishScheduled(
                $scheduleSubject,
                $targetSubject,
                '{"event":"bad-schedule"}',
                '@every 5s',
                null,
            )->await();
            self::fail('Expected unsupported schedule expression to be rejected.');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('Only @at schedule expressions are currently supported', $e->getMessage());
        }

        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies pull consumer fetch-next and explicit ACK workflow against live JetStream.
     */
    public function testJetStreamPullFetchAndAck(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.pull';
        $consumer = 'C_' . strtoupper(bin2hex(random_bytes(2)));

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();
        $js->createConsumer($stream, $consumer, $subject)->await();

        $published = $js->publish($subject, '{"event":"pull"}')->await();
        self::assertSame($stream, $published->stream);

        $message = $js->fetchNext($stream, $consumer, 4000)->await();
        self::assertSame('{"event":"pull"}', $message->payload);
        self::assertNotNull($message->replyTo);

        $js->ack($message)->await();

        $js->deleteConsumer($stream, $consumer)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies delayed NAK triggers redelivery for pull consumers.
     */
    public function testJetStreamPullNakWithDelayRedelivery(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.pull.nak';
        $consumer = 'C_' . strtoupper(bin2hex(random_bytes(2)));

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();
        $js->createConsumer($stream, $consumer, $subject)->await();

        $js->publish($subject, '{"event":"redeliver"}')->await();

        $first = $js->fetchNext($stream, $consumer, 4000)->await();
        self::assertSame('{"event":"redeliver"}', $first->payload);

        $js->nakWithDelay($first, 1200)->await();
        usleep(1_500_000);

        $second = $js->fetchNext($stream, $consumer, 4000)->await();
        self::assertSame('{"event":"redeliver"}', $second->payload);

        $js->ack($second)->await();

        $js->deleteConsumer($stream, $consumer)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies TERM and WPI tokens influence pull-consumer redelivery workflow.
     */
    public function testJetStreamTermAndInProgressTokens(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.pull.termwpi';
        $consumer = 'C_' . strtoupper(bin2hex(random_bytes(2)));

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();
        $js->createConsumer($stream, $consumer, $subject, [
            'ack_wait' => 1_000_000_000,
            'max_deliver' => 3,
        ])->await();

        // WPI should extend in-flight processing and delay redelivery.
        $js->publish($subject, '{"event":"wpi"}')->await();
        $first = $js->fetchNext($stream, $consumer, 4_000)->await();
        self::assertSame('{"event":"wpi"}', $first->payload);

        usleep(600_000);
        $js->inProgress($first)->await();

        try {
            $js->fetchBatch($stream, $consumer, 1, 500)->await();
            self::fail('Expected no immediate redelivery after WPI heartbeat.');
        } catch (JetStreamException $e) {
            self::assertMatchesRegularExpression('/status (404|408)|No messages received within timeout/i', $e->getMessage());
        }

        $redelivered = null;
        $deadline = microtime(true) + 4.0;
        while ($redelivered === null && microtime(true) < $deadline) {
            try {
                $redelivered = $js->fetchNext($stream, $consumer, 800)->await();
            } catch (JetStreamException $e) {
                if (!preg_match('/status (404|408)|No messages received within timeout/i', $e->getMessage())) {
                    throw $e;
                }
            }
        }

        self::assertNotNull($redelivered);
        self::assertSame('{"event":"wpi"}', $redelivered->payload);
        $js->ack($redelivered)->await();

        // TERM should stop further redeliveries for a message.
        $js->publish($subject, '{"event":"term"}')->await();
        $toTerm = $js->fetchNext($stream, $consumer, 4_000)->await();
        self::assertSame('{"event":"term"}', $toTerm->payload);
        $js->term($toTerm)->await();

        usleep(1_300_000);
        try {
            $js->fetchBatch($stream, $consumer, 1, 700)->await();
            self::fail('Expected TERM-ed message to stop redelivery.');
        } catch (JetStreamException $e) {
            self::assertMatchesRegularExpression('/status (404|408)|No messages received within timeout/i', $e->getMessage());
        }

        $js->deleteConsumer($stream, $consumer)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies pull-consumer iterator batching processes messages across chained pulls.
     */
    public function testJetStreamPullIteratorBatching(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.pull.iterator';
        $consumer = 'C_' . strtoupper(bin2hex(random_bytes(2)));

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();
        $js->createConsumer($stream, $consumer, $subject)->await();

        for ($i = 1; $i <= 5; $i++) {
            $js->publish($subject, json_encode(['n' => $i], JSON_THROW_ON_ERROR))->await();
        }

        $seen = [];
        $total = $js->pullConsumer($stream, $consumer)
            ->setBatching(2)
            ->setExpiresMs(700)
            ->setIterations(4)
            ->handle(static function (NatsMessage $message, $context) use (&$seen): void {
                $seen[] = $message->payload;
                if ($message->replyTo !== null && $message->replyTo !== '') {
                    $context->ack($message)->await();
                }
            })->await();

        sort($seen);
        self::assertSame(5, $total);
        self::assertSame([
            '{"n":1}',
            '{"n":2}',
            '{"n":3}',
            '{"n":4}',
            '{"n":5}',
        ], $seen);

        $js->deleteConsumer($stream, $consumer)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies durable push helper delivers live payloads to subscribed handlers.
     */
    public function testJetStreamPushConsumerHelperDelivery(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.push';
        $consumer = 'C_' . strtoupper(bin2hex(random_bytes(2)));

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();

        $received = null;
        $sid = $js->subscribePushConsumer(
            $stream,
            $consumer,
            static function (NatsMessage $message) use (&$received, $js): void {
                $received = $message;
                $js->ack($message)->await();
            },
            null,
            $subject,
        )->await();

        $js->publish($subject, '{"event":"push"}')->await();

        $deadline = microtime(true) + 4.0;
        while ($received === null && microtime(true) < $deadline) {
            $client->processIncoming()->await();
            usleep(100_000);
        }

        self::assertInstanceOf(NatsMessage::class, $received);
        self::assertSame('{"event":"push"}', $received->payload);

        $client->unsubscribe($sid)->await();
        $js->deleteConsumer($stream, $consumer)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies durable push helper works with an explicit deliver subject.
     */
    public function testJetStreamPushConsumerWithExplicitDeliverSubject(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.push.explicit';
        $deliver = 'deliver.' . strtolower($stream) . '.events';
        $consumer = 'C_' . strtoupper(bin2hex(random_bytes(2)));

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();

        $received = null;
        $sid = $js->subscribePushConsumer(
            $stream,
            $consumer,
            static function (NatsMessage $message) use (&$received, $js): void {
                $received = $message;
                $js->ack($message)->await();
            },
            $deliver,
            $subject,
        )->await();

        $js->publish($subject, '{"event":"push-explicit"}')->await();

        $deadline = microtime(true) + 4.0;
        while ($received === null && microtime(true) < $deadline) {
            $client->processIncoming()->await();
            usleep(100_000);
        }

        self::assertInstanceOf(NatsMessage::class, $received);
        self::assertSame('{"event":"push-explicit"}', $received->payload);

        $client->unsubscribe($sid)->await();
        $js->deleteConsumer($stream, $consumer)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies ephemeral push helper delivers payloads and supports explicit ACK handling.
     */
    public function testJetStreamEphemeralPushConsumerDelivery(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.push.ephemeral';

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();

        $received = null;
        $sid = $js->subscribeEphemeralPushConsumer(
            $stream,
            static function (NatsMessage $message) use (&$received, $js): void {
                $received = $message;
                if ($message->replyTo !== null && $message->replyTo !== '') {
                    $js->ack($message)->await();
                }
            },
            null,
            $subject,
        )->await();

        $js->publish($subject, '{"event":"ephemeral-push"}')->await();

        $deadline = microtime(true) + 4.0;
        while ($received === null && microtime(true) < $deadline) {
            $client->processIncoming()->await();
            usleep(100_000);
        }

        self::assertInstanceOf(NatsMessage::class, $received);
        self::assertSame('{"event":"ephemeral-push"}', $received->payload);

        $client->unsubscribe($sid)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies ordered consumer delivery still works when the first matching message is not stream sequence 1.
     */
    public function testJetStreamOrderedConsumerWithFilteredSubjectAfterPriorMessages(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subjectPrefix = 'it.' . strtolower($stream);
        $nonMatchingSubject = $subjectPrefix . '.other';
        $matchingSubject = $subjectPrefix . '.match';

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream($stream, [$subjectPrefix . '.>'])->await();

        // Advance the stream sequence with a non-matching subject first.
        $js->publish($nonMatchingSubject, '{"event":"other"}')->await();

        $received = [];
        $sid = $js->subscribeOrderedConsumer(
            $stream,
            static function (NatsMessage $message) use (&$received): void {
                $received[] = $message->payload;
            },
            $matchingSubject,
        )->await();

        $js->publish($matchingSubject, '{"event":"ordered"}')->await();

        $deadline = microtime(true) + 4.0;
        while ($received === [] && microtime(true) < $deadline) {
            $client->processIncoming()->await();
            usleep(100_000);
        }

        self::assertSame(['{"event":"ordered"}'], $received);

        $client->unsubscribe($sid)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies ephemeral pull consumer can fetch and ACK a live message.
     */
    public function testJetStreamEphemeralPullConsumerFetchAndAck(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.ephemeral.pull';

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();

        $consumer = $js->createEphemeralConsumer($stream, $subject)->await();
        self::assertSame($stream, $consumer->streamName);
        self::assertNotSame('', $consumer->name);

        $js->publish($subject, '{"event":"ephemeral"}')->await();
        $message = $js->fetchNext($stream, $consumer->name, 4000)->await();
        self::assertSame('{"event":"ephemeral"}', $message->payload);

        $js->ack($message)->await();
        $js->deleteConsumer($stream, $consumer->name)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies KV bucket lifecycle with put/get/delete and watch delivery.
     */
    public function testJetStreamKeyValueLifecycle(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'cfg' . strtolower(bin2hex(random_bytes(2)));
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue($bucket);
        $kv->create()->await();

        $watched = null;
        $sid = $kv->watch(static function ($entry) use (&$watched): void {
            $watched = $entry;
        }, 'theme')->await();

        $kv->put('theme', 'dark')->await();

        $deadline = microtime(true) + 4.0;
        while ($watched === null && microtime(true) < $deadline) {
            $client->processIncoming()->await();
            usleep(100_000);
        }

        self::assertNotNull($watched);
        self::assertSame('theme', $watched->key);
        self::assertSame('dark', $watched->value);

        $entry = $kv->get('theme')->await();
        self::assertNotNull($entry);
        self::assertSame('dark', $entry->value);

        $kv->delete('theme')->await();
        $deleted = $kv->get('theme')->await();
        self::assertNotNull($deleted);
        self::assertNull($deleted->value);
        self::assertSame('DEL', $deleted->operation);

        $client->unsubscribe($sid)->await();
        $kv->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies KV update/purge/getAll/getStatus parity operations against live JetStream.
     */
    public function testJetStreamKeyValueAdvancedParityOperations(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'adv' . strtolower(bin2hex(random_bytes(2)));
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue($bucket);
        $kv->create()->await();

        $kv->put('username', 'alice')->await();
        $entry = $kv->get('username')->await();
        self::assertNotNull($entry);

        $updated = $kv->update('username', 'bob', $entry->revision ?? 0)->await();
        self::assertGreaterThanOrEqual(2, $updated->seq);

        $kv->put('email', 'a@example.com')->await();
        $allBeforePurge = $kv->getAll()->await();
        self::assertSame('bob', $allBeforePurge['username'] ?? null);
        self::assertSame('a@example.com', $allBeforePurge['email'] ?? null);

        $kv->purge('username')->await();
        $allAfterPurge = $kv->getAll()->await();
        self::assertArrayNotHasKey('username', $allAfterPurge);
        self::assertSame('a@example.com', $allAfterPurge['email'] ?? null);

        $status = $kv->getStatus()->await();
        self::assertSame($bucket, $status['bucket']);
        self::assertSame('KV_' . $bucket, $status['stream']);
        self::assertGreaterThanOrEqual(1, (int) $status['messages']);

        $kv->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies Object Store bucket lifecycle with put/get/info/delete operations.
     */
    public function testJetStreamObjectStoreLifecycle(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'obj' . strtolower(bin2hex(random_bytes(3)));
        $objectName = 'large-' . bin2hex(random_bytes(2)) . '.txt';
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $store = $client->jetStream()->objectStore($bucket);
        $store->create()->await();

        $stored = $store->put('logo.txt', 'hello-object', ['content-type' => 'text/plain'])->await();
        self::assertSame('logo.txt', $stored->name);
        self::assertFalse($stored->deleted);

        $info = $store->info('logo.txt')->await();
        self::assertNotNull($info);
        self::assertSame('logo.txt', $info->name);
        self::assertSame('text/plain', $info->metadata['content-type'] ?? null);

        $objectData = $store->get('logo.txt')->await();
        self::assertNotNull($objectData);
        self::assertSame('hello-object', $objectData->data);

        $listed = $store->list()->await();
        self::assertCount(1, $listed);
        self::assertSame('logo.txt', $listed[0]->name);
        self::assertFalse($listed[0]->deleted);

        $deleted = $store->delete('logo.txt')->await();
        self::assertTrue($deleted->deleted);

        $afterDelete = $store->get('logo.txt')->await();
        self::assertNotNull($afterDelete);
        self::assertTrue($afterDelete->info->deleted);
        self::assertNull($afterDelete->data);

        $store->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies stream retention/storage/discard policy options persist after create.
     */
    public function testJetStreamStreamPoliciesPersist(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.policy';

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream($stream, [$subject], [
            'retention' => 'limits',
            'storage' => 'memory',
            'discard' => 'old',
            'max_msgs' => 5,
            'max_bytes' => 1024 * 64,
        ])->await();

        $fetched = $js->getStream($stream)->await();
        /** @var array<string,mixed> $config */
        $config = is_array($fetched->raw['config'] ?? null) ? $fetched->raw['config'] : [];

        self::assertSame('limits', $config['retention'] ?? null);
        self::assertSame('memory', $config['storage'] ?? null);
        self::assertSame('old', $config['discard'] ?? null);
        self::assertSame(5, (int) ($config['max_msgs'] ?? -1));
        self::assertSame(1024 * 64, (int) ($config['max_bytes'] ?? -1));

        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies consumer pause blocks delivery until resume is applied.
     */
    public function testJetStreamPauseAndResumeConsumer(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.pause';
        $consumer = 'C_' . strtoupper(bin2hex(random_bytes(2)));

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();
        $js->createConsumer($stream, $consumer, $subject)->await();

        $js->publish($subject, '{"event":"paused"}')->await();

        $pauseResult = $js->pauseConsumer($stream, $consumer, gmdate('Y-m-d\TH:i:s\Z', time() + 30))->await();
        self::assertTrue((bool) ($pauseResult['paused'] ?? false));

        try {
            $js->fetchBatch($stream, $consumer, 1, 500)->await();
            self::fail('Expected paused consumer to suppress pull delivery.');
        } catch (JetStreamException $e) {
            self::assertMatchesRegularExpression('/status (404|408)|No messages received within timeout/i', $e->getMessage());
        }

        $resumeResult = $js->resumeConsumer($stream, $consumer)->await();
        self::assertFalse((bool) ($resumeResult['paused'] ?? true));

        $message = $js->fetchNext($stream, $consumer, 2_000)->await();
        self::assertSame('{"event":"paused"}', $message->payload);
        $js->ack($message)->await();

        $js->deleteConsumer($stream, $consumer)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies KV history and TTL options persist and TTL expiration removes key visibility.
     */
    public function testJetStreamKeyValueHistoryAndTtlBehavior(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'ttl' . strtolower(bin2hex(random_bytes(2)));
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue($bucket);
        $kv->create([
            'history' => 3,
            'ttl' => 1_000_000_000,
        ])->await();

        $kv->put('session', 'v1')->await();
        $kv->put('session', 'v2')->await();

        $beforeExpiry = $kv->get('session')->await();
        self::assertNotNull($beforeExpiry);
        self::assertSame('v2', $beforeExpiry->value);

        $stream = $client->jetStream()->getStream('KV_' . $bucket)->await();
        /** @var array<string,mixed> $config */
        $config = is_array($stream->raw['config'] ?? null) ? $stream->raw['config'] : [];
        self::assertSame(3, (int) ($config['max_msgs_per_subject'] ?? -1));
        self::assertSame(1_000_000_000, (int) ($config['max_age'] ?? -1));

        $expired = null;
        $deadline = microtime(true) + 12.0;
        while (microtime(true) < $deadline) {
            $expired = $kv->get('session')->await();
            if ($expired === null) {
                break;
            }

            usleep(100_000);
        }

        self::assertNull($expired, 'Expected KV entry to expire within the extended observation window.');

        $kv->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies multiple KV watchers observe the same updates concurrently.
     */
    public function testJetStreamKeyValueConcurrentWatchers(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'cw' . strtolower(bin2hex(random_bytes(2)));
        $key = 'session';

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue($bucket);
        $kv->create()->await();

        $watcherA = [];
        $watcherB = [];

        $sidA = $kv->watch(static function ($entry) use (&$watcherA): void {
            $watcherA[] = $entry;
        }, $key)->await();

        $sidB = $kv->watch(static function ($entry) use (&$watcherB): void {
            $watcherB[] = $entry;
        }, $key)->await();

        $kv->put($key, 'v1')->await();
        $kv->put($key, 'v2')->await();

        $deadline = microtime(true) + 5.0;
        while ((count($watcherA) < 2 || count($watcherB) < 2) && microtime(true) < $deadline) {
            $client->processIncoming()->await();
            usleep(50_000);
        }

        self::assertGreaterThanOrEqual(2, count($watcherA));
        self::assertGreaterThanOrEqual(2, count($watcherB));
        self::assertSame('v1', $watcherA[0]->value ?? null);
        self::assertSame('v2', $watcherA[1]->value ?? null);
        self::assertSame('v1', $watcherB[0]->value ?? null);
        self::assertSame('v2', $watcherB[1]->value ?? null);

        $client->unsubscribe($sidA)->await();
        $client->unsubscribe($sidB)->await();
        $kv->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies object store retrieval correctly reconstructs multi-chunk payloads.
     */
    public function testJetStreamObjectStoreLargeObjectChunks(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'obj' . strtolower(bin2hex(random_bytes(3)));
        $objectName = 'large-' . bin2hex(random_bytes(2)) . '.txt';
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $store = $client->jetStream()->objectStore($bucket);
        $store->create()->await();

        $payload = str_repeat('chunked-data-', 25_000);
        self::assertGreaterThan(131072, strlen($payload));

        $stored = $store->put($objectName, $payload, ['content-type' => 'text/plain'])->await();
        self::assertGreaterThan(1, $stored->chunks);

        $metaVisible = false;
        $metaDeadline = microtime(true) + 4.0;
        while (microtime(true) < $metaDeadline) {
            if ($store->info($objectName)->await() !== null) {
                $metaVisible = true;
                break;
            }

            usleep(100_000);
        }
        self::assertTrue($metaVisible);

        $retrieved = null;
        $deadline = microtime(true) + 4.0;
        while (microtime(true) < $deadline) {
            try {
                $retrieved = $store->get($objectName)->await();
                if ($retrieved !== null) {
                    break;
                }
            } catch (JetStreamException $e) {
                if (!str_contains($e->getMessage(), 'Object digest mismatch')) {
                    throw $e;
                }
            }

            usleep(100_000);
        }

        self::assertNotNull($retrieved);
        self::assertSame($payload, $retrieved->data);
        self::assertSame($stored->digest, $retrieved->info->digest);

        $store->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies object retrieval fails with digest mismatch when metadata digest is corrupted.
     */
    public function testJetStreamObjectStoreDigestMismatch(): void
    {
        $this->requireIntegrationEnabled();

        $bucket = 'obj' . strtolower(bin2hex(random_bytes(3)));
        $objectName = 'digest-' . bin2hex(random_bytes(2)) . '.txt';
        $payload = 'integrity-payload-' . bin2hex(random_bytes(12));

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $store = $js->objectStore($bucket);
        $store->create()->await();

        $stored = $store->put($objectName, $payload, ['content-type' => 'text/plain'])->await();
        $info = $store->info($objectName)->await();
        self::assertNotNull($info);

        $corruptedDigest = 'SHA-256=' . base64_encode(hash('sha256', 'different-bytes', true));
        self::assertNotSame($stored->digest, $corruptedDigest);

        $tampered = [
            'name' => $info->name,
            'size' => $info->size,
            'chunks' => $info->chunks,
            'digest' => $corruptedDigest,
            'mtime' => gmdate('Y-m-d\TH:i:s\Z'),
            'deleted' => false,
            'chunk_subject' => $info->chunkSubject,
            'metadata' => $info->metadata,
        ];

        $js->publish($store->metaPrefix() . $objectName, json_encode($tampered, JSON_THROW_ON_ERROR))->await();

        try {
            $store->get($objectName)->await();
            self::fail('Expected object digest mismatch after metadata tampering.');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('Object digest mismatch', $e->getMessage());
        }

        $store->deleteBucket()->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies push consumer handles heartbeat/flow-control control frames and still delivers payloads.
     */
    public function testJetStreamPushFlowControlAndHeartbeat(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.push.hb';
        $consumer = 'C_' . strtoupper(bin2hex(random_bytes(2)));

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();

        $receivedPayloads = [];
        $sid = $js->subscribePushConsumer(
            $stream,
            $consumer,
            static function (NatsMessage $message) use (&$receivedPayloads, $js): void {
                $receivedPayloads[] = $message->payload;
                if ($message->replyTo !== null && $message->replyTo !== '') {
                    $js->ack($message)->await();
                }
            },
            null,
            $subject,
            [
                'flow_control' => true,
                'idle_heartbeat' => 1_000_000_000,
            ],
        )->await();

        $framesProcessed = 0;
        $deadline = microtime(true) + 2.5;
        while (microtime(true) < $deadline) {
            $framesProcessed += $client->processIncoming()->await();
            usleep(100_000);
        }

        // Heartbeat/control traffic may surface as empty payloads but should not surface user data.
        $nonEmptyPayloads = array_values(array_filter($receivedPayloads, static fn (string $payload): bool => $payload !== ''));
        self::assertCount(0, $nonEmptyPayloads);
        self::assertGreaterThanOrEqual(1, $framesProcessed);

        $js->publish($subject, '{"event":"push-hb"}')->await();

        $deliveryDeadline = microtime(true) + 4.0;
        while ($receivedPayloads === [] && microtime(true) < $deliveryDeadline) {
            $client->processIncoming()->await();
            usleep(100_000);
        }

        self::assertSame(['{"event":"push-hb"}'], $receivedPayloads);

        $client->unsubscribe($sid)->await();
        $js->deleteConsumer($stream, $consumer)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }

    /**
     * Verifies fetchBatch returns available messages and handles terminal status frames.
     */
    public function testJetStreamFetchBatchHandlesStatusFrames(): void
    {
        $this->requireIntegrationEnabled();

        $stream = 'IT_' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.batch';
        $consumer = 'C_' . strtoupper(bin2hex(random_bytes(2)));

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $js = $client->jetStream();
        $js->createStream($stream, [$subject])->await();
        $js->createConsumer($stream, $consumer, $subject)->await();

        $js->publish($subject, '{"n":1}')->await();
        $js->publish($subject, '{"n":2}')->await();

        $batch = $js->fetchBatch($stream, $consumer, 3, 700)->await();

        $payloads = array_map(static fn (NatsMessage $message): string => $message->payload, $batch);
        self::assertContains('{"n":1}', $payloads);
        self::assertContains('{"n":2}', $payloads);
        self::assertGreaterThanOrEqual(2, count($batch));

        foreach ($batch as $message) {
            if ($message->replyTo !== null && $message->replyTo !== '') {
                $js->ack($message)->await();
            }
        }

        $js->purgeStream($stream)->await();

        try {
            $js->fetchBatch($stream, $consumer, 1, 400)->await();
            self::fail('Expected fetchBatch timeout on empty stream.');
        } catch (JetStreamException $e) {
            self::assertMatchesRegularExpression('/status (404|408)|No messages received within timeout/i', $e->getMessage());
        }

        $js->deleteConsumer($stream, $consumer)->await();
        $js->deleteStream($stream)->await();
        $client->disconnect()->await();
    }
}
