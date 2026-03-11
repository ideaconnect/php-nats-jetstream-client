<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Integration;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
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

        $bucket = 'obj' . strtolower(bin2hex(random_bytes(2)));
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
}
