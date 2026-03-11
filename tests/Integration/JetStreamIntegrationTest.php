<?php

declare(strict_types=1);

namespace Idct\Nats\Tests\Integration;

use Idct\Nats\Connection\NatsOptions;
use Idct\Nats\Core\NatsClient;
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
}
