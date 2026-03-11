<?php

declare(strict_types=1);

namespace Idct\Nats\Tests\Unit;

use DateTimeImmutable;
use DateTimeZone;
use Idct\Nats\Connection\NatsOptions;
use Idct\Nats\Core\NatsClient;
use Idct\Nats\Exception\JetStreamException;
use Idct\Nats\JetStream\Schedule;
use Idct\Nats\JetStream\JetStreamContext;
use Idct\Nats\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

final class JetStreamContextTest extends TestCase
{
    /**
     * Verifies accountInfo() returns parsed account metrics.
     */
    public function testAccountInfo(): void
    {
        $accountPayload = '{"memory":11,"storage":22,"streams":3,"consumers":4}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.any 1 %d\r\n%s\r\n", strlen($accountPayload), $accountPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $account = $client->jetStream()->accountInfo()->await();

        self::assertSame(11, $account->memory);
        self::assertSame(22, $account->storage);
        self::assertStringStartsWith('PUB $JS.API.INFO _INBOX.', $transport->writes[3]);
    }

    /**
     * Verifies create/get/delete stream operations map expected payload fields.
     */
    public function testStreamCrud(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            "MSG _INBOX.a 1 52\r\n{\"config\":{\"name\":\"ORDERS\",\"subjects\":[\"orders.*\"]}}\r\n",
            "MSG _INBOX.b 2 52\r\n{\"config\":{\"name\":\"ORDERS\",\"subjects\":[\"orders.*\"]}}\r\n",
            "MSG _INBOX.c 3 16\r\n{\"success\":true}\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $js = $client->jetStream();
        $created = $js->createStream('ORDERS', ['orders.*'])->await();
        $fetched = $js->getStream('ORDERS')->await();
        $deleted = $js->deleteStream('ORDERS')->await();

        self::assertSame('ORDERS', $created->name);
        self::assertSame(['orders.*'], $created->subjects);
        self::assertSame('ORDERS', $fetched->name);
        self::assertTrue($deleted);
        self::assertStringContainsString('$JS.API.STREAM.CREATE.ORDERS', $transport->writes[3]);
        self::assertStringContainsString('$JS.API.STREAM.INFO.ORDERS', $transport->writes[6]);
        self::assertStringContainsString('$JS.API.STREAM.DELETE.ORDERS', $transport->writes[9]);
    }

    /**
     * Verifies JetStream API error payloads are converted to JetStreamException.
     */
    public function testJetStreamApiErrorMapping(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            "MSG _INBOX.a 1 48\r\n{\"error\":{\"code\":404,\"description\":\"not found\"}}\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('not found');

        $client->jetStream()->getStream('MISSING')->await();
    }

    /**
     * Verifies the client returns the same JetStream context instance on repeated access.
     */
    public function testJetStreamContextIsCached(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $a = $client->jetStream();
        $b = $client->jetStream();

        self::assertInstanceOf(JetStreamContext::class, $a);
        self::assertSame($a, $b);
    }

    /**
     * Verifies consumer create/get/delete operations map expected payload fields.
     */
    public function testConsumerCrud(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC"}}';
        $infoPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC"}}';
        $deletePayload = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($infoPayload), $infoPayload),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($deletePayload), $deletePayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $js = $client->jetStream();
        $created = $js->createConsumer('ORDERS', 'PROC', 'orders.*')->await();
        $fetched = $js->getConsumer('ORDERS', 'PROC')->await();
        $deleted = $js->deleteConsumer('ORDERS', 'PROC')->await();

        self::assertSame('ORDERS', $created->streamName);
        self::assertSame('PROC', $created->name);
        self::assertSame('PROC', $fetched->name);
        self::assertTrue($deleted);
        self::assertStringContainsString('$JS.API.CONSUMER.CREATE.ORDERS.PROC', $transport->writes[3]);
        self::assertStringContainsString('$JS.API.CONSUMER.INFO.ORDERS.PROC', $transport->writes[6]);
        self::assertStringContainsString('$JS.API.CONSUMER.DELETE.ORDERS.PROC', $transport->writes[9]);
    }

    /**
     * Verifies JetStream publish returns stream/sequence acknowledgment.
     */
    public function testPublishWithAck(): void
    {
        $ackPayload = '{"stream":"ORDERS","seq":42,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->publish('orders.created', '{"id":1}')->await();

        self::assertSame('ORDERS', $ack->stream);
        self::assertSame(42, $ack->seq);
        self::assertFalse($ack->duplicate);
        self::assertStringStartsWith('PUB orders.created _INBOX.', $transport->writes[3]);
    }

    /**
     * Verifies stream creation forwards additional stream configuration options.
     */
    public function testCreateStreamWithOptions(): void
    {
        $streamPayload = '{"config":{"name":"SCHED","subjects":["schedules.>","events.>"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamPayload), $streamPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->createStream(
            'SCHED',
            ['schedules.>', 'events.>'],
            ['allow_msg_schedules' => true],
        )->await();

        self::assertStringContainsString('"allow_msg_schedules":true', $transport->writes[3]);
    }

    /**
     * Verifies scheduled publish sends scheduler headers through HPUB request.
     */
    public function testPublishScheduled(): void
    {
        $ackPayload = '{"stream":"SCHED","seq":7,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $when = new DateTimeImmutable('2030-01-01 00:00:00', new DateTimeZone('UTC'));

        $ack = $client->jetStream()->publishScheduled(
            'schedules.orders.one',
            'events.orders',
            '{"event":"scheduled"}',
            Schedule::at($when),
            '5m',
        )->await();

        self::assertSame('SCHED', $ack->stream);
        self::assertSame(7, $ack->seq);
        self::assertStringStartsWith('HPUB schedules.orders.one _INBOX.', $transport->writes[3]);
        self::assertStringContainsString('Nats-Schedule:@at 2030-01-01T00:00:00Z', $transport->writes[3]);
        self::assertStringContainsString('Nats-Schedule-Target:events.orders', $transport->writes[3]);
        self::assertStringContainsString('Nats-Schedule-TTL:5m', $transport->writes[3]);
    }

    /**
     * Verifies non-@at schedule expressions are rejected before request dispatch.
     */
    public function testPublishScheduledRejectsUnsupportedPattern(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Only @at schedule expressions are currently supported');

        try {
            $client->jetStream()->publishScheduled(
                'schedules.orders.one',
                'events.orders',
                '{"event":"scheduled"}',
                '@every 10s',
            )->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }
}
