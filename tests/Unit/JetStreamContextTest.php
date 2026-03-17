<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use DateTimeImmutable;
use DateTimeZone;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsHeaders;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\Configuration\StreamSource;
use IDCT\NATS\JetStream\Schedule;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\JetStream\Consumers\PullConsumerIterator;
use IDCT\NATS\JetStream\KeyValue\KeyValueBucket;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

final class JetStreamContextTest extends TestCase
{
    private function jsOkResponse(string $json): string
    {
        return sprintf("MSG _INBOX.any 1 %d\r\n%s\r\n", strlen($json), $json);
    }

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
     * Verifies object store context is cached per bucket.
     */
    public function testObjectStoreContextIsCachedPerBucket(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $a = $client->jetStream()->objectStore('assets');
        $b = $client->jetStream()->objectStore('assets');
        $c = $client->jetStream()->objectStore('other');

        self::assertSame($a, $b);
        self::assertNotSame($a, $c);
    }

    /**
     * Verifies key-value context is cached per bucket and typed correctly.
     */
    public function testKeyValueContextIsCachedPerBucket(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $a = $client->jetStream()->keyValue('profiles');
        $b = $client->jetStream()->keyValue('profiles');
        $c = $client->jetStream()->keyValue('sessions');

        self::assertInstanceOf(KeyValueBucket::class, $a);
        self::assertSame($a, $b);
        self::assertNotSame($a, $c);
    }

    /**
     * Verifies pullConsumer helper returns an iterator wrapper.
     */
    public function testPullConsumerReturnsIterator(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $iterator = $client->jetStream()->pullConsumer('ORDERS', 'PROC');

        self::assertInstanceOf(PullConsumerIterator::class, $iterator);
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
     * Verifies JetStream publish maps API errors to JetStreamException.
     */
    public function testPublishMapsApiError(): void
    {
        $errorPayload = '{"error":{"code":500,"description":"publish failed"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('publish failed');

        $client->jetStream()->publish('orders.created', '{"id":1}')->await();
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

    /**
     * Verifies schedule publish omits TTL header when optional value is null.
     */
    public function testPublishScheduledOmitsTtlWhenNotProvided(): void
    {
        $ackPayload = '{"stream":"SCHED","seq":8,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $when = new DateTimeImmutable('2030-01-01 00:00:00', new DateTimeZone('UTC'));

        $client->jetStream()->publishScheduled(
            'schedules.orders.one',
            'events.orders',
            '{"event":"scheduled"}',
            Schedule::at($when),
            null,
        )->await();

        self::assertStringNotContainsString('Nats-Schedule-TTL', $transport->writes[3]);
    }

    /**
     * Verifies schedule publish maps error payloads to JetStreamException.
     */
    public function testPublishScheduledMapsApiError(): void
    {
        $errorPayload = '{"error":{"code":503,"description":"scheduler down"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $when = new DateTimeImmutable('2030-01-01 00:00:00', new DateTimeZone('UTC'));

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('scheduler down');

        $client->jetStream()->publishScheduled(
            'schedules.orders.one',
            'events.orders',
            '{"event":"scheduled"}',
            Schedule::at($when),
        )->await();
    }

    /**
     * Verifies pull consumer fetch uses MSG.NEXT endpoint and returns message payload.
     */
    public function testFetchNext(): void
    {
        $deliveryPayload = '{"event":"created"}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($deliveryPayload), $deliveryPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $message = $client->jetStream()->fetchNext('ORDERS', 'PROC', 2500)->await();

        self::assertSame('{"event":"created"}', $message->payload);
        self::assertStringStartsWith('PUB $JS.API.CONSUMER.MSG.NEXT.ORDERS.PROC _INBOX.', $transport->writes[3]);
        self::assertStringContainsString('"expires":2500000000', $transport->writes[3]);
    }

    /**
     * Verifies pull fetch rejects invalid expiration values.
     */
    public function testFetchNextRejectsInvalidExpiresMs(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Pull fetch expiresMs must be greater than zero');

        $client->jetStream()->fetchNext('ORDERS', 'PROC', 0)->await();
    }

    /**
     * Verifies ACK helpers publish expected protocol tokens to reply subject.
     */
    public function testAckHelpersPublishProtocolTokens(): void
    {
        $deliveryPayload = '{"event":"created"}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 reply.ack %d\r\n%s\r\n", strlen($deliveryPayload), $deliveryPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $message = $client->request('$JS.API.CONSUMER.MSG.NEXT.ORDERS.PROC', '{}')->await();

        $js = $client->jetStream();
        $js->ack($message)->await();
        $js->nak($message)->await();
        $js->nakWithDelay($message, 1500)->await();
        $js->term($message)->await();
        $js->inProgress($message)->await();

        $ackWrites = array_slice($transport->writes, -5);

        self::assertCount(5, $ackWrites);
        self::assertStringStartsWith('PUB reply.ack 4', $ackWrites[0]);
        self::assertStringStartsWith('PUB reply.ack 4', $ackWrites[1]);
        self::assertStringStartsWith('PUB reply.ack ', $ackWrites[2]);
        self::assertStringStartsWith('PUB reply.ack 5', $ackWrites[3]);
        self::assertStringStartsWith('PUB reply.ack 4', $ackWrites[4]);
        self::assertStringContainsString("\r\n+ACK\r\n", $ackWrites[0]);
        self::assertStringContainsString("\r\n-NAK\r\n", $ackWrites[1]);
        self::assertStringContainsString("\r\n-NAK {\"delay\":1500000000}\r\n", $ackWrites[2]);
        self::assertStringContainsString("\r\n+TERM\r\n", $ackWrites[3]);
        self::assertStringContainsString("\r\n+WPI\r\n", $ackWrites[4]);
    }

    /**
     * Verifies delayed NAK rejects invalid delay values.
     */
    public function testNakWithDelayRejectsInvalidDelay(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('JetStream delayed NAK requires delayMs greater than zero');

        $message = new \IDCT\NATS\Core\NatsMessage('orders.created', 1, 'reply.ack', '{"event":"created"}');
        $client->jetStream()->nakWithDelay($message, 0)->await();
    }

    /**
     * Verifies ACK helpers fail fast for messages without reply subject.
     */
    public function testAckRequiresReplySubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('JetStream ACK requires a reply subject on the delivered message');

        $message = new \IDCT\NATS\Core\NatsMessage('orders.created', 1, null, '{"event":"created"}');
        $client->jetStream()->ack($message)->await();
    }

    /**
     * Verifies push consumer creation sets deliver subject in consumer config.
     */
    public function testCreatePushConsumer(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC","deliver_subject":"deliver.proc"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $created = $client->jetStream()->createPushConsumer('ORDERS', 'PROC', 'deliver.proc', 'orders.*')->await();

        self::assertSame('PROC', $created->name);
        self::assertTrue($created->push);
        self::assertStringContainsString('$JS.API.CONSUMER.CREATE.ORDERS.PROC', $transport->writes[3]);
        self::assertStringContainsString('"ack_policy":"explicit"', $transport->writes[3]);
        self::assertStringContainsString('"deliver_subject":"deliver.proc"', $transport->writes[3]);
    }

    /**
     * Verifies explicit ephemeral push consumer creation omits durable_name in payload.
     */
    public function testCreateEphemeralPushConsumer(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"EP1","config":{"deliver_subject":"deliver.ep"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $consumer = $client->jetStream()->createEphemeralPushConsumer('ORDERS', 'deliver.ep', 'orders.*')->await();

        self::assertSame('EP1', $consumer->name);
        self::assertStringContainsString('$JS.API.CONSUMER.CREATE.ORDERS', $transport->writes[3]);
        self::assertStringContainsString('"deliver_subject":"deliver.ep"', $transport->writes[3]);
        self::assertStringNotContainsString('"durable_name"', $transport->writes[3]);
    }

    /**
     * Verifies push subscription auto-responds to flow-control and forwards payload deliveries.
     */
    public function testSubscribePushConsumerHandlesFlowControl(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC","deliver_subject":"deliver.proc"}}';
        $flowHeaders = NatsHeaders::toWireBlock([
            'Status' => '100',
            'Description' => 'FlowControl Request',
        ]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            sprintf(
                "HMSG deliver.proc 2 fc.reply %d %d\r\n%s\r\n",
                strlen($flowHeaders),
                strlen($flowHeaders),
                $flowHeaders,
            ),
            "MSG deliver.proc 2 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $received = null;
        $client->jetStream()->subscribePushConsumer(
            'ORDERS',
            'PROC',
            static function (\IDCT\NATS\Core\NatsMessage $message) use (&$received): void {
                $received = $message;
            },
            'deliver.proc',
            'orders.*',
        )->await();

        $client->processIncoming()->await();
        $client->processIncoming()->await();

        self::assertStringContainsString("PUB fc.reply 0\r\n\r\n", implode('', $transport->writes));
        self::assertInstanceOf(\IDCT\NATS\Core\NatsMessage::class, $received);
        self::assertSame('hello', $received->payload);
    }

    /**
     * Verifies heartbeat control messages are ignored and not forwarded to user handlers.
     */
    public function testSubscribePushConsumerIgnoresHeartbeat(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC","deliver_subject":"deliver.proc"}}';
        $heartbeatHeaders = NatsHeaders::toWireBlock([
            'Status' => '100',
            'Description' => 'Idle Heartbeat',
        ]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            sprintf(
                "HMSG deliver.proc 2 hb.reply %d %d\r\n%s\r\n",
                strlen($heartbeatHeaders),
                strlen($heartbeatHeaders),
                $heartbeatHeaders,
            ),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $handled = false;
        $client->jetStream()->subscribePushConsumer(
            'ORDERS',
            'PROC',
            static function () use (&$handled): void {
                $handled = true;
            },
            'deliver.proc',
            'orders.*',
        )->await();

        $client->processIncoming()->await();

        self::assertFalse($handled);
        self::assertStringNotContainsString('PUB hb.reply 0', implode('', $transport->writes));
    }

    /**
     * Verifies ephemeral pull consumer creation uses stream-level create endpoint.
     */
    public function testCreateEphemeralConsumer(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"E1","config":{"ack_policy":"explicit"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $consumer = $client->jetStream()->createEphemeralConsumer('ORDERS', 'orders.*')->await();

        self::assertSame('E1', $consumer->name);
        self::assertStringContainsString('$JS.API.CONSUMER.CREATE.ORDERS', $transport->writes[3]);
        self::assertStringNotContainsString('$JS.API.CONSUMER.CREATE.ORDERS.', $transport->writes[3]);
        self::assertStringContainsString('"ack_policy":"explicit"', $transport->writes[3]);
        self::assertStringContainsString('"filter_subject":"orders.*"', $transport->writes[3]);
        self::assertStringNotContainsString('"durable_name"', $transport->writes[3]);
    }

    /**
     * Verifies ephemeral push subscription helper creates consumer and receives payload.
     */
    public function testSubscribeEphemeralPushConsumer(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"E_PUSH","config":{"deliver_subject":"deliver.ephemeral"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            "MSG deliver.ephemeral 2 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $received = null;
        $client->jetStream()->subscribeEphemeralPushConsumer(
            'ORDERS',
            static function (\IDCT\NATS\Core\NatsMessage $message) use (&$received): void {
                $received = $message;
            },
            'deliver.ephemeral',
            'orders.*',
        )->await();

        $client->processIncoming()->await();

        self::assertInstanceOf(\IDCT\NATS\Core\NatsMessage::class, $received);
        self::assertSame('hello', $received->payload);
        self::assertStringContainsString('"deliver_subject":"deliver.ephemeral"', $transport->writes[3]);
        self::assertStringNotContainsString('"durable_name"', $transport->writes[3]);
    }

    // ─── Input Validation ─────────────────────────────────────────────

    public function testCreateStreamRejectsEmptySubjects(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Stream subjects must not be empty unless mirror configuration is provided');
        $client->jetStream()->createStream('test', [])->await();
    }

    public function testCreateStreamAllowsMirrorWithoutSubjects(): void
    {
        $streamPayload = '{"config":{"name":"MIRROR","subjects":[],"mirror":{"name":"ORIGIN"}}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamPayload), $streamPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $created = $client->jetStream()->createStream('MIRROR', [], [
            'mirror' => StreamSource::mirror('ORIGIN')->toArray(),
        ])->await();

        self::assertSame('MIRROR', $created->name);
        self::assertSame([], $created->subjects);
        self::assertStringContainsString('"mirror":{"name":"ORIGIN"}', $transport->writes[3]);
        self::assertStringContainsString('"subjects":[]', $transport->writes[3]);
    }

    public function testCreateConsumerRejectsEmptyFilterSubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Consumer filter subject must not be empty');
        $client->jetStream()->createConsumer('ORDERS', 'c1', '')->await();
    }

    public function testRequestJsonWrapsJsonException(): void
    {
        $malformedPayload = 'NOT_JSON{';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.any 1 %d\r\n%s\r\n", strlen($malformedPayload), $malformedPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Malformed JetStream API response');
        $client->jetStream()->accountInfo()->await();
    }

    // ─── Phase 3: Feature Gaps ────────────────────────────────────────

    public function testUpdateStream(): void
    {
        $responsePayload = '{"config":{"name":"ORDERS","subjects":["orders.>","events.>"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($responsePayload), $responsePayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $updated = $client->jetStream()->updateStream('ORDERS', [
            'subjects' => ['orders.>', 'events.>'],
        ])->await();

        self::assertSame('ORDERS', $updated->name);
        self::assertSame(['orders.>', 'events.>'], $updated->subjects);
        self::assertStringContainsString('$JS.API.STREAM.UPDATE.ORDERS', $transport->writes[3]);
    }

    public function testCreateConsumerWithOptions(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC","max_deliver":5}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $consumer = $client->jetStream()->createConsumer('ORDERS', 'PROC', 'orders.*', [
            'ack_policy' => 'all',
            'max_deliver' => 5,
            'ack_wait' => 30_000_000_000,
            'max_ack_pending' => 100,
        ])->await();

        self::assertSame('PROC', $consumer->name);
        $written = $transport->writes[3];
        self::assertStringContainsString('"ack_policy":"all"', $written);
        self::assertStringContainsString('"max_deliver":5', $written);
        self::assertStringContainsString('"ack_wait":30000000000', $written);
        self::assertStringContainsString('"max_ack_pending":100', $written);
        self::assertStringContainsString('"filter_subject":"orders.*"', $written);
    }

    public function testCreatePushConsumerAllowsAckPolicyOverride(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC","deliver_subject":"deliver.proc","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->createPushConsumer('ORDERS', 'PROC', 'deliver.proc', 'orders.*', [
            'ack_policy' => 'none',
        ])->await();

        self::assertStringContainsString('"ack_policy":"none"', $transport->writes[3]);
    }

    public function testFetchBatch(): void
    {
        $msg1 = '{"event":"first"}';
        $msg2 = '{"event":"second"}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg1), $msg1),
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg2), $msg2),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $messages = $client->jetStream()->fetchBatch('ORDERS', 'PROC', 2, 2500)->await();

        self::assertCount(2, $messages);
        self::assertSame('{"event":"first"}', $messages[0]->payload);
        self::assertSame('{"event":"second"}', $messages[1]->payload);

        $written = $transport->writes[3];
        self::assertStringContainsString('"batch":2', $written);
        self::assertStringContainsString('"expires":2500000000', $written);
    }

    public function testFetchBatchRejectsInvalidBatch(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Pull fetch batch must be greater than zero');
        $client->jetStream()->fetchBatch('ORDERS', 'PROC', 0)->await();
    }

    public function testFetchBatchIgnoresTerminalStatusFrames(): void
    {
        $msg1 = '{"event":"first"}';
        $statusHeaders = "NATS/1.0 404 No Messages\r\nStatus: 404\r\nDescription: No Messages\r\n\r\n";
        $headerBytes = strlen($statusHeaders);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg1), $msg1),
            sprintf("HMSG _INBOX.JS.FETCH.a 1 %d %d\r\n%s\r\n", $headerBytes, $headerBytes, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $messages = $client->jetStream()->fetchBatch('ORDERS', 'PROC', 2, 2500)->await();

        self::assertCount(1, $messages);
        self::assertSame('{"event":"first"}', $messages[0]->payload);
    }

    public function testFetchBatchIgnoresStatus100ControlFrames(): void
    {
        $msg1 = '{"event":"first"}';
        $controlHeaders = "NATS/1.0 100 Idle Heartbeat\r\nStatus: 100\r\nDescription: Idle Heartbeat\r\n\r\n";
        $headerBytes = strlen($controlHeaders);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("HMSG _INBOX.JS.FETCH.a 1 %d %d\r\n%s\r\n", $headerBytes, $headerBytes, $controlHeaders),
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg1), $msg1),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $messages = $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500)->await();

        self::assertCount(1, $messages);
        self::assertSame('{"event":"first"}', $messages[0]->payload);
    }

    public function testFetchBatchThrowsWhenNoMessagesArrive(): void
    {
        $statusHeaders = "NATS/1.0 404 No Messages\r\nStatus: 404\r\nDescription: No Messages\r\n\r\n";
        $headerBytes = strlen($statusHeaders);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("HMSG _INBOX.JS.FETCH.a 1 %d %d\r\n%s\r\n", $headerBytes, $headerBytes, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('JetStream pull request ended with status 404: No Messages');

        $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500)->await();
    }

    public function testFetchBatchThrowsTerminalStatusDescription(): void
    {
        $statusHeaders = "NATS/1.0 409 MaxAckPending Exceeded\r\nStatus: 409\r\nDescription: MaxAckPending Exceeded\r\n\r\n";
        $headerBytes = strlen($statusHeaders);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("HMSG _INBOX.JS.FETCH.a 1 %d %d\r\n%s\r\n", $headerBytes, $headerBytes, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500)->await();
            self::fail('Expected terminal pull status to raise JetStreamException.');
        } catch (JetStreamException $e) {
            self::assertSame(409, $e->getCode());
            self::assertStringContainsString('status 409: MaxAckPending Exceeded', $e->getMessage());
        }
    }

    // ─── Consumer Pause/Resume ──────────────────────────────────────────

    public function testPauseConsumerSendsCorrectPayload(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            $this->jsOkResponse('{"paused":true,"pause_until":"2026-12-01T00:00:00Z"}'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $result = $client->jetStream()->pauseConsumer('ORDERS', 'PROC', '2026-12-01T00:00:00Z')->await();

        self::assertTrue($result['paused'] ?? false);

        $written = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.CONSUMER.PAUSE.ORDERS.PROC', $written);
        self::assertStringContainsString('"pause_until":"2026-12-01T00:00:00Z"', $written);
    }

    public function testResumeConsumerSendsEmptyBody(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            $this->jsOkResponse('{"paused":false}'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $result = $client->jetStream()->resumeConsumer('ORDERS', 'PROC')->await();

        self::assertFalse($result['paused'] ?? true);

        $written = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.CONSUMER.PAUSE.ORDERS.PROC', $written);
    }

    // ─── Ordered Consumer ───────────────────────────────────────────────

    public function testSubscribeOrderedConsumerSendsCorrectConfig(): void
    {
        $consumerCreateResponse = json_encode([
            'stream_name' => 'ORDERS',
            'name' => 'ephemeral_ordered',
            'config' => [
                'ack_policy' => 'none',
                'flow_control' => true,
                'idle_heartbeat' => 5000000000,
                'mem_storage' => true,
                'max_deliver' => 1,
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            $this->jsOkResponse($consumerCreateResponse),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->subscribeOrderedConsumer('ORDERS', function (NatsMessage $msg): void {
        })->await();

        $written = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.CONSUMER.CREATE.ORDERS', $written);
        self::assertStringContainsString('"flow_control":true', $written);
        self::assertStringContainsString('"idle_heartbeat":5000000000', $written);
        self::assertStringContainsString('"ack_policy":"none"', $written);
        self::assertStringContainsString('"mem_storage":true', $written);
    }

    // ─── Stream Purge / List / Consumer List / Direct Get ────────────────

    public function testPurgeStream(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            $this->jsOkResponse('{"purged":42}'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $result = $client->jetStream()->purgeStream('ORDERS')->await();

        self::assertSame(42, $result['purged']);
        self::assertStringContainsString('$JS.API.STREAM.PURGE.ORDERS', implode('', $transport->writes));
    }

    public function testPurgeStreamWithSubjectFilter(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            $this->jsOkResponse('{"purged":10}'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $result = $client->jetStream()->purgeStream('ORDERS', ['filter' => 'orders.old'])->await();

        self::assertSame(10, $result['purged']);
        self::assertStringContainsString('"filter":"orders.old"', implode('', $transport->writes));
    }

    public function testListStreams(): void
    {
        $listPayload = json_encode([
            'streams' => [
                ['config' => ['name' => 'ORDERS', 'subjects' => ['orders.>']]],
                ['config' => ['name' => 'EVENTS', 'subjects' => ['events.>']]],
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            $this->jsOkResponse($listPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $streams = $client->jetStream()->listStreams()->await();

        self::assertCount(2, $streams);
        self::assertSame('ORDERS', $streams[0]->name);
        self::assertSame('EVENTS', $streams[1]->name);
        self::assertStringContainsString('$JS.API.STREAM.LIST', implode('', $transport->writes));
    }

    public function testListStreamsWithSubjectFilter(): void
    {
        $listPayload = json_encode([
            'streams' => [
                ['config' => ['name' => 'ORDERS', 'subjects' => ['orders.>']]],
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            $this->jsOkResponse($listPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $streams = $client->jetStream()->listStreams(['subject' => 'orders.>'])->await();

        self::assertCount(1, $streams);
        self::assertSame('ORDERS', $streams[0]->name);
        self::assertStringContainsString('"subject":"orders.>"', implode('', $transport->writes));
    }

    public function testListConsumers(): void
    {
        $listPayload = json_encode([
            'consumers' => [
                ['stream_name' => 'ORDERS', 'name' => 'A', 'config' => ['durable_name' => 'A']],
                ['stream_name' => 'ORDERS', 'name' => 'B', 'config' => ['durable_name' => 'B', 'deliver_subject' => 'push']],
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            $this->jsOkResponse($listPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $consumers = $client->jetStream()->listConsumers('ORDERS')->await();

        self::assertCount(2, $consumers);
        self::assertSame('A', $consumers[0]->name);
        self::assertFalse($consumers[0]->push);
        self::assertSame('B', $consumers[1]->name);
        self::assertTrue($consumers[1]->push);
        self::assertStringContainsString('$JS.API.CONSUMER.LIST.ORDERS', implode('', $transport->writes));
    }

    public function testGetStreamMessage(): void
    {
        $msgPayload = json_encode([
            'message' => [
                'subject' => 'orders.created',
                'data' => base64_encode('{"id":1}'),
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            $this->jsOkResponse($msgPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $message = $client->jetStream()->getStreamMessage('ORDERS', 1)->await();

        self::assertSame('orders.created', $message->subject);
        self::assertSame('{"id":1}', $message->payload);
        $written = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.STREAM.MSG.GET.ORDERS', $written);
        self::assertStringContainsString('"seq":1', $written);
    }

    public function testExtractStreamSequenceParsesReplySubject(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $method = new \ReflectionMethod($js, 'extractStreamSequence');

        $message = new NatsMessage('s', 1, '$JS.ACK.ORDERS.CONS.1.42.2.123.0', 'x');
        $parsed = $method->invoke($js, $message);

        self::assertSame(42, $parsed);
    }

    public function testExtractStreamSequenceReturnsNullForInvalidReplySubject(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $method = new \ReflectionMethod($js, 'extractStreamSequence');

        $noReply = new NatsMessage('s', 1, null, 'x');
        $shortReply = new NatsMessage('s', 1, '$JS.ACK.short', 'x');
        $wrongPrefix = new NatsMessage('s', 1, '$JS.FC.ORDERS.token', 'x');
        $nonInt = new NatsMessage('s', 1, '$JS.ACK.ORDERS.CONS.1.NaN.2.123.0', 'x');

        self::assertNull($method->invoke($js, $noReply));
        self::assertNull($method->invoke($js, $shortReply));
        self::assertNull($method->invoke($js, $wrongPrefix));
        self::assertNull($method->invoke($js, $nonInt));
    }

    public function testHandlePushControlMessageReturnsFalseForNonControlStatus(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $method = new \ReflectionMethod($js, 'handlePushControlMessage');

        $headers = NatsHeaders::toWireBlock([
            'Status' => '404',
            'Description' => 'No Messages',
        ]);
        $message = new NatsMessage('deliver', 1, null, '', $headers);

        self::assertFalse($method->invoke($js, $message)->await());
    }

    public function testHandlePushControlMessageHeartbeatWithoutReplyReturnsTrue(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $method = new \ReflectionMethod($js, 'handlePushControlMessage');

        $headers = NatsHeaders::toWireBlock([
            'Status' => '100',
            'Description' => 'Idle Heartbeat',
        ]);
        $message = new NatsMessage('deliver', 1, null, '', $headers);

        self::assertTrue($method->invoke($js, $message)->await());
    }

    public function testHandlePushControlMessageRepliesToJetStreamFlowControlSubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();
        $js = $client->jetStream();

        $method = new \ReflectionMethod($js, 'handlePushControlMessage');

        $headers = "NATS/1.0 100 Idle Heartbeat\r\nStatus: 100\r\nDescription: Idle Heartbeat\r\n\r\n";
        $message = new NatsMessage('deliver', 1, '$JS.FC.ORDERS.token', '', $headers);

        self::assertTrue($method->invoke($js, $message)->await());
        self::assertStringContainsString('PUB $JS.FC.ORDERS.token 0' . "\r\n\r\n", implode('', $transport->writes));
    }
}
