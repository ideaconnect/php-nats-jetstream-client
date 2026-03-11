<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\KeyValueEntry;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

final class KeyValueBucketTest extends TestCase
{
    /**
     * Verifies KV bucket create/delete map to KV stream lifecycle APIs.
     */
    public function testBucketCreateAndDelete(): void
    {
        $createPayload = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]}}';
        $deletePayload = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($deletePayload), $deletePayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('cfg');
        $created = $kv->create()->await();
        $deleted = $kv->deleteBucket()->await();

        self::assertSame('KV_cfg', $created->name);
        self::assertTrue($deleted);
        self::assertStringContainsString('$JS.API.STREAM.CREATE.KV_cfg', $transport->writes[3]);
        self::assertStringContainsString('$JS.API.STREAM.DELETE.KV_cfg', $transport->writes[6]);
    }

    /**
     * Verifies KV put/get/delete operations map and parse values correctly.
     */
    public function testPutGetDelete(): void
    {
        $putAck = '{"stream":"KV_cfg","seq":1,"duplicate":false}';
        $getPayload = sprintf(
            '{"message":{"subject":"$KV.cfg.theme","seq":1,"data":"%s"}}',
            base64_encode('blue'),
        );
        $deleteAck = '{"stream":"KV_cfg","seq":2,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($getPayload), $getPayload),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($deleteAck), $deleteAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('cfg');
        $put = $kv->put('theme', 'blue')->await();
        $entry = $kv->get('theme')->await();
        $delete = $kv->delete('theme')->await();

        self::assertSame('KV_cfg', $put->stream);
        self::assertInstanceOf(KeyValueEntry::class, $entry);
        self::assertSame('theme', $entry->key);
        self::assertSame('blue', $entry->value);
        self::assertSame('PUT', $entry->operation);
        self::assertSame('KV_cfg', $delete->stream);

        self::assertStringStartsWith('PUB $KV.cfg.theme _INBOX.', $transport->writes[3]);
        self::assertStringStartsWith('PUB $JS.API.STREAM.MSG.GET.KV_cfg _INBOX.', $transport->writes[6]);
        self::assertStringStartsWith('HPUB $KV.cfg.theme _INBOX.', $transport->writes[9]);
        self::assertStringContainsString('KV-Operation:DEL', $transport->writes[9]);
    }

    /**
     * Verifies get() returns null for missing keys.
     */
    public function testGetMissingReturnsNull(): void
    {
        $missingPayload = '{"error":{"code":404,"description":"message not found"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($missingPayload), $missingPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $entry = $client->jetStream()->keyValue('cfg')->get('missing')->await();

        self::assertNull($entry);
    }

    /**
     * Verifies invalid KV keys are rejected.
     */
    public function testInvalidKeyRejected(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid KV key');

        $client->jetStream()->keyValue('cfg')->put('a b', 'x')->await();
    }

    /**
     * Verifies optimistic update sends expected revision header.
     */
    public function testUpdateWithExpectedRevision(): void
    {
        $updateAck = '{"stream":"KV_cfg","seq":3,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($updateAck), $updateAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->keyValue('cfg')->update('theme', 'green', 2)->await();

        self::assertSame(3, $ack->seq);
        self::assertStringStartsWith('HPUB $KV.cfg.theme _INBOX.', $transport->writes[3]);
        self::assertStringContainsString('Nats-Expected-Last-Subject-Sequence:2', $transport->writes[3]);
    }

    /**
     * Verifies purge sends KV-Operation PURGE and Nats-Rollup headers.
     */
    public function testPurge(): void
    {
        $purgeAck = '{"stream":"KV_cfg","seq":4,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($purgeAck), $purgeAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->keyValue('cfg')->purge('theme')->await();

        self::assertSame(4, $ack->seq);
        self::assertStringContainsString('KV-Operation:PURGE', $transport->writes[3]);
        self::assertStringContainsString('Nats-Rollup:sub', $transport->writes[3]);
    }

    /**
     * Verifies getStatus maps stream state counters.
     */
    public function testGetStatus(): void
    {
        $streamInfo = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]},"state":{"messages":7,"bytes":128,"subjects":{"$KV.cfg.theme":3}}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $status = $client->jetStream()->keyValue('cfg')->getStatus()->await();

        self::assertSame('cfg', $status['bucket']);
        self::assertSame('KV_cfg', $status['stream']);
        self::assertSame(7, $status['messages']);
        self::assertSame(128, $status['bytes']);
    }

    /**
     * Verifies getAll returns only the latest non-deleted values by key.
     */
    public function testGetAll(): void
    {
        $streamInfo = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]},"state":{"messages":4,"bytes":256,"subjects":{"$KV.cfg.username":2,"$KV.cfg.email":2}}}';
        $purgeHeaders = base64_encode("NATS/1.0\r\nKV-Operation:PURGE\r\n\r\n");
        // last_by_subj for username → purged
        $mUsername = sprintf('{"message":{"subject":"$KV.cfg.username","seq":3,"data":"","hdrs":"%s"}}', $purgeHeaders);
        // last_by_subj for email → latest value
        $mEmail = sprintf('{"message":{"subject":"$KV.cfg.email","seq":4,"data":"%s"}}', base64_encode('b@example.com'));

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($mUsername), $mUsername),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($mEmail), $mEmail),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $all = $client->jetStream()->keyValue('cfg')->getAll()->await();

        self::assertSame(['email' => 'b@example.com'], $all);
    }

    // ─── Key Validation ─────────────────────────────────────────────

    public function testPutAcceptsKeyWithDotsColonsSlashes(): void
    {
        $putAck = '{"stream":"KV_cfg","seq":1,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->keyValue('cfg')->put('config/v2:main.yaml', 'data')->await();
        self::assertSame(1, $ack->seq);
    }

    public function testPutRejectsKeyWithWildcard(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid KV key');
        $client->jetStream()->keyValue('cfg')->put('foo*bar', 'data')->await();
    }

    public function testPutRejectsKeyWithTab(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid KV key');
        $client->jetStream()->keyValue('cfg')->put("foo\tbar", 'data')->await();
    }

    // ─── KV Options Mapping ─────────────────────────────────────────

    public function testCreateWithSemanticOptions(): void
    {
        $createPayload = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('cfg')->create([
            'history' => 5,
            'ttl' => 86400000000000,
            'max_value_size' => 1024,
            'storage' => 'memory',
            'num_replicas' => 3,
        ])->await();

        $written = $transport->writes[3];
        self::assertStringContainsString('"max_msgs_per_subject":5', $written);
        self::assertStringContainsString('"max_age":86400000000000', $written);
        self::assertStringContainsString('"max_msg_size":1024', $written);
        self::assertStringContainsString('"storage":"memory"', $written);
        self::assertStringContainsString('"num_replicas":3', $written);
    }
}
