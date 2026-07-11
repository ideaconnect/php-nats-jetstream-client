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
use IDCT\NATS\Exception\NatsException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Exception\UnsupportedFeatureException;
use IDCT\NATS\JetStream\Configuration\StreamSource;
use IDCT\NATS\JetStream\Consumers\PullConsumerIterator;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\JetStream\KeyValue\KeyValueBucket;
use IDCT\NATS\JetStream\Models\StreamInfo;
use IDCT\NATS\JetStream\Schedule;
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
     * Verifies addStream() builds the CREATE payload from a typed StreamConfiguration (#53).
     */
    public function testAddStreamFromBuilder(): void
    {
        $reply = '{"config":{"name":"ORDERS","subjects":["orders.*"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $config = \IDCT\NATS\JetStream\Configuration\StreamConfiguration::create('ORDERS')
            ->subjects('orders.*', 'orders.archive')
            ->retention(\IDCT\NATS\JetStream\Enum\RetentionPolicy::WorkQueue)
            ->storage(\IDCT\NATS\JetStream\Enum\StorageBackend::Memory)
            ->maxBytes(4096)
            ->maxAge(60)
            ->replicas(3);

        $info = $client->jetStream()->addStream($config)->await();

        self::assertSame('ORDERS', $info->name);
        $create = $transport->writes[3];
        self::assertStringContainsString('$JS.API.STREAM.CREATE.ORDERS', $create);
        self::assertStringContainsString('"subjects":["orders.*","orders.archive"]', $create);
        self::assertStringContainsString('"retention":"workqueue"', $create);
        self::assertStringContainsString('"storage":"memory"', $create);
        self::assertStringContainsString('"max_bytes":4096', $create);
        self::assertStringContainsString('"max_age":60000000000', $create);
        self::assertStringContainsString('"num_replicas":3', $create);
    }

    /**
     * Verifies addConsumer() builds the CREATE payload from a typed ConsumerConfiguration (#54).
     */
    public function testAddConsumerFromBuilder(): void
    {
        $reply = '{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $config = \IDCT\NATS\JetStream\Configuration\ConsumerConfiguration::create()
            ->durable('worker')
            ->ackPolicy(\IDCT\NATS\JetStream\Enum\AckPolicy::Explicit)
            ->maxDeliver(5)
            ->ackWait(1000)
            ->backoff([1000, 2000]);

        $info = $client->jetStream()->addConsumer('ORDERS', $config)->await();

        self::assertSame('worker', $info->name);
        $create = $transport->writes[3];
        self::assertStringContainsString('$JS.API.CONSUMER.CREATE.ORDERS.worker', $create);
        self::assertStringContainsString('"durable_name":"worker"', $create);
        self::assertStringContainsString('"ack_policy":"explicit"', $create);
        self::assertStringContainsString('"max_deliver":5', $create);
        self::assertStringContainsString('"ack_wait":1000000000', $create);
        self::assertStringContainsString('"backoff":[1000000000,2000000000]', $create);
    }

    /**
     * Verifies keyValueBucketNames() lists KV_-prefixed streams with the prefix stripped (#60).
     */
    public function testKeyValueBucketNames(): void
    {
        $reply = '{"streams":["KV_cfg","OBJ_assets","ORDERS","KV_sessions"],"total":4}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        self::assertSame(['cfg', 'sessions'], $client->jetStream()->keyValueBucketNames()->await());
    }

    /**
     * Verifies objectStoreBucketNames() lists OBJ_-prefixed streams with the prefix stripped (#60).
     */
    public function testObjectStoreBucketNames(): void
    {
        $reply = '{"streams":["KV_cfg","OBJ_assets","ORDERS","OBJ_media"],"total":4}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        self::assertSame(['assets', 'media'], $client->jetStream()->objectStoreBucketNames()->await());
    }

    /**
     * Verifies streamNames() returns names from STREAM.NAMES (#35).
     */
    public function testStreamNames(): void
    {
        $reply = '{"streams":["ORDERS","EVENTS"],"total":2,"offset":0,"limit":256}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $names = $client->jetStream()->streamNames()->await();

        self::assertSame(['ORDERS', 'EVENTS'], $names);
        self::assertStringContainsString('$JS.API.STREAM.NAMES', $transport->writes[3]);
    }

    /**
     * Verifies consumerNames() returns names from CONSUMER.NAMES (#35).
     */
    public function testConsumerNames(): void
    {
        $reply = '{"consumers":["worker-1","worker-2"],"total":2}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $names = $client->jetStream()->consumerNames('ORDERS')->await();

        self::assertSame(['worker-1', 'worker-2'], $names);
        self::assertStringContainsString('$JS.API.CONSUMER.NAMES.ORDERS', $transport->writes[3]);
    }

    /**
     * Verifies getLastMessageForSubject() uses last_by_subj and parses the stored message (#36).
     */
    public function testGetLastMessageForSubject(): void
    {
        $reply = '{"message":{"subject":"orders.new","seq":7,"data":"' . base64_encode('hello') . '"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $message = $client->jetStream()->getLastMessageForSubject('ORDERS', 'orders.new')->await();

        self::assertSame('orders.new', $message->subject);
        self::assertSame('hello', $message->payload);
        self::assertStringContainsString('$JS.API.STREAM.MSG.GET.ORDERS', $transport->writes[3]);
        self::assertStringContainsString('"last_by_subj":"orders.new"', $transport->writes[3]);
    }

    /**
     * Verifies getLastMessageForSubject() rejects wildcard subjects (#36).
     */
    public function testGetLastMessageForSubjectRejectsWildcard(): void
    {
        $client = new NatsClient(new NatsOptions());
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('non-wildcard');
        $client->jetStream()->getLastMessageForSubject('ORDERS', 'orders.*')->await();
    }

    /**
     * Verifies createOrUpdateStream() falls back to UPDATE when the stream already exists (#44).
     */
    public function testCreateOrUpdateStreamFallsBackToUpdate(): void
    {
        $createErr = '{"error":{"code":400,"err_code":10058,"description":"stream name already in use"}}';
        $updateOk = '{"config":{"name":"ORDERS","subjects":["orders.*","orders.archive"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createErr), $createErr),   // CREATE -> already in use
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($updateOk), $updateOk),     // UPDATE -> ok
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $info = $client->jetStream()->createOrUpdateStream('ORDERS', ['orders.*', 'orders.archive'])->await();

        self::assertSame('ORDERS', $info->name);
        self::assertStringContainsString('$JS.API.STREAM.CREATE.ORDERS', $transport->writes[3]);
        self::assertStringContainsString('$JS.API.STREAM.UPDATE.ORDERS', $transport->writes[6]);
    }

    /**
     * Verifies a JetStream API error envelope's err_code is exposed via getErrCode() alongside the
     * HTTP-like code kept in getCode() (#154).
     */
    public function testApiErrorEnvelopeExposesErrCode(): void
    {
        $err = '{"error":{"code":404,"err_code":10059,"description":"stream not found"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($err), $err),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->getStream('MISSING')->await();
            self::fail('Expected a JetStreamException');
        } catch (JetStreamException $e) {
            self::assertSame(404, $e->getCode());
            self::assertSame(10059, $e->getErrCode());
        }
    }

    /**
     * Verifies a publish-expectation error ack exposes err_code 10071 via getErrCode(), and that an
     * envelope without err_code yields null (old servers) (#154).
     */
    public function testPublishExpectationMismatchExposesErrCode(): void
    {
        $withErrCode = '{"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 5"}}';
        $withoutErrCode = '{"error":{"code":400,"description":"wrong last sequence: 5"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($withErrCode), $withErrCode),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($withoutErrCode), $withoutErrCode),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->publish('orders.created', '{"id":1}', expectedLastSequence: 99)->await();
            self::fail('Expected a JetStreamException');
        } catch (JetStreamException $e) {
            self::assertSame(400, $e->getCode());
            self::assertSame(10071, $e->getErrCode());
        }

        try {
            $client->jetStream()->publish('orders.created', '{"id":1}', expectedLastSequence: 99)->await();
            self::fail('Expected a JetStreamException');
        } catch (JetStreamException $e) {
            self::assertSame(400, $e->getCode());
            self::assertNull($e->getErrCode());
        }
    }

    /**
     * Verifies createOrUpdateStream() discriminates "stream name already in use" by err_code 10058,
     * not by description wording: a reworded description still falls back to UPDATE (#154).
     */
    public function testCreateOrUpdateStreamFallsBackToUpdateByErrCode(): void
    {
        $createErr = '{"error":{"code":400,"err_code":10058,"description":"cannot add stream: that name is taken"}}';
        $updateOk = '{"config":{"name":"ORDERS","subjects":["orders.*"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createErr), $createErr),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($updateOk), $updateOk),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $info = $client->jetStream()->createOrUpdateStream('ORDERS', ['orders.*'])->await();

        self::assertSame('ORDERS', $info->name);
        self::assertStringContainsString('$JS.API.STREAM.UPDATE.ORDERS', $transport->writes[6]);
    }

    /**
     * Verifies createOrUpdateStream() trusts a present err_code over a misleading description: an
     * error whose wording mentions "already in use" but whose err_code is not 10058 is re-thrown
     * instead of triggering the UPDATE fallback (#154).
     */
    public function testCreateOrUpdateStreamRethrowsWhenErrCodeIsNotStreamNameInUse(): void
    {
        $createErr = '{"error":{"code":400,"err_code":10065,"description":"subjects [orders.*] already in use by an existing stream"}}';
        $updateOk = '{"config":{"name":"ORDERS","subjects":["orders.*"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createErr), $createErr),
            // An UPDATE reply is queued so a wrong fallback would succeed silently instead of hanging.
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($updateOk), $updateOk),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('already in use by an existing stream');

        $client->jetStream()->createOrUpdateStream('ORDERS', ['orders.*'])->await();
    }

    /**
     * Verifies createOrUpdateStream() still falls back to UPDATE via the description substring when
     * the envelope carries no err_code (old servers) (#154).
     */
    public function testCreateOrUpdateStreamFallsBackToUpdateWithoutErrCode(): void
    {
        $createErr = '{"error":{"code":400,"description":"stream name already in use"}}';
        $updateOk = '{"config":{"name":"ORDERS","subjects":["orders.*"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createErr), $createErr),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($updateOk), $updateOk),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $info = $client->jetStream()->createOrUpdateStream('ORDERS', ['orders.*'])->await();

        self::assertSame('ORDERS', $info->name);
        self::assertStringContainsString('$JS.API.STREAM.UPDATE.ORDERS', $transport->writes[6]);
    }

    /**
     * Verifies create/get/delete stream operations map expected payload fields.
     */
    public function testStreamCrud(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
     * Verifies objectStore() constructs an equivalent bucket wrapper per call instead of memoizing:
     * the per-name cache had no eviction, so a long-lived context accumulated one wrapper per bucket
     * name touched, forever (#133). Fresh instances must stay behaviorally interchangeable (equal
     * state, same backing stream).
     */
    public function testObjectStoreConstructsEquivalentBucketPerCall(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $a = $client->jetStream()->objectStore('assets');
        $b = $client->jetStream()->objectStore('assets');
        $c = $client->jetStream()->objectStore('other');

        // No memoization: each call builds a new wrapper (nothing is retained on the context)...
        self::assertNotSame($a, $b);
        // ...and the wrappers are all-readonly value objects, so repeated calls are interchangeable.
        self::assertEquals($a, $b);
        self::assertSame('OBJ_assets', $a->streamName());
        self::assertSame('OBJ_assets', $b->streamName());
        self::assertSame('OBJ_other', $c->streamName());
    }

    /**
     * Verifies keyValue() constructs an equivalent bucket wrapper per call instead of memoizing -
     * same no-eviction rationale as the object store accessor (#133).
     */
    public function testKeyValueConstructsEquivalentBucketPerCall(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $a = $client->jetStream()->keyValue('profiles');
        $b = $client->jetStream()->keyValue('profiles');
        $c = $client->jetStream()->keyValue('sessions');

        self::assertInstanceOf(KeyValueBucket::class, $a);
        // No memoization: each call builds a new wrapper (nothing is retained on the context)...
        self::assertNotSame($a, $b);
        // ...and the wrappers are all-readonly value objects, so repeated calls are interchangeable.
        self::assertEquals($a, $b);
        self::assertSame('KV_profiles', $a->streamName());
        self::assertSame('KV_profiles', $b->streamName());
        self::assertSame('KV_sessions', $c->streamName());
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
     * Verifies createConsumer sends filter_subjects (and omits the singular filter_subject) when an
     * array of filters is supplied via options (issue #10).
     */
    public function testCreateConsumerWithFilterSubjects(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->createConsumer('ORDERS', 'PROC', null, ['filter_subjects' => ['orders.eu.>', 'orders.us.>']])->await();

        self::assertStringContainsString('"filter_subjects":["orders.eu.>","orders.us.>"]', $transport->writes[3]);
        self::assertStringNotContainsString('"filter_subject"', $transport->writes[3]);
    }

    /**
     * Verifies combining a single filter subject with filter_subjects is rejected before dispatch.
     */
    public function testCreateConsumerRejectsBothFilterForms(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Use either a single filter subject or filter_subjects, not both');

        try {
            $client->jetStream()->createConsumer('ORDERS', 'PROC', 'orders.*', ['filter_subjects' => ['orders.eu.>']])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies a filter_subjects array containing an empty subject is rejected before dispatch.
     */
    public function testCreateConsumerRejectsEmptyFilterSubjectEntry(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('filter_subjects must contain only non-empty subject strings');

        try {
            $client->jetStream()->createConsumer('ORDERS', 'PROC', null, ['filter_subjects' => ['orders.eu.>', '']])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies the mutual-exclusion guard also fires when the singular filter_subject is smuggled in
     * via the options bag alongside filter_subjects (issue #10).
     */
    public function testCreateConsumerRejectsFilterSubjectInOptionsConflict(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Use either a single filter subject or filter_subjects, not both');

        try {
            $client->jetStream()->createConsumer('ORDERS', 'PROC', null, [
                'filter_subject' => 'orders.eu.>',
                'filter_subjects' => ['orders.us.>'],
            ])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies an empty filter subject is rejected uniformly on the ephemeral path too (issue #10).
     */
    public function testCreateEphemeralConsumerRejectsEmptyFilterSubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Consumer filter subject must not be empty');

        try {
            $client->jetStream()->createEphemeralConsumer('ORDERS', '')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies filter_subjects flows through the push-consumer create path too (issue #10).
     */
    public function testCreatePushConsumerWithFilterSubjects(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->createPushConsumer('ORDERS', 'PROC', '_INBOX.deliver', null, [
            'filter_subjects' => ['orders.eu.>', 'orders.us.>'],
        ])->await();

        self::assertStringContainsString('"filter_subjects":["orders.eu.>","orders.us.>"]', $transport->writes[3]);
        self::assertStringNotContainsString('"filter_subject"', $transport->writes[3]);
    }

    /**
     * Verifies createConsumer validates and forwards priority-group config (issue #7).
     */
    public function testCreateConsumerWithPriorityGroups(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->createConsumer('ORDERS', 'PROC', null, [
            'priority_groups' => ['g1'],
            'priority_policy' => 'pinned_client',
        ])->await();

        self::assertStringContainsString('"priority_groups":["g1"]', $transport->writes[3]);
        self::assertStringContainsString('"priority_policy":"pinned_client"', $transport->writes[3]);
    }

    /**
     * Verifies an invalid priority policy is rejected before dispatch.
     */
    public function testCreateConsumerRejectsInvalidPriorityPolicy(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('priority_policy must be one of');

        try {
            $client->jetStream()->createConsumer('ORDERS', 'PROC', null, ['priority_policy' => 'bogus'])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies the pull request carries the priority/group fields (issue #7).
     */
    public function testFetchBatchWithPullOptions(): void
    {
        $msg = '{"event":"x"}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg), $msg),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500, [
            'group' => 'g1',
            'min_pending' => 5,
            'max_bytes' => 1048576,
            'no_wait' => true,
        ])->await();

        $written = $transport->writes[3];
        self::assertStringContainsString('"group":"g1"', $written);
        self::assertStringContainsString('"min_pending":5', $written);
        self::assertStringContainsString('"max_bytes":1048576', $written);
        self::assertStringContainsString('"no_wait":true', $written);
    }

    /**
     * Verifies an out-of-range pull priority is rejected before dispatch.
     */
    public function testFetchBatchRejectsInvalidPriority(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Pull priority must be an integer between 0 and 9');

        try {
            $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500, ['priority' => 10])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies an unknown pull-request field is rejected before dispatch instead of being silently
     * dropped (#132).
     */
    public function testFetchBatchRejectsUnknownPullField(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Unknown pull request field "heartbeat"; supported fields: group, id, min_pending, min_ack_pending, priority, max_bytes, no_wait, idle_heartbeat');

        try {
            $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500, ['heartbeat' => 1_000_000_000])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies the ADR-13 idle_heartbeat pull field reaches the wire in the pull request JSON (#132).
     */
    public function testFetchBatchSendsIdleHeartbeat(): void
    {
        $msg = '{"event":"x"}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg), $msg),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500, [
            'idle_heartbeat' => 1_000_000_000,
        ])->await();

        self::assertStringContainsString('"idle_heartbeat":1000000000', $transport->writes[3]);
    }

    /**
     * Verifies ADR-13 client-side validation: an idle_heartbeat above 50% of expires is rejected
     * with a clear InvalidArgumentException before anything reaches the wire, instead of being
     * forwarded for the server to reject (or silently misbehave) (#153).
     */
    public function testFetchBatchRejectsIdleHeartbeatAboveHalfOfExpires(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('must not exceed 50% of expires');

        try {
            // 150ms heartbeat against a 200ms expiry: above the ADR-13 50% ceiling (100ms).
            $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 200, ['idle_heartbeat' => 150_000_000])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies a non-positive (or non-integer) idle_heartbeat is rejected client-side before
     * dispatch (#153).
     */
    public function testFetchBatchRejectsNonPositiveIdleHeartbeat(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('idle_heartbeat must be a positive integer');

        try {
            $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500, ['idle_heartbeat' => 0])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies the ADR-13 boundary: an idle_heartbeat of exactly 50% of expires is accepted and
     * reaches the wire (#153).
     */
    public function testFetchBatchAcceptsIdleHeartbeatAtExactlyHalfOfExpires(): void
    {
        $msg = '{"event":"x"}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg), $msg),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // 1250ms heartbeat against a 2500ms expiry: exactly the 50% ceiling - allowed.
        $messages = $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500, [
            'idle_heartbeat' => 1_250_000_000,
        ])->await();

        self::assertCount(1, $messages);
        self::assertStringContainsString('"idle_heartbeat":1250000000', $transport->writes[3]);
    }

    /**
     * Verifies missed pull idle heartbeats are detected: with idle_heartbeat requested and a
     * silent transport (no message, no status-100 heartbeat frame), fetchBatch() fails within
     * ~2 heartbeat intervals with a heartbeat-miss error instead of sitting out the full
     * expires+slack deadline (nats.go ErrNoHeartbeat semantics) (#153).
     */
    public function testFetchBatchFailsFastOnMissedIdleHeartbeats(): void
    {
        $transport = new FakeTransport(
            readQueue: [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                "PONG\r\n",
            ],
            blockWhenEmpty: true,
        );

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $startNs = hrtime(true);
        try {
            // 50ms heartbeat, 2000ms expiry: the server should heartbeat every 50ms, so silence
            // for 2 intervals (100ms) means it is gone.
            $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2000, ['idle_heartbeat' => 50_000_000])->await();
            self::fail('Expected a heartbeat-miss JetStreamException.');
        } catch (JetStreamException $e) {
            $elapsedNs = hrtime(true) - $startNs;
            self::assertStringContainsString('missed idle heartbeats', $e->getMessage());
            // ~2 heartbeat intervals (100ms), far below the expires+slack deadline (3000ms).
            self::assertLessThan(1_000_000_000, $elapsedNs, 'a heartbeat miss must fail fast, not wait out expires');
        }
    }

    /**
     * Verifies unpinConsumer issues the UNPIN request with the group (issue #7).
     */
    public function testUnpinConsumer(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen('{}'), '{}'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $ok = $client->jetStream()->unpinConsumer('ORDERS', 'PROC', 'g1')->await();

        self::assertTrue($ok);
        self::assertStringContainsString('$JS.API.CONSUMER.UNPIN.ORDERS.PROC', $transport->writes[3]);
        self::assertStringContainsString('"group":"g1"', $transport->writes[3]);
    }

    /**
     * Verifies pinIdOf extracts the Nats-Pin-Id header (issue #7).
     */
    public function testPinIdOf(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $pinned = new NatsMessage(
            subject: 'orders.created',
            sid: 1,
            replyTo: null,
            payload: 'x',
            rawHeaders: "NATS/1.0\r\nNats-Pin-Id: pin-123\r\n\r\n",
        );
        $plain = new NatsMessage('orders.created', 1, null, 'x', null);

        self::assertSame('pin-123', $js->pinIdOf($pinned));
        self::assertNull($js->pinIdOf($plain));
    }

    /**
     * Verifies a batched Direct Get collects multiple replies and stops at the 204 EOB (issue #13).
     */
    public function testDirectGetBatchCollectsUntilEob(): void
    {
        $h1 = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.a\r\nNats-Sequence: 5\r\n\r\n";
        $b1 = 'aaa';
        $h2 = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.b\r\nNats-Sequence: 6\r\n\r\n";
        $b2 = 'bbb';
        $eob = "NATS/1.0 204 EOB\r\n\r\n";
        $h3 = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.c\r\nNats-Sequence: 7\r\n\r\n";
        $b3 = 'ccc';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s%s\r\n", strlen($h1), strlen($h1) + strlen($b1), $h1, $b1),
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s%s\r\n", strlen($h2), strlen($h2) + strlen($b2), $h2, $b2),
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s\r\n", strlen($eob), strlen($eob), $eob),
            // A frame AFTER the EOB must NOT be consumed: if termination were broken the loop would
            // read this and return 3 messages.
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s%s\r\n", strlen($h3), strlen($h3) + strlen($b3), $h3, $b3),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $messages = $client->jetStream()->directGetBatch('ORDERS', ['batch' => 10])->await();

        self::assertCount(2, $messages);
        self::assertSame('aaa', $messages[0]->payload);
        self::assertSame('orders.a', $messages[0]->subject);
        self::assertSame('bbb', $messages[1]->payload);
        self::assertSame('orders.b', $messages[1]->subject);
        self::assertStringContainsString('$JS.API.DIRECT.GET.ORDERS', $transport->writes[3]);
        self::assertStringContainsString('"batch":10', $transport->writes[3]);
    }

    /**
     * Verifies directGetLastForSubjects sends multi_last and terminates on Nats-Num-Pending: 0.
     */
    public function testDirectGetLastForSubjects(): void
    {
        $h1 = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.a\r\nNats-Sequence: 5\r\n\r\n";
        $b1 = 'aaa';
        $h2 = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.b\r\nNats-Sequence: 6\r\nNats-Num-Pending: 0\r\n\r\n";
        $b2 = 'bbb';
        // A frame after the Nats-Num-Pending:0 terminator must NOT be consumed.
        $h3 = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.c\r\nNats-Sequence: 7\r\n\r\n";
        $b3 = 'ccc';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s%s\r\n", strlen($h1), strlen($h1) + strlen($b1), $h1, $b1),
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s%s\r\n", strlen($h2), strlen($h2) + strlen($b2), $h2, $b2),
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s%s\r\n", strlen($h3), strlen($h3) + strlen($b3), $h3, $b3),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $messages = $client->jetStream()->directGetLastForSubjects('ORDERS', ['orders.a', 'orders.b'])->await();

        self::assertCount(2, $messages);
        self::assertStringContainsString('"multi_last":["orders.a","orders.b"]', $transport->writes[3]);
        self::assertStringContainsString('"batch":2', $transport->writes[3]);
    }

    /**
     * Verifies a batched Direct Get error status surfaces as a JetStreamException (issue #13).
     */
    public function testDirectGetBatchSurfacesError(): void
    {
        $err = "NATS/1.0 408 Request Timeout\r\n\r\n";

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s\r\n", strlen($err), strlen($err), $err),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionCode(408);

        $client->jetStream()->directGetBatch('ORDERS', ['batch' => 10])->await();
    }

    /**
     * Verifies JetStream publish returns stream/sequence acknowledgment.
     */
    public function testPublishWithAck(): void
    {
        $ackPayload = '{"stream":"ORDERS","seq":42,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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

    public function testPublishWrapsMalformedAckAsJetStreamException(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG _INBOX.a 1 7\r\nnotjson\r\n", // a non-JSON publish ack
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Malformed JetStream publish ack');
        $client->jetStream()->publish('orders.created', '{"id":1}')->await();
    }

    /**
     * Verifies JetStream publish maps API errors to JetStreamException.
     */
    public function testPublishMapsApiError(): void
    {
        $errorPayload = '{"error":{"code":500,"description":"publish failed"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
     * Verifies a version-gated config field rejected by an older server surfaces as a typed
     * UnsupportedFeatureException carrying the feature, required version, and the server's reported
     * version (from the INFO handshake) - without any per-request version probe.
     */
    public function testUnsupportedFeatureRaisesTypedExceptionWithServerVersion(): void
    {
        $errorPayload = '{"error":{"code":400,"description":"invalid JSON: json: unknown field \"allow_atomic\""}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.5","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->createStream('S', ['s.>'], ['allow_atomic' => true])->await();
            self::fail('Expected UnsupportedFeatureException');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame('allow_atomic', $e->feature);
            self::assertSame('2.12', $e->requiredVersion);
            self::assertSame('2.10.5', $e->serverVersion);
            self::assertSame(400, $e->getCode());
            // Still catchable as a JetStreamException (subclass).
            self::assertInstanceOf(JetStreamException::class, $e);
        }
    }

    /**
     * Verifies scheduled publish sends scheduler headers through HPUB request.
     */
    public function testPublishScheduled(): void
    {
        $ackPayload = '{"stream":"SCHED","seq":7,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
     * Verifies malformed schedule expressions are rejected before request dispatch.
     */
    public function testPublishScheduledRejectsUnsupportedPattern(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Unsupported schedule expression');

        try {
            $client->jetStream()->publishScheduled(
                'schedules.orders.one',
                'events.orders',
                '{"event":"scheduled"}',
                'not-a-schedule',
            )->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies a recurring "@every" schedule emits the scheduler headers, including the optional
     * source and rollup headers.
     */
    public function testPublishScheduledEveryWithSourceAndRollup(): void
    {
        $ackPayload = '{"stream":"SCHED","seq":9,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->publishScheduled(
            'schedules.heartbeat',
            'events.heartbeat',
            '{"event":"tick"}',
            Schedule::every('1h'),
            source: 'cluster-a',
            rollup: true,
        )->await();

        self::assertStringContainsString('Nats-Schedule:@every 1h', $transport->writes[3]);
        self::assertStringContainsString('Nats-Schedule-Target:events.heartbeat', $transport->writes[3]);
        self::assertStringContainsString('Nats-Schedule-Source:cluster-a', $transport->writes[3]);
        self::assertStringContainsString('Nats-Schedule-Rollup:sub', $transport->writes[3]);
        self::assertStringNotContainsString('Nats-Schedule-Time-Zone', $transport->writes[3]);
    }

    /**
     * Verifies a cron schedule emits the cron expression plus the time-zone header.
     */
    public function testPublishScheduledCronWithTimeZone(): void
    {
        $ackPayload = '{"stream":"SCHED","seq":10,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->publishScheduled(
            'schedules.report',
            'events.report',
            '{"event":"daily"}',
            Schedule::cron('0 0 0 * * *'),
            timeZone: 'Europe/Warsaw',
        )->await();

        self::assertStringContainsString('Nats-Schedule:0 0 0 * * *', $transport->writes[3]);
        self::assertStringContainsString('Nats-Schedule-Time-Zone:Europe/Warsaw', $transport->writes[3]);
    }

    /**
     * Verifies a predefined alias schedule (ADR-51) reaches the wire and may carry a time zone.
     */
    public function testPublishScheduledPredefinedAlias(): void
    {
        $ackPayload = '{"stream":"SCHED","seq":11,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->publishScheduled(
            'schedules.report',
            'events.report',
            '{"event":"daily"}',
            Schedule::predefined('daily'),
            timeZone: 'Europe/Warsaw',
        )->await();

        self::assertStringContainsString('Nats-Schedule:@daily', $transport->writes[3]);
        self::assertStringContainsString('Nats-Schedule-Time-Zone:Europe/Warsaw', $transport->writes[3]);
    }

    /**
     * Verifies an @at schedule with a numeric RFC3339 offset (not just "Z") reaches the wire.
     */
    public function testPublishScheduledAtWithTimezoneOffset(): void
    {
        $ackPayload = '{"stream":"SCHED","seq":12,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->publishScheduled(
            'schedules.orders.one',
            'events.orders',
            '{"event":"scheduled"}',
            '@at 2030-01-01T02:00:00+02:00',
        )->await();

        self::assertStringContainsString('Nats-Schedule:@at 2030-01-01T02:00:00+02:00', $transport->writes[3]);
    }

    /**
     * Verifies a time zone supplied with a non-cron schedule is rejected before dispatch.
     */
    public function testPublishScheduledRejectsTimeZoneForNonCron(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Nats-Schedule-Time-Zone is only valid for cron');

        try {
            $client->jetStream()->publishScheduled(
                'schedules.heartbeat',
                'events.heartbeat',
                '{"event":"tick"}',
                Schedule::every('1h'),
                timeZone: 'Europe/Warsaw',
            )->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies publish with a de-duplication id emits the Nats-Msg-Id header (issue #11).
     */
    public function testPublishWithMsgId(): void
    {
        $ackPayload = '{"stream":"ORDERS","seq":43,"duplicate":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->publish('orders.created', '{"id":1}', msgId: 'order-1')->await();

        self::assertTrue($ack->duplicate);
        self::assertStringStartsWith('HPUB orders.created _INBOX.', $transport->writes[3]);
        self::assertStringContainsString('Nats-Msg-Id:order-1', $transport->writes[3]);
    }

    /**
     * Verifies publish emits optimistic-concurrency expectation headers (#16).
     */
    public function testPublishWithExpectationHeaders(): void
    {
        $ackPayload = '{"stream":"ORDERS","seq":43,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->publish(
            'orders.created',
            '{"id":1}',
            expectedStream: 'ORDERS',
            expectedLastSequence: 42,
            expectedLastSubjectSequence: 0,
            expectedLastMsgId: 'order-41',
        )->await();

        $hpub = $transport->writes[3];
        self::assertStringStartsWith('HPUB orders.created _INBOX.', $hpub);
        self::assertStringContainsString('Nats-Expected-Stream:ORDERS', $hpub);
        self::assertStringContainsString('Nats-Expected-Last-Sequence:42', $hpub);
        self::assertStringContainsString('Nats-Expected-Last-Subject-Sequence:0', $hpub);
        self::assertStringContainsString('Nats-Expected-Last-Msg-Id:order-41', $hpub);
    }

    /**
     * Verifies a precondition mismatch (error ack) surfaces as a JetStreamException and is NOT retried (#16).
     */
    public function testPublishExpectationMismatchThrows(): void
    {
        $errorAck = '{"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 5"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorAck), $errorAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('wrong last sequence');
        $client->jetStream()->publish('orders.created', '{"id":1}', expectedLastSequence: 99)->await();
    }

    /**
     * Verifies a publish that hits a transient no-responders (503) is retried and then succeeds (#29).
     */
    public function testPublishRetriesOnNoResponders(): void
    {
        $status = "NATS/1.0 503\r\n\r\n";
        $ackPayload = '{"stream":"ORDERS","seq":50,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // First publish request -> 503 no-responders on inbox sid 1.
            'HMSG _INBOX.a 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
            // Retry publish request -> success ack on inbox sid 2.
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        // Tight retry wait so the test is fast.
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();
        $js = new JetStreamContext($client, publishRetryAttempts: 3, publishRetryWaitMs: 1);

        $ack = $js->publish('orders.created', '{"id":1}')->await();

        self::assertSame(50, $ack->seq);
    }

    /**
     * Verifies ackSync sends +ACK as a request and resolves on the server confirmation (#18).
     */
    public function testAckSyncSendsAckAsRequestAndAwaitsConfirmation(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // Empty confirmation reply on the double-ack inbox (sid 1).
            "MSG _INBOX.any 1 0\r\n\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $delivered = new NatsMessage('events.x', 9, '$JS.ACK.ORDERS.c1.1.5.5.0.0', 'body');
        $client->jetStream()->ackSync($delivered, 100)->await();

        // The +ACK travelled as a request: a SUB on a fresh inbox then a PUB carrying the reply inbox.
        self::assertStringStartsWith('SUB _INBOX.', $transport->writes[2]);
        self::assertStringStartsWith('PUB $JS.ACK.ORDERS.c1.1.5.5.0.0 _INBOX.', $transport->writes[3]);
        self::assertStringEndsWith("\r\n+ACK\r\n", $transport->writes[3]);
    }

    /**
     * Verifies deleteMessage issues a fast (no_erase) delete by default and a secure erase on request (#20).
     */
    public function testDeleteMessageFastAndSecure(): void
    {
        $ok = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ok), $ok),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($ok), $ok),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();
        $js = $client->jetStream();

        self::assertTrue($js->deleteMessage('ORDERS', 7)->await());
        self::assertTrue($js->deleteMessage('ORDERS', 8, secureErase: true)->await());

        self::assertStringContainsString('$JS.API.STREAM.MSG.DELETE.ORDERS', $transport->writes[3]);
        self::assertStringContainsString('"no_erase":true', $transport->writes[3]);
        self::assertStringContainsString('"seq":7', $transport->writes[3]);
        // Secure erase omits no_erase so the server overwrites the data (second request's PUB).
        self::assertStringNotContainsString('no_erase', $transport->writes[6]);
        self::assertStringContainsString('"seq":8', $transport->writes[6]);
    }

    /**
     * Verifies messageMetadata parses the full $JS.ACK tuple, incl. domain form (#30).
     */
    public function testMessageMetadataParsesAckTuple(): void
    {
        $client = new NatsClient(new NatsOptions());
        $js = $client->jetStream();

        // 9-token form: $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending>
        $short = new NatsMessage('events.x', 1, '$JS.ACK.ORDERS.worker.3.42.40.1700000000000000000.7', 'body');
        $meta = $js->messageMetadata($short);
        self::assertSame('ORDERS', $meta->stream);
        self::assertSame('worker', $meta->consumer);
        self::assertSame(3, $meta->numDelivered);
        self::assertSame(42, $meta->streamSequence);
        self::assertSame(40, $meta->consumerSequence);
        self::assertSame(7, $meta->numPending);
        self::assertNull($meta->domain);
        self::assertSame(1700000000000000000, $meta->timestampNanos);

        // Domain-qualified (11-token) form.
        $domainMsg = new NatsMessage('events.x', 1, '$JS.ACK.hub.ACCT.ORDERS.worker.2.99.50.1700000000000000000.4', 'body');
        $dmeta = $js->messageMetadata($domainMsg);
        self::assertSame('hub', $dmeta->domain);
        self::assertSame('ORDERS', $dmeta->stream);
        self::assertSame(99, $dmeta->streamSequence);
        self::assertSame(4, $dmeta->numPending);
    }

    /**
     * Verifies messageMetadata rejects a non-JetStream message (#30).
     */
    public function testMessageMetadataThrowsForNonJetStreamMessage(): void
    {
        $client = new NatsClient(new NatsOptions());
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('not a JetStream delivery');
        $client->jetStream()->messageMetadata(new NatsMessage('events.x', 1, '_INBOX.plain', 'body'));
    }

    /**
     * Verifies publish with an integer TTL emits Nats-TTL in seconds (issue #4).
     */
    public function testPublishWithTtlSeconds(): void
    {
        $ackPayload = '{"stream":"ORDERS","seq":44,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->publish('orders.created', '{"id":1}', ttl: 30)->await();

        self::assertStringStartsWith('HPUB orders.created _INBOX.', $transport->writes[3]);
        self::assertStringContainsString('Nats-TTL:30s', $transport->writes[3]);
    }

    /**
     * Verifies the "never" TTL passes through unchanged.
     */
    public function testPublishWithTtlNever(): void
    {
        $ackPayload = '{"stream":"ORDERS","seq":45,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->publish('orders.created', '{"id":1}', ttl: 'never')->await();

        self::assertStringContainsString('Nats-TTL:never', $transport->writes[3]);
    }

    /**
     * Verifies an invalid (sub-second / zero) TTL is rejected before dispatch.
     */
    public function testPublishRejectsZeroTtl(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Per-message TTL must be at least 1 second');

        try {
            $client->jetStream()->publish('orders.created', '{"id":1}', ttl: 0)->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies an empty Nats-Msg-Id is rejected before dispatch.
     */
    public function testPublishRejectsEmptyMsgId(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Nats-Msg-Id must not be empty');

        try {
            $client->jetStream()->publish('orders.created', '{"id":1}', msgId: '')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies incrementCounter emits Nats-Incr and returns the new value (issue #9).
     */
    public function testIncrementCounter(): void
    {
        $ackPayload = '{"stream":"COUNTERS","seq":1,"val":"5"}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $value = $client->jetStream()->incrementCounter('counters.visits', '+5')->await();

        self::assertSame('5', $value);
        self::assertStringStartsWith('HPUB counters.visits _INBOX.', $transport->writes[3]);
        self::assertStringContainsString('Nats-Incr:+5', $transport->writes[3]);
    }

    /**
     * Verifies a counter value beyond PHP_INT_MAX is preserved as an exact string.
     */
    public function testIncrementCounterPreservesBigValue(): void
    {
        // Unquoted JSON number beyond PHP_INT_MAX: only JSON_BIGINT_AS_STRING preserves it exactly,
        // so this payload makes that flag load-bearing (a quoted string would pass regardless).
        $ackPayload = '{"stream":"COUNTERS","seq":2,"val":99999999999999999999}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $value = $client->jetStream()->incrementCounter('counters.visits', '+1')->await();

        self::assertSame('99999999999999999999', $value);
    }

    /**
     * Verifies a malformed counter delta is rejected before dispatch.
     */
    public function testIncrementCounterRejectsMalformedDelta(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Counter increment must be an integer string');

        try {
            $client->jetStream()->incrementCounter('counters.visits', '5x')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies counterValue reads the latest value via Direct Get (issue #9).
     */
    public function testCounterValue(): void
    {
        $hdrs = "NATS/1.0\r\nNats-Stream: COUNTERS\r\nNats-Subject: counters.visits\r\nNats-Sequence: 7\r\n\r\n";
        $body = '{"val":"42"}';
        $h = strlen($hdrs);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("HMSG _INBOX.a 1 %d %d\r\n%s%s\r\n", $h, $h + strlen($body), $hdrs, $body),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $value = $client->jetStream()->counterValue('COUNTERS', 'counters.visits')->await();

        self::assertSame('42', $value);
        self::assertStringStartsWith('PUB $JS.API.DIRECT.GET.COUNTERS _INBOX.', $transport->writes[3]);
    }

    /**
     * Verifies counterValue returns "0" for a counter with no stored message.
     */
    public function testCounterValueMissingReturnsZero(): void
    {
        $hdrs = "NATS/1.0 404 Message Not Found\r\n\r\n";
        $h = strlen($hdrs);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("HMSG _INBOX.a 1 %d %d\r\n%s\r\n", $h, $h, $hdrs),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $value = $client->jetStream()->counterValue('COUNTERS', 'counters.visits')->await();

        self::assertSame('0', $value);
    }

    /**
     * Verifies schedule publish omits TTL header when optional value is null.
     */
    public function testPublishScheduledOmitsTtlWhenNotProvided(): void
    {
        $ackPayload = '{"stream":"SCHED","seq":8,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
     * Verifies a stalled idle heartbeat is answered on the Nats-Consumer-Stalled subject (not the
     * empty message reply), so the server's flow-control stall is cleared instead of hanging.
     */
    public function testSubscribePushConsumerAnswersStalledHeartbeat(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC","deliver_subject":"deliver.proc"}}';
        // Status-100 heartbeat with NO message reply; the FC reply subject is in the header value.
        $stalledHeaders = NatsHeaders::toWireBlock([
            'Status' => '100',
            'Description' => 'Idle Heartbeat',
            'Nats-Consumer-Stalled' => 'stall.reply',
        ]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            sprintf(
                "HMSG deliver.proc 2 %d %d\r\n%s\r\n", // no reply subject
                strlen($stalledHeaders),
                strlen($stalledHeaders),
                $stalledHeaders,
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

        self::assertFalse($handled); // not a user payload delivery
        self::assertStringContainsString("PUB stall.reply 0\r\n\r\n", implode('', $transport->writes));
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Stream subjects must not be empty unless mirror or sources configuration is provided');
        $client->jetStream()->createStream('test', [])->await();
    }

    public function testCreateStreamAllowsSourcesWithoutSubjects(): void
    {
        // A pure aggregate stream ingests only from sources and legitimately has no subjects of its own;
        // createStream() must not reject empty subjects when a non-empty sources config is provided.
        $streamPayload = '{"config":{"name":"AGG","subjects":[],"sources":[{"name":"ORDERS"},{"name":"PAYMENTS"}]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamPayload), $streamPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $created = $client->jetStream()->createStream('AGG', [], [
            'sources' => [
                StreamSource::source('ORDERS')->toArray(),
                StreamSource::source('PAYMENTS')->toArray(),
            ],
        ])->await();

        self::assertSame('AGG', $created->name);
        self::assertSame([], $created->subjects);
        self::assertStringContainsString('"sources":[', $transport->writes[3]);
        self::assertStringContainsString('"subjects":[]', $transport->writes[3]);
    }

    public function testCreateStreamAllowsMirrorWithoutSubjects(): void
    {
        $streamPayload = '{"config":{"name":"MIRROR","subjects":[],"mirror":{"name":"ORIGIN"}}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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

    public function testCreateConsumerDefaultsAckPolicyToExplicit(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC","ack_policy":"explicit"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // No ack_policy passed: the durable createConsumer() path must default it to explicit.
        $client->jetStream()->createConsumer('ORDERS', 'PROC', 'orders.created')->await();

        self::assertStringContainsString('"ack_policy":"explicit"', $transport->writes[3]);
    }

    public function testCreatePushConsumerAllowsAckPolicyOverride(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC","deliver_subject":"deliver.proc","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg1), $msg1),
            sprintf("HMSG _INBOX.JS.FETCH.a 1 %d %d\r\n%s\r\n", $headerBytes, $headerBytes, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $messages = $client->jetStream()->fetchBatch('ORDERS', 'PROC', 2, 2500)->await();

        self::assertCount(1, $messages);
        self::assertSame('{"event":"first"}', $messages[0]->payload);
    }

    /**
     * Verifies a mid-batch terminal status (after >=1 message) is surfaced to the optional
     * onTerminalStatus callback while the partial batch is still returned (#92).
     */
    public function testFetchBatchSurfacesMidBatchTerminalStatusToCallback(): void
    {
        $msg1 = '{"event":"first"}';
        // A non-routine mid-batch termination: the consumer was deleted (409).
        $statusHeaders = "NATS/1.0 409 Consumer Deleted\r\nStatus: 409\r\nDescription: Consumer Deleted\r\n\r\n";
        $headerBytes = strlen($statusHeaders);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg1), $msg1),
            sprintf("HMSG _INBOX.JS.FETCH.a 1 %d %d\r\n%s\r\n", $headerBytes, $headerBytes, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $observed = null;
        $messages = $client->jetStream()->fetchBatch(
            'ORDERS',
            'PROC',
            5,
            2500,
            [],
            static function (int $code, string $description) use (&$observed): void {
                $observed = ['code' => $code, 'description' => $description];
            },
        )->await();

        // The partial batch is still returned (nats.go parity)...
        self::assertCount(1, $messages);
        self::assertSame('{"event":"first"}', $messages[0]->payload);
        // ...and the mid-batch terminal status was surfaced to the callback.
        self::assertSame(['code' => 409, 'description' => 'Consumer Deleted'], $observed);
    }

    public function testFetchBatchIgnoresStatus100ControlFrames(): void
    {
        $msg1 = '{"event":"first"}';
        $controlHeaders = "NATS/1.0 100 Idle Heartbeat\r\nStatus: 100\r\nDescription: Idle Heartbeat\r\n\r\n";
        $headerBytes = strlen($controlHeaders);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->jsOkResponse($consumerCreateResponse),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->subscribeOrderedConsumer('ORDERS', function (NatsMessage $msg): void {})->await();

        $written = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.CONSUMER.CREATE.ORDERS', $written);
        self::assertStringContainsString('"flow_control":true', $written);
        self::assertStringContainsString('"idle_heartbeat":5000000000', $written);
        self::assertStringContainsString('"ack_policy":"none"', $written);
        self::assertStringContainsString('"mem_storage":true', $written);
        // ADR-17 / nats.go ordered.go parity: ordered consumers pin R1 (#132).
        self::assertStringContainsString('"num_replicas":1', $written);
    }

    // ─── Stream Purge / List / Consumer List / Direct Get ────────────────

    public function testPurgeStream(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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

    public function testListStreamsPaginatesAcrossPages(): void
    {
        $page1 = json_encode([
            'total' => 3,
            'offset' => 0,
            'streams' => [
                ['config' => ['name' => 'S1', 'subjects' => ['a.>']]],
                ['config' => ['name' => 'S2', 'subjects' => ['b.>']]],
            ],
        ], JSON_THROW_ON_ERROR);
        $page2 = json_encode([
            'total' => 3,
            'offset' => 2,
            'streams' => [
                ['config' => ['name' => 'S3', 'subjects' => ['c.>']]],
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen((string) $page1), (string) $page1),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen((string) $page2), (string) $page2),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $streams = $client->jetStream()->listStreams()->await();

        // All three streams are returned across two pages (server page size would otherwise truncate).
        self::assertSame(['S1', 'S2', 'S3'], array_map(static fn(StreamInfo $s): string => $s->name, $streams));

        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('"offset":0', $writes);
        self::assertStringContainsString('"offset":2', $writes);
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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

    public function testExtractStreamSequenceParsesDomainQualifiedReplySubject(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $method = new \ReflectionMethod($js, 'extractStreamSequence');

        // Domain-qualified ACK subject (12 tokens): stream sequence sits at index 7.
        $message = new NatsMessage('s', 1, '$JS.ACK.hub.ACC123.ORDERS.CONS.1.42.2.123.0.rnd', 'x');
        $parsed = $method->invoke($js, $message);

        self::assertSame(42, $parsed);
    }

    /**
     * A 13-token ACK subject (a future server form with tokens beyond the known 12) still yields
     * the stream sequence at index 7 - offsets anchor from the front, extras are ignored, matching
     * nats.go's tolerant parser (#155).
     */
    public function testExtractStreamSequenceParses13TokenReplySubject(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $method = new \ReflectionMethod($js, 'extractStreamSequence');

        $message = new NatsMessage('s', 1, '$JS.ACK.hub.ACC123.ORDERS.CONS.1.42.2.123.0.rnd.extra', 'x');

        self::assertSame(42, $method->invoke($js, $message));
    }

    public function testKeyValueRejectsInvalidBucketName(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $this->expectException(\IDCT\NATS\Exception\JetStreamException::class);
        $this->expectExceptionMessage('Invalid bucket name');
        // A dotted name would mis-scope $KV.<bucket>.> subjects.
        $client->jetStream()->keyValue('bad.bucket');
    }

    public function testObjectStoreRejectsInvalidBucketName(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $this->expectException(\IDCT\NATS\Exception\JetStreamException::class);
        $this->expectExceptionMessage('Invalid bucket name');
        $client->jetStream()->objectStore('bad/bucket');
    }

    public function testExtractSequencesParseElevenTokenDomainReplySubject(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $streamMethod = new \ReflectionMethod($js, 'extractStreamSequence');

        // Domain-qualified ACK subject WITHOUT a trailing random token (11 tokens): stream sequence at
        // index 7. Previously fell through to null, silently disabling KV/ObjectStore revision on
        // JetStream-domain/leaf deployments. (Consumer-sequence parsing for every token form is now
        // covered by JsMessageMetadataTest, which the ordered consumer uses.)
        $message = new NatsMessage('s', 1, '$JS.ACK.hub.ACC123.ORDERS.CONS.1.42.7.123.0', 'x');

        self::assertSame(42, $streamMethod->invoke($js, $message));
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

        self::assertFalse($method->invoke($js, $message));
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

        self::assertTrue($method->invoke($js, $message));
    }

    public function testHandlePushControlMessageRepliesToJetStreamFlowControlSubject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();
        $js = $client->jetStream();

        $method = new \ReflectionMethod($js, 'handlePushControlMessage');

        $headers = "NATS/1.0 100 Idle Heartbeat\r\nStatus: 100\r\nDescription: Idle Heartbeat\r\n\r\n";
        $message = new NatsMessage('deliver', 1, '$JS.FC.ORDERS.token', '', $headers);

        self::assertTrue($method->invoke($js, $message));
        self::assertStringContainsString('PUB $JS.FC.ORDERS.token 0' . "\r\n\r\n", implode('', $transport->writes));
    }

    /**
     * Verifies getStreamMessage() preserves a falsy body such as "0" instead of dropping it.
     */
    public function testGetStreamMessagePreservesZeroPayload(): void
    {
        $apiResponse = json_encode([
            'message' => ['subject' => 'events.zero', 'seq' => 1, 'data' => base64_encode('0')],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($apiResponse), $apiResponse),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $message = $client->jetStream()->getStreamMessage('EVENTS', 1)->await();

        self::assertSame('0', $message->payload);
        self::assertSame('events.zero', $message->subject);
        self::assertNull($message->rawHeaders);
    }

    /**
     * Verifies getStreamMessage() decodes the stored header block onto rawHeaders.
     */
    public function testGetStreamMessageDecodesHeaders(): void
    {
        $headerBlock = "NATS/1.0\r\nX-Custom: present\r\n\r\n";
        $apiResponse = json_encode([
            'message' => [
                'subject' => 'events.hdr',
                'seq' => 2,
                'data' => base64_encode('body'),
                'hdrs' => base64_encode($headerBlock),
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($apiResponse), $apiResponse),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $message = $client->jetStream()->getStreamMessage('EVENTS', 2)->await();

        self::assertSame('body', $message->payload);
        self::assertSame($headerBlock, $message->rawHeaders);
        self::assertSame('present', NatsHeaders::fromWireBlock($message->rawHeaders)['X-Custom'] ?? null);
    }

    /**
     * Verifies getStreamMessage() leaves rawHeaders null when no header block is stored.
     */
    public function testGetStreamMessageWithoutHeadersReturnsNullRawHeaders(): void
    {
        $apiResponse = json_encode([
            'message' => ['subject' => 'events.plain', 'seq' => 3, 'data' => base64_encode('hello')],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($apiResponse), $apiResponse),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $message = $client->jetStream()->getStreamMessage('EVENTS', 3)->await();

        self::assertSame('hello', $message->payload);
        self::assertNull($message->rawHeaders);
    }

    public function testDirectGetStreamMessageReturnsRawBodyAndHeaders(): void
    {
        $headerBlock = "NATS/1.0\r\nNats-Stream: EVENTS\r\nNats-Subject: events.order\r\nNats-Sequence: 2\r\nNats-Time-Stamp: 2024-01-01T00:00:00.000000000Z\r\n\r\n";
        $body = '{"id":1}';
        $hdrLen = strlen($headerBlock);
        $totalLen = $hdrLen + strlen($body);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("HMSG _INBOX.any 1 %d %d\r\n%s%s\r\n", $hdrLen, $totalLen, $headerBlock, $body),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $message = $client->jetStream()->directGetStreamMessage('EVENTS', 2)->await();

        // The original subject travels in Nats-Subject; the body is the raw payload.
        self::assertSame('events.order', $message->subject);
        self::assertSame($body, $message->payload);
        self::assertSame('2', NatsHeaders::fromWireBlock($message->rawHeaders)['Nats-Sequence'] ?? null);

        $written = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.DIRECT.GET.EVENTS', $written);
        self::assertStringContainsString('"seq":2', $written);
    }

    public function testDirectGetLastMessageForSubjectRequestsLastBySubj(): void
    {
        $headerBlock = "NATS/1.0\r\nNats-Stream: EVENTS\r\nNats-Subject: events.order\r\nNats-Sequence: 7\r\n\r\n";
        $body = 'last';
        $hdrLen = strlen($headerBlock);
        $totalLen = $hdrLen + strlen($body);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("HMSG _INBOX.any 1 %d %d\r\n%s%s\r\n", $hdrLen, $totalLen, $headerBlock, $body),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $message = $client->jetStream()->directGetLastMessageForSubject('EVENTS', 'events.order')->await();

        self::assertSame('events.order', $message->subject);
        self::assertSame('last', $message->payload);

        $written = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.DIRECT.GET.EVENTS', $written);
        self::assertStringContainsString('"last_by_subj":"events.order"', $written);
    }

    public function testDirectGetStreamMessageThrowsOnNotFound(): void
    {
        $statusBlock = "NATS/1.0 404 Message Not Found\r\n\r\n";
        $len = strlen($statusBlock);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("HMSG _INBOX.any 1 %d %d\r\n%s\r\n", $len, $len, $statusBlock),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Message Not Found');
        $client->jetStream()->directGetStreamMessage('EVENTS', 999)->await();
    }

    public function testSubscribeOrderedConsumerRecreatesOnSequenceGap(): void
    {
        $createReply = static fn(string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // Initial ephemeral push consumer create (request sid 1).
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply('ORD1')), $createReply('ORD1')),
            // In-order delivery: consumer seq 1 / stream seq 1 -> next expected consumer seq 2.
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
            // A missed push: consumer seq jumps to 3 (expected 2). The consumer is recreated from the
            // stream sequence after the last in-order message (1+1=2) and THIS message is DISCARDED.
            // Reply tokens: num_delivered=3, stream_seq=4, consumer_seq=3.
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
            // Gap recovery deletes the old consumer (request sid 3) ...
            sprintf("MSG _INBOX.b 3 %d\r\n%s\r\n", strlen($deleteReply), $deleteReply),
            // ... and recreates it from the expected sequence (request sid 4).
            sprintf("MSG _INBOX.c 4 %d\r\n%s\r\n", strlen($createReply('ORD2')), $createReply('ORD2')),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 6; $i++) {
            $client->processIncoming()->await();
        }

        // The out-of-order message that exposed the gap (seq 3) is DISCARDED, never delivered out of
        // order. (The in-order replay the server then produces from opt_start_seq is exercised
        // end-to-end against a real server in JetStreamIntegrationTest, since FakeTransport cannot
        // model server-side replay.)
        self::assertSame(['msg1'], $received);

        $written = implode('', $transport->writes);
        // Exactly one recreate: one DELETE of the original consumer ...
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS.ORD1'));
        // ... and two CREATEs total (the initial consumer plus the single recreate) - no storm.
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
        // The recreate resumes from the expected (first missing) sequence, not the out-of-order one.
        self::assertStringContainsString('"opt_start_seq":2', $written);
    }

    /**
     * Verifies a stale delivery from a previous (deleted) consumer instance - arriving on the reused
     * deliver inbox after a recreate - is ignored by consumer-instance name, so it cannot be
     * mis-delivered or trigger a spurious recreate even when its consumer sequence matches (#86).
     */
    public function testSubscribeOrderedConsumerIgnoresStaleDeliveryFromPreviousConsumerInstance(): void
    {
        $createReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD1',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen((string) $createReply), (string) $createReply),
            // In-order delivery from the current consumer ORD1 (consumer seq 1 -> expected next 2).
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
            // A STALE delivery from a DIFFERENT consumer instance (ORDX) whose consumer seq (2) would
            // otherwise match the expected next sequence and be (wrongly) delivered/advanced.
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORDX.1.2.2.0.0 5\r\nstale\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 4; $i++) {
            $client->processIncoming()->await();
        }

        // Only the in-order ORD1 message is delivered; the stale ORDX delivery is ignored.
        self::assertSame(['msg1'], $received);

        $written = implode('', $transport->writes);
        // The stale delivery did not trigger a recreate (no delete, only the initial create).
        self::assertSame(0, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS'));
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
    }

    /**
     * Verifies an idle heartbeat whose Nats-Last-Consumer is ahead of what was processed triggers a
     * proactive recreate from the last in-order point - detecting a missed TAIL of deliveries that no
     * further message would otherwise expose (#86).
     */
    public function testSubscribeOrderedConsumerRecreatesOnHeartbeatTailGap(): void
    {
        $createReply = static fn (string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';
        // Heartbeat reporting the server delivered up to consumer seq 3, while we only processed seq 1.
        $hbHeaders = NatsHeaders::toWireBlock([
            'Status' => '100',
            'Description' => 'Idle Heartbeat',
            'Nats-Last-Consumer' => '3',
        ]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply('ORD1')), $createReply('ORD1')),
            // In-order msg1 (consumer seq 1, stream seq 1) -> expected next 2, lastStreamSeq 1.
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
            // Idle heartbeat: last delivered consumer seq 3 > processed (1) -> tail gap -> recreate.
            sprintf("HMSG deliver.ord 2 %d %d\r\n%s\r\n", strlen($hbHeaders), strlen($hbHeaders), $hbHeaders),
            sprintf("MSG _INBOX.b 3 %d\r\n%s\r\n", strlen($deleteReply), $deleteReply),
            sprintf("MSG _INBOX.c 4 %d\r\n%s\r\n", strlen($createReply('ORD2')), $createReply('ORD2')),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 6; $i++) {
            $client->processIncoming()->await();
        }

        self::assertSame(['msg1'], $received);

        $written = implode('', $transport->writes);
        // Exactly one recreate from the last in-order point (stream seq 1 + 1 = 2).
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS.ORD1'));
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
        self::assertStringContainsString('"opt_start_seq":2', $written);
    }

    public function testSubscribeOrderedConsumerContainsRecreateFailure(): void
    {
        $createReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD1',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';
        $createError = '{"error":{"code":404,"description":"stream not found"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // Initial create (sid 1).
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen((string) $createReply), (string) $createReply),
            // In-order msg1 (consumer seq 1).
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
            // Gap (consumer seq 3) triggers recovery: delete OK (sid 3), then EVERY create attempt
            // fails (sids 4-6, one per retry) so the recreate is terminally dead (#114).
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
            sprintf("MSG _INBOX.b 3 %d\r\n%s\r\n", strlen($deleteReply), $deleteReply),
            sprintf("MSG _INBOX.c 4 %d\r\n%s\r\n", strlen($createError), $createError),
            sprintf("MSG _INBOX.d 5 %d\r\n%s\r\n", strlen($createError), $createError),
            sprintf("MSG _INBOX.e 6 %d\r\n%s\r\n", strlen($createError), $createError),
        ]);

        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        // Pump all frames. A failed recreate must be CONTAINED - it must not throw out of the shared
        // subscription dispatch loop (which would abort delivery for every other subscription).
        for ($i = 0; $i < 9; $i++) {
            $client->processIncoming()->await();
        }

        // The in-order message was delivered; the out-of-order one was discarded; the recreate was
        // retried per attempt and the terminal failure surfaced via the error listener (#114) rather
        // than escaping the dispatch loop or staying silent.
        self::assertSame(['msg1'], $received);
        self::assertCount(1, $errors);
        self::assertInstanceOf(JetStreamException::class, $errors[0]);
        self::assertStringContainsString('after 3 attempts', $errors[0]->getMessage());
    }

    public function testSubscribeOrderedConsumerRecreateRetriesThroughTransientFailure(): void
    {
        $createReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD1',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $recreateReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD2',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';
        $createError = '{"error":{"code":10008,"description":"transient failure"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen((string) $createReply), (string) $createReply),
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
            // Gap: delete OK (sid 3), the first create attempt fails (sid 4), the RETRY succeeds
            // (sid 5) as ORD2, and delivery resumes from the new consumer instance (#114).
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
            sprintf("MSG _INBOX.b 3 %d\r\n%s\r\n", strlen($deleteReply), $deleteReply),
            sprintf("MSG _INBOX.c 4 %d\r\n%s\r\n", strlen($createError), $createError),
            sprintf("MSG _INBOX.d 5 %d\r\n%s\r\n", strlen((string) $recreateReply), (string) $recreateReply),
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD2.1.4.1.0.0 4\r\nmsg4\r\n",
        ]);

        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 9; $i++) {
            $client->processIncoming()->await();
        }

        // Recovery succeeded on the second attempt: the new instance's first in-order message is
        // delivered and nothing was reported to the error listener.
        self::assertSame(['msg1', 'msg4'], $received);
        self::assertSame([], $errors);
    }

    /**
     * Verifies a deleteConsumer TimeoutException during gap recovery is treated as best-effort
     * cleanup: control still reaches the create-retry loop, the consumer is recreated, and
     * delivery resumes (#151). TimeoutException extends NatsException, NOT JetStreamException,
     * so a delete-leg-only catch would bypass the #114 retry loop entirely.
     */
    public function testSubscribeOrderedConsumerRecreatesWhenDeleteConsumerTimesOut(): void
    {
        $createReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD1',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $recreateReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD2',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        // The delete request (sid 3) gets NO reply: its wait loop spins on an empty read queue
        // until the 25 ms request deadline fires -> TimeoutException. The recreate reply and the
        // resumed delivery are enqueued only when the recreate CREATE request is actually written
        // (its payload is the only one carrying opt_start_seq 2), so the delete's timeout wait
        // cannot drain them from the queue first.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen((string) $createReply), (string) $createReply),
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
            // Gap (consumer seq 3) triggers recovery; the queue is empty from here on.
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
        ]);
        $transport->enqueueOnWriteContaining = [
            '"opt_start_seq":2' => [
                sprintf("MSG _INBOX.c 4 %d\r\n%s\r\n", strlen((string) $recreateReply), (string) $recreateReply),
                "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD2.1.4.1.0.0 4\r\nmsg4\r\n",
            ],
        ];

        $errors = [];
        $options = new NatsOptions(requestTimeoutMs: 25, errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 6; $i++) {
            $client->processIncoming()->await();
        }

        // The timed-out delete fell through to the create-retry loop: the consumer was recreated
        // as ORD2, its first in-order message was delivered, and no terminal recreate failure was
        // reported (the create leg was never exhausted).
        self::assertSame(['msg1', 'msg4'], $received);
        self::assertSame([], $errors);

        $written = implode('', $transport->writes);
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS.ORD1'));
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
    }

    /**
     * Verifies a deleteConsumer ConnectionException during gap recovery is treated as best-effort
     * cleanup: control still reaches the create-retry loop, the consumer is recreated, and
     * delivery resumes (#151). ConnectionException extends NatsException, NOT JetStreamException,
     * so a delete-leg-only catch would bypass the #114 retry loop entirely.
     */
    public function testSubscribeOrderedConsumerRecreatesWhenDeleteConsumerFailsWithConnectionError(): void
    {
        $createReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD1',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $recreateReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD2',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen((string) $createReply), (string) $createReply),
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
            // Gap (consumer seq 3) triggers recovery. The delete request's (sid 3) reply wait
            // reads the next chunk: a fatal -ERR frame, which handleFrame() raises as a
            // ConnectionException out of deleteConsumer() without closing the connection.
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
            "-ERR 'Stale Connection'\r\n",
            // The create-retry loop still runs and succeeds (sid 4) as ORD2; delivery resumes.
            sprintf("MSG _INBOX.c 4 %d\r\n%s\r\n", strlen((string) $recreateReply), (string) $recreateReply),
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD2.1.4.1.0.0 4\r\nmsg4\r\n",
        ]);

        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 8; $i++) {
            $client->processIncoming()->await();
        }

        // The failed delete fell through to the create-retry loop: the consumer was recreated as
        // ORD2, its first in-order message was delivered, and no terminal recreate failure was
        // reported (the create leg was never exhausted).
        self::assertSame(['msg1', 'msg4'], $received);
        self::assertSame([], $errors);

        $written = implode('', $transport->writes);
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS.ORD1'));
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
    }

    public function testSubscribeOrderedConsumerDeliversFilteredMessagesWithoutSpuriousRecreate(): void
    {
        $createReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD1',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        // A filtered ordered consumer over a stream that also carries non-matching messages: the
        // matching deliveries have CONSECUTIVE consumer sequences (1,2,3) but NON-contiguous stream
        // sequences (2,4,6, because non-matching messages occupy 1,3,5). They must all be delivered
        // in order with NO recreate - detecting gaps by stream sequence would wrongly recreate here.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.1.2.1.0.0 4\r\nmsg1\r\n", // cseq 1, sseq 2
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.2.4.2.0.0 4\r\nmsg2\r\n", // cseq 2, sseq 4
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.3.6.3.0.0 4\r\nmsg3\r\n", // cseq 3, sseq 6
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 5; $i++) {
            $client->processIncoming()->await();
        }

        self::assertSame(['msg1', 'msg2', 'msg3'], $received);

        $written = implode('', $transport->writes);
        // No gap was detected, so the consumer is never deleted/recreated (one initial CREATE only).
        self::assertSame(0, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS'));
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
    }

    // ─── New coverage additions ─────────────────────────────────────────

    /**
     * Verifies createOrUpdateStream re-throws a JetStreamException whose message is NOT "already in use"
     * (the guard only swallows the "already in use" variant).
     */
    public function testCreateOrUpdateStreamRethrowsNonAlreadyInUseError(): void
    {
        $createErr = '{"error":{"code":503,"description":"JetStream not enabled"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createErr), $createErr),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('JetStream not enabled');

        $client->jetStream()->createOrUpdateStream('ORDERS', ['orders.*'])->await();
    }

    /**
     * Verifies streamNames() handles a missing / null 'streams' key in the API response by
     * returning an empty list (the ternary else branch).
     */
    public function testStreamNamesWithNullStreamsKeyReturnsEmpty(): void
    {
        // The server returns a valid-but-empty response (no 'streams' key at all).
        $reply = '{"total":0,"offset":0}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $names = $client->jetStream()->streamNames()->await();

        self::assertSame([], $names);
    }

    /**
     * Verifies directGet (via directGetLastMessageForSubject) throws when the reply carries no
     * Nats-Stream or Nats-Sequence header - the "unrecognized response" guard.
     */
    public function testDirectGetThrowsForUnrecognizedResponse(): void
    {
        // A response with Nats-* headers missing (no status, no Nats-Stream, no Nats-Sequence).
        $hdrs = "NATS/1.0\r\nX-Unrelated: yes\r\n\r\n";
        $body = 'some payload';
        $h = strlen($hdrs);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("HMSG _INBOX.a 1 %d %d\r\n%s%s\r\n", $h, $h + strlen($body), $hdrs, $body),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('JetStream direct get returned an unrecognized response');

        $client->jetStream()->directGetLastMessageForSubject('ORDERS', 'orders.1')->await();
    }

    /**
     * Verifies directGetLastForSubjects() returns an empty list immediately when the subjects
     * array is empty (early return).
     */
    public function testDirectGetLastForSubjectsWithEmptySubjectsReturnsEmpty(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $messages = $client->jetStream()->directGetLastForSubjects('ORDERS', [])->await();

        self::assertSame([], $messages);
    }

    /**
     * Verifies directGetLastForSubjects() rejects a subject containing '*'.
     */
    public function testDirectGetLastForSubjectsRejectsWildcardSubjectWithStar(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('directGetLastForSubjects expects exact subjects');

        $client->jetStream()->directGetLastForSubjects('ORDERS', ['orders.*'])->await();
    }

    /**
     * Verifies directGetLastForSubjects() rejects a subject containing '>'.
     */
    public function testDirectGetLastForSubjectsRejectsWildcardSubjectWithGreaterThan(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('directGetLastForSubjects expects exact subjects');

        $client->jetStream()->directGetLastForSubjects('ORDERS', ['orders.>'])->await();
    }

    /**
     * Verifies directGetBatch() rejects a non-positive expiresMs.
     */
    public function testDirectGetBatchRejectsZeroExpiresMs(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Direct Get batch expiresMs must be greater than zero');

        $client->jetStream()->directGetBatch('ORDERS', ['batch' => 1], 0)->await();
    }

    /**
     * Verifies addOrUpdateConsumer delegates to createConsumer and produces the same
     * wire payload.
     */
    public function testAddOrUpdateConsumerDelegatesToCreateConsumer(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $info = $client->jetStream()->addOrUpdateConsumer('ORDERS', 'PROC', 'orders.*')->await();

        self::assertSame('PROC', $info->name);
        self::assertStringContainsString('$JS.API.CONSUMER.CREATE.ORDERS.PROC', $transport->writes[3]);
    }

    /**
     * Verifies consumerNames() handles a missing / null 'consumers' key in the API response by
     * returning an empty list (the ternary else branch).
     */
    public function testConsumerNamesWithMissingConsumersKeyReturnsEmpty(): void
    {
        $reply = '{"total":0,"offset":0}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $names = $client->jetStream()->consumerNames('ORDERS')->await();

        self::assertSame([], $names);
    }

    /**
     * Verifies subscribeEphemeralPushConsumer silently absorbs a flow-control status 100 frame
     * and does not forward it to the user handler (early return for control message).
     */
    public function testSubscribeEphemeralPushConsumerIgnoresControlMessages(): void
    {
        $createPayload = '{"stream_name":"ORDERS","name":"E1","config":{"deliver_subject":"deliver.eph"}}';
        $fcHeaders = NatsHeaders::toWireBlock(['Status' => '100', 'Description' => 'FlowControl Request']);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            sprintf(
                "HMSG deliver.eph 2 fc.reply %d %d\r\n%s\r\n",
                strlen($fcHeaders),
                strlen($fcHeaders),
                $fcHeaders,
            ),
            "MSG deliver.eph 2 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeEphemeralPushConsumer(
            'ORDERS',
            static function (NatsMessage $message) use (&$received): void {
                $received[] = $message->payload;
            },
            'deliver.eph',
        )->await();

        $client->processIncoming()->await(); // flow control - must NOT appear in $received
        $client->processIncoming()->await(); // real message

        self::assertSame(['hello'], $received);
        // The FC reply was published.
        self::assertStringContainsString("PUB fc.reply 0\r\n\r\n", implode('', $transport->writes));
    }

    /**
     * Verifies subscribeOrderedConsumer silently absorbs a flow-control / heartbeat status 100
     * and does not forward it to the user handler (early return for control message).
     */
    public function testSubscribeOrderedConsumerIgnoresControlMessages(): void
    {
        $createPayload = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD_HB',
            'config' => ['ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $hbHeaders = NatsHeaders::toWireBlock(['Status' => '100', 'Description' => 'Idle Heartbeat']);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            sprintf(
                "HMSG _INBOX.JS.ORD.x 2 %d %d\r\n%s\r\n",
                strlen($hbHeaders),
                strlen($hbHeaders),
                $hbHeaders,
            ),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer(
            'EVENTS',
            static function (NatsMessage $message) use (&$received): void {
                $received[] = $message->payload;
            },
        )->await();

        $client->processIncoming()->await();

        self::assertSame([], $received);
    }

    /**
     * Verifies subscribeOrderedConsumer delivers a message that has no $JS.ACK reply subject
     * (no ordering metadata) best-effort to the user handler (null seq path), and does so
     * SILENTLY - the unparseable-ack protocol error (#155) applies only to reply subjects that
     * claim the $JS.ACK form, not to plain or absent reply subjects.
     */
    public function testSubscribeOrderedConsumerDeliversMessageWithoutAckMetadata(): void
    {
        $createPayload = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD_NO_ACK',
            'config' => ['ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // Ephemeral push consumer create response.
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            // A delivery with no $JS.ACK reply subject - no ordering metadata.
            "MSG _INBOX.JS.ORD.x 2 5\r\nhello\r\n",
            // A delivery with a plain (non-$JS.ACK) reply subject - also no ordering metadata.
            "MSG _INBOX.JS.ORD.x 2 plain.reply 6\r\nhello2\r\n",
        ]);

        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer(
            'EVENTS',
            static function (NatsMessage $message) use (&$received): void {
                $received[] = $message->payload;
            },
        )->await();

        $client->processIncoming()->await();
        $client->processIncoming()->await();

        // Best-effort delivery: the messages are forwarded despite having no ordering information,
        // and no error is emitted for reply subjects that never claimed the $JS.ACK form.
        self::assertSame(['hello', 'hello2'], $received);
        self::assertSame([], $errors);
    }

    /**
     * Verifies subscribeOrderedConsumer surfaces an unparseable $JS.ACK reply subject (an ack-form
     * claim the parser cannot read, so gap detection and the stale-consumer filter cannot run)
     * through the error listener while still delivering the message best-effort (#155). Silent
     * before the fix: the null-metadata branch bypassed every ordering check without a trace.
     */
    public function testSubscribeOrderedConsumerEmitsErrorForUnparseableAckSubject(): void
    {
        $createPayload = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD_BAD_ACK',
            'config' => ['ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            // 10 tokens: claims the $JS.ACK form but matches neither the 9-token v1 form nor the
            // >= 11-token v2 form, so it is unparseable even for the tolerant parser.
            "MSG _INBOX.JS.ORD.x 2 \$JS.ACK.EVENTS.ORD_BAD_ACK.1.1.1.0.0.X 4\r\nmsg1\r\n",
        ]);

        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer(
            'EVENTS',
            static function (NatsMessage $message) use (&$received): void {
                $received[] = $message->payload;
            },
        )->await();

        $client->processIncoming()->await();

        // The message is still delivered best-effort (no recreate, no drop) ...
        self::assertSame(['msg1'], $received);
        // ... but the degradation is loud: ordering checks could not run for this delivery.
        self::assertCount(1, $errors);
        self::assertInstanceOf(JetStreamException::class, $errors[0]);
        self::assertStringContainsString('unparseable $JS.ACK reply subject', $errors[0]->getMessage());
        self::assertStringContainsString('EVENTS', $errors[0]->getMessage());
        // A parse failure must NOT trigger a recreate - only the initial CREATE goes on the wire.
        $written = implode('', $transport->writes);
        self::assertSame(0, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS'));
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
    }

    /**
     * Verifies the unparseable-ack error is emitted ONCE per consumer instance, not once per
     * message - a stream of unparseable deliveries must not become an error storm (#155). All
     * messages are still delivered best-effort.
     */
    public function testSubscribeOrderedConsumerEmitsUnparseableAckErrorOncePerConsumer(): void
    {
        $createPayload = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD_BAD_ACK',
            'config' => ['ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            // Two consecutive 10-token (unparseable) ack subjects on the same consumer instance.
            "MSG _INBOX.JS.ORD.x 2 \$JS.ACK.EVENTS.ORD_BAD_ACK.1.1.1.0.0.X 4\r\nmsg1\r\n",
            "MSG _INBOX.JS.ORD.x 2 \$JS.ACK.EVENTS.ORD_BAD_ACK.2.2.2.0.0.X 4\r\nmsg2\r\n",
        ]);

        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer(
            'EVENTS',
            static function (NatsMessage $message) use (&$received): void {
                $received[] = $message->payload;
            },
        )->await();

        $client->processIncoming()->await();
        $client->processIncoming()->await();

        // Both messages delivered best-effort; the protocol error fired exactly once.
        self::assertSame(['msg1', 'msg2'], $received);
        self::assertCount(1, $errors);
    }

    /**
     * Verifies the once-per-consumer-instance latch of the unparseable-ack error re-arms on a
     * recreate: a new consumer epoch that again produces unparseable ack subjects emits a fresh
     * error, so long-lived subscriptions do not go permanently silent after the first one (#155).
     */
    public function testSubscribeOrderedConsumerUnparseableAckErrorRearmsAfterRecreate(): void
    {
        $createReply = static fn (string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';
        // Heartbeat reporting the server delivered up to consumer seq 3 while only seq 1 was
        // processed in order - triggers the tail-gap recreate (a new consumer epoch).
        $hbHeaders = NatsHeaders::toWireBlock([
            'Status' => '100',
            'Description' => 'Idle Heartbeat',
            'Nats-Last-Consumer' => '3',
        ]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply('ORD1')), $createReply('ORD1')),
            // Unparseable (10-token) ack subject on the first epoch -> first error.
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.1.1.1.0.0.X 4\r\nbad1\r\n",
            // In-order delivery (consumer seq 1 / stream seq 1) so the tail-gap heartbeat below has
            // a processed baseline to compare against.
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
            // Tail-gap heartbeat -> recreate: delete ORD1 (sid 3), create ORD2 (sid 4).
            sprintf("HMSG deliver.ord 2 %d %d\r\n%s\r\n", strlen($hbHeaders), strlen($hbHeaders), $hbHeaders),
            sprintf("MSG _INBOX.b 3 %d\r\n%s\r\n", strlen($deleteReply), $deleteReply),
            sprintf("MSG _INBOX.c 4 %d\r\n%s\r\n", strlen($createReply('ORD2')), $createReply('ORD2')),
            // Unparseable ack subject on the SECOND epoch -> the latch re-armed, second error.
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD2.1.1.1.0.0.X 4\r\nbad2\r\n",
        ]);

        $errors = [];
        $options = new NatsOptions(errorListener: static function (\Throwable $error) use (&$errors): void {
            $errors[] = $error;
        });

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 8; $i++) {
            $client->processIncoming()->await();
        }

        // Best-effort deliveries from both epochs plus the one in-order message.
        self::assertSame(['bad1', 'msg1', 'bad2'], $received);
        // One unparseable-ack error per consumer epoch: first epoch and post-recreate epoch.
        self::assertCount(2, $errors);
        self::assertStringContainsString('unparseable $JS.ACK reply subject', $errors[0]->getMessage());
        self::assertStringContainsString('unparseable $JS.ACK reply subject', $errors[1]->getMessage());
    }

    /**
     * Verifies subscribeOrderedConsumer best-effort delivers when deleteConsumer throws a
     * JetStreamException during gap recovery (the inner catch block).
     */
    public function testSubscribeOrderedConsumerToleratesDeleteConsumerFailure(): void
    {
        $createReply = static fn(string $name): string => json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => '_INBOX.JS.ORD.test', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        // deleteConsumer returns an error payload, triggering JetStreamException inside the try block.
        $deleteError = '{"error":{"code":404,"description":"consumer not found"}}';
        $recreateReply = $createReply('ORD2');

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // Initial create (sid 1).
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply('ORD1')), $createReply('ORD1')),
            // In-order message (consumer seq 1 / stream seq 1).
            "MSG _INBOX.JS.ORD.test 2 \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
            // Out-of-order message (consumer seq jumps to 3, triggers recreation).
            "MSG _INBOX.JS.ORD.test 2 \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
            // deleteConsumer returns an error - JetStreamException caught.
            sprintf("MSG _INBOX.b 3 %d\r\n%s\r\n", strlen($deleteError), $deleteError),
            // createEphemeralPushConsumer still proceeds and succeeds (sid 4).
            sprintf("MSG _INBOX.c 4 %d\r\n%s\r\n", strlen($recreateReply), $recreateReply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        for ($i = 0; $i < 6; $i++) {
            $client->processIncoming()->await();
        }

        // msg1 is delivered; bad3 (out-of-order) is discarded; delete failure is absorbed.
        self::assertSame(['msg1'], $received);
    }

    /**
     * Verifies unpinConsumer rejects an empty priority group name.
     */
    public function testUnpinConsumerRejectsEmptyGroup(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Priority group must not be empty');

        $client->jetStream()->unpinConsumer('ORDERS', 'PROC', '')->await();
    }

    /**
     * Verifies publish() with an expectation header immediately re-throws a precondition-mismatch
     * JetStreamException without retrying (non-503 code triggers immediate throw).
     *
     * This variant uses expectedLastSubjectSequence to produce the header path, so the publish
     * goes via requestWithHeaders (HPUB), not plain PUB.
     */
    public function testPublishWithLastSubjectSequenceHeaderMismatchThrowsImmediately(): void
    {
        $errorAck = '{"error":{"code":400,"err_code":10071,"description":"wrong last sequence"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorAck), $errorAck),
        ]);

        // Use retryAttempts=3 so that a retry would produce a second request (proving we don't retry).
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();
        $js = new JetStreamContext($client, publishRetryAttempts: 3, publishRetryWaitMs: 1);

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('wrong last sequence');

        $js->publish('orders.created', 'body', expectedLastSubjectSequence: 0)->await();

        // If the exception propagates there was only one request (no retry).
        self::assertCount(4, $transport->writes); // CONNECT + PING + SUB + HPUB
    }

    /**
     * Verifies counterValue() re-throws a non-404 JetStreamException from the Direct Get
     * (the throw-if-not-404 branch).
     */
    public function testCounterValueRethrowsNon404Exception(): void
    {
        // A 403 status from directGet maps to a JetStreamException with code 403.
        $hdrs = "NATS/1.0 403 Forbidden\r\nDescription: access denied\r\n\r\n";
        $h = strlen($hdrs);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("HMSG _INBOX.a 1 %d %d\r\n%s\r\n", $h, $h, $hdrs),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionCode(403);

        $client->jetStream()->counterValue('COUNTERS', 'counters.visits')->await();
    }

    /**
     * Verifies parseCounterValue() wraps a JsonException in a JetStreamException
     * (malformed counter payload).
     */
    public function testIncrementCounterWithMalformedResponsePayload(): void
    {
        // The server returns a non-JSON body for the counter response.
        $badPayload = 'NOT_JSON{{{';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($badPayload), $badPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Malformed counter response');

        $client->jetStream()->incrementCounter('counters.visits', '+1')->await();
    }

    /**
     * Verifies parseCounterValue() maps an embedded API error to a JetStreamException
     * (error key present in counter response).
     */
    public function testIncrementCounterWithApiErrorInResponse(): void
    {
        $errorPayload = '{"error":{"code":400,"description":"counter not enabled on stream"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('counter not enabled on stream');

        $client->jetStream()->incrementCounter('counters.visits', '+1')->await();
    }

    /**
     * Verifies parseCounterValue() returns a string for an integer val field
     * (is_int($val) branch, returns string cast).
     */
    public function testIncrementCounterWithIntegerValField(): void
    {
        // The server may return an unquoted integer for the val field on some code paths.
        $payload = '{"stream":"COUNTERS","seq":3,"val":7}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($payload), $payload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $value = $client->jetStream()->incrementCounter('counters.visits', '+1')->await();

        self::assertSame('7', $value);
    }

    /**
     * Verifies parseCounterValue() throws when neither int nor string val is present
     * (missing val field).
     */
    public function testIncrementCounterWithMissingValFieldThrows(): void
    {
        $payload = '{"stream":"COUNTERS","seq":4}'; // no "val" key

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($payload), $payload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Counter response did not include a value');

        $client->jetStream()->incrementCounter('counters.visits', '+1')->await();
    }

    /**
     * Verifies fetchBatch() throws a 408 JetStreamException when no messages arrive and no
     * terminal status is received (pure timeout path, empty messages, no status).
     */
    public function testFetchBatchThrowsTimeoutWhenNoMessagesAndNoTerminalStatus(): void
    {
        $transport = new FakeTransport(
            readQueue: [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                "PONG\r\n",
            ],
            blockWhenEmpty: true,
        );

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('No messages received within timeout');
        $this->expectExceptionCode(408);

        // expiresMs=1 causes the TimeoutCancellation to fire almost immediately with no messages.
        $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 1)->await();
    }

    /**
     * Verifies ackSync() throws JetStreamException when replyTo is an empty string.
     */
    public function testAckSyncThrowsForEmptyReplySubject(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('JetStream ACK requires a reply subject on the delivered message');

        $message = new NatsMessage('events.x', 1, '', 'body');
        $client->jetStream()->ackSync($message)->await();
    }

    /**
     * Verifies applyFilterSubjects() (via createConsumer) throws when filter_subjects is not an
     * array (the non-array branch of the array_key_exists guard).
     */
    public function testCreateConsumerRejectsNonArrayFilterSubjects(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('filter_subjects must be a non-empty array of subjects');

        try {
            // Passing a string instead of an array for filter_subjects.
            $client->jetStream()->createConsumer('ORDERS', 'PROC', null, ['filter_subjects' => 'orders.>'])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies applyFilterSubjects() (via createConsumer) throws when filter_subjects is an empty
     * array (the $subjects === [] branch).
     */
    public function testCreateConsumerRejectsEmptyArrayFilterSubjects(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('filter_subjects must be a non-empty array of subjects');

        try {
            $client->jetStream()->createConsumer('ORDERS', 'PROC', null, ['filter_subjects' => []])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies buildPullRequest() (via fetchBatch) throws when the pull group name is invalid
     * (group validation failure).
     */
    public function testFetchBatchRejectsInvalidPullGroupName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Pull group must be 1..16 characters of [A-Za-z0-9-_/=]');

        try {
            // A group name with spaces is invalid.
            $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500, ['group' => 'invalid group!'])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies assertValidPriorityConfig() (via createConsumer) throws when priority_groups is
     * an empty array.
     */
    public function testCreateConsumerRejectsEmptyPriorityGroups(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('priority_groups must be a non-empty array of group names');

        try {
            $client->jetStream()->createConsumer('ORDERS', 'PROC', null, ['priority_groups' => []])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies assertValidPriorityConfig() (via createConsumer) throws when a priority group name
     * contains invalid characters.
     */
    public function testCreateConsumerRejectsInvalidPriorityGroupName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('priority_groups names must be 1..16 characters of [A-Za-z0-9-_/=]');

        try {
            // A group name with spaces is invalid.
            $client->jetStream()->createConsumer('ORDERS', 'PROC', null, ['priority_groups' => ['invalid name!']])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies directGetBatch() catches CancelledException when the wait-cancellation
     * fires while processIncoming is blocked on an idle socket (blockWhenEmpty path).
     *
     * The method should return an empty array rather than propagating the cancellation.
     */
    public function testDirectGetBatchReturnsEmptyArrayOnTimeout(): void
    {
        $transport = new FakeTransport(
            readQueue: [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                "PONG\r\n",
            ],
            blockWhenEmpty: true,
        );

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // expiresMs=1 makes the TimeoutCancellation fire after ~1001 ms; the blocking transport
        // keeps processIncoming suspended so the cancellation is the only way out - the
        // catch (CancelledException) in directGetBatch().
        $messages = $client->jetStream()->directGetBatch('ORDERS', ['batch' => 10], 1)->await();

        self::assertSame([], $messages);
    }

    /**
     * Verifies jsRequest() re-throws a non-"No responders" NatsException unchanged
     * when the underlying client->request() fails with a TimeoutException.
     *
     * Triggered via incrementCounter(), which calls jsRequest() directly.
     */
    public function testJsRequestRethrowsNonNoRespondersNatsException(): void
    {
        // blockWhenEmpty keeps processIncoming() suspended so the request's TimeoutCancellation
        // (requestTimeoutMs=1 ms) fires during the read, producing a TimeoutException.  That
        // exception is a NatsException whose message does NOT contain "No responders", so
        // jsRequest() re-throws it unchanged.
        $transport = new FakeTransport(
            readQueue: [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
                "PONG\r\n",
            ],
            blockWhenEmpty: true,
        );

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1), $transport);
        $client->connect()->await();

        $this->expectException(TimeoutException::class);
        $this->expectExceptionMessage('Request timed out');

        // incrementCounter calls jsRequest() directly (without publishWithRetry), so any
        // NatsException that isn't "No responders" surfaces.
        $client->jetStream()->incrementCounter('counters.hits', '+1')->await();
    }

    /**
     * Verifies publishWithRetry() re-throws a JetStreamException when all configured
     * retry attempts are exhausted on transient 503 "no-responder" failures.
     *
     * The publish() path goes: publish() -> publishWithRetry() -> jsRequest() -> client->request().
     * A 503 HMSG status makes requestInternal() throw NatsException("No responders..."), which
     * jsRequest() converts to JetStreamException(503).  publishWithRetry() retries up to
     * $publishRetryAttempts times; on the final attempt ($attempt >= $attempts) it re-throws.
     */
    public function testPublishWithRetryRethrowsWhenRetriesExhausted(): void
    {
        $status = "NATS/1.0 503\r\n\r\n";

        // Two 503 frames: attempt 1 gets the first 503 (retried), attempt 2 gets the second 503
        // (attempt >= attempts=2 -> the retries-exhausted re-throw fires).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            'HMSG _INBOX.a 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
            'HMSG _INBOX.b 2 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // publishRetryAttempts=2, publishRetryWaitMs=1 (tight loop so the test is fast).
        $js = new JetStreamContext($client, publishRetryAttempts: 2, publishRetryWaitMs: 1);

        $this->expectException(JetStreamException::class);
        $this->expectExceptionCode(503);
        $this->expectExceptionMessage('No JetStream responder');

        $js->publish('orders.created', '{"id":1}')->await();
    }

    /**
     * Verifies createStream() rejects a dotted stream name before any $JS.API write reaches the
     * wire: a '.' in the name would corrupt the API subject and hit the wrong endpoint (#131).
     */
    public function testCreateStreamRejectsDottedStreamNameBeforeDispatch(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "bad.name"');

        try {
            $client->jetStream()->createStream('bad.name', ['s.*'])->await();
        } finally {
            // Only CONNECT and PING were written - the invalid name never produced a $JS.API PUB.
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies createConsumer() rejects a dotted consumer name before dispatch: the server would
     * route CONSUMER.CREATE.S.a.b as the filtered-create form (consumer "a", filter "b") (#131).
     */
    public function testCreateConsumerRejectsDottedConsumerName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid consumer name "a.b"');

        try {
            $client->jetStream()->createConsumer('ORDERS', 'a.b')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies getConsumer() rejects a dotted consumer name before dispatch: CONSUMER.INFO.S.a.b
     * hits no API route and would surface a misleading 503 "subject is not bound to a stream" (#131).
     */
    public function testGetConsumerRejectsDottedConsumerName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid consumer name "a.b"');

        try {
            $client->jetStream()->getConsumer('ORDERS', 'a.b')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies deleteConsumer() rejects a dotted consumer name before dispatch (#131).
     */
    public function testDeleteConsumerRejectsDottedConsumerName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid consumer name "a.b"');

        try {
            $client->jetStream()->deleteConsumer('ORDERS', 'a.b')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies directGetStreamMessage() rejects a dotted stream name before dispatch: the server
     * could route DIRECT.GET.FOO.BAR as DIRECT.GET.<stream>.<last_by_subject> and silently return
     * data from a SIBLING stream (#131).
     */
    public function testDirectGetStreamMessageRejectsDottedStreamName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "FOO.BAR"');

        try {
            $client->jetStream()->directGetStreamMessage('FOO.BAR', 1)->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies an empty stream name is rejected before dispatch (#131).
     */
    public function testCreateStreamRejectsEmptyStreamName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('must be non-empty');

        try {
            $client->jetStream()->createStream('', ['s.*'])->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies a wildcard '*' stream name is rejected before dispatch (#131).
     */
    public function testGetStreamRejectsWildcardStreamName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid stream name "*"');

        try {
            $client->jetStream()->getStream('*')->await();
        } finally {
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * Verifies a valid name (letters, digits, '-' and '_') passes validation and produces the
     * expected $JS.API subject on the wire (#131).
     */
    public function testCreateStreamAcceptsValidNameWithHyphenAndUnderscore(): void
    {
        $streamPayload = '{"config":{"name":"ORDERS-2_prod","subjects":["orders.*"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamPayload), $streamPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $created = $client->jetStream()->createStream('ORDERS-2_prod', ['orders.*'])->await();

        self::assertSame('ORDERS-2_prod', $created->name);
        self::assertStringContainsString('$JS.API.STREAM.CREATE.ORDERS-2_prod', $transport->writes[3]);
    }
}
