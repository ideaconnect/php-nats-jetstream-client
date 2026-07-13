<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\KeyValue\KeyValueBucket;
use IDCT\NATS\JetStream\KeyValue\KeyValueEntry;
use IDCT\NATS\JetStream\KeyValue\KeyWatchOptions;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\delay;

final class KeyValueBucketTest extends TestCase
{
    /**
     * A KV watch now arms an idle-heartbeat watchdog (a live EventLoop repeat timer, #113) that outlives
     * a test if its client is not disconnected. Cancel every registered callback before each test so a
     * leaked watchdog cannot fire against a later test's clean event loop.
     */
    protected function setUp(): void
    {
        foreach (EventLoop::getIdentifiers() as $id) {
            EventLoop::cancel($id);
        }
    }

    /** Builds a Direct Get reply (HMSG): the stored value as the body, with Nats-* + optional KV-Operation. */
    private function kvDirectReply(string $subject, string $value, int $seq, int $sid, ?string $operation = null): string
    {
        $hdrs = "NATS/1.0\r\nNats-Stream: KV_cfg\r\nNats-Subject: {$subject}\r\nNats-Sequence: {$seq}\r\n";
        if ($operation !== null) {
            $hdrs .= "KV-Operation: {$operation}\r\n";
        }
        $hdrs .= "\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s%s\r\n", $sid, $h, $h + strlen($value), $hdrs, $value);
    }

    /** Builds a Direct Get status-only reply (HMSG), e.g. a 404 miss or a non-404 error. */
    private function kvDirectStatus(int $sid, int $code, string $description): string
    {
        $hdrs = "NATS/1.0 {$code} {$description}\r\nStatus: {$code}\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s\r\n", $sid, $h, $h, $hdrs);
    }

    public function testGetFallsBackToStreamMessageWhenDirectGetUnavailable(): void
    {
        // Direct Get -> 503 no-responders (allow_direct disabled / interop bucket); get() must fall
        // back to the leader STREAM.MSG.GET path and still return the value.
        $envelope = sprintf(
            '{"message":{"subject":"$KV.cfg.theme","seq":9,"data":"%s"}}',
            base64_encode('blue'),
        );

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->kvDirectStatus(1, 503, 'No Responders'),                          // Direct Get (sid 1)
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($envelope), $envelope),    // STREAM.MSG.GET fallback (sid 2)
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $entry = $client->jetStream()->keyValue('cfg')->get('theme')->await();

        self::assertNotNull($entry);
        self::assertSame('blue', $entry->value);
        self::assertSame('PUT', $entry->operation);
        self::assertSame(9, $entry->revision);

        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('PUB $JS.API.DIRECT.GET.KV_cfg', $writes);
        self::assertStringContainsString('PUB $JS.API.STREAM.MSG.GET.KV_cfg', $writes);
    }

    /**
     * Verifies KV bucket create/delete map to KV stream lifecycle APIs.
     */
    public function testBucketCreateAndDelete(): void
    {
        $createPayload = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]}}';
        $deletePayload = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
     * Verifies create() defaults include deny_delete and discard:new (ADR-8 / nats.go parity) and
     * that a user override wins (#132).
     */
    public function testBucketCreateSendsKvDefaultsAndAllowsOverride(): void
    {
        $createPayload = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('cfg');
        $kv->create()->await();
        $kv->create(['deny_delete' => false, 'discard' => 'old'])->await();

        self::assertStringContainsString('"deny_delete":true', $transport->writes[3]);
        self::assertStringContainsString('"discard":"new"', $transport->writes[3]);
        self::assertStringContainsString('"deny_delete":false', $transport->writes[6]);
        self::assertStringContainsString('"discard":"old"', $transport->writes[6]);
    }

    /**
     * Verifies KV put/get/delete operations map and parse values correctly.
     */
    public function testPutGetDelete(): void
    {
        $putAck = '{"stream":"KV_cfg","seq":1,"duplicate":false}';
        $deleteAck = '{"stream":"KV_cfg","seq":2,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
            $this->kvDirectReply('$KV.cfg.theme', 'blue', 1, 2),
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
        self::assertStringStartsWith('PUB $JS.API.DIRECT.GET.KV_cfg _INBOX.', $transport->writes[6]);
        self::assertStringStartsWith('HPUB $KV.cfg.theme _INBOX.', $transport->writes[9]);
        self::assertStringContainsString('KV-Operation:DEL', $transport->writes[9]);
    }

    /**
     * A no-responders reply to a KV header request (every put/update/delete with headers routes
     * through publishWithHeadersAck) must surface as JetStreamException(503) - the same taxonomy
     * jsRequest() produces - not a bare NatsException, so a caller catching JetStreamException on a
     * JetStream-disabled server or unbound subject is not surprised (#161).
     */
    public function testDeleteHeaderRequestNormalizesNoRespondersTo503(): void
    {
        $status = "NATS/1.0 503\r\n\r\n";

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // delete() issues an HPUB request on the first request inbox (sid 1); the server has no
            // responder bound, so it replies with a 503 no-responders status.
            'HMSG _INBOX.a 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionCode(503);
        $this->expectExceptionMessage('No JetStream responder');

        $client->jetStream()->keyValue('cfg')->delete('theme')->await();
    }

    /**
     * Verifies createKey() succeeds on an absent key, asserting expected-last-subject-sequence 0 (#19).
     */
    public function testCreateKeySucceedsWhenAbsent(): void
    {
        $putAck = '{"stream":"KV_cfg","seq":1,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->keyValue('cfg')->createKey('theme', 'blue')->await();

        self::assertSame(1, $ack->seq);
        self::assertStringStartsWith('HPUB $KV.cfg.theme _INBOX.', $transport->writes[3]);
        self::assertStringContainsString('Nats-Expected-Last-Subject-Sequence:0', $transport->writes[3]);
    }

    /**
     * Verifies createKey() throws when the key already has a live value (#19).
     */
    public function testCreateKeyThrowsWhenKeyExists(): void
    {
        // First attempt (expected seq 0) is rejected with a wrong-last-sequence error ack...
        $errAck = '{"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 4"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
            // ...then get() (Direct Get) shows a live value, so the key really exists.
            $this->kvDirectReply('$KV.cfg.theme', 'green', 4, 2),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Key already exists');
        $client->jetStream()->keyValue('cfg')->createKey('theme', 'blue')->await();
    }

    /**
     * Verifies createKey() detects the wrong-last-sequence rejection by err_code 10071 even when the
     * server description is reworded (#154).
     */
    public function testCreateKeyDetectsWrongLastSequenceByErrCode(): void
    {
        // Same rejection as testCreateKeyThrowsWhenKeyExists, but the description shares no wording
        // with "wrong last sequence" - only err_code 10071 identifies it.
        $errAck = '{"error":{"code":400,"err_code":10071,"description":"expected stream sequence does not match"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
            $this->kvDirectReply('$KV.cfg.theme', 'green', 4, 2),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Key already exists');
        $client->jetStream()->keyValue('cfg')->createKey('theme', 'blue')->await();
    }

    /**
     * Verifies createKey() still detects wrong-last-sequence via the description substring when the
     * envelope carries no err_code (old servers) (#154).
     */
    public function testCreateKeyDetectsWrongLastSequenceWithoutErrCode(): void
    {
        $errAck = '{"error":{"code":400,"description":"wrong last sequence: 4"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
            $this->kvDirectReply('$KV.cfg.theme', 'green', 4, 2),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Key already exists');
        $client->jetStream()->keyValue('cfg')->createKey('theme', 'blue')->await();
    }

    /**
     * Verifies a present err_code wins over a misleading description: an envelope whose err_code is
     * NOT 10071 must be rethrown as-is even when the description says "wrong last sequence", so a
     * different API error can never be misread as a key collision (#154).
     */
    public function testCreateKeyRethrowsWhenErrCodeIsNotWrongLastSequence(): void
    {
        $errAck = '{"error":{"code":400,"err_code":10052,"description":"wrong last sequence wording, different error"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->keyValue('cfg')->createKey('theme', 'blue')->await();
            self::fail('a non-10071 err_code must be rethrown, not treated as a key collision');
        } catch (JetStreamException $e) {
            self::assertSame(10052, $e->getErrCode());
            self::assertStringNotContainsString('Key already exists', $e->getMessage());
        }
    }

    /**
     * Verifies the "Key already exists" collision exception keeps the server taxonomy: the HTTP-like
     * 400 in getCode() and the API err_code 10071 in getErrCode() (#154).
     */
    public function testCreateKeyExistsExceptionCarriesBothCodes(): void
    {
        $errAck = '{"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 4"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
            $this->kvDirectReply('$KV.cfg.theme', 'green', 4, 2),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->keyValue('cfg')->createKey('theme', 'blue')->await();
            self::fail('Expected a JetStreamException');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('Key already exists', $e->getMessage());
            self::assertSame(400, $e->getCode());
            self::assertSame(10071, $e->getErrCode());
        }
    }

    /**
     * Verifies create() with a mirror translates the bucket name to KV_ and emits no subjects (#62).
     */
    public function testCreateWithMirrorTranslatesBucketName(): void
    {
        $reply = '{"config":{"name":"KV_dst"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('dst')->create(['mirror' => 'src'])->await();

        $create = $transport->writes[3];
        self::assertStringContainsString('$JS.API.STREAM.CREATE.KV_dst', $create);
        self::assertStringContainsString('"mirror":{"name":"KV_src"}', $create);
        self::assertStringContainsString('"subjects":[]', $create);
    }

    /**
     * Verifies create() with sources + extended config translates source names and passes config (#62).
     */
    public function testCreateWithSourcesAndExtendedConfig(): void
    {
        $reply = '{"config":{"name":"KV_agg"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('agg')->create([
            'sources' => ['b1', 'b2'],
            'compression' => 's2',
            'placement' => ['cluster' => 'c1'],
        ])->await();

        $create = $transport->writes[3];
        self::assertStringContainsString('"sources":[{"name":"KV_b1"},{"name":"KV_b2"}]', $create);
        self::assertStringContainsString('"compression":"s2"', $create);
        self::assertStringContainsString('"placement":{"cluster":"c1"}', $create);
    }

    /**
     * Verifies getRevision returns the entry stored at a specific sequence (#33).
     */
    public function testGetRevisionReturnsEntryAtSequence(): void
    {
        $reply = '{"message":{"subject":"$KV.cfg.theme","seq":2,"data":"' . base64_encode('blue') . '"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $entry = $client->jetStream()->keyValue('cfg')->getRevision('theme', 2)->await();

        self::assertNotNull($entry);
        self::assertSame('theme', $entry->key);
        self::assertSame('blue', $entry->value);
        self::assertSame(2, $entry->revision);
        self::assertStringContainsString('$JS.API.STREAM.MSG.GET.KV_cfg', $transport->writes[3]);
        self::assertStringContainsString('"seq":2', $transport->writes[3]);
    }

    /**
     * Verifies getRevision returns null when the sequence stores a different key (#33).
     */
    public function testGetRevisionReturnsNullForDifferentKey(): void
    {
        $reply = '{"message":{"subject":"$KV.cfg.other","seq":2,"data":"' . base64_encode('x') . '"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        self::assertNull($client->jetStream()->keyValue('cfg')->getRevision('theme', 2)->await());
    }

    /**
     * Verifies delete() with an expected revision emits the compare-and-delete header (#34).
     */
    public function testDeleteWithExpectedRevisionSendsHeader(): void
    {
        $ack = '{"stream":"KV_cfg","seq":5,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ack), $ack),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('cfg')->delete('theme', null, 4)->await();

        self::assertStringStartsWith('HPUB $KV.cfg.theme _INBOX.', $transport->writes[3]);
        self::assertStringContainsString('KV-Operation:DEL', $transport->writes[3]);
        self::assertStringContainsString('Nats-Expected-Last-Subject-Sequence:4', $transport->writes[3]);
    }

    /**
     * Verifies history() returns an empty list when the key has no stored revisions (#41).
     */
    public function testHistoryReturnsEmptyWhenNoPending(): void
    {
        $createReply = '{"stream_name":"KV_cfg","name":"HIST","num_pending":0,"config":{"deliver_subject":"d","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        self::assertSame([], $client->jetStream()->keyValue('cfg')->history('theme')->await());
    }

    /**
     * Verifies history() collects all stored revisions in order, stopping when caught up (#41).
     */
    public function testHistoryCollectsAllRevisions(): void
    {
        $createReply = '{"stream_name":"KV_cfg","name":"HIST","num_pending":2,"config":{"deliver_subject":"d","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            // Two deliveries on the push deliver subject (sid 2); pending counts down to 0.
            "MSG dlv 2 \$JS.ACK.KV_cfg.HIST.1.5.1.0.1 2\r\nv1\r\n",
            "MSG dlv 2 \$JS.ACK.KV_cfg.HIST.1.6.2.0.0 2\r\nv2\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $history = $client->jetStream()->keyValue('cfg')->history('theme')->await();

        self::assertCount(2, $history);
        self::assertSame(['v1', 'v2'], array_map(static fn($e): ?string => $e->value, $history));
        self::assertSame([5, 6], array_map(static fn($e): ?int => $e->revision, $history));
    }

    public function testHistoryToleratesDeliveryWithoutMetadataAndKeepsCollecting(): void
    {
        // #96: a delivery lacking a parseable $JS.ACK reply subject (a control / non-conformant frame)
        // must be skipped - not thrown out of the shared dispatch loop (which would tear down delivery for
        // every subscription, the #90 class) and not recorded as a bogus history entry.
        $createReply = '{"stream_name":"KV_cfg","name":"HIST","num_pending":1,"config":{"deliver_subject":"d","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            // A metadata-less delivery (no $JS.ACK reply subject) on the history subscription (sid 2).
            "MSG dlv 2 2\r\nxx\r\n",
            // A well-formed delivery follows and completes the replay (num_pending=0, last token).
            "MSG dlv 2 \$JS.ACK.KV_cfg.HIST.1.6.1.0.0 2\r\nv1\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $history = $client->jetStream()->keyValue('cfg')->history('theme')->await();

        // The metadata-less frame was skipped; only the valid revision is recorded.
        self::assertCount(1, $history);
        self::assertSame('v1', $history[0]->value);
        self::assertSame(6, $history[0]->revision);
    }

    /**
     * Verifies keys() returns the live key names (deleted keys excluded) WITHOUT downloading any value
     * bytes: it drives a last_per_subject + headers_only ephemeral consumer and filters DEL tombstones
     * by the KV-Operation header alone (#25/#110). The consumer replays each key's latest record with
     * headers only; the DEL record is excluded and no Direct Get / value body crosses the wire.
     */
    public function testKeysReturnsLiveKeyNames(): void
    {
        // CONSUMER.CREATE reply (sid 1): a headers-only last_per_subject consumer with four records pending.
        $consumerReply = '{"stream_name":"KV_cfg","name":"KEYS","num_pending":4,'
            . '"config":{"deliver_subject":"_INBOX.KV.KEYS.x","ack_policy":"none",'
            . '"deliver_policy":"last_per_subject","headers_only":true}}';
        // Headers-only deliveries on the deliver subscription (sid 2). The $JS.ACK reply subject's last
        // token is num_pending; the final delivery reports 0, signalling the replay has caught up.
        // A frame on a subject OUTSIDE this bucket's prefix (keyFromSubject -> null) must be dropped, not
        // enumerated - a non-null-AND-non-empty guard, so a truthy key is required.
        $otherHdrs = "NATS/1.0\r\nNats-Sequence: 2\r\n\r\n";                              // $KV.other.k -> non-key (excluded)
        $usernameHdrs = "NATS/1.0\r\nNats-Sequence: 3\r\nKV-Operation: DEL\r\n\r\n";      // username -> DEL (excluded)
        $emailHdrs = "NATS/1.0\r\nNats-Sequence: 4\r\n\r\n";                              // email -> live
        $phoneHdrs = "NATS/1.0\r\nNats-Sequence: 5\r\n\r\n";                              // phone -> live (caught up)
        $oh = strlen($otherHdrs);
        $uh = strlen($usernameHdrs);
        $eh = strlen($emailHdrs);
        $ph = strlen($phoneHdrs);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($consumerReply), $consumerReply),
            sprintf("HMSG \$KV.other.k 2 \$JS.ACK.KV_cfg.KEYS.1.2.1.0.3 %d %d\r\n%s\r\n", $oh, $oh, $otherHdrs),
            sprintf("HMSG \$KV.cfg.username 2 \$JS.ACK.KV_cfg.KEYS.1.3.2.0.2 %d %d\r\n%s\r\n", $uh, $uh, $usernameHdrs),
            sprintf("HMSG \$KV.cfg.email 2 \$JS.ACK.KV_cfg.KEYS.1.4.3.0.1 %d %d\r\n%s\r\n", $eh, $eh, $emailHdrs),
            sprintf("HMSG \$KV.cfg.phone 2 \$JS.ACK.KV_cfg.KEYS.1.5.4.0.0 %d %d\r\n%s\r\n", $ph, $ph, $phoneHdrs),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $keys = $client->jetStream()->keyValue('cfg')->keys()->await();

        // BOTH live keys are returned (the DEL tombstone and the out-of-prefix frame are excluded). The
        // replay order (stream-sequence order) is unspecified, so compare canonically.
        self::assertCount(2, $keys);
        self::assertEqualsCanonicalizing(['email', 'phone'], $keys);

        // The enumeration consumer must filter on this bucket's whole keyspace (prefix + '>'), request
        // headers_only + last_per_subject (no value bytes), and keys() must never issue a Direct Get.
        $consumerCreate = '';
        foreach ($transport->writes as $w) {
            if (str_contains($w, '$JS.API.CONSUMER.CREATE.KV_cfg')) {
                $consumerCreate = $w;
                break;
            }
        }
        self::assertStringContainsString('"$KV.cfg.>"', $consumerCreate);
        self::assertStringContainsString('"headers_only":true', $consumerCreate);
        self::assertStringContainsString('"deliver_policy":"last_per_subject"', $consumerCreate);
        self::assertStringNotContainsString('$JS.API.DIRECT.GET', implode('', $transport->writes));

        // The ephemeral deliver subscription (sid 2) is torn down once enumeration completes.
        self::assertStringContainsString("UNSUB 2\r\n", implode('', $transport->writes));
    }

    /**
     * Connects a client whose CONSUMER.CREATE reply (sid 1) is $consumerReply and whose deliver
     * subscription (sid 2) is fed $deliverFrames, for the keys() enumeration tests.
     *
     * @param list<string> $deliverFrames
     * @return array{0: NatsClient, 1: FakeTransport}
     */
    private function connectForKeys(string $consumerReply, array $deliverFrames = []): array
    {
        $reads = array_merge([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($consumerReply), $consumerReply),
        ], $deliverFrames);

        $transport = new FakeTransport($reads);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        return [$client, $transport];
    }

    /** A single live headers-only delivery on sid 2 whose ACK reports num_pending 0 (immediately caught up). */
    private function keysLiveTerminatorFrame(string $key): string
    {
        $hdrs = "NATS/1.0\r\nNats-Sequence: 3\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG \$KV.cfg.%s 2 \$JS.ACK.KV_cfg.KEYS.1.3.1.0.0 %d %d\r\n%s\r\n", $key, $h, $h, $hdrs);
    }

    /**
     * A consumer reporting num_pending 0 means the bucket has no live keys: keys() must short-circuit to
     * [] WITHOUT ever subscribing to the deliver inbox (no replay to consume). A live frame is seeded so
     * that any code path which wrongly proceeds past the short-circuit would enumerate it and be caught.
     *
     * Falsifies the `=== 0` -> `=== -1` mutation of the short-circuit guard, which would proceed to the
     * replay, subscribe, drain the seeded frame, and return ['ghost'].
     */
    public function testKeysReturnsEmptyWithoutSubscribingWhenConsumerReportsNoPending(): void
    {
        $consumerReply = '{"stream_name":"KV_cfg","name":"KEYS","num_pending":0,'
            . '"config":{"deliver_subject":"_INBOX.KV.KEYS.x","ack_policy":"none",'
            . '"deliver_policy":"last_per_subject","headers_only":true}}';

        [$client, $transport] = $this->connectForKeys($consumerReply, [$this->keysLiveTerminatorFrame('ghost')]);

        $keys = $client->jetStream()->keyValue('cfg')->keys()->await();

        self::assertSame([], $keys);
        self::assertStringNotContainsString('SUB _INBOX.KV.KEYS', implode('', $transport->writes));
    }

    /**
     * An ABSENT num_pending must be read as 0 (the `?? 0` default) and short-circuit to []. A seeded live
     * frame catches any mutation that resolves the absent value to a non-zero pending and proceeds.
     *
     * Falsifies the `?? 0` -> `?? -1` and `?? 1` mutations (both make an absent num_pending non-zero, so
     * keys() would subscribe, drain the seeded frame, and return ['ghost']).
     */
    public function testKeysTreatsAbsentNumPendingAsNoLiveKeys(): void
    {
        // No num_pending field at all.
        $consumerReply = '{"stream_name":"KV_cfg","name":"KEYS",'
            . '"config":{"deliver_subject":"_INBOX.KV.KEYS.x","ack_policy":"none",'
            . '"deliver_policy":"last_per_subject","headers_only":true}}';

        [$client, $transport] = $this->connectForKeys($consumerReply, [$this->keysLiveTerminatorFrame('ghost')]);

        $keys = $client->jetStream()->keyValue('cfg')->keys()->await();

        self::assertSame([], $keys);
        self::assertStringNotContainsString('SUB _INBOX.KV.KEYS', implode('', $transport->writes));
    }

    /**
     * num_pending must be coerced with (int) before the `=== 0` test: a JSON float 0.0 (as a proxy or
     * non-canonical server might send) is a zero pending count and must short-circuit to []. Without the
     * cast, `0.0 === 0` is false and keys() would wrongly proceed.
     *
     * Falsifies the (int) cast removal on the num_pending read.
     */
    public function testKeysCastsFloatZeroNumPendingToNoLiveKeys(): void
    {
        $consumerReply = '{"stream_name":"KV_cfg","name":"KEYS","num_pending":0.0,'
            . '"config":{"deliver_subject":"_INBOX.KV.KEYS.x","ack_policy":"none",'
            . '"deliver_policy":"last_per_subject","headers_only":true}}';

        [$client, $transport] = $this->connectForKeys($consumerReply, [$this->keysLiveTerminatorFrame('ghost')]);

        $keys = $client->jetStream()->keyValue('cfg')->keys()->await();

        self::assertSame([], $keys);
        self::assertStringNotContainsString('SUB _INBOX.KV.KEYS', implode('', $transport->writes));
    }

    /**
     * When the consumer reports pending records but the replay makes NO progress (no delivery ever
     * arrives), keys() must fail with a JetStreamException bounded by the CALLER-supplied progress
     * timeout - and it must still tear down the deliver subscription (the finally), never leak it.
     *
     * Falsifies (a) the `??=` -> `=` mutation that discards the caller's timeout and uses the 5 s default
     * (the message would quote 5.000 s, not the caller's 0.050 s); and (b) the finally-unwrap mutation,
     * which on the throw path would skip the unsubscribe, leaving no UNSUB for the deliver sid.
     */
    public function testKeysThrowsOnStalledReplayAndStillUnsubscribes(): void
    {
        // num_pending > 0 so keys() proceeds to the replay, but no deliver frame is ever seeded, so the
        // consumer never reports "caught up" and the progress clock expires.
        $consumerReply = '{"stream_name":"KV_cfg","name":"KEYS","num_pending":2,'
            . '"config":{"deliver_subject":"_INBOX.KV.KEYS.x","ack_policy":"none",'
            . '"deliver_policy":"last_per_subject","headers_only":true}}';

        [$client, $transport] = $this->connectForKeys($consumerReply);

        try {
            $client->jetStream()->keyValue('cfg')->keys(0.05)->await();
            self::fail('expected a stalled-replay JetStreamException');
        } catch (JetStreamException $e) {
            // Pin the whole message shape: it must OPEN with the stalled banner, quote the CALLER's
            // 0.05 s bound (not the 5 s default), and CLOSE with the not-caught-up tail.
            self::assertStringStartsWith('Key enumeration stalled', $e->getMessage());
            self::assertStringContainsString('0.050 s', $e->getMessage());
            self::assertStringContainsString('replay not caught up', $e->getMessage());
        }

        // The deliver subscription (sid 2) is unsubscribed even on the throw path.
        self::assertStringContainsString("UNSUB 2\r\n", implode('', $transport->writes));

        // Tear the client down and cancel any event-loop timers the timed-out replay may have left
        // pending (delay()/TimeoutCancellation), so no residual callback fires into a later test's clean
        // loop and force-closes its fiber - the same quiescing setUp() performs before each test.
        $client->disconnect()->await();
        foreach (EventLoop::getIdentifiers() as $id) {
            EventLoop::cancel($id);
        }
    }

    /**
     * Verifies watch() options drive deliver policy + headers-only on the consumer config (#26).
     */
    public function testWatchOptionsConfigureConsumer(): void
    {
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('cfg')->watch(
            static function (KeyValueEntry $entry): void {},
            '>',
            new KeyWatchOptions(includeHistory: true, metaOnly: true, ignoreDeletes: true),
        )->await();

        $createRequest = implode('', $transport->writes);
        self::assertStringContainsString('"deliver_policy":"all"', $createRequest);
        self::assertStringContainsString('"headers_only":true', $createRequest);
    }

    /**
     * Verifies a resume-from-revision watch uses by_start_sequence with the given start (#26).
     */
    public function testWatchResumeFromRevisionUsesStartSequence(): void
    {
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('cfg')->watch(
            static function (KeyValueEntry $entry): void {},
            '>',
            new KeyWatchOptions(resumeFromRevision: 42),
        )->await();

        $createRequest = implode('', $transport->writes);
        self::assertStringContainsString('"deliver_policy":"by_start_sequence"', $createRequest);
        self::assertStringContainsString('"opt_start_seq":42', $createRequest);
    }

    /**
     * Verifies a KV watch requests an idle heartbeat and its missed-heartbeat watchdog surfaces a
     * "not active" error when the watch consumer goes silent (#113). The default KV watch config
     * previously requested no idle_heartbeat, so a reaped/lost watch consumer left the watcher hanging
     * forever with no data, heartbeat, or error - total silence indistinguishable from an idle stream.
     * The watch now requests a heartbeat (tunable via KeyWatchOptions::$idleHeartbeat), arming the same
     * push watchdog other push subscriptions use. Falsifiable: the branch requested no heartbeat, so no
     * watchdog armed and no error was ever surfaced.
     */
    public function testWatchSilentConsumerSurfacesNotActiveError(): void
    {
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ], blockWhenEmpty: true);

        $errors = [];
        $options = new NatsOptions(pingIntervalSeconds: 0, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        });
        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        // 30 ms heartbeat -> the watchdog surfaces a stall after ~60 ms of silence.
        $client->jetStream()->keyValue('cfg')->watch(
            static function (KeyValueEntry $entry): void {},
            '>',
            new KeyWatchOptions(idleHeartbeat: 30_000_000),
        )->await();

        // The watch consumer must actually request the heartbeat, or the server would never emit the
        // status-100 frames the watchdog relies on.
        self::assertStringContainsString('"idle_heartbeat":30000000', implode('', $transport->writes));

        $deadlineNs = hrtime(true) + 2_000_000_000;
        while ($errors === [] && hrtime(true) < $deadlineNs) {
            delay(0.01);
        }

        $client->disconnect()->await();

        self::assertCount(1, $errors, 'a silent KV watch must surface exactly one stall error');
        self::assertInstanceOf(JetStreamException::class, $errors[0]);
        self::assertStringContainsString('not active', $errors[0]->getMessage());
        self::assertStringContainsString('KV_cfg', $errors[0]->getMessage());
        // A KV watch consumer is caller-owned from the library's view; it is never recreated, only surfaced.
        self::assertSame(0, substr_count(implode('', $transport->writes), '$JS.API.CONSUMER.DELETE'), 'a KV watch stall must not delete/recreate the consumer');
    }

    /**
     * Verifies a default KV watch (no options) still requests the default idle heartbeat so the
     * watchdog protects it too - the option-less form is the common case (#113).
     */
    public function testWatchRequestsDefaultIdleHeartbeat(): void
    {
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ], blockWhenEmpty: true);

        $client = new NatsClient(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('cfg')->watch(static function (KeyValueEntry $entry): void {})->await();

        self::assertStringContainsString(
            '"idle_heartbeat":' . KeyValueBucket::WATCH_IDLE_HEARTBEAT_NS,
            implode('', $transport->writes),
            'an option-less KV watch must request the default idle heartbeat so the watchdog arms',
        );

        $client->disconnect()->await();
    }

    /**
     * Verifies get() returns null for missing keys.
     */
    public function testGetMissingReturnsNull(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->kvDirectStatus(1, 404, 'Message Not Found'),
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
     * Verifies a per-key TTL on put emits Nats-TTL (issue #4).
     */
    public function testPutWithTtl(): void
    {
        $putAck = '{"stream":"KV_cfg","seq":5,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('cfg')->put('theme', 'green', ttl: 60)->await();

        self::assertStringStartsWith('HPUB $KV.cfg.theme _INBOX.', $transport->writes[3]);
        self::assertStringContainsString('Nats-TTL:60s', $transport->writes[3]);
    }

    /**
     * Verifies a tombstone TTL on delete emits Nats-TTL alongside the delete marker (issue #4).
     */
    public function testDeleteWithTombstoneTtl(): void
    {
        $delAck = '{"stream":"KV_cfg","seq":6,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($delAck), $delAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('cfg')->delete('theme', tombstoneTtl: 120)->await();

        self::assertStringContainsString('KV-Operation:DEL', $transport->writes[3]);
        self::assertStringContainsString('Nats-TTL:120s', $transport->writes[3]);
    }

    /**
     * Verifies getStatus maps stream state counters.
     */
    public function testGetStatus(): void
    {
        $streamInfo = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]},"state":{"messages":7,"bytes":128,"subjects":{"$KV.cfg.theme":3}}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
        // STREAM.INFO subjects map is paginated: page 1 lists both keys, an empty page 2 ends the loop.
        $streamInfoPage1 = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]},"state":{"messages":4,"bytes":256,"subjects":{"$KV.cfg.username":2,"$KV.cfg.email":2}}}';
        $streamInfoPage2 = '{"config":{"name":"KV_cfg"},"state":{"messages":4,"bytes":256,"subjects":{}}}';
        // getAll() then reads each key concurrently via Direct Get: subjects enumerate as
        // [username, email] (sids 3, 4). Direct Get returns the raw value as the body with Nats-* headers.
        $usernameHdrs = "NATS/1.0\r\nNats-Stream: KV_cfg\r\nNats-Subject: \$KV.cfg.username\r\nNats-Sequence: 3\r\nKV-Operation: PURGE\r\n\r\n";
        $emailHdrs = "NATS/1.0\r\nNats-Stream: KV_cfg\r\nNats-Subject: \$KV.cfg.email\r\nNats-Sequence: 4\r\n\r\n";
        $emailBody = 'b@example.com';
        $uh = strlen($usernameHdrs);
        $eh = strlen($emailHdrs);
        $et = $eh + strlen($emailBody);

        // Pre-2.11 server: getAll() takes the per-subject Direct Get fan-out this test scripts (batched
        // multi_last Direct Get requires 2.11+ and is covered by the #110 batched-path tests).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfoPage1), $streamInfoPage1),   // STREAM.INFO page 1 (sid 1)
            sprintf("MSG _INBOX.p 2 %d\r\n%s\r\n", strlen($streamInfoPage2), $streamInfoPage2),   // STREAM.INFO page 2 empty (sid 2)
            sprintf("HMSG _INBOX.b 3 %d %d\r\n%s\r\n", $uh, $uh, $usernameHdrs),                  // username -> PURGE (skipped)
            sprintf("HMSG _INBOX.c 4 %d %d\r\n%s%s\r\n", $eh, $et, $emailHdrs, $emailBody),       // email -> value
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $all = $client->jetStream()->keyValue('cfg')->getAll()->await();

        self::assertSame(['email' => 'b@example.com'], $all);
        self::assertStringContainsString('$JS.API.DIRECT.GET.KV_cfg', implode('', $transport->writes));
    }

    /**
     * Verifies get() treats a server delete-marker (Nats-Marker-Reason) as a PURGE tombstone with a
     * null value, instead of a live empty-string value (issue #5).
     */
    public function testGetTreatsMarkerAsTombstone(): void
    {
        $hdrs = "NATS/1.0\r\nNats-Stream: KV_cfg\r\nNats-Subject: \$KV.cfg.theme\r\nNats-Sequence: 9\r\nNats-Marker-Reason: MaxAge\r\n\r\n";
        $h = strlen($hdrs);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("HMSG _INBOX.x 1 %d %d\r\n%s\r\n", $h, $h, $hdrs),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $entry = $client->jetStream()->keyValue('cfg')->get('theme')->await();

        self::assertInstanceOf(KeyValueEntry::class, $entry);
        /** @var KeyValueEntry $entry */
        self::assertSame('PURGE', $entry->operation);
        self::assertNull($entry->value);
        self::assertSame(9, $entry->revision);
    }

    /**
     * Verifies getAll() omits a key whose latest record is a server delete-marker (issue #5).
     */
    public function testGetAllOmitsMarker(): void
    {
        $streamInfoPage1 = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]},"state":{"messages":4,"bytes":256,"subjects":{"$KV.cfg.username":2,"$KV.cfg.email":2}}}';
        $streamInfoPage2 = '{"config":{"name":"KV_cfg"},"state":{"messages":4,"bytes":256,"subjects":{}}}';
        // username's latest record is a server delete-marker (aged out) -> must be omitted.
        $usernameHdrs = "NATS/1.0\r\nNats-Stream: KV_cfg\r\nNats-Subject: \$KV.cfg.username\r\nNats-Sequence: 3\r\nNats-Marker-Reason: MaxAge\r\n\r\n";
        $emailHdrs = "NATS/1.0\r\nNats-Stream: KV_cfg\r\nNats-Subject: \$KV.cfg.email\r\nNats-Sequence: 4\r\n\r\n";
        $emailBody = 'b@example.com';
        $uh = strlen($usernameHdrs);
        $eh = strlen($emailHdrs);
        $et = $eh + strlen($emailBody);

        // Pre-2.11 server: getAll() takes the per-subject Direct Get fan-out this test scripts (batched
        // multi_last Direct Get requires 2.11+ and is covered by the #110 batched-path tests).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfoPage1), $streamInfoPage1),
            sprintf("MSG _INBOX.p 2 %d\r\n%s\r\n", strlen($streamInfoPage2), $streamInfoPage2),
            sprintf("HMSG _INBOX.b 3 %d %d\r\n%s\r\n", $uh, $uh, $usernameHdrs),
            sprintf("HMSG _INBOX.c 4 %d %d\r\n%s%s\r\n", $eh, $et, $emailHdrs, $emailBody),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $all = $client->jetStream()->keyValue('cfg')->getAll()->await();

        self::assertSame(['email' => 'b@example.com'], $all);
    }

    /**
     * Verifies watch() delivers a server delete-marker as a PURGE tombstone (null value), not a live
     * empty value (issue #5).
     */
    public function testWatchTreatsMarkerAsTombstone(): void
    {
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';
        $markerHdrs = "NATS/1.0\r\nNats-Marker-Reason: MaxAge\r\n\r\n";
        $mh = strlen($markerHdrs);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            sprintf("HMSG \$KV.cfg.theme 2 \$JS.ACK.KV_cfg.KVWATCH.1.7.1.0.0 %d %d\r\n%s\r\n", $mh, $mh, $markerHdrs),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $seen = null;
        $client->jetStream()->keyValue('cfg')->watch(static function (KeyValueEntry $entry) use (&$seen): void {
            $seen = $entry;
        })->await();

        self::assertSame(1, $client->processIncoming()->await());
        self::assertInstanceOf(KeyValueEntry::class, $seen);
        /** @var KeyValueEntry $seen */
        self::assertSame('theme', $seen->key);
        self::assertSame('PURGE', $seen->operation);
        self::assertNull($seen->value);
    }

    /**
     * Verifies subject_delete_marker_ttl is forwarded into the KV stream config (issue #5 passthrough).
     */
    public function testCreateWithSubjectDeleteMarkerTtl(): void
    {
        $createPayload = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('cfg')->create(['subject_delete_marker_ttl' => 3_600_000_000_000])->await();

        self::assertStringContainsString('"subject_delete_marker_ttl":3600000000000', $transport->writes[3]);
    }

    // ─── Key Validation ─────────────────────────────────────────────

    public function testPutAcceptsAdr8KeyCharset(): void
    {
        $putAck = '{"stream":"KV_cfg","seq":1,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // Full ADR-8 charset: dots, hyphens, underscores, equals, slashes, mixed case, digits.
        $ack = $client->jetStream()->keyValue('cfg')->put('config/v2=main.BAR-2_ok.yaml', 'data')->await();
        self::assertSame(1, $ack->seq);
    }

    /**
     * Verifies keys outside the ADR-8 charset ('@', '#', non-ASCII) are rejected (#132).
     */
    public function testPutRejectsKeyOutsideAdr8Charset(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();
        $kv = $client->jetStream()->keyValue('cfg');

        foreach (['user@host', 'tag#1', "caf\u{00E9}", 'a:b'] as $key) {
            try {
                $kv->put($key, 'x')->await();
                self::fail(sprintf('Key "%s" should have been rejected', $key));
            } catch (JetStreamException $e) {
                self::assertStringContainsString('Invalid KV key', $e->getMessage());
            }
        }

        // Nothing beyond CONNECT+PING may reach the wire for a rejected key.
        self::assertCount(2, $transport->writes);
    }

    /**
     * Verifies the reserved "_kv" key prefix is rejected (ADR-8, #132).
     */
    public function testPutRejectsReservedKvPrefixKey(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid KV key: the "_kv" prefix is reserved');

        $client->jetStream()->keyValue('cfg')->put('_kv.x', 'x')->await();
    }

    public function testPutRejectsKeyWithWildcard(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid KV key');
        $client->jetStream()->keyValue('cfg')->put('foo*bar', 'data')->await();
    }

    public function testPutRejectsKeyWithLeadingTrailingOrConsecutiveDots(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();
        $kv = $client->jetStream()->keyValue('cfg');

        // Leading/trailing/consecutive dots make an empty subject token; all must be rejected.
        // (Dots, colons and slashes elsewhere remain valid - see testPutAcceptsKeyWithDotsColonsSlashes.)
        foreach (['.theme', 'theme.', 'a..b'] as $key) {
            try {
                $kv->put($key, 'data')->await();
                self::fail("Expected rejection for malformed key: {$key}");
            } catch (JetStreamException $e) {
                self::assertStringContainsString('Invalid KV key', $e->getMessage());
            }
        }
    }

    public function testPutRejectsKeyWithTab(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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

    /**
     * Verifies watch callback receives KV entries from subscription dispatch.
     */
    public function testWatchDispatchesEntries(): void
    {
        // watch() now runs over a JetStream push consumer: create the consumer (request sid 1), then
        // the update is delivered on the deliver inbox (sid 2) carrying its stream sequence in the
        // $JS.ACK reply, which becomes the entry revision.
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            "MSG \$KV.cfg.theme 2 \$JS.ACK.KV_cfg.KVWATCH.1.7.1.0.0 4\r\nblue\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $seen = null;
        $sid = $client->jetStream()->keyValue('cfg')->watch(static function (KeyValueEntry $entry) use (&$seen): void {
            $seen = $entry;
        })->await();

        self::assertSame(2, $sid);
        self::assertSame(1, $client->processIncoming()->await());
        self::assertInstanceOf(KeyValueEntry::class, $seen);
        /** @var KeyValueEntry $seenEntry */
        $seenEntry = $seen;
        self::assertSame('theme', $seenEntry->key);
        self::assertSame('blue', $seenEntry->value);
        // The revision is now populated from the delivery's stream sequence (was always null before).
        self::assertSame(7, $seenEntry->revision);

        $createRequest = implode('', $transport->writes);
        self::assertStringContainsString('"deliver_policy":"new"', $createRequest);
        self::assertStringContainsString('"ack_policy":"none"', $createRequest);
        // The ephemeral watch consumer carries an inactive_threshold so the server reaps it after the
        // caller unsubscribes, rather than leaking server-side.
        self::assertStringContainsString('"inactive_threshold"', $createRequest);
    }

    /**
     * Verifies non-404 API errors are propagated by get().
     */
    public function testGetPropagatesNon404ApiErrors(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->kvDirectStatus(1, 500, 'internal error'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('internal error');

        $client->jetStream()->keyValue('cfg')->get('theme')->await();
    }

    public function testDeleteWrapsMalformedReplyAsJetStreamException(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG _INBOX.a 1 7\r\nnotjson\r\n", // a non-JSON ack
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // A malformed ack must surface as the library's JetStreamException, not a raw \JsonException.
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Malformed JetStream reply');

        $client->jetStream()->keyValue('cfg')->delete('theme')->await();
    }

    /**
     * Verifies DEL marker headers are mapped to tombstone entry values.
     */
    public function testGetMapsDeleteMarkerToNullValue(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->kvDirectReply('$KV.cfg.theme', 'ignored', 3, 1, 'DEL'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $entry = $client->jetStream()->keyValue('cfg')->get('theme')->await();

        self::assertNotNull($entry);
        self::assertSame('DEL', $entry->operation);
        self::assertNull($entry->value);
        self::assertSame(3, $entry->revision);
    }

    public function testBucketNameHelpers(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('cfg');
        self::assertSame('KV_cfg', $kv->streamName());
        self::assertSame('$KV.cfg.', $kv->subjectPrefix());
    }

    public function testUpdateRejectsNonPositiveExpectedRevision(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Expected revision must be greater than zero');
        $client->jetStream()->keyValue('cfg')->update('theme', 'v', 0)->await();
    }

    public function testGetAllSkipsKeysThatReturnNotFound(): void
    {
        // A key whose Direct Get races a deletion/expiry returns 404; getAll must skip it, not fail.
        $streamInfoPage1 = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]},"state":{"messages":2,"bytes":64,"subjects":{"$KV.cfg.gone":1,"$KV.cfg.theme":1}}}';
        $streamInfoPage2 = '{"config":{"name":"KV_cfg"},"state":{"messages":2,"bytes":64,"subjects":{}}}';
        $notFound = "NATS/1.0 404 Message Not Found\r\nStatus: 404\r\n\r\n";
        $themeHdrs = "NATS/1.0\r\nNats-Stream: KV_cfg\r\nNats-Subject: \$KV.cfg.theme\r\nNats-Sequence: 7\r\n\r\n";
        $themeBody = 'dark';
        $nf = strlen($notFound);
        $th = strlen($themeHdrs);
        $tt = $th + strlen($themeBody);

        // Pre-2.11 server: getAll() takes the per-subject Direct Get fan-out this test scripts (batched
        // multi_last Direct Get requires 2.11+ and is covered by the #110 batched-path tests).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfoPage1), $streamInfoPage1),   // STREAM.INFO page 1 (sid 1)
            sprintf("MSG _INBOX.p 2 %d\r\n%s\r\n", strlen($streamInfoPage2), $streamInfoPage2),   // STREAM.INFO page 2 empty (sid 2)
            sprintf("HMSG _INBOX.b 3 %d %d\r\n%s\r\n", $nf, $nf, $notFound),                      // gone -> 404 (skipped)
            sprintf("HMSG _INBOX.c 4 %d %d\r\n%s%s\r\n", $th, $tt, $themeHdrs, $themeBody),       // theme -> value
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $all = $client->jetStream()->keyValue('cfg')->getAll()->await();

        self::assertSame(['theme' => 'dark'], $all);
    }

    public function testGetAllThrowsOnStreamInfoApiError(): void
    {
        // A STREAM.INFO API error must surface, not be swallowed into an empty result.
        $error = '{"error":{"code":404,"description":"stream not found"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($error), $error),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('stream not found');

        $client->jetStream()->keyValue('cfg')->getAll()->await();
    }

    public function testGetStatusFallsBackLastSequenceToMessagesWhenMissing(): void
    {
        $streamInfo = '{"config":{"name":"KV_cfg"},"state":{"messages":11,"bytes":128}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $status = $client->jetStream()->keyValue('cfg')->getStatus()->await();

        self::assertSame(11, $status['messages']);
        self::assertSame(11, $status['last_sequence']);
    }

    public function testDeletePropagatesApiError(): void
    {
        $errorPayload = '{"error":{"code":500,"description":"delete failed"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('delete failed');

        $client->jetStream()->keyValue('cfg')->delete('theme')->await();
    }

    // ─── kvSourceConfig: array source with 'bucket' key ──

    /**
     * Verifies that create() with a mirror given as an array with a 'bucket' key
     * translates it to 'name' => 'KV_<bucket>' and strips the 'bucket' key (#62).
     */
    public function testCreateWithMirrorArrayBucketKeyTranslatesName(): void
    {
        $reply = '{"config":{"name":"KV_dst"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // 'mirror' is an array with 'bucket' key - kvSourceConfig should replace
        // 'bucket' with 'name' = 'KV_src' and retain extra fields.
        $client->jetStream()->keyValue('dst')->create([
            'mirror' => ['bucket' => 'src', 'start_seq' => 5],
        ])->await();

        $create = $transport->writes[3];
        self::assertStringContainsString('"mirror":{"start_seq":5,"name":"KV_src"}', $create);
        // 'bucket' key must be absent from the translated config.
        self::assertStringNotContainsString('"bucket"', $create);
        // A mirrored bucket has no subjects of its own.
        self::assertStringContainsString('"subjects":[]', $create);
    }

    /**
     * Verifies that create() with sources given as arrays with a 'bucket' key
     * translates each entry to 'name' => 'KV_<bucket>' (#62).
     */
    public function testCreateWithSourcesArrayBucketKeyTranslatesNames(): void
    {
        $reply = '{"config":{"name":"KV_agg"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('agg')->create([
            'sources' => [
                ['bucket' => 'alpha', 'start_seq' => 1],
                ['bucket' => 'beta'],
            ],
        ])->await();

        $create = $transport->writes[3];
        self::assertStringContainsString('"name":"KV_alpha"', $create);
        self::assertStringContainsString('"name":"KV_beta"', $create);
        self::assertStringNotContainsString('"bucket"', $create);
    }

    // ─── purge() with TTL + expectedRevision ───────────────

    /**
     * Verifies purge() with both a tombstone TTL and an expected revision emits both headers.
     */
    public function testPurgeWithTombstoneTtlAndExpectedRevision(): void
    {
        $purgeAck = '{"stream":"KV_cfg","seq":7,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($purgeAck), $purgeAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->keyValue('cfg')->purge('theme', tombstoneTtl: 300, expectedRevision: 6)->await();

        self::assertSame(7, $ack->seq);
        self::assertStringContainsString('KV-Operation:PURGE', $transport->writes[3]);
        self::assertStringContainsString('Nats-TTL:300s', $transport->writes[3]);
        self::assertStringContainsString('Nats-Expected-Last-Subject-Sequence:6', $transport->writes[3]);
    }

    // ─── getRevision() guards ─────────────────────

    /**
     * Verifies getRevision() throws when revision is zero or negative.
     */
    public function testGetRevisionThrowsOnNonPositiveRevision(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Revision must be greater than zero');
        $client->jetStream()->keyValue('cfg')->getRevision('theme', 0)->await();
    }

    /**
     * Verifies getRevision() returns null when the server replies with a 404.
     */
    public function testGetRevisionReturnsNullOnNotFound(): void
    {
        // STREAM.MSG.GET returns a JSON 404 error reply when the sequence does not exist.
        $errorReply = '{"error":{"code":404,"description":"Message Not Found"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorReply), $errorReply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $entry = $client->jetStream()->keyValue('cfg')->getRevision('theme', 99)->await();

        self::assertNull($entry);
    }

    /**
     * Verifies getRevision() re-throws non-404 errors from the server.
     */
    public function testGetRevisionPropagatesNon404Error(): void
    {
        $errorReply = '{"error":{"code":500,"description":"internal server error"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorReply), $errorReply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('internal server error');
        $client->jetStream()->keyValue('cfg')->getRevision('theme', 5)->await();
    }

    // ─── getViaStreamMessage() branches ──

    /**
     * Verifies the STREAM.MSG.GET fallback returns null when the API returns a 404 error.
     */
    public function testGetFallbackReturnsNullOnStreamMessage404(): void
    {
        $errorReply = '{"error":{"code":404,"description":"Message Not Found"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->kvDirectStatus(1, 503, 'No Responders'),                                     // Direct Get -> 503 fallback trigger
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($errorReply), $errorReply),           // STREAM.MSG.GET -> 404
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $entry = $client->jetStream()->keyValue('cfg')->get('theme')->await();

        self::assertNull($entry);
    }

    /**
     * Verifies the STREAM.MSG.GET fallback propagates a non-404 API error.
     */
    public function testGetFallbackPropagatesNon404StreamMessageError(): void
    {
        $errorReply = '{"error":{"code":503,"description":"service unavailable"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->kvDirectStatus(1, 503, 'No Responders'),
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($errorReply), $errorReply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('service unavailable');
        $client->jetStream()->keyValue('cfg')->get('theme')->await();
    }

    /**
     * Verifies the STREAM.MSG.GET fallback returns null when the reply has no 'message' field.
     */
    public function testGetFallbackReturnsNullWhenMessageFieldMissing(): void
    {
        $emptyReply = '{}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->kvDirectStatus(1, 503, 'No Responders'),
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($emptyReply), $emptyReply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $entry = $client->jetStream()->keyValue('cfg')->get('theme')->await();

        self::assertNull($entry);
    }

    /**
     * Verifies the STREAM.MSG.GET fallback decodes base64-encoded headers from the 'hdrs' field.
     */
    public function testGetFallbackDecodesEncodedHeaders(): void
    {
        // Build a wire-format header block, then base64-encode it as the server would send in 'hdrs'.
        $rawHeaders = "NATS/1.0\r\nKV-Operation: DEL\r\n\r\n";
        $encodedHeaders = base64_encode($rawHeaders);
        $encodedData = base64_encode('');
        $envelope = sprintf(
            '{"message":{"subject":"$KV.cfg.theme","seq":11,"data":"%s","hdrs":"%s"}}',
            $encodedData,
            $encodedHeaders,
        );

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->kvDirectStatus(1, 503, 'No Responders'),
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($envelope), $envelope),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $entry = $client->jetStream()->keyValue('cfg')->get('theme')->await();

        self::assertNotNull($entry);
        // A DEL header in the fallback path must be resolved to operation DEL with null value.
        self::assertSame('DEL', $entry->operation);
        self::assertNull($entry->value);
        self::assertSame(11, $entry->revision);
    }

    /**
     * Verifies the STREAM.MSG.GET fallback throws when message.data contains invalid base64.
     */
    public function testGetFallbackThrowsOnMalformedBase64Data(): void
    {
        // '!!!' is not valid base64 and base64_decode('!!!', true) returns false.
        $envelope = '{"message":{"subject":"$KV.cfg.theme","seq":5,"data":"!!!"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            $this->kvDirectStatus(1, 503, 'No Responders'),                                     // Direct Get -> 503 fallback trigger
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($envelope), $envelope),              // STREAM.MSG.GET -> malformed data
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Malformed KV payload for key theme');
        $client->jetStream()->keyValue('cfg')->get('theme')->await();
    }

    // ─── watch() callback: non-KV subject ────────────────────────

    /**
     * Verifies that watch() silently skips messages whose subject does not belong to the KV bucket
     * prefix, i.e. keyFromSubject() returns null for them.
     */
    public function testWatchIgnoresMessagesOnNonKvSubject(): void
    {
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            // A message on a completely different subject - keyFromSubject() will return null.
            "MSG some.other.subject 2 \$JS.ACK.KV_cfg.KVWATCH.1.1.1.0.0 4\r\ndata\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $called = false;
        $client->jetStream()->keyValue('cfg')->watch(static function (KeyValueEntry $entry) use (&$called): void {
            $called = true;
        })->await();

        $client->processIncoming()->await();

        // The handler must NOT have been invoked because the subject doesn't match the bucket prefix.
        self::assertFalse($called);
    }

    // ─── createKey() non-wrong-seq error re-throw ─────────────────

    /**
     * Verifies createKey() re-throws errors that are not "wrong last sequence".
     */
    public function testCreateKeyRethrowsNonWrongLastSequenceError(): void
    {
        // Server returns a generic publish error (not the wrong-last-sequence code 10071).
        $errAck = '{"error":{"code":500,"err_code":10000,"description":"internal error"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('internal error');
        $client->jetStream()->keyValue('cfg')->createKey('theme', 'blue')->await();
    }

    /**
     * Verifies createKey() succeeds when the key was previously deleted (tombstone entry)
     * by publishing against the tombstone's revision; tests the null-entry revision=0 branch
     * when get() returns null after a wrong-last-sequence error.
     */
    public function testCreateKeySucceedsAfterKeyDeletedEntryIsNull(): void
    {
        // First attempt (expected seq 0) -> wrong-last-sequence.
        $errAck = '{"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 3"}}';
        // get() via Direct Get returns 404 (key fully gone / race condition).
        // Then the second put (seq 0 from null entry) succeeds.
        $putAck = '{"stream":"KV_cfg","seq":4,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),   // first put -> wrong-last-seq
            $this->kvDirectStatus(2, 404, 'Message Not Found'),                   // get() -> null
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($putAck), $putAck),   // second put -> success
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->keyValue('cfg')->createKey('theme', 'blue')->await();

        self::assertSame(4, $ack->seq);
        // The second put must use expected-seq 0 (entry was null -> revision=0).
        $writes = implode('||', $transport->writes);
        // There should be two HPUB writes for 'theme' with expected seq 0.
        self::assertStringContainsString('Nats-Expected-Last-Subject-Sequence:0', $writes);
    }

    /**
     * Verifies createKey() succeeds after a tombstone (DEL) entry by publishing against
     * the tombstone revision.
     */
    public function testCreateKeySucceedsAfterTombstoneRevision(): void
    {
        // First attempt (expected seq 0) -> wrong-last-sequence.
        $errAck = '{"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 5"}}';
        // get() returns a DEL tombstone at revision 5.
        $putAck2 = '{"stream":"KV_cfg","seq":6,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),          // first put -> wrong-last-seq
            $this->kvDirectReply('$KV.cfg.theme', '', 5, 2, 'DEL'),                      // get() -> DEL tombstone at seq 5
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($putAck2), $putAck2),         // second put -> success
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->keyValue('cfg')->createKey('theme', 'newval')->await();

        self::assertSame(6, $ack->seq);
        // The second put must use expected-seq 5 (the tombstone's revision).
        self::assertStringContainsString('Nats-Expected-Last-Subject-Sequence:5', $transport->writes[9]);
    }

    // ─── mapKvOptions: description and max_bytes ─────────────

    /**
     * Verifies create() passes through 'description' and 'max_bytes' KV options to the stream config.
     */
    public function testCreateWithDescriptionAndMaxBytesOptions(): void
    {
        $createPayload = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('cfg')->create([
            'description' => 'My KV bucket',
            'max_bytes' => 10485760,
        ])->await();

        $written = $transport->writes[3];
        self::assertStringContainsString('"description":"My KV bucket"', $written);
        self::assertStringContainsString('"max_bytes":10485760', $written);
    }

    // ─── assertValidKey: empty key and '>' wildcard ─────────────────────────

    /**
     * Verifies assertValidKey() throws on an empty key.
     */
    public function testPutRejectsEmptyKey(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid KV key');
        $client->jetStream()->keyValue('cfg')->put('', 'value')->await();
    }

    /**
     * Verifies assertValidKey() throws on a key containing '>'.
     */
    public function testPutRejectsKeyWithGreaterThan(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid KV key');
        $client->jetStream()->keyValue('cfg')->put('foo>bar', 'value')->await();
    }

    // ─── getAll(): empty subjects short-circuit ───────────────────

    /**
     * Verifies getAll() returns an empty array immediately when STREAM.INFO reports no subjects.
     */
    public function testGetAllReturnsEmptyWhenNoSubjects(): void
    {
        $streamInfoPage1 = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]},"state":{"messages":0,"bytes":0,"subjects":{}}}';
        $streamInfoPage2 = '{"config":{"name":"KV_cfg"},"state":{"messages":0,"bytes":0,"subjects":{}}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfoPage1), $streamInfoPage1),
            sprintf("MSG _INBOX.p 2 %d\r\n%s\r\n", strlen($streamInfoPage2), $streamInfoPage2),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $all = $client->jetStream()->keyValue('cfg')->getAll()->await();

        self::assertSame([], $all);
    }

    /**
     * Verifies getAll() propagates non-404 errors from Direct Get.
     */
    public function testGetAllPropagatesNon404DirectGetError(): void
    {
        $streamInfoPage1 = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]},"state":{"messages":1,"bytes":32,"subjects":{"$KV.cfg.theme":1}}}';
        $streamInfoPage2 = '{"config":{"name":"KV_cfg"},"state":{"messages":1,"bytes":32,"subjects":{}}}';
        // Direct Get returns a non-404 status code.
        $errHdrs = "NATS/1.0 500 Internal Error\r\nStatus: 500\r\n\r\n";
        $eh = strlen($errHdrs);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfoPage1), $streamInfoPage1),
            sprintf("MSG _INBOX.p 2 %d\r\n%s\r\n", strlen($streamInfoPage2), $streamInfoPage2),
            sprintf("HMSG _INBOX.b 3 %d %d\r\n%s\r\n", $eh, $eh, $errHdrs),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $client->jetStream()->keyValue('cfg')->getAll()->await();
    }

    // ─── putExpectingSubjectSeq() with TTL ────────────────────────

    /**
     * Verifies createKey() with a TTL passes Nats-TTL alongside the expected-sequence header.
     */
    public function testCreateKeyWithTtlPassesTtlHeader(): void
    {
        $putAck = '{"stream":"KV_cfg","seq":1,"duplicate":false}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->keyValue('cfg')->createKey('session', 'tok', ttl: 3600)->await();

        self::assertSame(1, $ack->seq);
        // Both the CAS header and the TTL must appear in the published message.
        self::assertStringContainsString('Nats-Expected-Last-Subject-Sequence:0', $transport->writes[3]);
        self::assertStringContainsString('Nats-TTL:3600s', $transport->writes[3]);
    }

    // ─── getAll(): subjects with non-KV prefix are skipped ───────

    /**
     * Verifies getAll() skips subjects from STREAM.INFO that do not match the bucket's KV prefix,
     * i.e. keyFromSubject() returns null and the continue branch is taken.
     */
    public function testGetAllSkipsSubjectsWithNonKvPrefix(): void
    {
        // Include a subject that does NOT start with '$KV.cfg.' so keyFromSubject returns null.
        $streamInfoPage1 = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]},"state":{"messages":1,"bytes":16,"subjects":{"other.subject.key":1,"$KV.cfg.theme":1}}}';
        $streamInfoPage2 = '{"config":{"name":"KV_cfg"},"state":{"messages":1,"bytes":16,"subjects":{}}}';
        $themeHdrs = "NATS/1.0\r\nNats-Stream: KV_cfg\r\nNats-Subject: \$KV.cfg.theme\r\nNats-Sequence: 3\r\n\r\n";
        $themeBody = 'dark';
        $th = strlen($themeHdrs);
        $tt = $th + strlen($themeBody);

        // Pre-2.11 server: getAll() takes the per-subject Direct Get fan-out this test scripts (batched
        // multi_last Direct Get requires 2.11+ and is covered by the #110 batched-path tests).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfoPage1), $streamInfoPage1),
            sprintf("MSG _INBOX.p 2 %d\r\n%s\r\n", strlen($streamInfoPage2), $streamInfoPage2),
            // Only theme gets a Direct Get request (the non-KV subject is skipped).
            sprintf("HMSG _INBOX.b 3 %d %d\r\n%s%s\r\n", $th, $tt, $themeHdrs, $themeBody),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $all = $client->jetStream()->keyValue('cfg')->getAll()->await();

        // Only 'theme' should appear; the non-KV subject is silently skipped.
        self::assertSame(['theme' => 'dark'], $all);
    }

    // ─── getAll(): batched multi_last Direct Get on a 2.11+ server (#110) ───────

    /**
     * Verifies getAll() on a 2.11+ server issues exactly ONE batched multi_last Direct Get for all keys
     * (instead of one Direct Get per key), returns the identical key => value map, and filters DEL/PURGE
     * tombstones by header (#110). The single-request assertion FAILS on the pre-#110 per-subject
     * fan-out, which writes one Direct Get PUB per key.
     */
    public function testGetAllUsesSingleBatchedDirectGetOnModernServer(): void
    {
        // The subject map leads with a NON-key subject (the bare prefix, keyFromSubject -> '') BEFORE the
        // real keys: the candidate loop must SKIP it and keep collecting the keys that follow, not stop.
        $streamInfoPage1 = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]},"state":{"messages":4,"subjects":'
            . '{"$KV.cfg.":1,"$KV.cfg.username":2,"$KV.cfg.email":2,"$KV.cfg.phone":2}}}';
        $streamInfoPage2 = '{"config":{"name":"KV_cfg"},"state":{"subjects":{}}}';
        // One batched reply stream on a single inbox (sid 3): a DEL tombstone (excluded), TWO live values,
        // and the last frame carrying Nats-Num-Pending: 0 to terminate the batch.
        $usernameHdrs = "NATS/1.0\r\nNats-Stream: KV_cfg\r\nNats-Subject: \$KV.cfg.username\r\nNats-Sequence: 3\r\nKV-Operation: DEL\r\n\r\n";
        $emailHdrs = "NATS/1.0\r\nNats-Stream: KV_cfg\r\nNats-Subject: \$KV.cfg.email\r\nNats-Sequence: 4\r\n\r\n";
        $phoneHdrs = "NATS/1.0\r\nNats-Stream: KV_cfg\r\nNats-Subject: \$KV.cfg.phone\r\nNats-Sequence: 5\r\nNats-Num-Pending: 0\r\n\r\n";
        $emailBody = 'e@example.com';
        $phoneBody = 'p@example.com';
        $uh = strlen($usernameHdrs);
        $eh = strlen($emailHdrs);
        $et = $eh + strlen($emailBody);
        $ph = strlen($phoneHdrs);
        $pt = $ph + strlen($phoneBody);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfoPage1), $streamInfoPage1),   // STREAM.INFO page 1 (sid 1)
            sprintf("MSG _INBOX.p 2 %d\r\n%s\r\n", strlen($streamInfoPage2), $streamInfoPage2),   // STREAM.INFO page 2 empty (sid 2)
            sprintf("HMSG \$KV.cfg.username 3 %d %d\r\n%s\r\n", $uh, $uh, $usernameHdrs),         // batched frame: username -> DEL (excluded)
            sprintf("HMSG \$KV.cfg.email 3 %d %d\r\n%s%s\r\n", $eh, $et, $emailHdrs, $emailBody), // batched frame: email -> value
            sprintf("HMSG \$KV.cfg.phone 3 %d %d\r\n%s%s\r\n", $ph, $pt, $phoneHdrs, $phoneBody), // batched frame: phone -> value (terminator)
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $all = $client->jetStream()->keyValue('cfg')->getAll()->await();

        // Identical returned data: BOTH live keys, tombstone excluded. Two live keys ensure the result is
        // not silently truncated to a single entry.
        self::assertSame(['email' => 'e@example.com', 'phone' => 'p@example.com'], $all);

        // The win: exactly ONE Direct Get request for all keys (the batched multi_last), carrying the
        // subject list - not one Direct Get PUB per key. The bare-prefix non-key subject is dropped.
        $directGetPubs = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_starts_with($w, 'PUB $JS.API.DIRECT.GET.KV_cfg'),
        ));
        self::assertCount(1, $directGetPubs);
        self::assertStringContainsString('"multi_last":["$KV.cfg.username","$KV.cfg.email","$KV.cfg.phone"]', $directGetPubs[0]);
        self::assertStringContainsString('"batch":3', $directGetPubs[0]);
    }

    // ─── watch() updatesOnly deliver policy ───────────────────────

    /**
     * Verifies watch() with updatesOnly option uses deliver_policy=new (KeyWatchOptions::toConsumerConfig()).
     */
    public function testWatchUpdatesOnlyUsesNewDeliverPolicy(): void
    {
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('cfg')->watch(
            static function (KeyValueEntry $entry): void {},
            '>',
            new KeyWatchOptions(updatesOnly: true),
        )->await();

        $createRequest = implode('', $transport->writes);
        self::assertStringContainsString('"deliver_policy":"new"', $createRequest);
    }

    // ─── watch() default (no options) uses last_per_subject (not new) ────────

    /**
     * Verifies watch() with no options (null) uses deliver_policy=new (the pre-options default).
     * With KeyWatchOptions() (default instance) it uses last_per_subject.
     */
    public function testWatchWithDefaultOptionsUsesLastPerSubject(): void
    {
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // Passing a default KeyWatchOptions() (no fields set) triggers deliver_policy=last_per_subject.
        $client->jetStream()->keyValue('cfg')->watch(
            static function (KeyValueEntry $entry): void {},
            '>',
            new KeyWatchOptions(),
        )->await();

        $createRequest = implode('', $transport->writes);
        self::assertStringContainsString('"deliver_policy":"last_per_subject"', $createRequest);
    }

    /**
     * Verifies a watch() with an onCaughtUp callback does NOT throw out of the shared dispatch loop
     * when a delivery lacks a parseable $JS.ACK reply subject (no JetStream metadata). Before the fix
     * the caught-up check called the throwing messageMetadata(), which would tear down delivery for
     * every subscription on the connection (#90).
     */
    public function testWatchWithOnCaughtUpToleratesMessageWithoutMetadata(): void
    {
        // num_pending:1 reflects the single pending delivery below, so the end-of-initial-data signal is
        // driven by a delivery (not fired immediately at creation, which only happens when nothing is
        // pending - see testWatchFiresOnCaughtUpImmediatelyOnEmptyBucket).
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","num_pending":1,"config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            // Delivered WITHOUT a $JS.ACK reply subject -> JsMessageMetadata::fromMessage() returns null.
            "MSG \$KV.cfg.theme 2 4\r\nblue\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $seen = null;
        $caughtUp = false;
        $client->jetStream()->keyValue('cfg')->watch(
            static function (KeyValueEntry $entry) use (&$seen): void {
                $seen = $entry;
            },
            '>',
            new KeyWatchOptions(onCaughtUp: static function () use (&$caughtUp): void {
                $caughtUp = true;
            }),
        )->await();

        // The metadata-less delivery is dispatched without throwing; the entry still reaches the handler.
        self::assertSame(1, $client->processIncoming()->await());
        self::assertInstanceOf(KeyValueEntry::class, $seen);
        /** @var KeyValueEntry $seenEntry */
        $seenEntry = $seen;
        self::assertSame('theme', $seenEntry->key);
        self::assertSame('blue', $seenEntry->value);
        // Caught-up cannot be determined without metadata, so the signal does not (mis)fire.
        self::assertFalse($caughtUp);
    }

    /**
     * #99: on an empty / no-match bucket the consumer starts with num_pending=0, so no delivery ever
     * arrives to drive the in-handler caught-up check. The end-of-initial-data signal must instead fire
     * from the created consumer's num_pending, or a caller blocking on it hangs forever.
     */
    public function testWatchFiresOnCaughtUpImmediatelyOnEmptyBucket(): void
    {
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","num_pending":0,"config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none","deliver_policy":"last_per_subject"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $caughtUp = false;
        // No deliveries are queued: the signal must fire purely from the consumer's num_pending=0.
        $client->jetStream()->keyValue('cfg')->watch(
            static function (KeyValueEntry $entry): void {},
            '>',
            new KeyWatchOptions(onCaughtUp: static function () use (&$caughtUp): void {
                $caughtUp = true;
            }),
        )->await();

        self::assertTrue($caughtUp, 'onCaughtUp must fire on an empty bucket without any delivery');
    }

    /**
     * #121: history() must NOT silently return a truncated prefix when its bounded wait elapses
     * before the replay is caught up (num_pending never reaches 0). A slow/stalled server that
     * yields only part of the history must surface as an error rather than a partial list that
     * looks complete. The bound is parameterised so the timeout path is exercised deterministically.
     */
    public function testHistoryThrowsWhenDeadlineFiresBeforeCaughtUp(): void
    {
        // num_pending=2 but only ONE revision is ever delivered (num_pending stays 1): the replay
        // never catches up and the bounded wait elapses.
        $createReply = '{"stream_name":"KV_cfg","name":"HIST","num_pending":2,"config":{"deliver_subject":"dlv","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            // One revision (num_pending token = 1, NOT 0) then silence.
            "MSG dlv 2 \$JS.ACK.KV_cfg.HIST.1.5.1.0.1 2\r\nv1\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        // A short bound so the incomplete-history timeout is exercised quickly.
        $client->jetStream()->keyValue('cfg')->history('theme', 0.2)->await();
    }

    /**
     * #121: the history() bound is PROGRESS-based, not a whole-replay deadline. A healthy-but-slow
     * replay whose revisions keep arriving (each gap shorter than the bound) must complete WITHOUT
     * throwing even when the TOTAL replay time exceeds the bound - only a genuinely stalled server (no
     * progress for the whole interval) throws. Revisions are fed spaced apart at runtime: each resets
     * the stall clock, and the total elapsed (~0.6 s) exceeds the 0.5 s bound, so a whole-replay
     * deadline would have thrown here.
     */
    public function testHistoryDoesNotThrowWhileReplayKeepsMakingProgress(): void
    {
        $createReply = '{"stream_name":"KV_cfg","name":"HIST","num_pending":6,"config":{"deliver_subject":"dlv","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // Start the replay with a 0.5 s PROGRESS bound but do not await yet.
        $future = $client->jetStream()->keyValue('cfg')->history('theme', 0.5);

        // Feed six revisions 0.1 s apart (each gap < 0.5 s bound); num_pending decrements to 0 on the
        // last so the replay catches up. Total elapsed ~0.6 s > 0.5 s bound.
        for ($i = 1; $i <= 6; $i++) {
            delay(0.1);
            $numPending = 6 - $i;
            $transport->pushReadChunk(sprintf(
                "MSG dlv 2 \$JS.ACK.KV_cfg.HIST.%d.%d.%d.0.%d 2\r\nv%d\r\n",
                $i,
                4 + $i,
                $i,
                $numPending,
                $i,
            ));
        }

        $entries = $future->await();

        // The replay completed with all six revisions, in order, without a stall error.
        self::assertCount(6, $entries);
        self::assertSame(['v1', 'v2', 'v3', 'v4', 'v5', 'v6'], array_map(static fn (KeyValueEntry $e): ?string => $e->value, $entries));
    }
}
