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

    /**
     * Post-#118 the request inbox is MUXED: request()/requestWithHeaders() no longer subscribe a fresh
     * inbox per call; instead ONE long-lived wildcard "<base>.*" serves every reply, and each request
     * publishes reply-to "<base>.<token>" and routes by that token. A pre-seeded fixed-subject
     * "MSG _INBOX.x <sid>" / "HMSG _INBOX.x <sid>" therefore no longer reaches the waiting request (its
     * subject is not the request's random "<base>.<token>", so dispatchMuxReply drops it and the request
     * times out).
     *
     * This installs a dynamic responder that mirrors a real server: it learns the mux base + sid from the
     * wildcard SUB, and for each request PUB/HPUB (a publish whose reply-to lives under that base) pops the
     * next reply frame and re-emits it on the CAPTURED reply-to with the learned mux sid. Frames are
     * delivered FIFO in the order requests are written, so a method that issues several requests (KV get's
     * Direct-Get->STREAM.MSG.GET fallback, createKey's put->get->put, getAll's STREAM.INFO pages then the
     * per-key Direct-Get fan-out) still gets its replies in sequence.
     *
     * Subscription deliveries are NOT mux replies (a push consumer's deliver subject for keys/history/
     * watch, or the batched Direct-Get inbox); pass those via $deliverOnSub keyed by a substring of their
     * own SUB subject, and they are enqueued VERBATIM (own sid preserved) once that SUB is written - after
     * the request that created the consumer has already drained its own reply, so they land on the live
     * deliver subscription instead of being dropped during the request read.
     *
     * @param list<string> $replyFrames Pre-built MSG/HMSG reply frames; only their subject+sid are rewritten.
     * @param array<string,list<string>> $deliverOnSub SUB-subject substring => frames emitted on that SUB.
     */
    private function muxServer(FakeTransport $transport, array $replyFrames, array $deliverOnSub = []): void
    {
        $queue = $replyFrames;
        $base = null;
        $muxSid = 1;
        // Tracks which $deliverOnSub groups have already fired (each emits at most once). Kept separate
        // from $deliverOnSub - captured by reference for persistence - so the map itself stays captured by
        // value and its precise list<string> frame type is preserved for the closure's return.
        $fired = [];

        $transport->onWrite = static function (string $bytes) use (&$queue, $deliverOnSub, &$fired, &$base, &$muxSid): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            if (str_starts_with($head, 'SUB ')) {
                $subject = explode(' ', $head)[1] ?? '';
                if (str_starts_with($subject, '_INBOX.') && str_ends_with($subject, '.*')) {
                    // Learn the mux base "<hex>." (strip trailing "*") and the sid the server assigned it.
                    $base = substr($subject, 0, -1);
                    $muxSid = (int) (explode(' ', $head)[2] ?? 1);

                    return [];
                }

                // A non-mux SUB (a push deliver subject / Direct-Get inbox): emit its scripted frames now,
                // once the subscription that will receive them exists.
                foreach ($deliverOnSub as $needle => $frames) {
                    if (!isset($fired[$needle]) && str_contains($subject, $needle)) {
                        $fired[$needle] = true;

                        return $frames;
                    }
                }

                return [];
            }

            if ($base === null || (!str_starts_with($head, 'PUB ') && !str_starts_with($head, 'HPUB '))) {
                return [];
            }

            // PUB <subject> <replyTo> <len>  |  HPUB <subject> <replyTo> <hdrLen> <totLen>
            $replyTo = explode(' ', $head)[2] ?? '';
            if (!str_starts_with($replyTo, $base) || $queue === []) {
                // A plain publish, or a subscription's own inbox (Direct-Get batch) - not a mux request.
                return [];
            }

            return [self::rewriteReplyFrame(array_shift($queue), $replyTo, $muxSid)];
        };
    }

    /**
     * Rewrites a pre-built MSG/HMSG reply frame's subject and sid to the request's captured mux reply-to
     * and the learned mux sid, preserving the payload/header bytes and declared lengths verbatim so the
     * reply's content is delivered unchanged - only its addressing is corrected for the mux inbox.
     */
    private static function rewriteReplyFrame(string $frame, string $replyTo, int $muxSid): string
    {
        $pos = strpos($frame, "\r\n");
        self::assertNotFalse($pos, 'reply frame must have a head line');

        $tokens = explode(' ', substr($frame, 0, $pos));
        $tokens[1] = $replyTo;          // subject
        $tokens[2] = (string) $muxSid;  // mux sid

        return implode(' ', $tokens) . substr($frame, $pos);
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
        ]);
        $this->muxServer($transport, [
            $this->kvDirectStatus(1, 503, 'No Responders'),                          // Direct Get -> 503
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($envelope), $envelope),    // STREAM.MSG.GET fallback
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($deletePayload), $deletePayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('cfg');
        $created = $kv->create()->await();
        $deleted = $kv->deleteBucket()->await();

        self::assertSame('KV_cfg', $created->name);
        self::assertTrue($deleted);
        // Post-#118 there is one shared mux SUB (writes[2]) and no per-request SUB/UNSUB, so the two
        // request PUBs are consecutive: create at writes[3], delete at writes[4].
        self::assertStringContainsString('$JS.API.STREAM.CREATE.KV_cfg', $transport->writes[3]);
        self::assertStringContainsString('$JS.API.STREAM.DELETE.KV_cfg', $transport->writes[4]);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('cfg');
        $kv->create()->await();
        $kv->create(['deny_delete' => false, 'discard' => 'old'])->await();

        // Two consecutive request PUBs over the shared mux inbox: default create at writes[3], override at [4].
        self::assertStringContainsString('"deny_delete":true', $transport->writes[3]);
        self::assertStringContainsString('"discard":"new"', $transport->writes[3]);
        self::assertStringContainsString('"deny_delete":false', $transport->writes[4]);
        self::assertStringContainsString('"discard":"old"', $transport->writes[4]);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
            $this->kvDirectReply('$KV.cfg.theme', 'blue', 1, 2),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($deleteAck), $deleteAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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

        // Three consecutive request publishes over the shared mux inbox (writes[2] is the mux SUB):
        // put HPUB at [3], Direct-Get PUB at [4], delete HPUB at [5].
        self::assertStringStartsWith('PUB $KV.cfg.theme _INBOX.', $transport->writes[3]);
        self::assertStringStartsWith('PUB $JS.API.DIRECT.GET.KV_cfg _INBOX.', $transport->writes[4]);
        self::assertStringStartsWith('HPUB $KV.cfg.theme _INBOX.', $transport->writes[5]);
        self::assertStringContainsString('KV-Operation:DEL', $transport->writes[5]);
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
        ]);
        // delete() issues an HPUB request over the mux inbox; the server has no responder bound, so it
        // replies with a 503 no-responders status echoed on the captured reply-to.
        $this->muxServer($transport, [
            'HMSG _INBOX.a 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
            // ...then get() (Direct Get) shows a live value, so the key really exists.
            $this->kvDirectReply('$KV.cfg.theme', 'green', 4, 2),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
            $this->kvDirectReply('$KV.cfg.theme', 'green', 4, 2),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
            $this->kvDirectReply('$KV.cfg.theme', 'green', 4, 2),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
            $this->kvDirectReply('$KV.cfg.theme', 'green', 4, 2),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('agg')->create([
            'sources' => ['b1', 'b2'],
            'compression' => 's2',
            'placement' => ['cluster' => 'c1'],
        ])->await();

        $create = $transport->writes[3];
        // Each KV source carries the mandatory ADR-57 transform re-subjecting copied records into
        // THIS bucket's prefix - without it, sourced entries are invisible to every read/watch path.
        self::assertStringContainsString(
            '"sources":[{"name":"KV_b1","subject_transforms":[{"src":"$KV.b1.>","dest":"$KV.agg.>"}]},'
            . '{"name":"KV_b2","subject_transforms":[{"src":"$KV.b2.>","dest":"$KV.agg.>"}]}]',
            $create,
        );
        // A sourced bucket keeps its OWN subjects (only mirrors omit them).
        self::assertStringContainsString('"subjects":["$KV.agg.>"]', $create);
        self::assertStringContainsString('"compression":"s2"', $create);
        self::assertStringContainsString('"placement":{"cluster":"c1"}', $create);
    }

    /**
     * A source carrying its own subject_transforms passes through VERBATIM (nats.go jetstream /
     * ADR-57 full-control rule): no KV_ name prefixing, no auto transform - this is how non-KV
     * streams are sourced into a bucket.
     */
    public function testCreateSourceWithCustomTransformsPassesThroughVerbatim(): void
    {
        $reply = '{"config":{"name":"KV_agg"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('agg')->create([
            'sources' => [[
                'name' => 'RAW_EVENTS',
                'subject_transforms' => [['src' => 'raw.>', 'dest' => '$KV.agg.>']],
            ]],
        ])->await();

        $create = $transport->writes[3];
        self::assertStringContainsString(
            '"sources":[{"name":"RAW_EVENTS","subject_transforms":[{"src":"raw.>","dest":"$KV.agg.>"}]}]',
            $create,
            'a source with custom transforms must pass through untouched (no KV_ prefix, no auto transform)',
        );
    }

    /**
     * A mirror bucket gets mirror_direct (nats.go CreateKeyValue parity) and this handle's WRITES
     * go through to the ORIGIN's prefix: the mirror stream ingests no subjects of its own, so a
     * put to the mirror bucket's own `$KV.<bucket>.` prefix would answer 503 no-responders - the
     * pre-fix behavior that made mirror buckets unwritable.
     */
    public function testCreateMirrorEnablesMirrorDirectAndWritesThroughToOrigin(): void
    {
        $createReply = '{"config":{"name":"KV_dst"}}';
        $putAck = '{"stream":"KV_src","seq":11}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            sprintf("MSG _INBOX.b 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('dst');
        $kv->create(['mirror' => 'src'])->await();
        $ack = $kv->put('theme', 'blue')->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('"mirror_direct":true', $writes);
        self::assertStringContainsString('"mirror":{"name":"KV_src"}', $writes);
        // Write-through: the put publishes to the ORIGIN's subject, not the mirror's own prefix.
        self::assertStringContainsString('PUB $KV.src.theme ', $writes);
        self::assertStringNotContainsString('PUB $KV.dst.theme', $writes);
        self::assertSame(11, $ack->seq);
    }

    /**
     * A cross-domain mirror (`domain` shorthand) converts to the external JetStream API prefix and
     * routes WRITES through `<api>.$KV.<origin>.` while READS use the ORIGIN's prefix against the
     * local mirror stream - mirrored records keep the origin's subjects (nats.go mapStreamToKVS
     * parity; pre-fix both sides used the mirror's own prefix and found nothing).
     */
    public function testCreateCrossDomainMirrorRoutesWritesAndReadsThroughOrigin(): void
    {
        $createReply = '{"config":{"name":"KV_dst"}}';
        $putAck = '{"stream":"KV_src","seq":3}';
        // Direct-get reply carrying the ORIGIN-prefixed subject the mirror stream stores.
        $direct = $this->kvDirectReply('$KV.src.cache', 'warm', 3, 1);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            sprintf("MSG _INBOX.b 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
            $direct,
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('dst');
        $kv->create(['mirror' => ['bucket' => 'src', 'domain' => 'HUB']])->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('"external":{"api":"$JS.HUB.API"}', $writes);
        self::assertStringNotContainsString('"domain"', $writes, 'domain is a client-side shorthand, never sent');

        $kv->put('cache', 'warm')->await();
        $kv->get('cache')->await();

        $writes = implode('', $transport->writes);
        // Writes cross domains through the external API prefix to the origin subject.
        self::assertStringContainsString('PUB $JS.HUB.API.$KV.src.cache ', $writes);
        // Reads hit the LOCAL mirror stream, but under the ORIGIN's subject.
        self::assertStringContainsString('"last_by_subj":"$KV.src.cache"', $writes);
        self::assertStringNotContainsString('$KV.dst.cache', $writes);
    }

    /**
     * bind() resolves the mirror prefixes for a handle ATTACHED to an existing mirror bucket
     * (created elsewhere): STREAM.INFO reveals the mirror config, after which writes go through to
     * the origin exactly as on a created handle. Pre-fix an attached mirror handle was unusable.
     */
    public function testBindResolvesMirrorPrefixesFromStreamInfo(): void
    {
        $info = '{"config":{"name":"KV_dst","max_msgs_per_subject":1,"mirror":{"name":"KV_src"}}}';
        $putAck = '{"stream":"KV_src","seq":4}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($info), $info),
            sprintf("MSG _INBOX.b 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('dst')->bind()->await();
        $kv->put('theme', 'blue')->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.STREAM.INFO.KV_dst', $writes);
        self::assertStringContainsString('PUB $KV.src.theme ', $writes, 'an attached mirror handle must write through to the origin after bind()');
        self::assertStringNotContainsString('PUB $KV.dst.theme', $writes);
    }

    /**
     * A FAILED mirror create must not poison the handle: mirror prefixes are applied only after
     * createStream() succeeds (nats.go builds the KV handle only from a successful create). Pre-fix
     * the prefixes were applied BEFORE the await and never reset, so after a rejected cross-domain
     * mirror create every put published through the foreign API to the origin bucket and every get
     * read the origin subject - silent write misdirection and read blindness on a bucket whose
     * create failed.
     */
    public function testFailedMirrorCreateLeavesHandleOnPlainBucketSubjects(): void
    {
        $createError = '{"error":{"code":400,"err_code":10058,"description":"stream name already in use with a different configuration"}}';
        $putAck = '{"stream":"KV_dst","seq":1}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createError), $createError),   // STREAM.CREATE -> rejected
            sprintf("MSG _INBOX.b 1 %d\r\n%s\r\n", strlen($putAck), $putAck),             // put ack
            $this->kvDirectReply('$KV.dst.theme', 'blue', 1, 1),                          // Direct Get
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('dst');
        try {
            $kv->create(['mirror' => ['bucket' => 'src', 'domain' => 'HUB']])->await();
            self::fail('the rejected mirror create must surface as an error');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('already in use', $e->getMessage());
        }

        $kv->put('theme', 'blue')->await();
        $entry = $kv->get('theme')->await();

        self::assertNotNull($entry);
        self::assertSame('blue', $entry->value);

        $writes = implode('', $transport->writes);
        // Writes stay on THIS bucket's own subject - not routed through the foreign API to the
        // origin of the mirror that was never created. (The STREAM.CREATE payload legitimately
        // carries the external api string, so only PUBLISH frames are pinned here.)
        self::assertStringContainsString('PUB $KV.dst.theme ', $writes);
        self::assertStringNotContainsString('PUB $JS.HUB.API.', $writes);
        // Reads target this bucket's own subject too, not the origin's.
        self::assertStringContainsString('"last_by_subj":"$KV.dst.theme"', $writes);
        self::assertStringNotContainsString('$KV.src.theme', $writes);
    }

    /**
     * Re-binding a handle to a stream that is NO LONGER a mirror must clear the stale mirror
     * prefixes: bind() re-resolves from the confirmed server config. Pre-fix bind() only ever SET
     * prefixes, so a handle that once fronted a mirror kept routing writes to the old origin after
     * the bucket was re-created as a plain bucket.
     */
    public function testRebindToNonMirrorStreamClearsStalePrefixes(): void
    {
        $mirrorInfo = '{"config":{"name":"KV_dst","max_msgs_per_subject":1,"mirror":{"name":"KV_src"}}}';
        $plainInfo = '{"config":{"name":"KV_dst","max_msgs_per_subject":1}}';
        $putAck = '{"stream":"KV_dst","seq":4}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($mirrorInfo), $mirrorInfo),  // bind #1: mirror
            sprintf("MSG _INBOX.b 1 %d\r\n%s\r\n", strlen($putAck), $putAck),          // put #1 ack
            sprintf("MSG _INBOX.c 1 %d\r\n%s\r\n", strlen($plainInfo), $plainInfo),    // bind #2: plain
            sprintf("MSG _INBOX.d 1 %d\r\n%s\r\n", strlen($putAck), $putAck),          // put #2 ack
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('dst');

        // While bound to the mirror, writes go through to the origin.
        $kv->bind()->await();
        $kv->put('theme', 'blue')->await();
        self::assertStringContainsString('PUB $KV.src.theme ', implode('', $transport->writes));

        // Re-bound to a non-mirror stream: the stale origin prefixes are gone.
        $kv->bind()->await();
        $kv->put('theme', 'green')->await();

        $writes = implode('', $transport->writes);
        self::assertSame(1, substr_count($writes, 'PUB $KV.src.theme '), 'only the pre-rebind put may target the origin');
        self::assertStringContainsString('PUB $KV.dst.theme ', $writes, 'after re-bind writes must use the bucket\'s own subject');
    }

    /**
     * The `bucket` source alias explicitly declares a KV BUCKET, so it is KV_-prefixed
     * UNCONDITIONALLY: a bucket legitimately named "KV_x" is backed by stream KV_KV_x and its
     * records live under $KV.KV_x.>. Pre-fix the name-key idempotence guard was also applied to
     * the alias, silently resolving `['bucket' => 'KV_x']` to bucket x's stream and subjects -
     * the aggregate then copied the WRONG bucket's records.
     */
    public function testCreateSourceBucketAliasIsKvPrefixedUnconditionally(): void
    {
        $reply = '{"config":{"name":"KV_agg"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('agg')->create(['sources' => [['bucket' => 'KV_x']]])->await();

        $create = $transport->writes[3];
        self::assertStringContainsString(
            '"sources":[{"name":"KV_KV_x","subject_transforms":[{"src":"$KV.KV_x.>","dest":"$KV.agg.>"}]}]',
            $create,
            'the bucket alias must map bucket "KV_x" to ITS backing stream/subjects, not bucket "x"\'s',
        );
    }

    /**
     * The `name` source key keeps nats.go's stream-name semantics: "KV_x" already names the
     * backing stream (of bucket x) and is used as-is - no double prefix, transform on bucket x's
     * subjects. Complements the unconditional `bucket`-alias prefixing above.
     */
    public function testCreateSourceNameKeyKeepsKvStreamNameSemantics(): void
    {
        $reply = '{"config":{"name":"KV_agg"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('agg')->create(['sources' => [['name' => 'KV_x']]])->await();

        $create = $transport->writes[3];
        self::assertStringContainsString(
            '"sources":[{"name":"KV_x","subject_transforms":[{"src":"$KV.x.>","dest":"$KV.agg.>"}]}]',
            $create,
            'a KV_-prefixed name key must resolve as the backing stream name itself (nats.go idempotence)',
        );
        self::assertStringNotContainsString('KV_KV_', $create);
    }

    /**
     * The `bucket` MIRROR alias is KV_-prefixed unconditionally too: mirroring bucket "KV_x"
     * targets stream KV_KV_x, and this handle's write-through prefix resolves to that bucket's
     * live subjects ($KV.KV_x.). Pre-fix the alias resolved to bucket x's stream and
     * applyMirrorPrefixes routed the mirror's writes into the wrong bucket.
     */
    public function testCreateMirrorBucketAliasIsKvPrefixedUnconditionally(): void
    {
        $createReply = '{"config":{"name":"KV_dst"}}';
        $putAck = '{"stream":"KV_KV_x","seq":2}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            sprintf("MSG _INBOX.b 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('dst');
        $kv->create(['mirror' => ['bucket' => 'KV_x']])->await();
        $kv->put('theme', 'blue')->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('"mirror":{"name":"KV_KV_x"}', $writes);
        // Write-through targets bucket "KV_x"'s live subjects, not bucket "x"'s.
        self::assertStringContainsString('PUB $KV.KV_x.theme ', $writes);
        self::assertStringNotContainsString('PUB $KV.x.theme', $writes);
    }

    /**
     * A mirror NAME that is already the backing `KV_` stream name keeps nats.go's stream-name
     * semantics: used as-is, never double-prefixed (the counterpart of the unconditional
     * `bucket`-alias prefixing - only the alias escapes the ambiguity).
     */
    public function testCreateMirrorNameKeyKeepsKvStreamNameSemantics(): void
    {
        $createReply = '{"config":{"name":"KV_dst"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('dst')->create(['mirror' => ['name' => 'KV_src']])->await();

        $create = $transport->writes[3];
        self::assertStringContainsString('"mirror":{"name":"KV_src"}', $create);
        self::assertStringNotContainsString('KV_KV_', $create, 'an already-prefixed mirror name must not be double-prefixed');
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ack), $ack),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        // CONSUMER.CREATE is a mux request; the two replay deliveries land on the push deliver
        // subscription (sid 2), enqueued once its SUB is written so the request read cannot drain them.
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ], [
            '_INBOX.KV.HIST' => [
                // Two deliveries on the push deliver subject (sid 2); pending counts down to 0.
                "MSG dlv 2 \$JS.ACK.KV_cfg.HIST.1.5.1.0.1 2\r\nv1\r\n",
                "MSG dlv 2 \$JS.ACK.KV_cfg.HIST.1.6.2.0.0 2\r\nv2\r\n",
            ],
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ], [
            '_INBOX.KV.HIST' => [
                // A metadata-less delivery (no $JS.ACK reply subject) on the history subscription (sid 2).
                "MSG dlv 2 2\r\nxx\r\n",
                // A well-formed delivery follows and completes the replay (num_pending=0, last token).
                "MSG dlv 2 \$JS.ACK.KV_cfg.HIST.1.6.1.0.0 2\r\nv1\r\n",
            ],
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        // CONSUMER.CREATE is a mux request; the headers-only replay lands on the deliver subscription
        // (sid 2), enqueued once its SUB is written so the request read cannot drain it first.
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($consumerReply), $consumerReply),
        ], [
            '_INBOX.KV.KEYS' => [
                sprintf("HMSG \$KV.other.k 2 \$JS.ACK.KV_cfg.KEYS.1.2.1.0.3 %d %d\r\n%s\r\n", $oh, $oh, $otherHdrs),
                sprintf("HMSG \$KV.cfg.username 2 \$JS.ACK.KV_cfg.KEYS.1.3.2.0.2 %d %d\r\n%s\r\n", $uh, $uh, $usernameHdrs),
                sprintf("HMSG \$KV.cfg.email 2 \$JS.ACK.KV_cfg.KEYS.1.4.3.0.1 %d %d\r\n%s\r\n", $eh, $eh, $emailHdrs),
                sprintf("HMSG \$KV.cfg.phone 2 \$JS.ACK.KV_cfg.KEYS.1.5.4.0.0 %d %d\r\n%s\r\n", $ph, $ph, $phoneHdrs),
            ],
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
     * Connects a client whose CONSUMER.CREATE reply is $consumerReply (a mux request, echoed on its
     * captured reply-to) and whose deliver subscription (sid 2) is fed $deliverFrames once its SUB is
     * written, for the keys() enumeration tests. When keys() short-circuits without subscribing (no live
     * keys), $deliverFrames are never enqueued - so a seeded "ghost" frame stays unconsumed unless a
     * mutant wrongly proceeds to subscribe.
     *
     * @param list<string> $deliverFrames
     * @return array{0: NatsClient, 1: FakeTransport}
     */
    private function connectForKeys(string $consumerReply, array $deliverFrames = []): array
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($consumerReply), $consumerReply),
        ], $deliverFrames === [] ? [] : ['_INBOX.KV.KEYS' => $deliverFrames]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
     * A silent/reaped KV watch consumer is RECREATED by the ordered-consumer watchdog instead of
     * hanging forever (#113) - and, having delivered nothing yet, the replacement re-applies the
     * watch's INITIAL deliver policy rather than replaying the whole stream via by_start_sequence
     * from sequence 1. Recovery, not failure: no error is surfaced. (Watches are built on the
     * ordered machinery for lossless gap handling; pre-ordered behavior merely surfaced a
     * "not active" error and left the watch dead.)
     */
    public function testWatchSilentConsumerIsRecreatedByWatchdog(): void
    {
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","config":{"deliver_subject":"_INBOX.JS.ORD.x","ack_policy":"none"}}';
        $deleteReply = '{"success":true}';
        $recreateReply = '{"stream_name":"KV_cfg","name":"KVWATCH2","config":{"deliver_subject":"_INBOX.JS.ORD.y","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ], blockWhenEmpty: true);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            sprintf("MSG _INBOX.b 1 %d\r\n%s\r\n", strlen($deleteReply), $deleteReply),
            sprintf("MSG _INBOX.c 1 %d\r\n%s\r\n", strlen($recreateReply), $recreateReply),
        ]);

        $errors = [];
        $options = new NatsOptions(pingIntervalSeconds: 0, errorListener: static function (\Throwable $e) use (&$errors): void {
            $errors[] = $e;
        });
        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        // 30 ms heartbeat -> the watchdog reacts after ~60 ms of silence.
        $client->jetStream()->keyValue('cfg')->watch(
            static function (KeyValueEntry $entry): void {},
            '>',
            new KeyWatchOptions(idleHeartbeat: 30_000_000),
        )->await();

        // The watch consumer must actually request the heartbeat, or the server would never emit the
        // status-100 frames the watchdog relies on.
        self::assertStringContainsString('"idle_heartbeat":30000000', implode('', $transport->writes));

        $deadlineNs = hrtime(true) + 2_000_000_000;
        while (substr_count(implode('', $transport->writes), '$JS.API.CONSUMER.CREATE.KV_cfg') < 2 && hrtime(true) < $deadlineNs) {
            delay(0.01);
        }

        $client->disconnect()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.CONSUMER.DELETE.KV_cfg.KVWATCH', $writes, 'the silent instance must be replaced');
        self::assertGreaterThanOrEqual(2, substr_count($writes, '$JS.API.CONSUMER.CREATE.KV_cfg'), 'a replacement consumer must be created');
        // Nothing was delivered before the stall: the replacement must re-apply the INITIAL watch
        // policy, never a by_start_sequence replay from stream sequence 1.
        self::assertStringNotContainsString('by_start_sequence', $writes);
        self::assertSame([], $errors, 'watchdog recreation is recovery, not an error');
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
        ], blockWhenEmpty: true);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

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
     * The headline lossless-watch guarantee: a KV watch that MISSES a delivery (consumer sequence
     * skips - a slow-consumer drop, a reconnect window) must detect the gap and recreate its
     * consumer from the last delivered revision + 1 so the server REPLAYS the missed range, instead
     * of silently continuing with a permanent gap. Pre-fix the watch ran on a plain ack-none
     * ephemeral with no sequence tracking at all: ack-none deliveries are never redelivered on
     * their own, so every drop was invisible, unrecoverable loss.
     */
    public function testWatchDetectsDeliveryGapAndRecreatesFromLastRevision(): void
    {
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","config":{"deliver_subject":"_INBOX.JS.ORD.x","ack_policy":"none"}}';
        $deleteReply = '{"success":true}';
        $recreateReply = '{"stream_name":"KV_cfg","name":"KVWATCH2","config":{"deliver_subject":"_INBOX.JS.ORD.y","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            sprintf("MSG _INBOX.b 1 %d\r\n%s\r\n", strlen($deleteReply), $deleteReply),
            sprintf("MSG _INBOX.c 1 %d\r\n%s\r\n", strlen($recreateReply), $recreateReply),
        ], [
            // Delivered once the deliver-inbox SUB exists (muxServer's deliverOnSub is SUB-gated;
            // a raw write-containing hook would also match the CREATE payload's deliver_subject
            // and enqueue these before the subscription, dropping them on an unknown sid).
            '_INBOX.JS.ORD' => [
                // In-order: consumer seq 1, stream seq (revision) 7.
                "MSG \$KV.cfg.a 2 \$JS.ACK.KV_cfg.KVWATCH.1.7.1.0.1 2\r\nv1\r\n",
                // Gap: consumer seq jumps to 3 (expected 2) - a delivery was missed. This entry is
                // DISCARDED (never delivered out of order) and the consumer is recreated.
                "MSG \$KV.cfg.b 2 \$JS.ACK.KV_cfg.KVWATCH.3.9.3.0.0 2\r\nv3\r\n",
            ],
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000, pingIntervalSeconds: 0), $transport);
        $client->connect()->await();

        $revisions = [];
        $client->jetStream()->keyValue('cfg')->watch(static function (KeyValueEntry $entry) use (&$revisions): void {
            $revisions[] = $entry->revision;
        })->await();

        for ($i = 0; $i < 8; $i++) {
            $client->processIncoming()->await();
        }

        // Only the in-order entry reached the watcher; the out-of-order one was discarded.
        self::assertSame([7], $revisions);

        $written = implode('', $transport->writes);
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.DELETE.KV_cfg.KVWATCH'), 'the gapped consumer must be replaced');
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.CREATE.KV_cfg'), 'exactly one recreate');
        // Resume from the last delivered revision + 1 (7 + 1 = 8): the missed range is REPLAYED.
        self::assertStringContainsString('"deliver_policy":"by_start_sequence"', $written);
        self::assertStringContainsString('"opt_start_seq":8', $written);
    }

    /**
     * getAll() must survive a bucket without Direct Get (allow_direct disabled / legacy interop
     * bucket): the per-key lookup falls back to the leader STREAM.MSG.GET path like get() does,
     * instead of failing the whole enumeration with the 503 on a bucket whose single-key reads work.
     */
    public function testGetAllFallsBackToStreamMessageWhenDirectGetUnavailable(): void
    {
        $subjectsReply = '{"state":{"subjects":{"$KV.cfg.theme":1}}}';
        $envelope = sprintf(
            '{"message":{"subject":"$KV.cfg.theme","seq":9,"data":"%s"}}',
            base64_encode('blue'),
        );

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($subjectsReply), $subjectsReply),  // STREAM.INFO subjects page 1
            sprintf("MSG _INBOX.a2 1 %d\r\n%s\r\n", strlen('{"state":{"subjects":{}}}'), '{"state":{"subjects":{}}}'),  // page 2: empty -> pagination ends
            $this->kvDirectStatus(1, 503, 'No Responders'),                                  // Direct Get -> 503
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($envelope), $envelope),            // STREAM.MSG.GET fallback
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $all = $client->jetStream()->keyValue('cfg')->getAll()->await();

        self::assertSame(['theme' => 'blue'], $all, 'the enumeration must fall back per key, not fail on the 503');
        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('PUB $JS.API.STREAM.MSG.GET.KV_cfg', $writes);
    }

    /**
     * On a MODERN server (2.11+, batched Direct Get supported) an allow_direct-disabled bucket
     * answers the batched multi_last with 503 - the server-version gate cannot see per-STREAM
     * Direct Get availability. getAll() must fall through to the per-subject fan-out (whose
     * lookups fall back to the leader read) instead of failing the whole enumeration.
     */
    public function testGetAllFallsBackWhenBatchedDirectGetAnswers503(): void
    {
        $subjectsReply = '{"state":{"subjects":{"$KV.cfg.theme":1}}}';
        $emptyPage = '{"state":{"subjects":{}}}';
        $batch503 = "NATS/1.0 503 No Responders Available For Request\r\n\r\n";
        $envelope = sprintf(
            '{"message":{"subject":"$KV.cfg.theme","seq":9,"data":"%s"}}',
            base64_encode('blue'),
        );

        $transport = new FakeTransport([
            // 2.12 server: the BATCHED path is taken first.
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // The batched request's replies arrive on the DGET inbox (sid 2: mux SUB=1, DGET SUB=2).
        $transport->enqueueOnWriteContaining = [
            'SUB _INBOX.JS.DGET' => [
                sprintf("HMSG _INBOX.JS.DGET.x 2 %d %d\r\n%s\r\n", strlen($batch503), strlen($batch503), $batch503),
            ],
        ];
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($subjectsReply), $subjectsReply),
            sprintf("MSG _INBOX.a2 1 %d\r\n%s\r\n", strlen($emptyPage), $emptyPage),
            $this->kvDirectStatus(1, 503, 'No Responders'),                          // per-key Direct Get -> 503
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($envelope), $envelope),    // STREAM.MSG.GET fallback
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $all = $client->jetStream()->keyValue('cfg')->getAll()->await();

        self::assertSame(['theme' => 'blue'], $all, 'the batched 503 must fall through to the fan-out + leader read');
    }

    /**
     * Verifies get() returns null for missing keys.
     */
    public function testGetMissingReturnsNull(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            $this->kvDirectStatus(1, 404, 'Message Not Found'),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($updateAck), $updateAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($purgeAck), $purgeAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($delAck), $delAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        // All four replies are mux requests (two STREAM.INFO pages, then the per-key Direct Get fan-out,
        // written username-then-email in iteration order), echoed FIFO on each captured reply-to.
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfoPage1), $streamInfoPage1),   // STREAM.INFO page 1
            sprintf("MSG _INBOX.p 2 %d\r\n%s\r\n", strlen($streamInfoPage2), $streamInfoPage2),   // STREAM.INFO page 2 empty
            sprintf("HMSG _INBOX.b 3 %d %d\r\n%s\r\n", $uh, $uh, $usernameHdrs),                  // username -> PURGE (skipped)
            sprintf("HMSG _INBOX.c 4 %d %d\r\n%s%s\r\n", $eh, $et, $emailHdrs, $emailBody),       // email -> value
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("HMSG _INBOX.x 1 %d %d\r\n%s\r\n", $h, $h, $hdrs),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfoPage1), $streamInfoPage1),
            sprintf("MSG _INBOX.p 2 %d\r\n%s\r\n", strlen($streamInfoPage2), $streamInfoPage2),
            sprintf("HMSG _INBOX.b 3 %d %d\r\n%s\r\n", $uh, $uh, $usernameHdrs),
            sprintf("HMSG _INBOX.c 4 %d %d\r\n%s%s\r\n", $eh, $et, $emailHdrs, $emailBody),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        // CONSUMER.CREATE is a mux request; the marker delivery lands on the push deliver subscription
        // (sid 2), enqueued once its SUB is written.
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ], [
            '_INBOX.JS.ORD' => [
                sprintf("HMSG \$KV.cfg.theme 2 \$JS.ACK.KV_cfg.KVWATCH.1.7.1.0.0 %d %d\r\n%s\r\n", $mh, $mh, $markerHdrs),
            ],
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ], [
            '_INBOX.JS.ORD' => [
                "MSG \$KV.cfg.theme 2 \$JS.ACK.KV_cfg.KVWATCH.1.7.1.0.0 4\r\nblue\r\n",
            ],
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            $this->kvDirectStatus(1, 500, 'internal error'),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            "MSG _INBOX.a 1 7\r\nnotjson\r\n", // a non-JSON ack
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            $this->kvDirectReply('$KV.cfg.theme', 'ignored', 3, 1, 'DEL'),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfoPage1), $streamInfoPage1),   // STREAM.INFO page 1
            sprintf("MSG _INBOX.p 2 %d\r\n%s\r\n", strlen($streamInfoPage2), $streamInfoPage2),   // STREAM.INFO page 2 empty
            sprintf("HMSG _INBOX.b 3 %d %d\r\n%s\r\n", $nf, $nf, $notFound),                      // gone -> 404 (skipped)
            sprintf("HMSG _INBOX.c 4 %d %d\r\n%s%s\r\n", $th, $tt, $themeHdrs, $themeBody),       // theme -> value
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($error), $error),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($purgeAck), $purgeAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorReply), $errorReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorReply), $errorReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            $this->kvDirectStatus(1, 503, 'No Responders'),                                     // Direct Get -> 503 fallback trigger
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($errorReply), $errorReply),           // STREAM.MSG.GET -> 404
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            $this->kvDirectStatus(1, 503, 'No Responders'),
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($errorReply), $errorReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            $this->kvDirectStatus(1, 503, 'No Responders'),
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($emptyReply), $emptyReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            $this->kvDirectStatus(1, 503, 'No Responders'),
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($envelope), $envelope),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            $this->kvDirectStatus(1, 503, 'No Responders'),                                     // Direct Get -> 503 fallback trigger
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($envelope), $envelope),              // STREAM.MSG.GET -> malformed data
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ], [
            // A message on a completely different subject - keyFromSubject() will return null.
            '_INBOX.JS.ORD' => [
                "MSG some.other.subject 2 \$JS.ACK.KV_cfg.KVWATCH.1.1.1.0.0 4\r\ndata\r\n",
            ],
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),   // first put -> wrong-last-seq
            $this->kvDirectStatus(2, 404, 'Message Not Found'),                   // get() -> null
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($putAck), $putAck),   // second put -> success
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errAck), $errAck),          // first put -> wrong-last-seq
            $this->kvDirectReply('$KV.cfg.theme', '', 5, 2, 'DEL'),                      // get() -> DEL tombstone at seq 5
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($putAck2), $putAck2),         // second put -> success
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $ack = $client->jetStream()->keyValue('cfg')->createKey('theme', 'newval')->await();

        self::assertSame(6, $ack->seq);
        // The second put must use expected-seq 5 (the tombstone's revision). Over the shared mux inbox the
        // three request publishes are consecutive (writes[2] is the mux SUB): put1 HPUB [3], Direct-Get [4],
        // put2 HPUB [5].
        self::assertStringContainsString('Nats-Expected-Last-Subject-Sequence:5', $transport->writes[5]);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfoPage1), $streamInfoPage1),
            sprintf("MSG _INBOX.p 2 %d\r\n%s\r\n", strlen($streamInfoPage2), $streamInfoPage2),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfoPage1), $streamInfoPage1),
            sprintf("MSG _INBOX.p 2 %d\r\n%s\r\n", strlen($streamInfoPage2), $streamInfoPage2),
            sprintf("HMSG _INBOX.b 3 %d %d\r\n%s\r\n", $eh, $eh, $errHdrs),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfoPage1), $streamInfoPage1),
            sprintf("MSG _INBOX.p 2 %d\r\n%s\r\n", strlen($streamInfoPage2), $streamInfoPage2),
            // Only theme gets a Direct Get request (the non-KV subject is skipped).
            sprintf("HMSG _INBOX.b 3 %d %d\r\n%s%s\r\n", $th, $tt, $themeHdrs, $themeBody),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        // One batched reply stream on the batched Direct-Get inbox subscription (sid 2 - the mux inbox
        // is sid 1): a DEL tombstone (excluded), TWO live values, and the last frame carrying
        // Nats-Num-Pending: 0 to terminate the batch.
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
        ]);
        // Two STREAM.INFO pages are mux requests; the single batched multi_last Direct Get uses its own
        // inbox subscription (sid 2), whose frames are emitted once that SUB is written.
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfoPage1), $streamInfoPage1),   // STREAM.INFO page 1
            sprintf("MSG _INBOX.p 2 %d\r\n%s\r\n", strlen($streamInfoPage2), $streamInfoPage2),   // STREAM.INFO page 2 empty
        ], [
            '_INBOX.JS.DGET' => [
                sprintf("HMSG \$KV.cfg.username 2 %d %d\r\n%s\r\n", $uh, $uh, $usernameHdrs),         // batched frame: username -> DEL (excluded)
                sprintf("HMSG \$KV.cfg.email 2 %d %d\r\n%s%s\r\n", $eh, $et, $emailHdrs, $emailBody), // batched frame: email -> value
                sprintf("HMSG \$KV.cfg.phone 2 %d %d\r\n%s%s\r\n", $ph, $pt, $phoneHdrs, $phoneBody), // batched frame: phone -> value (terminator)
            ],
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ], [
            // Delivered WITHOUT a $JS.ACK reply subject -> JsMessageMetadata::fromMessage() returns null.
            '_INBOX.JS.ORD' => [
                "MSG \$KV.cfg.theme 2 4\r\nblue\r\n",
            ],
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ], [
            // One revision (num_pending token = 1, NOT 0) on the deliver subscription, then silence.
            '_INBOX.KV.HIST' => [
                "MSG dlv 2 \$JS.ACK.KV_cfg.HIST.1.5.1.0.1 2\r\nv1\r\n",
            ],
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
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

    /**
     * The `bucket` alias is an explicit KV-bucket declaration, so it is translated to the backing
     * `KV_` stream name EVEN in the custom-subject_transforms pass-through branch (which otherwise
     * leaves the source verbatim for non-KV streams - nats.go has no such alias, so nothing else
     * may be touched: the caller's transforms pass through and no auto transform is attached).
     */
    public function testCreateSourceBucketAliasWithCustomTransformsStillKvPrefixesName(): void
    {
        $reply = '{"config":{"name":"KV_agg"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('agg')->create([
            'sources' => [[
                'bucket' => 'legacy',
                'subject_transforms' => [['src' => 'raw.>', 'dest' => '$KV.agg.>']],
            ]],
        ])->await();

        $create = $transport->writes[3];
        // The alias resolves to the backing stream name, the caller's transforms pass through verbatim.
        self::assertStringContainsString(
            '"sources":[{"subject_transforms":[{"src":"raw.>","dest":"$KV.agg.>"}],"name":"KV_legacy"}]',
            $create,
        );
        // No mandatory KV transform is auto-attached in the full-control branch, and the client-side
        // alias key never reaches the wire.
        self::assertStringNotContainsString('"src":"$KV.legacy.>"', $create);
        self::assertStringNotContainsString('"bucket":"legacy"', $create);
    }

    /**
     * A source whose name is ALREADY the backing `KV_` stream name must not be double-prefixed: the
     * bucket part is derived by stripping the prefix, so the mandatory ADR-57 transform re-subjects
     * from the ORIGIN bucket's `$KV.<src>.>` keyspace - not from a bogus `$KV.KV_<src>.>`.
     */
    public function testCreateSourceWithKvPrefixedNameIsNotDoublePrefixed(): void
    {
        $reply = '{"config":{"name":"KV_agg"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('agg')->create(['sources' => ['KV_events']])->await();

        $create = $transport->writes[3];
        self::assertStringContainsString(
            '"sources":[{"name":"KV_events","subject_transforms":[{"src":"$KV.events.>","dest":"$KV.agg.>"}]}]',
            $create,
        );
        self::assertStringNotContainsString('KV_KV_', $create, 'an already-prefixed source name must be used as-is');
    }

    /**
     * `domain` is a client-side shorthand FOR `external`, so an entry carrying both is ambiguous and
     * must be rejected locally (nats.go parity) - before anything is sent, so no half-built stream
     * create ever reaches the server.
     */
    public function testCreateRejectsMirrorWithBothDomainAndExternal(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, []);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->keyValue('dst')->create([
                'mirror' => ['name' => 'src', 'domain' => 'HUB', 'external' => ['api' => '$JS.HUB.API']],
            ])->await();
            self::fail('a mirror with both domain and external must be rejected');
        } catch (JetStreamException $e) {
            self::assertSame('A KV mirror/source cannot set both domain and external', $e->getMessage());
        }

        // The rejection is client-side: no STREAM.CREATE request was published.
        self::assertStringNotContainsString('STREAM.CREATE', implode('', $transport->writes));
    }

    /**
     * A mirror entry with an EMPTY name resolves to no origin bucket, so the prefix re-resolution
     * must be a no-op: writes keep this handle's own `$KV.<bucket>.` prefix rather than forging a
     * broken `$KV..`-style origin prefix from the empty name.
     */
    public function testCreateMirrorWithEmptyNameLeavesPrefixesUnchanged(): void
    {
        $createReply = '{"config":{"name":"KV_dst"}}';
        $putAck = '{"stream":"KV_dst","seq":5}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            sprintf("MSG _INBOX.b 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('dst');
        $kv->create(['mirror' => ['name' => '']])->await();
        $ack = $kv->put('theme', 'blue')->await();

        $writes = implode('', $transport->writes);
        // The write stays on this bucket's own prefix - no origin could be resolved.
        self::assertStringContainsString('PUB $KV.dst.theme ', $writes);
        self::assertStringNotContainsString('PUB $KV..theme', $writes);
        self::assertSame(5, $ack->seq);
    }

    /**
     * bind() sanity-checks the existing stream is KV-shaped before adopting it (nats.go ErrBadBucket
     * parity): a stream without `max_msgs_per_subject >= 1` is not a KV bucket, and attaching to it
     * would corrupt reads/writes - so bind() must fail loudly, naming the stream.
     */
    public function testBindRejectsStreamThatIsNotAKvBucket(): void
    {
        // STREAM.INFO of a plain (non-KV) stream: no max_msgs_per_subject in the config.
        $info = '{"config":{"name":"KV_cfg","mirror":{"name":"KV_src"}}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($info), $info),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->keyValue('cfg')->bind()->await();
            self::fail('bind() must reject a stream that is not KV-shaped');
        } catch (JetStreamException $e) {
            self::assertSame('Stream "KV_cfg" is not a KV bucket (max_msgs_per_subject < 1)', $e->getMessage());
        }
    }

    /**
     * keys(): a delivery lacking a parseable $JS.ACK reply subject (a control / non-conformant
     * frame) must be SKIPPED - not thrown out of the shared dispatch loop (the #90 class) and not
     * enumerated as a key even when its subject looks like one. The frame uses a legitimate key
     * subject precisely so a mutant that collects it anyway is caught by the returned set.
     */
    public function testKeysToleratesDeliveryWithoutMetadataAndKeepsEnumerating(): void
    {
        $consumerReply = '{"stream_name":"KV_cfg","name":"KEYS","num_pending":2,'
            . '"config":{"deliver_subject":"_INBOX.KV.KEYS.x","ack_policy":"none",'
            . '"deliver_policy":"last_per_subject","headers_only":true}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($consumerReply), $consumerReply),
        ], [
            '_INBOX.KV.KEYS' => [
                // A metadata-less delivery (no $JS.ACK reply subject) on a real key subject.
                "MSG \$KV.cfg.ghost 2 2\r\nxx\r\n",
                // A well-formed delivery follows and completes the replay (num_pending token = 0).
                "MSG \$KV.cfg.real 2 \$JS.ACK.KV_cfg.KEYS.1.6.1.0.0 0\r\n\r\n",
            ],
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $keys = $client->jetStream()->keyValue('cfg')->keys()->await();

        // Only the well-formed delivery is enumerated; the metadata-less frame is skipped entirely.
        self::assertSame(['real'], $keys);
        self::assertStringContainsString("UNSUB 2\r\n", implode('', $transport->writes));
    }

    /**
     * #121 companion to testHistoryThrowsWhenDeadlineFiresBeforeCaughtUp: on a FULLY idle deliver
     * subscription (a live but silent socket, modelled by a blocking read) the bounded wait is
     * enforced by CANCELLING the in-flight socket read - the replay must surface the stall error
     * instead of hanging forever inside the read, and the cancelled read must not leak.
     */
    public function testHistoryStallsOnFullyIdleDeliverSubscriptionAndUnsubscribes(): void
    {
        // One revision pending, but the deliver subscription never receives anything at all.
        $createReply = '{"stream_name":"KV_cfg","name":"HIST","num_pending":1,"config":{"deliver_subject":"dlv","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ], blockWhenEmpty: true);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000, pingIntervalSeconds: 0), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->keyValue('cfg')->history('theme', 0.05)->await();
            self::fail('a fully idle replay must surface the stall, not hang in the blocked read');
        } catch (JetStreamException $e) {
            self::assertStringStartsWith('Key history for "theme" stalled', $e->getMessage());
            self::assertStringContainsString('0.050 s', $e->getMessage());
            self::assertStringContainsString('collected 0 revision(s)', $e->getMessage());
        }

        // The deliver subscription (sid 2) is torn down on the throw path, and the blocked socket
        // read was actually cancelled (every started read resolved - nothing orphaned).
        self::assertStringContainsString("UNSUB 2\r\n", implode('', $transport->writes));
        self::assertGreaterThan(0, $transport->startedReads);
        self::assertSame($transport->startedReads, $transport->resolvedReads);

        // Quiesce leftover timers exactly as testKeysThrowsOnStalledReplayAndStillUnsubscribes does.
        $client->disconnect()->await();
        foreach (EventLoop::getIdentifiers() as $id) {
            EventLoop::cancel($id);
        }
    }

    /**
     * getAll() per-subject fan-out (pre-2.11 server, no batched Direct Get): an error that is
     * neither 404 (missing key, skipped) nor 503 (Direct Get unavailable, leader fallback) must
     * propagate VERBATIM - code and description - and fail the enumeration.
     */
    public function testGetAllFanOutPropagatesNon404Non503DirectGetError(): void
    {
        $streamInfoPage1 = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]},"state":{"messages":1,"bytes":32,"subjects":{"$KV.cfg.theme":1}}}';
        $streamInfoPage2 = '{"config":{"name":"KV_cfg"},"state":{"messages":1,"bytes":32,"subjects":{}}}';
        $errHdrs = "NATS/1.0 500 Internal Error\r\nStatus: 500\r\nDescription: kv direct get exploded\r\n\r\n";
        $eh = strlen($errHdrs);

        // Pre-2.11 server: the batched multi_last gate is off, so the per-subject fan-out runs.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfoPage1), $streamInfoPage1),
            sprintf("MSG _INBOX.p 2 %d\r\n%s\r\n", strlen($streamInfoPage2), $streamInfoPage2),
            sprintf("HMSG _INBOX.b 3 %d %d\r\n%s\r\n", $eh, $eh, $errHdrs),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->keyValue('cfg')->getAll()->await();
            self::fail('a non-404/503 Direct Get error must fail the enumeration');
        } catch (JetStreamException $e) {
            self::assertSame(500, $e->getCode());
            self::assertSame('kv direct get exploded', $e->getMessage());
        }

        $writes = implode('', $transport->writes);
        // The fan-out path was taken: a per-subject last_by_subj lookup, no batched multi_last.
        self::assertStringContainsString('"last_by_subj":"$KV.cfg.theme"', $writes);
        self::assertStringNotContainsString('"multi_last"', $writes);
    }

    /**
     * getAll() batched fast path: a batch REPLY frame whose stored subject is outside this bucket's
     * keyspace (a rogue/foreign record in the multi_last stream) must be dropped from the result,
     * not surfaced under a garbage key - mirroring the request-side candidate filtering.
     */
    public function testGetAllBatchedDropsReplyFramesOutsideBucketPrefix(): void
    {
        $streamInfoPage1 = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]},"state":{"messages":1,"subjects":{"$KV.cfg.theme":1}}}';
        $streamInfoPage2 = '{"config":{"name":"KV_cfg"},"state":{"subjects":{}}}';
        // A frame on a FOREIGN subject precedes the real key's frame (which terminates the batch).
        $rogueHdrs = "NATS/1.0\r\nNats-Stream: KV_cfg\r\nNats-Subject: \$KV.other.rogue\r\nNats-Sequence: 7\r\n\r\n";
        $themeHdrs = "NATS/1.0\r\nNats-Stream: KV_cfg\r\nNats-Subject: \$KV.cfg.theme\r\nNats-Sequence: 8\r\nNats-Num-Pending: 0\r\n\r\n";
        $rogueBody = 'zzz';
        $themeBody = 'dark';
        $rh = strlen($rogueHdrs);
        $rt = $rh + strlen($rogueBody);
        $th = strlen($themeHdrs);
        $tt = $th + strlen($themeBody);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfoPage1), $streamInfoPage1),
            sprintf("MSG _INBOX.p 2 %d\r\n%s\r\n", strlen($streamInfoPage2), $streamInfoPage2),
        ], [
            '_INBOX.JS.DGET' => [
                sprintf("HMSG \$KV.other.rogue 2 %d %d\r\n%s%s\r\n", $rh, $rt, $rogueHdrs, $rogueBody),
                sprintf("HMSG \$KV.cfg.theme 2 %d %d\r\n%s%s\r\n", $th, $tt, $themeHdrs, $themeBody),
            ],
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $all = $client->jetStream()->keyValue('cfg')->getAll()->await();

        // Only the in-prefix record maps to a key; the rogue frame contributes nothing.
        self::assertSame(['theme' => 'dark'], $all);
        self::assertStringContainsString('"multi_last":["$KV.cfg.theme"]', implode('', $transport->writes));
    }

    /**
     * KeyWatchOptions rejects a non-positive idleHeartbeat at construction: a zero/negative interval
     * could never drive the missed-heartbeat watchdog and would otherwise be sent to the server as a
     * nonsense consumer config.
     */
    public function testWatchOptionsRejectNonPositiveIdleHeartbeat(): void
    {
        try {
            new KeyWatchOptions(idleHeartbeat: 0);
            self::fail('idleHeartbeat: 0 must be rejected');
        } catch (\InvalidArgumentException $e) {
            self::assertSame('idleHeartbeat must be a positive integer (nanoseconds)', $e->getMessage());
        }

        $this->expectException(\InvalidArgumentException::class);
        new KeyWatchOptions(idleHeartbeat: -1);
    }

    /**
     * A caller-provided `mirror_direct: false` must survive the mirror default (`??=` assigns only
     * when the caller left it unset): a bucket admin may deliberately keep origin DIRECT.GET traffic
     * off a mirror. Overwriting it with the default (plain `=`) would silently re-enable it.
     */
    public function testCreateMirrorRespectsExplicitMirrorDirectFalse(): void
    {
        $reply = '{"config":{"name":"KV_dst"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('dst')->create(['mirror' => 'src', 'mirror_direct' => false])->await();

        $create = $transport->writes[3];
        self::assertStringContainsString('"mirror_direct":false', $create, 'an explicit mirror_direct:false must not be overridden by the mirror default');
        self::assertStringNotContainsString('"mirror_direct":true', $create);
    }

    /**
     * The ADR-57 transform-skip rule is EXTERNAL-only: a same-domain (non-external) source of the
     * SAME bucket name still gets the mandatory transform. Only an external source whose bucket name
     * matches this bucket already has matching subjects (nats.go rule).
     */
    public function testCreateSameBucketSourceWithoutExternalStillGetsTransform(): void
    {
        $reply = '{"config":{"name":"KV_agg"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('agg')->create(['sources' => [['name' => 'KV_agg']]])->await();

        self::assertStringContainsString(
            '"sources":[{"name":"KV_agg","subject_transforms":[{"src":"$KV.agg.>","dest":"$KV.agg.>"}]}]',
            $transport->writes[3],
            'the transform is skipped ONLY for an external same-bucket source; a local one keeps it',
        );
    }

    /**
     * The two halves of the external-same-bucket skip rule, pinned on the wire in one create: an
     * EXTERNAL source of the SAME bucket name carries NO transform (its subjects already match),
     * while an external source of a DIFFERENT bucket keeps the mandatory transform.
     */
    public function testCreateExternalSourcesSkipTransformOnlyForSameBucketName(): void
    {
        $reply = '{"config":{"name":"KV_agg"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('agg')->create(['sources' => [
            ['name' => 'KV_agg', 'external' => ['api' => '$JS.HUB.API']],
            ['name' => 'KV_b1', 'external' => ['api' => '$JS.HUB.API']],
        ]])->await();

        self::assertStringContainsString(
            '"sources":[{"name":"KV_agg","external":{"api":"$JS.HUB.API"}},'
            . '{"name":"KV_b1","external":{"api":"$JS.HUB.API"},'
            . '"subject_transforms":[{"src":"$KV.b1.>","dest":"$KV.agg.>"}]}]',
            $transport->writes[3],
            'external same-bucket: no transform; external different-bucket: mandatory transform',
        );
    }

    /**
     * A numeric `bucket` source alias (JSON/user data is loosely typed) must be stringified BEFORE
     * the same-bucket comparison: on a bucket literally named "42", an external source aliasing
     * bucket 42 IS the same bucket, so the transform-skip rule applies. Without the cast the strict
     * int-vs-string comparison would silently attach a transform the rule forbids.
     */
    public function testCreateExternalSourceNumericBucketAliasComparesAsString(): void
    {
        $reply = '{"config":{"name":"KV_42"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('42')->create(['sources' => [
            ['bucket' => 42, 'external' => ['api' => '$JS.HUB.API']],
        ]])->await();

        self::assertStringContainsString(
            '"sources":[{"external":{"api":"$JS.HUB.API"},"name":"KV_42"}]',
            $transport->writes[3],
            'a numeric alias of THIS bucket is the same bucket: external same-bucket sources carry no transform',
        );
        self::assertStringNotContainsString('"subject_transforms"', $transport->writes[3]);
    }

    /**
     * A numeric source `name` (loosely typed input) is stringified before the KV_ prefix check, so
     * it resolves like the equivalent string name instead of crashing the prefix parse.
     */
    public function testCreateSourceNumericNameIsStringifiedBeforePrefixCheck(): void
    {
        $reply = '{"config":{"name":"KV_agg"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('agg')->create(['sources' => [['name' => 42]]])->await();

        self::assertStringContainsString(
            '"sources":[{"name":"KV_42","subject_transforms":[{"src":"$KV.42.>","dest":"$KV.agg.>"}]}]',
            $transport->writes[3],
            'a numeric source name must resolve exactly like its string form',
        );
    }

    /**
     * A numeric mirror `name` is stringified before the KV_ prefix check too (the mirror twin of the
     * source-name rule above).
     */
    public function testCreateMirrorNumericNameIsStringifiedBeforePrefixCheck(): void
    {
        $reply = '{"config":{"name":"KV_dst"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->keyValue('dst')->create(['mirror' => ['name' => 42]])->await();

        self::assertStringContainsString('"mirror":{"name":"KV_42"}', $transport->writes[3]);
    }

    /**
     * bind() against a STREAM.INFO whose mirror name arrives as a JSON number (a malformed but
     * decodable reply) must still resolve the write-through prefix from its string form instead of
     * crashing the prefix parse - streams may legally carry numeric names.
     */
    public function testBindResolvesPrefixesFromNumericMirrorName(): void
    {
        $info = '{"config":{"name":"KV_dst","max_msgs_per_subject":1,"mirror":{"name":314}}}';
        $putAck = '{"stream":"KV_314","seq":1}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($info), $info),
            sprintf("MSG _INBOX.b 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('dst')->bind()->await();
        $kv->put('theme', 'blue')->await();

        self::assertStringContainsString('PUB $KV.314.theme ', implode('', $transport->writes));
    }

    /**
     * An EMPTY external api string on a mirror is "no external at all": the handle keeps
     * same-domain semantics - writes go through to the origin's own subject (no api prefix glued
     * on) and reads stay on THIS bucket's prefix.
     */
    public function testCreateMirrorEmptyExternalApiTreatedAsSameDomain(): void
    {
        $createReply = '{"config":{"name":"KV_dst"}}';
        $putAck = '{"stream":"KV_src","seq":1}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            sprintf("MSG _INBOX.b 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
            $this->kvDirectReply('$KV.dst.theme', 'blue', 1, 1),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('dst');
        $kv->create(['mirror' => ['name' => 'src', 'external' => ['api' => '']]])->await();
        $kv->put('theme', 'blue')->await();
        $kv->get('theme')->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('PUB $KV.src.theme ', $writes, 'writes go through to the origin WITHOUT an api prefix');
        self::assertStringContainsString('"last_by_subj":"$KV.dst.theme"', $writes, 'reads stay on this bucket\'s own prefix (same-domain mirror)');
    }

    /**
     * A trailing dot on the external api is trimmed before the write prefix is assembled - without
     * the rtrim every write would target `<api>..$KV.<origin>.<key>`, an empty-token subject no
     * server routes.
     */
    public function testCreateMirrorExternalApiTrailingDotIsTrimmed(): void
    {
        $createReply = '{"config":{"name":"KV_dst"}}';
        $putAck = '{"stream":"KV_src","seq":1}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            sprintf("MSG _INBOX.b 1 %d\r\n%s\r\n", strlen($putAck), $putAck),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('dst');
        $kv->create(['mirror' => ['name' => 'src', 'external' => ['api' => '$JS.HUB.API.']]])->await();
        $kv->put('theme', 'blue')->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('PUB $JS.HUB.API.$KV.src.theme ', $writes);
        self::assertStringNotContainsString('API..$KV', $writes, 'the trailing dot must be trimmed, not doubled into an empty token');
    }

    /**
     * The KV-shape gate boundary: max_msgs_per_subject EXACTLY 0 is rejected with the full
     * diagnostic, while EXACTLY 1 (the smallest KV-shaped value) binds fine.
     */
    public function testBindKvShapeGateBoundaryZeroRejectedOneAccepted(): void
    {
        $zeroInfo = '{"config":{"name":"KV_cfg","max_msgs_per_subject":0}}';
        $oneInfo = '{"config":{"name":"KV_cfg","max_msgs_per_subject":1}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($zeroInfo), $zeroInfo),
            sprintf("MSG _INBOX.b 1 %d\r\n%s\r\n", strlen($oneInfo), $oneInfo),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('cfg');
        try {
            $kv->bind()->await();
            self::fail('max_msgs_per_subject 0 must be rejected as not KV-shaped');
        } catch (JetStreamException $e) {
            self::assertSame('Stream "KV_cfg" is not a KV bucket (max_msgs_per_subject < 1)', $e->getMessage());
        }

        self::assertSame($kv, $kv->bind()->await(), 'max_msgs_per_subject 1 is the smallest KV-shaped value and must bind');
    }

    /**
     * The leader STREAM.MSG.GET fallback tolerates loosely typed message fields: a numeric `data`
     * (its digits are a decodable base64 alphabet string) and numeric `hdrs` (decodes to bytes that
     * are not a NATS/1.0 block -> no headers) must produce the decoded value with the default PUT
     * operation instead of crashing the decode.
     */
    public function testGetFallbackToleratesNumericDataAndHdrsFields(): void
    {
        $envelope = '{"message":{"subject":"$KV.cfg.theme","seq":7,"data":1234,"hdrs":1010}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            $this->kvDirectStatus(1, 503, 'No Responders'),                          // Direct Get -> 503
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($envelope), $envelope),    // STREAM.MSG.GET fallback
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $entry = $client->jetStream()->keyValue('cfg')->get('theme')->await();

        self::assertNotNull($entry);
        self::assertSame(base64_decode('1234', true), $entry->value, 'numeric data must decode via its string form');
        self::assertSame('PUT', $entry->operation, 'non-header hdrs bytes decode to no headers -> default PUT');
        self::assertSame(7, $entry->revision);
    }

    /**
     * getAll() batched fast path: an error that is NOT 503 (stream-lacks-Direct-Get) must fail the
     * enumeration verbatim - falling through to the fan-out would retry against a server that just
     * reported a real failure and mask it.
     */
    public function testGetAllBatchedNonUnavailableErrorFailsEnumeration(): void
    {
        $subjectsReply = '{"state":{"subjects":{"$KV.cfg.theme":1}}}';
        $emptyPage = '{"state":{"subjects":{}}}';
        $errHdrs = "NATS/1.0 500 Internal Error\r\nStatus: 500\r\nDescription: direct get batch exploded\r\n\r\n";

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $transport->enqueueOnWriteContaining = [
            'SUB _INBOX.JS.DGET' => [
                sprintf("HMSG _INBOX.JS.DGET.x 2 %d %d\r\n%s\r\n", strlen($errHdrs), strlen($errHdrs), $errHdrs),
            ],
        ];
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($subjectsReply), $subjectsReply),
            sprintf("MSG _INBOX.b 1 %d\r\n%s\r\n", strlen($emptyPage), $emptyPage),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 300), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->keyValue('cfg')->getAll()->await();
            self::fail('a non-503 batched Direct Get error must fail getAll()');
        } catch (JetStreamException $e) {
            self::assertSame(500, $e->getCode());
            self::assertSame('direct get batch exploded', $e->getMessage());
        }

        // No fallback was attempted after the hard failure.
        self::assertStringNotContainsString('"last_by_subj"', implode('', $transport->writes));
    }

    /**
     * getAll() per-subject fan-out: when the per-key Direct Get answers 503 and the leader
     * STREAM.MSG.GET fallback reports 404 (no record), the key is simply OMITTED - a null fallback
     * entry must be handled null-safely, not dereferenced.
     */
    public function testGetAllFanOutLeaderFallback404OmitsKey(): void
    {
        $subjectsReply = '{"state":{"subjects":{"$KV.cfg.gone":1}}}';
        $emptyPage = '{"state":{"subjects":{}}}';
        $missing = '{"error":{"code":404,"description":"no message found"}}';

        $transport = new FakeTransport([
            // 2.10 server: no batched Direct Get, the fan-out runs directly.
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($subjectsReply), $subjectsReply),
            sprintf("MSG _INBOX.b 1 %d\r\n%s\r\n", strlen($emptyPage), $emptyPage),
            $this->kvDirectStatus(1, 503, 'No Responders'),                          // per-key Direct Get -> 503
            sprintf("MSG _INBOX.c 1 %d\r\n%s\r\n", strlen($missing), $missing),      // leader fallback -> 404
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        // The null fallback entry must be handled NULL-SAFELY: escalate any PHP warning (e.g.
        // "Attempt to read property on null" from a non-null-safe dereference) to a failure -
        // PHP 8 only warns on a null property read, so the result alone cannot pin the contract.
        set_error_handler(static function (int $errno, string $errstr): bool {
            throw new \ErrorException($errstr, 0, $errno);
        }, E_WARNING);
        try {
            $all = $client->jetStream()->keyValue('cfg')->getAll()->await();
        } finally {
            restore_error_handler();
        }

        self::assertSame([], $all, 'a key whose leader fallback 404s is omitted, not dereferenced');
    }

    /**
     * history() with revisions arriving only DURING the bounded wait loop (not during the
     * subscribe) still completes: the stall clock measures REAL elapsed nanoseconds against the
     * caller's bound, so a healthy replay that keeps arriving is never failed.
     */
    public function testHistoryCompletesWhenRevisionsArriveDuringTheWaitLoop(): void
    {
        $createReply = '{"stream_name":"KV_cfg","name":"HIST","num_pending":1,"config":{"deliver_subject":"dlv","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        // The lone revision lands only after ~50 ms, well inside the replay's wait loop.
        EventLoop::delay(0.05, static function () use ($transport): void {
            $transport->pushReadChunk("MSG \$KV.cfg.theme 2 \$JS.ACK.KV_cfg.HIST.1.5.1.0.0 4\r\nblue\r\n");
        });

        $entries = $client->jetStream()->keyValue('cfg')->history('theme')->await();

        self::assertCount(1, $entries);
        self::assertSame('blue', $entries[0]->value);
        self::assertSame('PUT', $entries[0]->operation);
        self::assertSame(5, $entries[0]->revision);
    }

    /**
     * keys() twin of the mid-loop arrival test above: a headers-only record landing only during the
     * wait loop is still enumerated and the replay completes within the (real-time) progress bound.
     */
    public function testKeysCompleteWhenRecordsArriveDuringTheWaitLoop(): void
    {
        $consumerReply = '{"stream_name":"KV_cfg","name":"KEYS","num_pending":1,'
            . '"config":{"deliver_subject":"_INBOX.KV.KEYS.x","ack_policy":"none",'
            . '"deliver_policy":"last_per_subject","headers_only":true}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($consumerReply), $consumerReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        EventLoop::delay(0.05, static function () use ($transport): void {
            $transport->pushReadChunk("MSG \$KV.cfg.alpha 2 \$JS.ACK.KV_cfg.KEYS.1.6.1.0.0 0\r\n\r\n");
        });

        self::assertSame(['alpha'], $client->jetStream()->keyValue('cfg')->keys()->await());
        self::assertStringContainsString("UNSUB 2\r\n", implode('', $transport->writes));
    }

    /**
     * The other half of the failed-create rule (issues/3rd-round): prefixes the server DID confirm
     * (via an earlier successful mirror create) must SURVIVE a later failed create() - the reset is
     * atomic with the re-apply AFTER the successful await, so a rejected re-create cannot blind a
     * working mirror handle (reads at the wrong subjects, writes into a stream that ingests
     * nothing). Complements testFailedMirrorCreateLeavesHandleOnPlainBucketSubjects, which pins the
     * fresh-handle side.
     */
    public function testFailedRecreateKeepsConfirmedMirrorPrefixes(): void
    {
        $createReply = '{"config":{"name":"KV_dst"}}';
        $createError = '{"error":{"code":400,"err_code":10058,"description":"stream name already in use with a different configuration"}}';
        $putAck = '{"stream":"KV_src","seq":3}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxServer($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),    // create #1 -> confirmed
            sprintf("MSG _INBOX.b 1 %d\r\n%s\r\n", strlen($createError), $createError),    // create #2 -> rejected
            sprintf("MSG _INBOX.c 1 %d\r\n%s\r\n", strlen($putAck), $putAck),              // put ack
            $this->kvDirectReply('$KV.src.cache', 'warm', 3, 1),                           // Direct Get
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $kv = $client->jetStream()->keyValue('dst');

        // The mirror is confirmed server-side: the handle fronts it.
        $kv->create(['mirror' => ['bucket' => 'src', 'domain' => 'HUB']])->await();

        // A later ensure/reconcile re-create is rejected - the handle must keep the CONFIRMED routing.
        try {
            $kv->create(['mirror' => ['bucket' => 'src', 'domain' => 'HUB']])->await();
            self::fail('the rejected re-create must surface as an error');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('already in use', $e->getMessage());
        }

        $kv->put('cache', 'warm')->await();
        $entry = $kv->get('cache')->await();

        self::assertNotNull($entry);
        self::assertSame('warm', $entry->value);

        $writes = implode('', $transport->writes);
        // Writes still cross domains through the external API prefix to the origin subject.
        self::assertStringContainsString('PUB $JS.HUB.API.$KV.src.cache ', $writes);
        self::assertStringNotContainsString('PUB $KV.dst.cache', $writes, 'a failed re-create must not strand writes on the plain bucket subject');
        // Reads still hit the LOCAL mirror stream under the ORIGIN's subject.
        self::assertStringContainsString('"last_by_subj":"$KV.src.cache"', $writes);
        self::assertStringNotContainsString('"last_by_subj":"$KV.dst.cache"', $writes);
    }
}
