<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\ObjectStore\ObjectData;
use IDCT\NATS\JetStream\ObjectStore\ObjectInfo;
use IDCT\NATS\JetStream\ObjectStore\ObjectStoreBucket;
use IDCT\NATS\JetStream\ObjectStore\ObjectStoreWatchOptions;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

final class ObjectStoreBucketTest extends TestCase
{
    /** URL-safe base64 (with padding), matching the official Object Store meta-subject encoding. */
    private function encodeName(string $name): string
    {
        return strtr(base64_encode($name), '+/', '-_');
    }

    private function digestOf(string $data): string
    {
        return 'SHA-256=' . strtr(base64_encode(hash('sha256', $data, true)), '+/', '-_');
    }

    /** @param array<string,mixed> $extra */
    private function metaGetResponse(string $name, array $extra): string
    {
        $meta = array_merge([
            'name' => $name,
            'bucket' => 'assets',
            'mtime' => '2030-01-01T00:00:00Z',
            'deleted' => false,
        ], $extra);

        return (string) json_encode([
            'message' => [
                'subject' => '$O.assets.M.' . $this->encodeName($name),
                'seq' => 2,
                'data' => base64_encode((string) json_encode($meta, JSON_THROW_ON_ERROR)),
            ],
        ], JSON_THROW_ON_ERROR);
    }

    private function notFound(): string
    {
        return '{"error":{"code":404,"description":"message not found"}}';
    }

    /**
     * Builds a Direct Get reply (HMSG) whose body is the raw meta JSON, as list() now reads it.
     *
     * @param array<string,mixed> $extra
     */
    private function directMetaReply(string $name, array $extra, int $sid): string
    {
        $meta = array_merge([
            'name' => $name,
            'bucket' => 'assets',
            'mtime' => '2030-01-01T00:00:00Z',
            'deleted' => false,
        ], $extra);
        $body = (string) json_encode($meta, JSON_THROW_ON_ERROR);
        $hdrs = "NATS/1.0\r\nNats-Stream: OBJ_assets\r\nNats-Subject: \$O.assets.M." . $this->encodeName($name) . "\r\nNats-Sequence: 2\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s%s\r\n", $sid, $h, $h + strlen($body), $hdrs, $body);
    }

    /** Builds a Direct Get reply (HMSG) for a single object chunk (raw bytes body on the NUID subject). */
    private function directChunkReply(string $nuid, string $payload, int $sid): string
    {
        $hdrs = "NATS/1.0\r\nNats-Stream: OBJ_assets\r\nNats-Subject: \$O.assets.C.{$nuid}\r\nNats-Sequence: 3\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s%s\r\n", $sid, $h, $h + strlen($payload), $hdrs, $payload);
    }

    /**
     * Converts a metaGetResponse() STREAM.MSG.GET envelope into the equivalent Direct Get reply
     * (HMSG with the raw meta JSON as the body), as info()/get()/getToCallback() now read it.
     */
    private function directReplyFromEnvelope(string $envelope, int $sid): string
    {
        /** @var array{message: array{seq?: int, data?: string}} $decoded */
        $decoded = json_decode($envelope, true, 512, JSON_THROW_ON_ERROR);
        $metaJson = (string) base64_decode((string) ($decoded['message']['data'] ?? ''), true);
        $seq = (int) ($decoded['message']['seq'] ?? 1);
        $hdrs = "NATS/1.0\r\nNats-Stream: OBJ_assets\r\nNats-Sequence: {$seq}\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s%s\r\n", $sid, $h, $h + strlen($metaJson), $hdrs, $metaJson);
    }

    /** Builds a Direct Get status-only reply (HMSG) such as a 404 miss or a non-404 error. */
    private function directStatusReply(int $sid, int $code, string $description): string
    {
        $hdrs = "NATS/1.0 {$code} {$description}\r\nStatus: {$code}\r\nDescription: {$description}\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s\r\n", $sid, $h, $h, $hdrs);
    }

    private function pubAck(int $seq): string
    {
        return sprintf('{"stream":"OBJ_assets","seq":%d,"duplicate":false}', $seq);
    }

    /**
     * Post-#118 the request inbox is MUXED: request()/requestWithHeaders() no longer subscribe a fresh
     * inbox per call; instead ONE long-lived wildcard "<base>.*" (sid 1) serves every reply, and each
     * request publishes reply-to "<base>.<token>" and routes by that token. A pre-seeded fixed-subject
     * "MSG/HMSG _INBOX.x <sid>" therefore no longer reaches the waiting request and it times out.
     *
     * Every Object Store operation that awaits a reply - JS publish acks (put/putStream chunk+meta),
     * Direct Get (info/get/list per-subject), STREAM.INFO/PURGE/CREATE/DELETE, CONSUMER.CREATE/DELETE -
     * funnels through request(), so their replies are mux replies. This installs a dynamic responder
     * that learns the mux base from the wildcard SUB and, for each request PUB/HPUB (a publish whose
     * reply-to lives under that base), pops the next reply frame FIFO and re-emits it on the CAPTURED
     * reply-to with the mux sid (1). Frames are delivered in the order requests are written, matching
     * the request/SID order the tests already encoded.
     *
     * Only request replies belong here; subscription deliveries (a pull fetch inbox, a batched Direct
     * Get inbox, a watch push-consumer deliver subject) are NOT mux replies - enqueue those on their SUB
     * via FakeTransport::$enqueueOnWriteContaining so they arrive with the sid the server assigned.
     *
     * @param list<string> $frames Pre-built MSG/HMSG reply frames; only their subject+sid are rewritten.
     */
    private function muxReplies(FakeTransport $transport, array $frames): void
    {
        $queue = $frames;
        $base = null;

        $transport->onWrite = static function (string $bytes) use (&$queue, &$base): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            if (str_starts_with($head, 'SUB ')) {
                // Learn the mux base from the wildcard registration "SUB <base>.* <sid>".
                $subject = explode(' ', $head)[1] ?? '';
                if (str_starts_with($subject, '_INBOX.') && str_ends_with($subject, '.*')) {
                    $base = substr($subject, 0, -1); // strip the trailing "*" -> "_INBOX.<hex>."
                }

                return [];
            }

            if ($base === null || (!str_starts_with($head, 'PUB ') && !str_starts_with($head, 'HPUB '))) {
                return [];
            }

            // PUB <subject> <replyTo> <len>  |  HPUB <subject> <replyTo> <hdrLen> <totLen>
            $replyTo = explode(' ', $head)[2] ?? '';
            if (!str_starts_with($replyTo, $base) || $queue === []) {
                // A plain publish, or a subscription's own inbox (fetch/direct-get) - not a mux request.
                return [];
            }

            return [self::rewriteReplyFrame(array_shift($queue), $replyTo)];
        };
    }

    /**
     * Rewrites a pre-built MSG/HMSG reply frame's subject and sid to the request's captured mux reply-to
     * and the mux sid (1), preserving the payload/header bytes and declared lengths verbatim so the
     * reply's content is delivered unchanged - only its addressing is corrected for the mux inbox.
     */
    private static function rewriteReplyFrame(string $frame, string $replyTo): string
    {
        $pos = strpos($frame, "\r\n");
        self::assertNotFalse($pos, 'reply frame must have a head line');

        $tokens = explode(' ', substr($frame, 0, $pos));
        $tokens[1] = $replyTo; // subject
        $tokens[2] = '1';      // mux sid

        return implode(' ', $tokens) . substr($frame, $pos);
    }

    /**
     * Verifies Object Store bucket create maps to a stream with chunk+meta subjects and rollup.
     */
    public function testBucketCreateAndDelete(): void
    {
        $createPayload = '{"config":{"name":"OBJ_assets","subjects":["$O.assets.C.>","$O.assets.M.>"]}}';
        $deletePayload = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($deletePayload), $deletePayload),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $bucket = $client->jetStream()->objectStore('assets');
        $created = $bucket->create()->await();
        $deleted = $bucket->deleteBucket()->await();

        self::assertSame('OBJ_assets', $created->name);
        self::assertTrue($deleted);
        self::assertStringContainsString('$JS.API.STREAM.CREATE.OBJ_assets', $transport->writes[3]);
        self::assertStringContainsString('"allow_rollup_hdrs":true', $transport->writes[3]);
        // Post-#118 the mux inbox has one shared "SUB <base>.* 1" and NO per-request UNSUB, so the
        // second request's PUB is writes[4] (was writes[6] under the per-request SUB+UNSUB scheme).
        self::assertStringContainsString('$JS.API.STREAM.DELETE.OBJ_assets', $transport->writes[4]);
    }

    /**
     * Verifies put writes chunks under a NUID subject and rollup meta under the encoded name subject.
     */
    public function testPutUsesEncodedMetaSubjectAndNuidChunks(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // 1) existing-object lookup -> not found
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            // 2) chunk publish ack
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            // 3) meta publish ack
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $stored = $client->jetStream()->objectStore('assets')
            ->put('logo.txt', 'hello', ['content-type' => 'text/plain'])->await();

        self::assertInstanceOf(ObjectInfo::class, $stored);
        self::assertSame('logo.txt', $stored->name);
        self::assertSame(5, $stored->size);
        self::assertSame(1, $stored->chunks);
        self::assertNotSame('', $stored->nuid);
        self::assertSame($this->digestOf('hello'), $stored->digest);

        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('PUB $O.assets.C.' . $stored->nuid . ' ', $writes);
        self::assertStringContainsString('HPUB $O.assets.M.' . $this->encodeName('logo.txt') . ' ', $writes);
        self::assertStringContainsString('Nats-Rollup:sub', $writes);
    }

    public function testPutOmitsEmptyMetadataSoOfficialClientsCanRead(): void
    {
        // #109: empty metadata must NOT serialize as a JSON array ("metadata":[]). The official Go client
        // unmarshals metadata into map[string]string and rejects an array with
        // "object-store meta information invalid"; it omits the field when empty. Match that so an object
        // stored with default metadata stays readable by the nats CLI / nats.go.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->objectStore('assets')->put('logo.txt', 'hello')->await();

        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('HPUB $O.assets.M.' . $this->encodeName('logo.txt') . ' ', $writes);
        self::assertStringNotContainsString('"metadata":[]', $writes);
        // Omitted entirely when empty, exactly like the official client.
        self::assertStringNotContainsString('"metadata":', $writes);
    }

    public function testPutSerializesNonEmptyMetadataAsJsonObject(): void
    {
        // #109: a populated metadata map must serialize as a JSON object so other clients can read it.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->objectStore('assets')->put('logo.txt', 'hello', ['team' => 'brand'])->await();

        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('"metadata":{"team":"brand"}', $writes);
    }

    public function testPutStreamReChunksAndComputesDigestIncrementally(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // lookup (concurrent), two chunk acks, meta ack - all mux request replies, FIFO by write order.
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        // 3-byte chunks: a single 'hello' block re-chunks to 'hel' + 'lo' (2 chunks).
        $store = new ObjectStoreBucket($client, $client->jetStream(), 'assets', 3);
        $blocks = ['hello'];
        $index = 0;
        $stored = $store->putStream('big.txt', static function () use (&$index, $blocks): ?string {
            return $blocks[$index++] ?? null;
        })->await();

        self::assertSame(5, $stored->size);
        self::assertSame(2, $stored->chunks);
        self::assertSame($this->digestOf('hello'), $stored->digest);

        $writes = implode('||', $transport->writes);
        self::assertSame(2, substr_count($writes, 'PUB $O.assets.C.' . $stored->nuid . ' '));
        self::assertStringContainsString('HPUB $O.assets.M.' . $this->encodeName('big.txt'), $writes);
    }

    public function testPutStreamReChunksLargeBlockAcrossManyChunks(): void
    {
        // A single producer block larger than chunkSize must split into multiple chunks via the
        // offset loop (no O(n^2) tail recopy), preserving order and digest.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),  // lookup
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),       // chunk1
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),       // chunk2
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),       // chunk3
            sprintf("MSG _INBOX.e 5 %d\r\n%s\r\n", strlen($this->pubAck(4)), $this->pubAck(4)),       // meta
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        // 2-byte chunks: one 'abcdef' block re-chunks to 'ab' + 'cd' + 'ef' (3 chunks).
        $store = new ObjectStoreBucket($client, $client->jetStream(), 'assets', 2);
        $blocks = ['abcdef'];
        $index = 0;
        $stored = $store->putStream('big.bin', static function () use (&$index, $blocks): ?string {
            return $blocks[$index++] ?? null;
        })->await();

        self::assertSame(6, $stored->size);
        self::assertSame(3, $stored->chunks);
        self::assertSame($this->digestOf('abcdef'), $stored->digest);
        self::assertSame(3, substr_count(implode('||', $transport->writes), 'PUB $O.assets.C.' . $stored->nuid . ' '));
    }

    public function testConstructorRejectsNonPositiveChunkSize(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('chunk size');

        new ObjectStoreBucket($client, $client->jetStream(), 'assets', 0);
    }

    /**
     * Verifies an empty object stores zero chunks and publishes no chunk message (only meta).
     */
    public function testPutStoresEmptyObjectWithZeroChunks(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // With no chunk to await first, the meta publish is issued before the concurrent lookup, so
        // the mux replies arrive meta-ack first, then the not-found lookup (FIFO by write order).
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $stored = $client->jetStream()->objectStore('assets')->put('empty.txt', '')->await();

        self::assertSame(0, $stored->size);
        self::assertSame(0, $stored->chunks);

        $writes = implode('||', $transport->writes);
        // No chunk was published for the empty object; only the meta record.
        self::assertStringNotContainsString('PUB $O.assets.C.', $writes);
        self::assertStringContainsString('HPUB $O.assets.M.' . $this->encodeName('empty.txt') . ' ', $writes);
    }

    /**
     * Verifies an overwrite purges the previous revision's chunk subject.
     */
    public function testPutOverwritePurgesPreviousChunks(): void
    {
        $oldNuid = 'oldnuid0001';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // 1) existing-object lookup -> returns previous meta with old nuid
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->metaGetResponse('logo.txt', ['nuid' => $oldNuid, 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')])), $this->metaGetResponse('logo.txt', ['nuid' => $oldNuid, 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')])),
            // 2) chunk publish ack
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),
            // 3) meta publish ack
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(4)), $this->pubAck(4)),
            // 4) purge old chunks ack
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen('{"success":true,"purged":1}'), '{"success":true,"purged":1}'),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->objectStore('assets')->put('logo.txt', 'world')->await();

        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('$JS.API.STREAM.PURGE.OBJ_assets', $writes);
        self::assertStringContainsString('$O.assets.C.' . $oldNuid, $writes);
    }

    /**
     * Verifies get downloads chunks via NUID subject and verifies the content digest.
     */
    public function testGetReturnsPayloadAndVerifiesDigest(): void
    {
        $nuid = 'nuidget0001';
        $meta = $this->metaGetResponse('doc.txt', [
            'nuid' => $nuid,
            'size' => 5,
            'chunks' => 1,
            'digest' => $this->digestOf('hello'),
        ]);
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directReplyFromEnvelope($meta, 1),     // info() (Direct Get)
            $this->directChunkReply($nuid, 'hello', 2),   // single-chunk fast path (Direct Get on the NUID subject)
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $fetched = $client->jetStream()->objectStore('assets')->get('doc.txt')->await();

        self::assertInstanceOf(ObjectData::class, $fetched);
        self::assertSame('hello', $fetched->data);
        self::assertSame('doc.txt', $fetched->info->name);
        self::assertSame($nuid, $fetched->info->nuid);

        // The single chunk is fetched via Direct Get on its NUID subject (no ephemeral consumer).
        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('"last_by_subj":"$O.assets.C.' . $nuid . '"', $writes);
        self::assertStringNotContainsString('CONSUMER.CREATE', $writes);
    }

    public function testGetVerifiesUnpaddedBase64UrlDigest(): void
    {
        $nuid = 'nuidunpad01';
        // A non-Go client may store the digest as UNPADDED base64url; byte comparison must still
        // verify it against our padded computation instead of throwing a spurious mismatch.
        $unpadded = rtrim($this->digestOf('hello'), '=');
        self::assertNotSame($unpadded, $this->digestOf('hello')); // guard: the fixture is actually unpadded

        $meta = $this->metaGetResponse('doc.txt', ['nuid' => $nuid, 'size' => 5, 'chunks' => 1, 'digest' => $unpadded]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directReplyFromEnvelope($meta, 1),
            $this->directChunkReply($nuid, 'hello', 2),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $fetched = $client->jetStream()->objectStore('assets')->get('doc.txt')->await();

        self::assertInstanceOf(ObjectData::class, $fetched);
        self::assertSame('hello', $fetched->data);
    }

    /**
     * Verifies get throws on a content digest mismatch.
     */
    public function testGetThrowsOnDigestMismatch(): void
    {
        $nuid = 'nuidbad0001';
        $meta = $this->metaGetResponse('doc.txt', [
            'nuid' => $nuid,
            'size' => 11,
            'chunks' => 1,
            'digest' => $this->digestOf('hello world'),
        ]);
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directReplyFromEnvelope($meta, 1),
            $this->directChunkReply($nuid, 'CORRUPTED!!', 2), // body does not match the metadata digest
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Object digest mismatch');
        $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
    }

    /**
     * Verifies getToCallback invokes the callback exactly once for a single-chunk object and
     * streams that chunk's payload to the handler. (Multi-chunk per-chunk streaming is proven by
     * testGetToCallbackInvokesCallbackOncePerChunk.)
     */
    public function testGetToCallbackInvokesCallbackOnceForSingleChunkObject(): void
    {
        $nuid = 'nuidcb00001';
        $meta = $this->metaGetResponse('report.txt', [
            'nuid' => $nuid,
            'size' => 5,
            'chunks' => 1,
            'digest' => $this->digestOf('hello'),
        ]);
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directReplyFromEnvelope($meta, 1),
            $this->directChunkReply($nuid, 'hello', 2),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $captured = '';
        $calls = 0;
        $info = $client->jetStream()->objectStore('assets')->getToCallback(
            'report.txt',
            static function (string $chunk) use (&$captured, &$calls): void {
                $captured .= $chunk;
                $calls++;
            },
        )->await();

        self::assertSame('hello', $captured);
        self::assertSame(1, $calls);
        self::assertNotNull($info);
        self::assertSame('report.txt', $info->name);
    }

    /**
     * Verifies getToCallback invokes the callback once per stored chunk (in order) for a multi-chunk
     * object, rather than buffering the whole object and calling back once.
     */
    public function testGetToCallbackInvokesCallbackOncePerChunk(): void
    {
        $nuid = 'nuidcbmulti';
        $chunks = ['abc', 'def', 'ghi'];
        $assembled = implode('', $chunks);
        $meta = $this->metaGetResponse('multi.txt', [
            'nuid' => $nuid,
            'size' => strlen($assembled),
            'chunks' => count($chunks),
            'digest' => $this->digestOf($assembled),
        ]);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPHCBM","config":{"ack_policy":"explicit"}}';
        $deleteConsumer = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // The three chunks arrive on the pull fetch inbox (sid 2: mux SUB=1, ephemeral consumer create
        // is a mux request, fetch SUB=2), not on the mux request inbox - enqueue on the fetch SUB.
        $transport->enqueueOnWriteContaining = [
            '_INBOX.JS.FETCH' => [
                "MSG _INBOX.JS.FETCH.c 2 3\r\nabc\r\n",
                "MSG _INBOX.JS.FETCH.c 2 3\r\ndef\r\n",
                "MSG _INBOX.JS.FETCH.c 2 3\r\nghi\r\n",
            ],
        ];
        $this->muxReplies($transport, [
            $this->directReplyFromEnvelope($meta, 1),                                     // info() Direct Get
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),         // CONSUMER.CREATE
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer), // CONSUMER.DELETE
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $captured = [];
        $info = $client->jetStream()->objectStore('assets')->getToCallback(
            'multi.txt',
            static function (string $chunk) use (&$captured): void {
                $captured[] = $chunk;
            },
        )->await();

        // One invocation per stored chunk, in order - the whole object is never assembled.
        self::assertSame(['abc', 'def', 'ghi'], $captured);
        self::assertNotNull($info);
        self::assertSame('multi.txt', $info->name);
        self::assertSame(3, $info->chunks);
    }

    /**
     * Verifies getToCallback returns null for a deleted object without invoking the callback.
     */
    public function testGetToCallbackReturnsNullForDeletedObjects(): void
    {
        $meta = $this->metaGetResponse('gone.txt', ['nuid' => '', 'size' => 0, 'chunks' => 0, 'digest' => '', 'deleted' => true]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directReplyFromEnvelope($meta, 1),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $called = false;
        $info = $client->jetStream()->objectStore('assets')->getToCallback(
            'gone.txt',
            static function (string $chunk) use (&$called): void {
                $called = true;
            },
        )->await();

        self::assertFalse($called);
        // A deleted object reads as not-available; the tombstone stays visible via info().
        self::assertNull($info);
    }

    /**
     * Verifies delete writes a tombstone via the encoded meta subject and purges chunks.
     */
    public function testDeleteWritesTombstoneAndPurgesChunks(): void
    {
        $oldNuid = 'delnuid0001';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // The tombstone publish is issued before the concurrent lookup (no chunk to await first), then
        // the purge; mux replies arrive in that write order.
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->pubAck(7)), $this->pubAck(7)),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->metaGetResponse('logo.txt', ['nuid' => $oldNuid, 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')])), $this->metaGetResponse('logo.txt', ['nuid' => $oldNuid, 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')])),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen('{"success":true,"purged":1}'), '{"success":true,"purged":1}'),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $deleted = $client->jetStream()->objectStore('assets')->delete('logo.txt')->await();

        self::assertTrue($deleted->deleted);
        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('HPUB $O.assets.M.' . $this->encodeName('logo.txt') . ' ', $writes);
        self::assertStringContainsString('$JS.API.STREAM.PURGE.OBJ_assets', $writes);
        self::assertStringContainsString('$O.assets.C.' . $oldNuid, $writes);
    }

    /**
     * Verifies put() stores a description and surfaces it on ObjectInfo (#58).
     */
    public function testPutWithDescription(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),     // lookupExisting -> none
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),       // chunk publish ack
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),       // meta publish ack
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $info = $client->jetStream()->objectStore('assets')->put('doc.txt', 'hello', [], 'A friendly doc')->await();

        self::assertSame('A friendly doc', $info->description);
        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('"description":"A friendly doc"', $writes);

        $client->disconnect()->await();
    }

    /**
     * Verifies get() transparently follows an object link to the target's content (#59).
     */
    public function testGetFollowsObjectLink(): void
    {
        $nuid = 'nuidlink001';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // info('shortcut') -> link meta pointing at doc.txt (Direct Get).
            $this->directMetaReply('shortcut', ['options' => ['link' => ['bucket' => 'assets', 'name' => 'doc.txt']]], 1),
            // info('doc.txt') -> the real object meta.
            $this->directMetaReply('doc.txt', ['nuid' => $nuid, 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')], 2),
            // single-chunk Direct Get on the target NUID subject.
            $this->directChunkReply($nuid, 'hello', 3),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $fetched = $client->jetStream()->objectStore('assets')->get('shortcut')->await();

        self::assertInstanceOf(ObjectData::class, $fetched);
        self::assertSame('hello', $fetched->data);
        self::assertSame('doc.txt', $fetched->info->name);

        $client->disconnect()->await();
    }

    /**
     * Verifies create() accepts a typed ObjectStoreConfig and maps it to stream config (#39).
     */
    public function testCreateWithTypedConfig(): void
    {
        $reply = '{"config":{"name":"OBJ_assets"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->objectStore('assets')->create(
            new \IDCT\NATS\JetStream\ObjectStore\ObjectStoreConfig(ttlSeconds: 60, maxBytes: 4096, storage: 'memory', replicas: 3, compression: 's2'),
        )->await();

        $create = $transport->writes[3];
        self::assertStringContainsString('$JS.API.STREAM.CREATE.OBJ_assets', $create);
        self::assertStringContainsString('"max_age":60000000000', $create);
        self::assertStringContainsString('"max_bytes":4096', $create);
        self::assertStringContainsString('"storage":"memory"', $create);
        self::assertStringContainsString('"num_replicas":3', $create);
        self::assertStringContainsString('"compression":"s2"', $create);
    }

    /**
     * Verifies seal() updates the backing stream with sealed=true, preserving existing config (#38).
     */
    public function testSeal(): void
    {
        $info = '{"config":{"name":"OBJ_assets","subjects":["$O.assets.>"],"max_bytes":1000}}';
        $updated = '{"config":{"name":"OBJ_assets","sealed":true}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($info), $info),       // STREAM.INFO
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($updated), $updated), // STREAM.UPDATE
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        self::assertTrue($client->jetStream()->objectStore('assets')->seal()->await());

        // seal() issues STREAM.INFO then STREAM.UPDATE; post-#118 (one shared mux SUB, no per-request
        // UNSUB) the second request's PUB is writes[4] (was writes[6] under the old per-request scheme).
        self::assertStringContainsString('$JS.API.STREAM.UPDATE.OBJ_assets', $transport->writes[4]);
        self::assertStringContainsString('"sealed":true', $transport->writes[4]);
        self::assertStringContainsString('"max_bytes":1000', $transport->writes[4]);
    }

    /**
     * Verifies addLink() writes a link meta record pointing at a target object (#48).
     */
    public function testAddLink(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // Name-free check (STREAM.MSG.GET): no existing object under "shortcut".
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            // Target check (Direct Get): "real.bin" exists and is a live stored object.
            $this->directMetaReply('real.bin', ['nuid' => 'n1', 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('abc')], 2),
            sprintf("MSG _INBOX.b 3 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $link = $client->jetStream()->objectStore('assets')->addLink('shortcut', 'real.bin')->await();

        self::assertTrue($link->isLink());
        self::assertSame(['bucket' => 'assets', 'name' => 'real.bin'], $link->link);

        $write = $transport->writes[5];
        self::assertStringContainsString('HPUB $O.assets.M.' . $this->encodeName('shortcut') . ' ', $write);
        self::assertStringContainsString('"link":{"bucket":"assets","name":"real.bin"}', $write);
    }

    /**
     * addLink() must refuse to overwrite a LIVE object (nats.go ErrObjectAlreadyExists): the
     * rollup meta publish would make the object's content unreachable in one call and orphan its
     * chunks forever (no purge runs on the link path). Pre-fix this silently succeeded.
     */
    public function testAddLinkRejectsExistingLiveObject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // "shortcut" already holds a live stored object (STREAM.MSG.GET envelope).
            sprintf(
                "MSG _INBOX.a 1 %d\r\n%s\r\n",
                strlen($this->metaGetResponse('shortcut', ['nuid' => 'nLive', 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('abc')])),
                $this->metaGetResponse('shortcut', ['nuid' => 'nLive', 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('abc')]),
            ),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->objectStore('assets')->addLink('shortcut', 'real.bin')->await();
            self::fail('a link must not silently replace a live object');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('already exists', $e->getMessage());
        }

        self::assertStringNotContainsString('HPUB $O.assets.M.', implode('', $transport->writes), 'no meta record may be written');
    }

    /**
     * Re-pointing an existing LINK is allowed (nats.go parity: `if !einfo.isLink() { return
     * ErrObjectAlreadyExists }`): a link stores no chunks, so nothing is orphaned - the standard
     * "latest"-alias flow must keep working.
     */
    public function testAddLinkAllowsRepointingAnExistingLink(): void
    {
        $existingLink = $this->metaGetResponse('latest', ['nuid' => '', 'options' => ['link' => ['bucket' => 'assets', 'name' => 'v1.bin']]]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // Name check: "latest" currently holds a LINK -> allowed to re-point.
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($existingLink), $existingLink),
            // Target check: v2.bin is a live stored object.
            $this->directMetaReply('v2.bin', ['nuid' => 'n2', 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('two')], 2),
            sprintf("MSG _INBOX.b 3 %d\r\n%s\r\n", strlen($this->pubAck(9)), $this->pubAck(9)),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $link = $client->jetStream()->objectStore('assets')->addLink('latest', 'v2.bin')->await();

        self::assertTrue($link->isLink());
        self::assertSame(['bucket' => 'assets', 'name' => 'v2.bin'], $link->link);
    }

    /**
     * addLink() must reject a deleted (or absent) target and a target that is itself a link -
     * both dangle by construction (nats.go parity).
     */
    public function testAddLinkRejectsDeletedTargetAndLinkTarget(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // Case 1: name free (STREAM.MSG.GET 404), target deleted (Direct Get).
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            $this->directMetaReply('gone.bin', ['nuid' => 'n1', 'deleted' => true], 2),
            // Case 2: name free, target is itself a link.
            sprintf("MSG _INBOX.b 3 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            $this->directMetaReply('alias.bin', ['nuid' => '', 'options' => ['link' => ['bucket' => 'assets', 'name' => 'real.bin']]], 4),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();
        $bucket = $client->jetStream()->objectStore('assets');

        try {
            $bucket->addLink('l1', 'gone.bin')->await();
            self::fail('a link to a deleted target must be rejected');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('deleted', $e->getMessage());
        }

        try {
            $bucket->addLink('l2', 'alias.bin')->await();
            self::fail('a link to another link must be rejected');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('itself a link', $e->getMessage());
        }
    }

    /**
     * A no-responders reply to the meta-record rollup publish (publishMeta, reached by every
     * put/link/updateMeta) must surface as JetStreamException(503) - the same taxonomy jsRequest()
     * produces - not a bare NatsException, so a JetStream-disabled server / unbound subject is
     * catchable as JetStreamException like every other JS path (#161).
     */
    public function testMetaPublishNormalizesNoRespondersTo503(): void
    {
        $status = "NATS/1.0 503\r\n\r\n";

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // addLink() issues one HPUB meta-publish request on the mux inbox; the server has no responder,
        // so it replies with a 503 no-responders status (echoed on the captured mux reply-to).
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            $this->directMetaReply('real.bin', ['nuid' => 'n1', 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('abc')], 2),
            'HMSG _INBOX.b 3 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionCode(503);
        $this->expectExceptionMessage('No JetStream responder');

        $client->jetStream()->objectStore('assets')->addLink('shortcut', 'real.bin')->await();
    }

    /**
     * Verifies addBucketLink() writes a link meta record pointing at a whole bucket (#48).
     */
    public function testAddBucketLink(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $link = $client->jetStream()->objectStore('assets')->addBucketLink('mirror', 'other-bucket')->await();

        self::assertTrue($link->isLink());
        self::assertSame(['bucket' => 'other-bucket'], $link->link);
        self::assertStringContainsString('"link":{"bucket":"other-bucket"}', $transport->writes[4]);
    }

    /**
     * Verifies updateMeta() renames an object without re-upload: preserves the NUID, writes the new
     * meta, and tombstones the old name without purging chunks (#28).
     */
    public function testUpdateMetaRenamesPreservingNuid(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // info('logo.txt') Direct Get -> existing object on nuid n1.
            $this->directMetaReply('logo.txt', ['nuid' => 'n1', 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old'), 'metadata' => ['team' => 'design']], 1),
            // info('brand.txt') clash check Direct Get -> 404 (target free).
            $this->directStatusReply(2, 404, 'Message Not Found'),
            // publishMeta('brand.txt') ack.
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(8)), $this->pubAck(8)),
            // publishMeta('logo.txt' tombstone) ack.
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($this->pubAck(9)), $this->pubAck(9)),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $info = $client->jetStream()->objectStore('assets')->updateMeta('logo.txt', 'brand.txt')->await();

        self::assertSame('brand.txt', $info->name);
        self::assertSame('n1', $info->nuid);     // chunks preserved by NUID, not re-uploaded
        self::assertSame(3, $info->size);

        $writes = implode('||', $transport->writes);
        // New meta written under the new name's encoded subject...
        self::assertStringContainsString('HPUB $O.assets.M.' . $this->encodeName('brand.txt') . ' ', $writes);
        // ...and the old name tombstoned (deleted:true) without any STREAM.PURGE of chunks.
        self::assertStringContainsString('HPUB $O.assets.M.' . $this->encodeName('logo.txt') . ' ', $writes);
        self::assertStringContainsString('"deleted":true', $writes);
        self::assertStringNotContainsString('$JS.API.STREAM.PURGE', $writes);
    }

    /**
     * Verifies updateMeta() replaces the metadata bag in place (no rename, no chunk churn) (#28).
     */
    public function testUpdateMetaReplacesMetadataInPlace(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directMetaReply('logo.txt', ['nuid' => 'n1', 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old'), 'metadata' => ['team' => 'design']], 1),
            sprintf("MSG _INBOX.c 2 %d\r\n%s\r\n", strlen($this->pubAck(8)), $this->pubAck(8)),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $info = $client->jetStream()->objectStore('assets')->updateMeta('logo.txt', null, ['team' => 'brand', 'owner' => 'x'])->await();

        self::assertSame('logo.txt', $info->name);
        self::assertSame(['team' => 'brand', 'owner' => 'x'], $info->metadata);

        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('HPUB $O.assets.M.' . $this->encodeName('logo.txt') . ' ', $writes);
        self::assertStringContainsString('"team":"brand"', $writes);
        // No tombstone / no second name involved.
        self::assertStringNotContainsString('"deleted":true', $writes);
    }

    /**
     * Verifies list enumerates meta subjects and reads the latest record per object.
     */
    public function testListEnumeratesMetaSubjects(): void
    {
        $logoEnc = $this->encodeName('logo.txt');
        $oldEnc = $this->encodeName('old.txt');

        $streamInfo = json_encode([
            'config' => ['name' => 'OBJ_assets'],
            'state' => [
                'messages' => 3,
                'last_seq' => 3,
                'subjects' => [
                    '$O.assets.M.' . $logoEnc => 1,
                    '$O.assets.M.' . $oldEnc => 1,
                ],
            ],
        ], JSON_THROW_ON_ERROR);

        $logoExtra = ['nuid' => 'n1', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')];
        $oldExtra = ['nuid' => '', 'size' => 0, 'chunks' => 0, 'digest' => '', 'deleted' => true];

        $emptyPage = '{"state":{"subjects":{}}}';

        // Pre-2.11 server: list() takes the per-subject Direct Get fan-out this test scripts (batched
        // multi_last Direct Get requires 2.11+ and is covered by the #110 batched-path tests).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // Pre-2.11 per-subject Direct Get fan-out, all mux requests: page 1 returns the subjects, the
        // empty page 2 terminates the loop, then a concurrent Direct Get per meta subject (logo, old,
        // in map order). list(includeDeleted: true) repeats the same sequence.
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen((string) $streamInfo), (string) $streamInfo),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($emptyPage), $emptyPage),
            $this->directMetaReply('logo.txt', $logoExtra, 3),
            $this->directMetaReply('old.txt', $oldExtra, 4),
            sprintf("MSG _INBOX.e 5 %d\r\n%s\r\n", strlen((string) $streamInfo), (string) $streamInfo),
            sprintf("MSG _INBOX.f 6 %d\r\n%s\r\n", strlen($emptyPage), $emptyPage),
            $this->directMetaReply('logo.txt', $logoExtra, 7),
            $this->directMetaReply('old.txt', $oldExtra, 8),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $bucket = $client->jetStream()->objectStore('assets');
        $active = $bucket->list()->await();
        $all = $bucket->list(includeDeleted: true)->await();

        self::assertCount(1, $active);
        self::assertSame('logo.txt', $active[0]->name);
        self::assertCount(2, $all);

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('"subjects_filter":"$O.assets.M.>"', $writes);
        self::assertStringContainsString('$JS.API.DIRECT.GET.OBJ_assets', $writes);
    }

    public function testListPaginatesAcrossSubjectPages(): void
    {
        // The subjects map is server-capped, so a large bucket is enumerated across pages. Here each
        // object arrives on its own page; list() must collect both (not truncate to the first page).
        $page1 = (string) json_encode(['state' => ['subjects' => ['$O.assets.M.' . $this->encodeName('a.txt') => 1]]], JSON_THROW_ON_ERROR);
        $page2 = (string) json_encode(['state' => ['subjects' => ['$O.assets.M.' . $this->encodeName('b.txt') => 1]]], JSON_THROW_ON_ERROR);
        $emptyPage = '{"state":{"subjects":{}}}';
        $extra = ['nuid' => 'n1', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')];

        // Pre-2.11 server: list() takes the per-subject Direct Get fan-out this test scripts (batched
        // multi_last Direct Get requires 2.11+ and is covered by the #110 batched-path tests).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($page1), $page1),           // page 1: a.txt
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($page2), $page2),           // page 2: b.txt
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($emptyPage), $emptyPage),   // page 3: empty -> stop
            $this->directMetaReply('a.txt', $extra, 4),
            $this->directMetaReply('b.txt', $extra, 5),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $objects = $client->jetStream()->objectStore('assets')->list()->await();

        // Both objects are returned (one came from page 2 - the old single-page code would miss it).
        self::assertCount(2, $objects);
        $names = array_map(static fn($o): string => $o->name, $objects);
        sort($names);
        self::assertSame(['a.txt', 'b.txt'], $names);

        // Verify three STREAM.INFO pages were requested at increasing offsets.
        $writes = implode('', $transport->writes);
        self::assertStringContainsString('"offset":0', $writes);
        self::assertStringContainsString('"offset":1', $writes);
        self::assertStringContainsString('"offset":2', $writes);
    }

    /**
     * Verifies list() on a 2.11+ server issues exactly ONE batched multi_last Direct Get for all
     * objects (instead of one Direct Get per object), returns the identical objects, and honours the
     * deleted filter (#110). The single-request assertion FAILS on the pre-#110 per-subject fan-out,
     * which writes one Direct Get PUB per object.
     */
    public function testListUsesSingleBatchedDirectGetOnModernServer(): void
    {
        $logoEnc = $this->encodeName('logo.txt');
        $docEnc = $this->encodeName('doc.txt');
        $streamInfo = (string) json_encode([
            'config' => ['name' => 'OBJ_assets'],
            'state' => ['subjects' => ['$O.assets.M.' . $logoEnc => 1, '$O.assets.M.' . $docEnc => 1]],
        ], JSON_THROW_ON_ERROR);
        $emptyPage = '{"state":{"subjects":{}}}';

        $logoMeta = (string) json_encode([
            'name' => 'logo.txt', 'bucket' => 'assets', 'mtime' => '2030-01-01T00:00:00Z', 'deleted' => false,
            'nuid' => 'n1', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello'),
        ], JSON_THROW_ON_ERROR);
        $docMeta = (string) json_encode([
            'name' => 'doc.txt', 'bucket' => 'assets', 'mtime' => '2030-01-01T00:00:00Z', 'deleted' => false,
            'nuid' => 'n2', 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('abc'),
        ], JSON_THROW_ON_ERROR);
        // One batched reply stream on a single inbox (sid 3); the last frame carries Nats-Num-Pending: 0.
        // TWO active objects so the batch result is not silently truncated to a single entry.
        $logoHdrs = "NATS/1.0\r\nNats-Stream: OBJ_assets\r\nNats-Subject: \$O.assets.M." . $logoEnc . "\r\nNats-Sequence: 3\r\n\r\n";
        $docHdrs = "NATS/1.0\r\nNats-Stream: OBJ_assets\r\nNats-Subject: \$O.assets.M." . $docEnc . "\r\nNats-Sequence: 4\r\nNats-Num-Pending: 0\r\n\r\n";
        $lh = strlen($logoHdrs);
        $dh = strlen($docHdrs);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // The batched multi_last Direct Get streams its replies on its OWN inbox (sid 2: mux SUB=1, the
        // two STREAM.INFO enumeration pages are mux requests, the batched Direct Get SUB=2), terminated
        // by the last frame's Nats-Num-Pending: 0. Enqueue those on the batched Direct Get SUB.
        $transport->enqueueOnWriteContaining = [
            '_INBOX.JS.DGET' => [
                sprintf("HMSG \$O.assets.M.%s 2 %d %d\r\n%s%s\r\n", $logoEnc, $lh, $lh + strlen($logoMeta), $logoHdrs, $logoMeta), // batched: logo (active)
                sprintf("HMSG \$O.assets.M.%s 2 %d %d\r\n%s%s\r\n", $docEnc, $dh, $dh + strlen($docMeta), $docHdrs, $docMeta),     // batched: doc (active, terminator)
            ],
        ];
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo),   // metaSubjects() page 1
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($emptyPage), $emptyPage),      // terminator page
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $objects = $client->jetStream()->objectStore('assets')->list()->await();

        // Identical returned data: BOTH active objects are listed, in batch-reply order.
        self::assertCount(2, $objects);
        self::assertSame('logo.txt', $objects[0]->name);
        self::assertSame(3, $objects[0]->revision);
        self::assertSame('doc.txt', $objects[1]->name);
        self::assertSame(4, $objects[1]->revision);

        // The win: exactly ONE Direct Get request for all objects (the batched multi_last).
        $directGetPubs = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_starts_with($w, 'PUB $JS.API.DIRECT.GET.OBJ_assets'),
        ));
        self::assertCount(1, $directGetPubs);
        self::assertStringContainsString('"multi_last":["$O.assets.M.' . $logoEnc . '","$O.assets.M.' . $docEnc . '"]', $directGetPubs[0]);
    }

    public function testInfoIncludesRevisionFromSequenceHeader(): void
    {
        // ObjectInfo::revision is the record's stream sequence, carried in the Direct Get Nats-Sequence
        // header (directReplyFromEnvelope encodes the envelope's seq=2).
        $meta = $this->metaGetResponse('doc.txt', ['nuid' => 'n1', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directReplyFromEnvelope($meta, 1),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $info = $client->jetStream()->objectStore('assets')->info('doc.txt')->await();

        self::assertNotNull($info);
        self::assertSame(2, $info->revision);
    }

    public function testInfoFallsBackToStreamMessageWhenDirectGetUnavailable(): void
    {
        // Direct Get -> 503 no-responders (allow_direct disabled / interop bucket); info() must fall
        // back to the leader STREAM.MSG.GET path and still return the metadata.
        $meta = $this->metaGetResponse('doc.txt', ['nuid' => 'n1', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directStatusReply(1, 503, 'No Responders'),                       // Direct Get -> 503
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($meta), $meta),            // STREAM.MSG.GET fallback
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $info = $client->jetStream()->objectStore('assets')->info('doc.txt')->await();

        self::assertNotNull($info);
        self::assertSame('doc.txt', $info->name);

        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('$JS.API.DIRECT.GET.OBJ_assets', $writes);
        self::assertStringContainsString('$JS.API.STREAM.MSG.GET.OBJ_assets', $writes);
    }

    /**
     * list() must survive a bucket without Direct Get (allow_direct disabled / legacy interop
     * bucket): the per-subject fan-out (pre-2.11 servers) falls back to the leader STREAM.MSG.GET
     * path like info() does, instead of failing the whole enumeration with the 503.
     */
    public function testListFallsBackToStreamMessageWhenDirectGetUnavailable(): void
    {
        $enc = $this->encodeName('doc.txt');
        $subjectsReply = sprintf('{"state":{"subjects":{"$O.assets.M.%s":1}}}', $enc);
        $meta = $this->metaGetResponse('doc.txt', ['nuid' => 'n1', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')]);

        $transport = new FakeTransport([
            // 2.10 server: no batched Direct Get -> the per-subject fan-out path under test.
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($subjectsReply), $subjectsReply),  // STREAM.INFO subjects page 1
            sprintf("MSG _INBOX.a2 1 %d\r\n%s\r\n", strlen('{"state":{"subjects":{}}}'), '{"state":{"subjects":{}}}'),  // page 2: empty -> pagination ends
            $this->directStatusReply(1, 503, 'No Responders'),                               // Direct Get -> 503
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($meta), $meta),                    // STREAM.MSG.GET fallback
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $objects = $client->jetStream()->objectStore('assets')->list()->await();

        self::assertCount(1, $objects, 'the enumeration must fall back per object, not fail on the 503');
        self::assertSame('doc.txt', $objects[0]->name);
        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('$JS.API.STREAM.MSG.GET.OBJ_assets', $writes);
    }

    /**
     * Verifies info returns null when JetStream reports the object metadata is not found.
     */
    public function testInfoReturnsNullWhenNotFound(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directStatusReply(1, 404, 'Message Not Found'),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        self::assertNull($client->jetStream()->objectStore('assets')->info('missing.txt')->await());
    }

    /**
     * Verifies watch parses metadata payloads and dispatches ObjectInfo updates.
     */
    public function testWatchDispatchesObjectInfo(): void
    {
        $enc = $this->encodeName('logo.txt');
        $metadata = json_encode([
            'name' => 'logo.txt',
            'bucket' => 'assets',
            'nuid' => 'wnuid1',
            'size' => 5,
            'chunks' => 1,
            'digest' => $this->digestOf('hello'),
            'mtime' => '2030-01-01T00:00:00Z',
            'deleted' => false,
        ], JSON_THROW_ON_ERROR);

        // watch() runs over a JetStream push consumer: create it (sid 1), then the update is delivered
        // on the deliver inbox (sid 2) carrying its stream sequence in the $JS.ACK reply -> revision.
        $createReply = '{"stream_name":"OBJ_assets","name":"OBJWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // The metadata update is delivered on the push consumer's deliver subscription (sid 2: mux
        // SUB=1, CONSUMER.CREATE is a mux request, deliver SUB=2), not the mux inbox - enqueue it when
        // that deliver SUB is written.
        $transport->enqueueOnWriteContaining = [
            'SUB _INBOX.JS.ORD' => [
                sprintf("MSG \$O.assets.M.%s 2 \$JS.ACK.OBJ_assets.OBJWATCH.1.7.1.0.0 %d\r\n%s\r\n", $enc, strlen((string) $metadata), (string) $metadata),
            ],
        ];
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $seen = null;
        $sid = $client->jetStream()->objectStore('assets')->watch(static function (ObjectInfo $info) use (&$seen): void {
            $seen = $info;
        })->await();

        self::assertSame(2, $sid);
        self::assertSame(1, $client->processIncoming()->await());
        self::assertInstanceOf(ObjectInfo::class, $seen);
        /** @var ObjectInfo $seenInfo */
        $seenInfo = $seen;
        self::assertSame('logo.txt', $seenInfo->name);
        self::assertSame('wnuid1', $seenInfo->nuid);
        // The revision is the delivery's stream sequence (from the $JS.ACK reply).
        self::assertSame(7, $seenInfo->revision);

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('"deliver_policy":"new"', $writes);
        self::assertStringContainsString('"ack_policy":"none"', $writes);
    }

    /**
     * An option-less Object Store watch must request the default idle heartbeat so the missed-
     * heartbeat watchdog arms (#113 KV-watch parity): previously the OS watch requested no
     * idle_heartbeat, so a reaped/lost watch consumer hung forever with no data and no error -
     * total silence indistinguishable from an idle bucket.
     */
    public function testWatchRequestsDefaultIdleHeartbeat(): void
    {
        $createReply = '{"stream_name":"OBJ_assets","name":"OBJWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->objectStore('assets')->watch(static function (ObjectInfo $info): void {})->await();

        self::assertStringContainsString(
            '"idle_heartbeat":' . ObjectStoreBucket::WATCH_IDLE_HEARTBEAT_NS,
            implode('', $transport->writes),
            'an option-less OS watch must request the default idle heartbeat so the watchdog arms',
        );

        $client->disconnect()->await();
    }

    /**
     * A silent/reaped Object Store watch consumer is RECREATED by the ordered-consumer watchdog
     * instead of hanging forever (#113 KV-watch parity), with the heartbeat interval tunable
     * through ObjectStoreWatchOptions::$idleHeartbeat. Having delivered nothing yet, the
     * replacement re-applies the watch's INITIAL deliver policy (never a by_start_sequence replay
     * from sequence 1). Falsifiable: pre-fix the OS watch never requested a heartbeat, so no
     * watchdog armed and a dead watch stayed silent forever.
     */
    public function testWatchSilentConsumerIsRecreatedByWatchdog(): void
    {
        $createReply = '{"stream_name":"OBJ_assets","name":"OBJWATCH","config":{"deliver_subject":"_INBOX.JS.ORD.x","ack_policy":"none"}}';
        $deleteReply = '{"success":true}';
        $recreateReply = '{"stream_name":"OBJ_assets","name":"OBJWATCH2","config":{"deliver_subject":"_INBOX.JS.ORD.y","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ], blockWhenEmpty: true);
        $this->muxReplies($transport, [
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
        $client->jetStream()->objectStore('assets')->watch(
            static function (ObjectInfo $info): void {},
            '>',
            new ObjectStoreWatchOptions(idleHeartbeat: 30_000_000),
        )->await();

        // The watch consumer must actually request the heartbeat, or the server would never emit
        // the status-100 frames the watchdog relies on.
        self::assertStringContainsString('"idle_heartbeat":30000000', implode('', $transport->writes));

        $deadlineNs = hrtime(true) + 2_000_000_000;
        while (substr_count(implode('', $transport->writes), '$JS.API.CONSUMER.CREATE.OBJ_assets') < 2 && hrtime(true) < $deadlineNs) {
            \Amp\delay(0.01);
        }

        $client->disconnect()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.CONSUMER.DELETE.OBJ_assets.OBJWATCH', $writes, 'the silent instance must be replaced');
        self::assertGreaterThanOrEqual(2, substr_count($writes, '$JS.API.CONSUMER.CREATE.OBJ_assets'), 'a replacement consumer must be created');
        self::assertStringNotContainsString('by_start_sequence', $writes);
        self::assertSame([], $errors, 'watchdog recreation is recovery, not an error');
    }

    /**
     * Meta subjects carry base64url-ENCODED names, so watch() must encode an exact-name pattern
     * (a raw name could never match) and REJECT wildcard patterns outright - previously a pattern
     * like "logs-*" subscribed successfully and silently observed nothing, indistinguishable from
     * an idle bucket.
     */
    public function testWatchEncodesExactNamePatternAndRejectsWildcards(): void
    {
        $createReply = '{"stream_name":"OBJ_assets","name":"OBJWATCH","config":{"deliver_subject":"_INBOX.JS.ORD.x","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();
        $bucket = $client->jetStream()->objectStore('assets');

        // Exact object name: the consumer filter must carry the ENCODED token.
        $bucket->watch(static function (ObjectInfo $info): void {}, 'logo.txt')->await();
        self::assertStringContainsString(
            '$O.assets.M.' . $this->encodeName('logo.txt'),
            implode('', $transport->writes),
            'an exact-name pattern must be base64url-encoded into the filter subject',
        );

        // Wildcards can never match encoded tokens: loud rejection, not a silent no-op watch.
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('wildcard');
        $bucket->watch(static function (ObjectInfo $info): void {}, 'logs-*')->await();
    }

    public function testWatchWithOptionsRequestsSnapshotDeliverPolicy(): void
    {
        // #98: supplying ObjectStoreWatchOptions (no flags) must request the reference snapshot-then-follow
        // policy (last_per_subject) on the CONSUMER.CREATE, so existing objects are replayed - unlike the
        // null-options default which stays updates-only (deliver_policy=new).
        $createReply = '{"stream_name":"OBJ_assets","name":"OBJWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $client->jetStream()->objectStore('assets')->watch(
            static function (ObjectInfo $info): void {},
            options: new ObjectStoreWatchOptions(),
        )->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('"deliver_policy":"last_per_subject"', $writes);
        self::assertStringNotContainsString('"deliver_policy":"new"', $writes);
        self::assertStringContainsString('"ack_policy":"none"', $writes);
    }

    public function testWatchToleratesMalformedMetadataAndKeepsDispatching(): void
    {
        $enc = $this->encodeName('logo.txt');
        $valid = json_encode([
            'name' => 'logo.txt',
            'bucket' => 'assets',
            'nuid' => 'wnuid2',
            'size' => 5,
            'chunks' => 1,
            'digest' => $this->digestOf('hello'),
        ], JSON_THROW_ON_ERROR);

        $createReply = '{"stream_name":"OBJ_assets","name":"OBJWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // Both meta frames arrive on the push consumer's deliver subscription (sid 2), not the mux
        // inbox; enqueue them when that deliver SUB is written.
        $transport->enqueueOnWriteContaining = [
            'SUB _INBOX.JS.ORD' => [
                // A malformed (non-JSON) meta delivery must be skipped, not throw out of the loop.
                sprintf("MSG \$O.assets.M.%s 2 \$JS.ACK.OBJ_assets.OBJWATCH.1.5.1.0.0 %d\r\n%s\r\n", $enc, strlen('not-json'), 'not-json'),
                // A subsequent valid meta on the same subscription is still delivered.
                sprintf("MSG \$O.assets.M.%s 2 \$JS.ACK.OBJ_assets.OBJWATCH.2.6.2.0.0 %d\r\n%s\r\n", $enc, strlen((string) $valid), (string) $valid),
            ],
        ];
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $seen = [];
        $client->jetStream()->objectStore('assets')->watch(static function (ObjectInfo $info) use (&$seen): void {
            $seen[] = $info->nuid;
        })->await();

        // Both frames are processed; the malformed one is silently skipped, the valid one delivered.
        $client->processIncoming()->await();
        $client->processIncoming()->await();

        self::assertSame(['wnuid2'], $seen);
    }

    /**
     * Verifies watch() skips a server delete-marker (Nats-Marker-Reason) instead of surfacing it as a
     * bogus ObjectInfo, while still delivering a subsequent valid metadata update (issue #5).
     */
    public function testWatchSkipsDeleteMarker(): void
    {
        $enc = $this->encodeName('logo.txt');
        $valid = json_encode([
            'name' => 'logo.txt',
            'bucket' => 'assets',
            'nuid' => 'wnuid3',
            'size' => 5,
            'chunks' => 1,
            'digest' => $this->digestOf('hello'),
        ], JSON_THROW_ON_ERROR);

        $createReply = '{"stream_name":"OBJ_assets","name":"OBJWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';
        // The marker carries a NON-EMPTY, valid-JSON body so the marker-header check is the only thing
        // preventing emission (an empty body would be skipped by the malformed-JSON guard regardless).
        $markerBody = json_encode([
            'name' => 'logo.txt',
            'bucket' => 'assets',
            'nuid' => 'stale',
            'size' => 5,
            'chunks' => 1,
            'digest' => $this->digestOf('hello'),
        ], JSON_THROW_ON_ERROR);
        $markerHdrs = "NATS/1.0\r\nNats-Marker-Reason: MaxAge\r\n\r\n";
        $mh = strlen($markerHdrs);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // Both frames arrive on the push consumer's deliver subscription (sid 2), not the mux inbox;
        // enqueue them when that deliver SUB is written.
        $transport->enqueueOnWriteContaining = [
            'SUB _INBOX.JS.ORD' => [
                // A server delete-marker must be skipped, not surfaced as an ObjectInfo.
                sprintf("HMSG \$O.assets.M.%s 2 \$JS.ACK.OBJ_assets.OBJWATCH.1.5.1.0.0 %d %d\r\n%s%s\r\n", $enc, $mh, $mh + strlen((string) $markerBody), $markerHdrs, (string) $markerBody),
                // A subsequent valid meta on the same subscription is still delivered.
                sprintf("MSG \$O.assets.M.%s 2 \$JS.ACK.OBJ_assets.OBJWATCH.2.6.2.0.0 %d\r\n%s\r\n", $enc, strlen((string) $valid), (string) $valid),
            ],
        ];
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $seen = [];
        $client->jetStream()->objectStore('assets')->watch(static function (ObjectInfo $info) use (&$seen): void {
            $seen[] = $info->nuid;
        })->await();

        $client->processIncoming()->await();
        $client->processIncoming()->await();

        self::assertSame(['wnuid3'], $seen);
    }

    /**
     * Verifies info() returns null when the latest meta record is a server delete-marker, even when it
     * carries a non-empty body (issue #5).
     */
    public function testInfoReturnsNullForDeleteMarker(): void
    {
        $enc = $this->encodeName('logo.txt');
        $markerBody = json_encode([
            'name' => 'logo.txt',
            'bucket' => 'assets',
            'nuid' => 'stale',
            'deleted' => false,
        ], JSON_THROW_ON_ERROR);
        $hdrs = "NATS/1.0\r\nNats-Stream: OBJ_assets\r\nNats-Subject: \$O.assets.M.{$enc}\r\nNats-Sequence: 9\r\nNats-Marker-Reason: MaxAge\r\n\r\n";
        $h = strlen($hdrs);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("HMSG _INBOX.x 1 %d %d\r\n%s%s\r\n", $h, $h + strlen((string) $markerBody), $hdrs, (string) $markerBody),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $info = $client->jetStream()->objectStore('assets')->info('logo.txt')->await();

        self::assertNull($info);
    }

    public function testBucketSubjectHelpers(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $bucket = $client->jetStream()->objectStore('assets');
        self::assertSame('OBJ_assets', $bucket->streamName());
        self::assertSame('$O.assets.C.', $bucket->chunkPrefix());
        self::assertSame('$O.assets.M.', $bucket->metaPrefix());
    }

    public function testPutRejectsEmptyName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid object name');
        $client->jetStream()->objectStore('assets')->put('', 'x')->await();
    }

    public function testPutSplitsIntoMultipleChunksWithSmallChunkSize(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // existing-object lookup -> not found
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            // three chunk acks
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),
            // meta ack
            sprintf("MSG _INBOX.e 5 %d\r\n%s\r\n", strlen($this->pubAck(4)), $this->pubAck(4)),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $bucket = new ObjectStoreBucket($client, $client->jetStream(), 'assets', 4);
        $stored = $bucket->put('multi.bin', 'abcdefghij')->await();

        self::assertSame(3, $stored->chunks);
        $writes = implode('', $transport->writes);
        self::assertSame(3, substr_count($writes, 'PUB $O.assets.C.' . $stored->nuid));
        self::assertStringContainsString('"chunks":3', $writes);
    }

    /**
     * A pipelined upload whose acked stream sequences are NOT strictly increasing in chunk order
     * (a 503-retried chunk landed after later chunks - ADR-21 retry inside publish()) must ABORT:
     * reassembly is in stream order, so completing the meta publish would store a permanently
     * corrupted object that put() reports as success (the digest check would only catch it at
     * read time). The partial NUID is purged and no meta record is written.
     */
    public function testPutAbortsAndPurgesWhenChunkAcksArriveReordered(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            // Chunk acks with sequence 2 BEFORE sequence 1: chunk #2 was stored ahead of chunk #1.
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),
            // Purge of the partial NUID (abandonPartialUpload).
            sprintf("MSG _INBOX.e 5 %d\r\n%s\r\n", strlen('{"success":true,"purged":2}'), '{"success":true,"purged":2}'),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $bucket = new ObjectStoreBucket($client, $client->jetStream(), 'assets', 4);

        try {
            $bucket->put('multi.bin', 'abcdefghij')->await();
            self::fail('a reordered upload must abort, not report success');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('reordered', $e->getMessage());
        }

        $writes = implode('', $transport->writes);
        // The partial chunks were purged and the corrupt object was never made visible via meta.
        self::assertStringContainsString('$JS.API.STREAM.PURGE.OBJ_assets', $writes);
        self::assertStringContainsString('$O.assets.C.', $writes);
        self::assertStringNotContainsString('PUB $O.assets.M.', $writes, 'no meta record may be published for a corrupt upload');
    }

    /**
     * The REAL-WORLD reorder shape: on a single connection, ack ARRIVAL order always equals server
     * storage order, so a 503-retried chunk's ack arrives LAST carrying the HIGHEST sequence while
     * the earlier-published chunk stored later. A completion-order collection (Amp Future\await
     * returns results in resolution order) sees a monotonic list and misses the corruption - the
     * sequences must be collected in CHUNK order for the guard to fire. Here chunk #1's ack is
     * withheld until after chunk #2's: arrival order [seq 1 (chunk2), seq 2 (chunk1)] is monotonic,
     * chunk order [2, 1] is not - the upload must abort and purge.
     */
    public function testPutDetectsReorderEvenWhenAcksArriveInMonotonicCompletionOrder(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        // Token-aware responder: replies to the lookup and purge immediately, but withholds chunk
        // #1's ack until chunk #2's request is seen, then answers #2 first (seq 1) and #1 second
        // (seq 2) - the ack pattern an actual delayed 503 retry produces.
        $base = null;
        $chunkReplyTos = [];
        $transport->onWrite = static function (string $bytes) use (&$base, &$chunkReplyTos): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            if (str_starts_with($head, 'SUB ')) {
                $subject = explode(' ', $head)[1] ?? '';
                if (str_starts_with($subject, '_INBOX.') && str_ends_with($subject, '.*')) {
                    $base = substr($subject, 0, -1);
                }

                return [];
            }

            if (!str_starts_with($head, 'PUB ') || $base === null) {
                return [];
            }

            $parts = explode(' ', $head);
            $subject = $parts[1] ?? '';
            $replyTo = $parts[2] ?? '';

            $msg = static fn(string $rt, string $body): string => sprintf("MSG %s 1 %d\r\n%s\r\n", $rt, strlen($body), $body);

            if (str_starts_with($subject, '$JS.API.DIRECT.GET') || str_starts_with($subject, '$JS.API.STREAM.MSG.GET')) {
                return [$msg($replyTo, '{"error":{"code":404,"description":"message not found"}}')];
            }

            if (str_starts_with($subject, '$JS.API.STREAM.PURGE')) {
                return [$msg($replyTo, '{"success":true,"purged":2}')];
            }

            if (str_starts_with($subject, '$O.assets.C.')) {
                $chunkReplyTos[] = $replyTo;
                if (count($chunkReplyTos) === 2) {
                    // Storage order was 2 then 1: acks arrive monotonically (seq 1 then seq 2) but
                    // belong to chunks [2, 1].
                    return [
                        $msg($chunkReplyTos[1], '{"stream":"OBJ_assets","seq":1,"duplicate":false}'),
                        $msg($chunkReplyTos[0], '{"stream":"OBJ_assets","seq":2,"duplicate":false}'),
                    ];
                }

                return [];
            }

            return [];
        };

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        // chunkSize 4, 8 bytes -> exactly 2 chunks.
        $bucket = new ObjectStoreBucket($client, $client->jetStream(), 'assets', 4);

        try {
            $bucket->put('multi.bin', 'abcdefgh')->await();
            self::fail('the reordered upload must abort even though ack arrival order was monotonic');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('reordered', $e->getMessage());
        }

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.STREAM.PURGE.OBJ_assets', $writes);
        self::assertStringNotContainsString('PUB $O.assets.M.', $writes);
    }

    /**
     * A chunk publish failing outright mid-upload must purge the already-stored chunks of the fresh
     * NUID before rethrowing: no meta record ever references that NUID, so without the purge the
     * chunks were orphaned in the stream FOREVER (nats.go purgePartial parity).
     */
    public function testPutFailurePurgesPartialChunksBeforeRethrowing(): void
    {
        $failure = '{"error":{"code":400,"err_code":10077,"description":"maximum bytes exceeded"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            // Chunk #2 is rejected by the server.
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($failure), $failure),
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),
            // Purge of the partial NUID (abandonPartialUpload).
            sprintf("MSG _INBOX.e 5 %d\r\n%s\r\n", strlen('{"success":true,"purged":2}'), '{"success":true,"purged":2}'),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $bucket = new ObjectStoreBucket($client, $client->jetStream(), 'assets', 4);

        try {
            $bucket->put('multi.bin', 'abcdefghij')->await();
            self::fail('a failed chunk publish must abort the upload');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('maximum bytes exceeded', $e->getMessage(), 'the ORIGINAL failure must surface, not a cleanup error');
        }

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('$JS.API.STREAM.PURGE.OBJ_assets', $writes, 'the partial chunks must be purged (no meta references the NUID)');
        self::assertStringNotContainsString('PUB $O.assets.M.', $writes);
    }

    /**
     * Verifies a multi-chunk object is downloaded with a single batched pull (not one pull per
     * chunk), reassembled in order, and digest-verified.
     */
    public function testGetDownloadsMultipleChunksInSingleBatch(): void
    {
        $nuid = 'nuidbatch01';
        $chunks = ['abc', 'def', 'ghi'];
        $assembled = implode('', $chunks);
        $meta = $this->metaGetResponse('multi.bin', [
            'nuid' => $nuid,
            'size' => strlen($assembled),
            'chunks' => count($chunks),
            'digest' => $this->digestOf($assembled),
        ]);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPHB","config":{"ack_policy":"explicit"}}';
        $deleteConsumer = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // All three chunks arrive on the single fetch-batch inbox (sid 2: mux SUB=1, ephemeral consumer
        // create is a mux request, fetch SUB=2), no per-chunk pull - enqueue on the fetch SUB.
        $transport->enqueueOnWriteContaining = [
            '_INBOX.JS.FETCH' => [
                "MSG _INBOX.JS.FETCH.c 2 3\r\nabc\r\n",
                "MSG _INBOX.JS.FETCH.c 2 3\r\ndef\r\n",
                "MSG _INBOX.JS.FETCH.c 2 3\r\nghi\r\n",
            ],
        ];
        $this->muxReplies($transport, [
            $this->directReplyFromEnvelope($meta, 1),                     // info()
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),             // create ephemeral consumer
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer),  // delete consumer
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $fetched = $client->jetStream()->objectStore('assets')->get('multi.bin')->await();

        self::assertInstanceOf(ObjectData::class, $fetched);
        self::assertSame($assembled, $fetched->data);
        self::assertSame(count($chunks), $fetched->info->chunks);
        self::assertSame($this->digestOf($assembled), $fetched->info->digest);

        $writes = implode('||', $transport->writes);
        // One batched pull request for all chunks (batch == chunk count), not three separate pulls.
        self::assertStringContainsString('"filter_subject":"$O.assets.C.' . $nuid . '"', $writes);
        self::assertSame(1, substr_count($writes, 'PUB $JS.API.CONSUMER.MSG.NEXT.OBJ_assets'));
        self::assertStringContainsString('"batch":3', $writes);
    }

    /**
     * Verifies list() rethrows a non-404 error raised while reading a meta record.
     */
    public function testListRethrowsNonNotFoundError(): void
    {
        $streamInfo = (string) json_encode([
            'state' => ['subjects' => ['$O.assets.M.' . $this->encodeName('doc.txt') => 1]],
        ], JSON_THROW_ON_ERROR);

        // Pre-2.11 server: list() takes the per-subject Direct Get fan-out this test scripts (batched
        // multi_last Direct Get requires 2.11+ and is covered by the #110 batched-path tests).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo),  // metaSubjects() page 1
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen('{"state":{"subjects":{}}}'), '{"state":{"subjects":{}}}'), // terminator page
            $this->directStatusReply(3, 500, 'boom'),                                 // Direct Get -> non-404 error
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('boom');
        $client->jetStream()->objectStore('assets')->list()->await();
    }

    /**
     * Verifies list() surfaces an error from the meta-subject enumeration request.
     */
    public function testListThrowsWhenSubjectEnumerationFails(): void
    {
        $error = '{"error":{"code":500,"description":"info failed"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($error), $error),  // metaSubjects() -> error
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('info failed');
        $client->jetStream()->objectStore('assets')->list()->await();
    }

    /**
     * Verifies delete() still succeeds when the best-effort chunk purge fails.
     */
    public function testDeleteToleratesPurgeFailure(): void
    {
        $meta = $this->metaGetResponse('logo.txt', ['nuid' => 'delnuidfail', 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')]);
        $purgeError = '{"error":{"code":500,"description":"purge failed"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // delete() publishes the tombstone before awaiting the concurrent lookup, then purges - so the
        // mux replies arrive tombstone-ack, lookup, purge (FIFO by write order, not the old sid order).
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(7)), $this->pubAck(7)),  // tombstone ack
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($meta), $meta),                       // existing-object lookup
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($purgeError), $purgeError),            // purge -> error (swallowed)
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $deleted = $client->jetStream()->objectStore('assets')->delete('logo.txt')->await();

        self::assertTrue($deleted->deleted);
    }

    /**
     * Verifies list() skips a meta subject whose record is no longer present (404).
     */
    public function testListSkipsNotFoundSubject(): void
    {
        $streamInfo = (string) json_encode([
            'state' => ['subjects' => [
                '$O.assets.M.' . $this->encodeName('gone.txt') => 1,
                '$O.assets.M.' . $this->encodeName('logo.txt') => 1,
            ]],
        ], JSON_THROW_ON_ERROR);
        // Pre-2.11 server: list() takes the per-subject Direct Get fan-out this test scripts (batched
        // multi_last Direct Get requires 2.11+ and is covered by the #110 batched-path tests).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo), // metaSubjects() page 1
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen('{"state":{"subjects":{}}}'), '{"state":{"subjects":{}}}'), // terminator page
            $this->directStatusReply(3, 404, 'message not found'),                    // gone.txt -> 404 (skipped)
            $this->directMetaReply('logo.txt', ['nuid' => 'n1', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')], 4), // logo.txt -> present
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $objects = $client->jetStream()->objectStore('assets')->list()->await();

        self::assertCount(1, $objects);
        self::assertSame('logo.txt', $objects[0]->name);
    }

    /**
     * Verifies delete() proceeds when the previous-metadata lookup fails with a non-404 error.
     */
    public function testDeleteToleratesMissingPreviousMetadata(): void
    {
        $lookupError = '{"error":{"code":500,"description":"lookup failed"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->pubAck(7)), $this->pubAck(7)),        // tombstone ack
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($lookupError), $lookupError),               // lookup -> 500 swallowed
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $deleted = $client->jetStream()->objectStore('assets')->delete('logo.txt')->await();

        self::assertTrue($deleted->deleted);
    }

    /**
     * Verifies delete() surfaces an error when the metadata (tombstone) publish is rejected.
     */
    public function testDeleteThrowsWhenMetadataPublishFails(): void
    {
        $publishError = '{"error":{"code":400,"description":"publish rejected"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($publishError), $publishError),            // tombstone publish -> error
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),    // lookup -> 404
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('publish rejected');
        $client->jetStream()->objectStore('assets')->delete('logo.txt')->await();
    }

    /**
     * Verifies a pull timeout (408) during download stops the loop and fails the completeness gate
     * (fewer chunks received than the metadata declares), rather than returning a truncated object.
     */
    public function testGetStopsOnPullTimeoutAndFailsCompleteness(): void
    {
        $meta = $this->metaGetResponse('doc.txt', ['nuid' => 'nuidto01', 'size' => 5, 'chunks' => 2, 'digest' => $this->digestOf('hello')]);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPHTO","config":{"ack_policy":"none"}}';
        $deleteConsumer = '{"success":true}';
        $status = "NATS/1.0 408 Request Timeout\r\nStatus: 408\r\nDescription: Request Timeout\r\n\r\n";
        $hb = strlen($status);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // The 408 pull-timeout status arrives on the fetch inbox (sid 2), not the mux inbox.
        $transport->enqueueOnWriteContaining = [
            '_INBOX.JS.FETCH' => [
                sprintf("HMSG _INBOX.JS.FETCH.c 2 %d %d\r\n%s\r\n", $hb, $hb, $status), // pull -> 408 (break)
            ],
        ];
        $this->muxReplies($transport, [
            $this->directReplyFromEnvelope($meta, 1),                          // info()
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),                  // create consumer
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer),       // delete consumer
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Incomplete object download');
        $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
    }

    /**
     * Verifies the completeness gate is digest-independent: a truncated download of an object whose
     * metadata has no digest (e.g. written by another client) still fails instead of returning a
     * partial payload.
     */
    public function testGetFailsTruncatedDownloadEvenWithoutDigest(): void
    {
        $meta = $this->metaGetResponse('doc.txt', ['nuid' => 'nuidnd01', 'size' => 6, 'chunks' => 2, 'digest' => '']);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPHND","config":{"ack_policy":"none"}}';
        $deleteConsumer = '{"success":true}';
        $status = "NATS/1.0 408 Request Timeout\r\nStatus: 408\r\nDescription: Request Timeout\r\n\r\n";
        $hb = strlen($status);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // The 408 pull-timeout status arrives on the fetch inbox (sid 2), not the mux inbox.
        $transport->enqueueOnWriteContaining = [
            '_INBOX.JS.FETCH' => [
                sprintf("HMSG _INBOX.JS.FETCH.c 2 %d %d\r\n%s\r\n", $hb, $hb, $status), // pull -> 408, 0 of 2 chunks
            ],
        ];
        $this->muxReplies($transport, [
            $this->directReplyFromEnvelope($meta, 1),                          // info() (chunks=2, no digest)
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),                  // create consumer
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer),       // delete consumer
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        // No digest to verify against, but the chunk-count gate still rejects the short read.
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Incomplete object download');
        $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
    }

    /**
     * Verifies a non-timeout pull error during download is propagated rather than swallowed.
     */
    public function testGetRethrowsNonTimeoutPullError(): void
    {
        $meta = $this->metaGetResponse('doc.txt', ['nuid' => 'nuiderr01', 'size' => 5, 'chunks' => 2, 'digest' => $this->digestOf('hello')]);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPHERR","config":{"ack_policy":"explicit"}}';
        $deleteConsumer = '{"success":true}';
        $status = "NATS/1.0 409 Consumer Deleted\r\nStatus: 409\r\nDescription: Consumer Deleted\r\n\r\n";
        $hb = strlen($status);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // The 409 pull status arrives on the fetch inbox (sid 2), not the mux inbox.
        $transport->enqueueOnWriteContaining = [
            '_INBOX.JS.FETCH' => [
                sprintf("HMSG _INBOX.JS.FETCH.c 2 %d %d\r\n%s\r\n", $hb, $hb, $status), // pull -> 409 (rethrow)
            ],
        ];
        $this->muxReplies($transport, [
            $this->directReplyFromEnvelope($meta, 1),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('status 409');
        $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
    }

    /**
     * get() returns null when info() finds no object (Direct Get returns 404).
     */
    public function testGetReturnsNullWhenObjectNotFound(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directStatusReply(1, 404, 'Message Not Found'), // info() -> 404
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $result = $client->jetStream()->objectStore('assets')->get('missing.txt')->await();

        self::assertNull($result);
    }

    /**
     * get() returns null for a deleted (tombstoned) object.
     */
    public function testGetReturnsNullForDeletedObject(): void
    {
        $meta = $this->metaGetResponse('gone.txt', ['nuid' => '', 'size' => 0, 'chunks' => 0, 'digest' => '', 'deleted' => true]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directReplyFromEnvelope($meta, 1), // info() -> deleted tombstone
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $result = $client->jetStream()->objectStore('assets')->get('gone.txt')->await();

        self::assertNull($result);
    }

    /**
     * get() throws after too many link hops (MAX_LINK_HOPS = 8, so depth 9 triggers).
     * We chain 9 self-referential links; on the 9th recursion depth > 8 and the exception is thrown.
     */
    public function testGetThrowsOnTooManyLinkHops(): void
    {
        // Build 9 link meta replies all pointing at the same name "loop.txt" to force a cycle.
        // The first info() call returns a link, which recursively calls getInternal with depth+1.
        // At depth > MAX_LINK_HOPS (8) the exception fires.
        $linkMeta = ['options' => ['link' => ['bucket' => 'assets', 'name' => 'loop.txt']]];

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // MAX_LINK_HOPS = 8; we need 9 info() replies (depth 0..8) before the 9th depth check fires.
        // Each info() is a sequential Direct Get mux request, so the replies are FIFO by hop depth.
        $frames = [];
        for ($i = 1; $i <= 9; $i++) {
            $frames[] = $this->directMetaReply('loop.txt', $linkMeta, $i);
        }
        $this->muxReplies($transport, $frames);
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Too many Object Store link hops');
        $client->jetStream()->objectStore('assets')->get('loop.txt')->await();
    }

    /**
     * get() throws when a bucket link is followed (link has no 'name' key).
     */
    public function testGetThrowsOnBucketLink(): void
    {
        // A bucket link has options.link = {bucket: 'other'} - no 'name' key.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directMetaReply('bucket-link', ['options' => ['link' => ['bucket' => 'other-bucket']]], 1),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('it points to a bucket, not an object');
        $client->jetStream()->objectStore('assets')->get('bucket-link')->await();
    }

    /**
     * getToCallback() throws after too many link hops.
     */
    public function testGetToCallbackThrowsOnTooManyLinkHops(): void
    {
        $linkMeta = ['options' => ['link' => ['bucket' => 'assets', 'name' => 'loop.txt']]];

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $frames = [];
        for ($i = 1; $i <= 9; $i++) {
            $frames[] = $this->directMetaReply('loop.txt', $linkMeta, $i);
        }
        $this->muxReplies($transport, $frames);
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Too many Object Store link hops');
        $client->jetStream()->objectStore('assets')->getToCallback('loop.txt', static function (string $c): void {})->await();
    }

    /**
     * getToCallback() returns null when object is not found.
     */
    public function testGetToCallbackReturnsNullWhenNotFound(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directStatusReply(1, 404, 'Message Not Found'),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $called = false;
        $result = $client->jetStream()->objectStore('assets')->getToCallback(
            'missing.txt',
            static function (string $chunk) use (&$called): void {
                $called = true;
            },
        )->await();

        self::assertNull($result);
        self::assertFalse($called);
    }

    /**
     * getToCallback() follows an object link and streams the target's content.
     */
    public function testGetToCallbackFollowsObjectLink(): void
    {
        $nuid = 'nuidcblink1';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // info('shortcut') -> link pointing at doc.txt in same bucket
            $this->directMetaReply('shortcut', ['options' => ['link' => ['bucket' => 'assets', 'name' => 'doc.txt']]], 1),
            // info('doc.txt') -> real object
            $this->directMetaReply('doc.txt', ['nuid' => $nuid, 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')], 2),
            // single-chunk Direct Get on the NUID subject
            $this->directChunkReply($nuid, 'hello', 3),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $captured = '';
        $info = $client->jetStream()->objectStore('assets')->getToCallback(
            'shortcut',
            static function (string $chunk) use (&$captured): void {
                $captured .= $chunk;
            },
        )->await();

        self::assertSame('hello', $captured);
        self::assertNotNull($info);
        self::assertSame('doc.txt', $info->name);
    }

    /**
     * streamChunks single-chunk path throws "Incomplete object download" on 404.
     */
    public function testGetSingleChunkThrowsIncompleteDownloadOnNotFound(): void
    {
        $nuid = 'nuidsingle04';
        $meta = $this->metaGetResponse('doc.txt', [
            'nuid' => $nuid,
            'size' => 5,
            'chunks' => 1,
            'digest' => $this->digestOf('hello'),
        ]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directReplyFromEnvelope($meta, 1),          // info() Direct Get
            $this->directStatusReply(2, 404, 'Not Found'),     // single-chunk Direct Get -> 404
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Incomplete object download: expected 1 chunks, received 0');
        $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
    }

    /**
     * streamChunks single-chunk path rethrows non-404, non-503 Direct Get errors.
     */
    public function testGetSingleChunkRethrowsNonNotFoundNonUnavailableError(): void
    {
        $nuid = 'nuidsingle05';
        $meta = $this->metaGetResponse('doc.txt', [
            'nuid' => $nuid,
            'size' => 5,
            'chunks' => 1,
            'digest' => $this->digestOf('hello'),
        ]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directReplyFromEnvelope($meta, 1),          // info() Direct Get
            $this->directStatusReply(2, 500, 'Stream Error Occurred'), // single-chunk Direct Get -> 500 (rethrow)
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Stream Error Occurred');
        $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
    }

    /**
     * streamChunks() 503 fall-through: single-chunk object, Direct Get returns 503 -> falls through to
     * the ephemeral consumer path and successfully downloads the chunk.
     */
    public function testGetSingleChunkFallsThrough503ToEphemeralConsumer(): void
    {
        $nuid = 'nuidsingle06';
        $meta = $this->metaGetResponse('doc.txt', [
            'nuid' => $nuid,
            'size' => 5,
            'chunks' => 1,
            'digest' => $this->digestOf('hello'),
        ]);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPHSINGLE","config":{"ack_policy":"none"}}';
        $deleteConsumer = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // The lone chunk arrives on the fetch inbox after the 503 fall-through (sid 2: mux SUB=1, the
        // info + single-chunk Direct Get + CONSUMER.CREATE are mux requests, fetch SUB=2).
        $transport->enqueueOnWriteContaining = [
            '_INBOX.JS.FETCH' => [
                "MSG _INBOX.JS.FETCH.c 2 5\r\nhello\r\n", // chunk delivered via pull
            ],
        ];
        $this->muxReplies($transport, [
            $this->directReplyFromEnvelope($meta, 1),                                                          // info() Direct Get
            $this->directStatusReply(2, 503, 'No Responders'),                                                 // single-chunk Direct Get -> 503 (fall-through)
            sprintf("MSG _INBOX.b 3 %d\r\n%s\r\n", strlen($consumer), $consumer),                            // create ephemeral consumer
            sprintf("MSG _INBOX.d 5 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer),                 // delete consumer
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $fetched = $client->jetStream()->objectStore('assets')->get('doc.txt')->await();

        self::assertInstanceOf(\IDCT\NATS\JetStream\ObjectStore\ObjectData::class, $fetched);
        self::assertSame('hello', $fetched->data);
    }

    /**
     * verifyDigest returns early (no throw) when the object's stored digest is empty.
     * The downloaded content digest does not match what was stored (but stored is ''), so no throw.
     */
    public function testGetSucceedsWhenStoredDigestIsEmpty(): void
    {
        $nuid = 'nuidnodigest';
        $meta = $this->metaGetResponse('doc.txt', [
            'nuid' => $nuid,
            'size' => 5,
            'chunks' => 1,
            'digest' => '', // empty -> verifyDigest returns early, no check performed
        ]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directReplyFromEnvelope($meta, 1),
            $this->directChunkReply($nuid, 'hello', 2),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        // No digest to check -> must succeed even though digest field is empty.
        $fetched = $client->jetStream()->objectStore('assets')->get('doc.txt')->await();

        self::assertInstanceOf(\IDCT\NATS\JetStream\ObjectStore\ObjectData::class, $fetched);
        self::assertSame('hello', $fetched->data);
    }

    /**
     * decodeDigest returns null for a non-"SHA-256=" prefixed digest, causing verifyDigest
     * to throw a mismatch exception even though the bytes match.
     */
    public function testGetThrowsOnUnknownDigestPrefix(): void
    {
        $nuid = 'nuidbadpfx01';
        // Store a digest with an unrecognised prefix - decodeDigest() returns null for both sides,
        // and verifyDigest throws because $expected === null.
        $badDigest = 'MD5=abc123'; // not SHA-256 prefixed
        $meta = $this->metaGetResponse('doc.txt', [
            'nuid' => $nuid,
            'size' => 5,
            'chunks' => 1,
            'digest' => $badDigest,
        ]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directReplyFromEnvelope($meta, 1),
            $this->directChunkReply($nuid, 'hello', 2),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Object digest mismatch');
        $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
    }

    /**
     * info() rethrows non-404, non-503 errors from Direct Get.
     */
    public function testInfoRethrowsNonNotFoundNonUnavailableError(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directStatusReply(1, 500, 'Downstream Error'),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Downstream Error');
        $client->jetStream()->objectStore('assets')->info('doc.txt')->await();
    }

    /**
     * info() returns null when the Direct Get body is not valid JSON.
     */
    public function testInfoReturnsNullWhenPayloadIsNotJson(): void
    {
        // Build a Direct-Get-style HMSG reply whose body is not valid JSON.
        $hdrs = "NATS/1.0\r\nNats-Stream: OBJ_assets\r\nNats-Sequence: 3\r\n\r\n";
        $h = strlen($hdrs);
        $body = 'NOT_JSON';
        $frame = sprintf("HMSG _INBOX.x 1 %d %d\r\n%s%s\r\n", $h, $h + strlen($body), $hdrs, $body);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $frame,
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $result = $client->jetStream()->objectStore('assets')->info('doc.txt')->await();

        self::assertNull($result);
    }

    /**
     * fetchInfo (used as info() fallback via 503) rethrows a non-404 error when
     * swallowErrors=false (i.e. when called from the 503 fallback path in info()).
     */
    public function testInfoFallbackRethrowsNon404Error(): void
    {
        // Direct Get returns 503 -> falls back to STREAM.MSG.GET; that returns a 500 error -> thrown.
        $msgGetError = '{"error":{"code":500,"description":"stream error"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directStatusReply(1, 503, 'No Responders'),                                      // Direct Get -> 503
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($msgGetError), $msgGetError),              // STREAM.MSG.GET -> 500
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('stream error');
        $client->jetStream()->objectStore('assets')->info('doc.txt')->await();
    }

    /**
     * fetchInfo returns null when decodeMetadataFromApiMessage returns null (empty data field).
     * Triggered via info() 503 fallback -> fetchInfo(name, false); response has no 'data' key.
     */
    public function testInfoFallbackReturnsNullWhenMessageDataIsEmpty(): void
    {
        // Direct Get -> 503; STREAM.MSG.GET returns a message with empty 'data' field.
        $msgGetNoData = '{"message":{"subject":"$O.assets.M.doc","seq":5}}'; // no 'data' key

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directStatusReply(1, 503, 'No Responders'),
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($msgGetNoData), $msgGetNoData),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $result = $client->jetStream()->objectStore('assets')->info('doc.txt')->await();

        self::assertNull($result);
    }

    /**
     * updateMeta() throws 404 when the object is deleted (tombstoned).
     */
    public function testUpdateMetaThrowsWhenObjectIsDeleted(): void
    {
        $meta = $this->metaGetResponse('gone.txt', ['nuid' => '', 'size' => 0, 'chunks' => 0, 'digest' => '', 'deleted' => true]);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directReplyFromEnvelope($meta, 1), // info('gone.txt') -> deleted tombstone
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Object not found: gone.txt');
        $client->jetStream()->objectStore('assets')->updateMeta('gone.txt')->await();
    }

    /**
     * updateMeta() throws 404 when the object does not exist at all.
     */
    public function testUpdateMetaThrowsWhenObjectNotFound(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directStatusReply(1, 404, 'Message Not Found'), // info() -> 404 (null)
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Object not found: missing.txt');
        $client->jetStream()->objectStore('assets')->updateMeta('missing.txt')->await();
    }

    /**
     * updateMeta() throws when renaming onto an existing non-deleted object.
     */
    public function testUpdateMetaThrowsWhenRenameTargetExists(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // info('logo.txt') -> found
            $this->directMetaReply('logo.txt', ['nuid' => 'n1', 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')], 1),
            // info('brand.txt') clash check -> found and not deleted
            $this->directMetaReply('brand.txt', ['nuid' => 'n2', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')], 2),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Cannot rename to an existing object: brand.txt');
        $client->jetStream()->objectStore('assets')->updateMeta('logo.txt', 'brand.txt')->await();
    }

    /**
     * list() returns an empty array when there are no meta subjects in the bucket.
     */
    public function testListReturnsEmptyArrayWhenBucketIsEmpty(): void
    {
        // metaSubjects() returns [] when subjects map is empty.
        $emptyStreamInfo = '{"state":{"subjects":{}}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($emptyStreamInfo), $emptyStreamInfo),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $result = $client->jetStream()->objectStore('assets')->list()->await();

        self::assertSame([], $result);
    }

    /**
     * list() skips a meta subject whose Direct Get reply body is not valid JSON.
     */
    public function testListSkipsSubjectWithNonJsonBody(): void
    {
        $enc = $this->encodeName('corrupt.txt');
        $streamInfo = (string) json_encode([
            'state' => ['subjects' => [
                '$O.assets.M.' . $enc => 1,
                '$O.assets.M.' . $this->encodeName('good.txt') => 1,
            ]],
        ], JSON_THROW_ON_ERROR);
        $emptyPage = '{"state":{"subjects":{}}}';

        // Build a Direct Get reply with a non-JSON body for corrupt.txt.
        $badHdrs = "NATS/1.0\r\nNats-Stream: OBJ_assets\r\nNats-Sequence: 1\r\n\r\n";
        $bh = strlen($badHdrs);
        $badBody = 'NOT_VALID_JSON';
        $badReply = sprintf("HMSG _INBOX.x 3 %d %d\r\n%s%s\r\n", $bh, $bh + strlen($badBody), $badHdrs, $badBody);

        // Pre-2.11 server: list() takes the per-subject Direct Get fan-out this test scripts (batched
        // multi_last Direct Get requires 2.11+ and is covered by the #110 batched-path tests).
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.10.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamInfo), $streamInfo), // page 1
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($emptyPage), $emptyPage),   // page 2 terminator
            $badReply,                                                                  // corrupt.txt -> non-JSON body
            $this->directMetaReply('good.txt', ['nuid' => 'n1', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')], 4),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $objects = $client->jetStream()->objectStore('assets')->list()->await();

        // corrupt.txt is skipped (null returned for non-JSON); only good.txt remains.
        self::assertCount(1, $objects);
        self::assertSame('good.txt', $objects[0]->name);
    }

    /**
     * getStatus() returns the correct mapped fields from stream state.
     */
    public function testGetStatusReturnsMappedStreamState(): void
    {
        $streamReply = (string) json_encode([
            'config' => ['name' => 'OBJ_assets'],
            'state' => [
                'messages' => 42,
                'last_seq' => 100,
                'bytes' => 8192,
                'subjects' => ['$O.assets.M.abc' => 1],
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamReply), $streamReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $status = $client->jetStream()->objectStore('assets')->getStatus()->await();

        self::assertSame('assets', $status['bucket']);
        self::assertSame('OBJ_assets', $status['stream']);
        self::assertSame(42, $status['messages']);
        self::assertSame(100, $status['last_sequence']);
        self::assertSame(8192, $status['bytes']);
        self::assertSame(['$O.assets.M.abc' => 1], $status['subjects']);
    }

    /**
     * getStatus() defaults state fields to zero/empty when state is missing.
     */
    public function testGetStatusDefaultsWhenStateIsAbsent(): void
    {
        $streamReply = '{"config":{"name":"OBJ_assets"}}'; // no 'state' key

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamReply), $streamReply),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $status = $client->jetStream()->objectStore('assets')->getStatus()->await();

        self::assertSame(0, $status['messages']);
        self::assertSame(0, $status['last_sequence']);
        self::assertSame(0, $status['bytes']);
        self::assertSame([], $status['subjects']);
    }

    /**
     * putStream() skips empty-string blocks from the producer without hashing or buffering them.
     */
    public function testPutStreamSkipsEmptyBlocks(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // lookup -> not found
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            // one chunk ack for the non-empty 'hello' block
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            // meta ack
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        // Producer yields: empty, empty, 'hello', empty, null.
        $blocks = ['', '', 'hello', '', null];
        $index = 0;
        $store = new ObjectStoreBucket($client, $client->jetStream(), 'assets');
        $stored = $store->putStream('skip-empty.txt', static function () use (&$index, $blocks): ?string {
            return $blocks[$index++];
        })->await();

        // Only 'hello' was processed; empty blocks are silently skipped.
        self::assertSame(5, $stored->size);
        self::assertSame(1, $stored->chunks);
        self::assertSame($this->digestOf('hello'), $stored->digest);
    }

    /**
     * putStream() purges previous revision's chunks when a previous nuid exists.
     */
    public function testPutStreamPurgesPreviousChunks(): void
    {
        $oldNuid = 'oldnuidstr01';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // lookup -> returns previous meta with old nuid
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->metaGetResponse('data.bin', ['nuid' => $oldNuid, 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')])), $this->metaGetResponse('data.bin', ['nuid' => $oldNuid, 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')])),
            // chunk ack
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),
            // meta ack
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(4)), $this->pubAck(4)),
            // purge old chunks ack
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen('{"success":true,"purged":1}'), '{"success":true,"purged":1}'),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $store = new ObjectStoreBucket($client, $client->jetStream(), 'assets');
        $blocks = ['new'];
        $index = 0;
        $stored = $store->putStream('data.bin', static function () use (&$index, $blocks): ?string {
            return $blocks[$index++] ?? null;
        })->await();

        self::assertSame(3, $stored->size);

        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('$JS.API.STREAM.PURGE.OBJ_assets', $writes);
        self::assertStringContainsString('$O.assets.C.' . $oldNuid, $writes);
    }

    /**
     * decodeMetadataFromApiMessage returns null (covered via fetchInfo 503 path)
     * when the STREAM.MSG.GET response has no 'message' key at all.
     */
    public function testInfoFallbackReturnsNullWhenMessageKeyAbsent(): void
    {
        // Direct Get -> 503; STREAM.MSG.GET returns a response with no 'message' key.
        $msgGetNoMessage = '{"ok":true}'; // no 'message' key

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directStatusReply(1, 503, 'No Responders'),
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($msgGetNoMessage), $msgGetNoMessage),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $result = $client->jetStream()->objectStore('assets')->info('doc.txt')->await();

        self::assertNull($result);
    }

    /**
     * decodeMetadataFromApiMessage returns null when base64 decodes to empty.
     * This happens via the fetchInfo/503 fallback path when the message data field is not valid base64.
     */
    public function testInfoFallbackReturnsNullWhenDataIsInvalidBase64(): void
    {
        // Direct Get -> 503; STREAM.MSG.GET returns a message with invalid base64 in 'data'.
        $invalidB64Data = '{"message":{"subject":"subj","seq":1,"data":"!!!NOT_BASE64!!!"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directStatusReply(1, 503, 'No Responders'),
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($invalidB64Data), $invalidB64Data),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $result = $client->jetStream()->objectStore('assets')->info('doc.txt')->await();

        self::assertNull($result);
    }

    /**
     * purgeChunks swallows JetStreamException (the surrounding operation succeeds).
     * Already covered by testDeleteToleratesPurgeFailure. This additional test verifies the same
     * behaviour via put() overwrite path: purge failure does NOT propagate.
     */
    public function testPutOverwriteSwallowsPurgeFailure(): void
    {
        $oldNuid = 'oldnuidswallow';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // lookup -> returns previous meta
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->metaGetResponse('f.txt', ['nuid' => $oldNuid, 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')])), $this->metaGetResponse('f.txt', ['nuid' => $oldNuid, 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')])),
            // chunk publish ack
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),
            // meta publish ack
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(4)), $this->pubAck(4)),
            // purge -> error (must be swallowed)
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen('{"error":{"code":500,"description":"purge failed"}}'), '{"error":{"code":500,"description":"purge failed"}}'),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        // Should not throw despite the purge failure.
        $stored = $client->jetStream()->objectStore('assets')->put('f.txt', 'new')->await();

        self::assertSame('f.txt', $stored->name);
    }

    /**
     * Complement of the rename-conflict guard: updateMeta() succeeds when rename target exists but IS deleted (tombstoned).
     */
    public function testUpdateMetaSucceedsWhenRenameTargetIsDeleted(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // info('logo.txt') -> found
            $this->directMetaReply('logo.txt', ['nuid' => 'n1', 'size' => 3, 'chunks' => 1, 'digest' => $this->digestOf('old')], 1),
            // info('brand.txt') clash check -> found but DELETED
            $this->directMetaReply('brand.txt', ['nuid' => '', 'size' => 0, 'chunks' => 0, 'digest' => '', 'deleted' => true], 2),
            // publishMeta('brand.txt') ack
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(8)), $this->pubAck(8)),
            // publishMeta('logo.txt' tombstone) ack
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($this->pubAck(9)), $this->pubAck(9)),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        // Renaming onto a deleted target must succeed (deleted != live conflict).
        $info = $client->jetStream()->objectStore('assets')->updateMeta('logo.txt', 'brand.txt')->await();

        self::assertSame('brand.txt', $info->name);
        self::assertSame('n1', $info->nuid);
    }

    /**
     * Complement of the bucket-link guard: linkTargetBucket throws when the link object has an explicit empty name.
     * An options.link = {bucket:'b', name:''} is treated the same as a bucket link (name absent).
     */
    public function testGetThrowsOnBucketLinkWithEmptyName(): void
    {
        // options.link with name='' - ObjectInfo.fromArray will NOT set 'name' in $link
        // because of the `$options['link']['name'] !== ''` guard.
        // So $info->link = ['bucket' => 'assets'] (no 'name' key) -> bucket link guard fires.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directMetaReply('blink', ['options' => ['link' => ['bucket' => 'assets', 'name' => '']]], 1),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('it points to a bucket, not an object');
        $client->jetStream()->objectStore('assets')->get('blink')->await();
    }

    /**
     * The 503 path in streamChunks(): getToCallback on a single-chunk object where Direct Get
     * returns 503 falls through to ephemeral consumer and delivers to the callback.
     */
    public function testGetToCallbackSingleChunkFallsThrough503(): void
    {
        $nuid = 'nuidsingle07';
        $meta = $this->metaGetResponse('doc.txt', [
            'nuid' => $nuid,
            'size' => 5,
            'chunks' => 1,
            'digest' => $this->digestOf('hello'),
        ]);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPHCB503","config":{"ack_policy":"none"}}';
        $deleteConsumer = '{"success":true}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        // The lone chunk arrives on the fetch inbox after the 503 fall-through (sid 2: mux SUB=1, the
        // info + single-chunk Direct Get + CONSUMER.CREATE are mux requests, fetch SUB=2).
        $transport->enqueueOnWriteContaining = [
            '_INBOX.JS.FETCH' => [
                "MSG _INBOX.JS.FETCH.c 2 5\r\nhello\r\n", // chunk delivered via pull
            ],
        ];
        $this->muxReplies($transport, [
            $this->directReplyFromEnvelope($meta, 1),                                                      // info() Direct Get
            $this->directStatusReply(2, 503, 'No Responders'),                                             // single-chunk Direct Get -> 503
            sprintf("MSG _INBOX.b 3 %d\r\n%s\r\n", strlen($consumer), $consumer),                        // create ephemeral consumer
            sprintf("MSG _INBOX.d 5 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer),             // delete consumer
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $captured = '';
        $info = $client->jetStream()->objectStore('assets')->getToCallback(
            'doc.txt',
            static function (string $chunk) use (&$captured): void {
                $captured .= $chunk;
            },
        )->await();

        self::assertSame('hello', $captured);
        self::assertNotNull($info);
        self::assertSame('doc.txt', $info->name);
    }

    /**
     * Empty-subjects variant: list() with an empty state (no 'state' key) returns empty array.
     */
    public function testListReturnsEmptyWhenStateKeyAbsent(): void
    {
        // STREAM.INFO response with no state/subjects at all (e.g. fresh empty stream).
        $emptyStreamInfo = '{"config":{"name":"OBJ_assets"}}';

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($emptyStreamInfo), $emptyStreamInfo),
        ]);

        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        $result = $client->jetStream()->objectStore('assets')->list()->await();

        self::assertSame([], $result);
    }
}
