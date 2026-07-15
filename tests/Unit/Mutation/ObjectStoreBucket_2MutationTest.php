<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\ObjectStore\ObjectData;
use IDCT\NATS\JetStream\ObjectStore\ObjectStoreBucket;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing tests for src/JetStream/ObjectStore/ObjectStoreBucket.php (chunk 2).
 *
 * Each test pins an exact observable behavior (return value, thrown message, bytes written, count,
 * boundary) that a specific surviving mutant would change. Frames are fed deterministically through
 * FakeTransport and the in-process Amp loop - no sockets, no sleeps.
 */
final class ObjectStoreBucket_2MutationTest extends TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /** URL-safe base64 (with padding), matching the Object Store meta-subject encoding. */
    private function encodeName(string $name): string
    {
        return strtr(base64_encode($name), '+/', '-_');
    }

    private function digestOf(string $data): string
    {
        return 'SHA-256=' . strtr(base64_encode(hash('sha256', $data, true)), '+/', '-_');
    }

    private function notFound(): string
    {
        return '{"error":{"code":404,"description":"message not found"}}';
    }

    private function pubAck(int $seq): string
    {
        return sprintf('{"stream":"OBJ_assets","seq":%d,"duplicate":false}', $seq);
    }

    /**
     * Direct Get reply (HMSG) whose body is the raw meta JSON for the given bucket/object.
     *
     * @param array<string,mixed> $extra
     */
    private function directMetaReply(string $bucket, string $name, array $extra, int $sid): string
    {
        $meta = array_merge([
            'name' => $name,
            'bucket' => $bucket,
            'mtime' => '2030-01-01T00:00:00Z',
            'deleted' => false,
        ], $extra);
        $body = (string) json_encode($meta, JSON_THROW_ON_ERROR);
        $hdrs = "NATS/1.0\r\nNats-Stream: OBJ_{$bucket}\r\nNats-Sequence: 2\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s%s\r\n", $sid, $h, $h + strlen($body), $hdrs, $body);
    }

    /** Direct Get reply (HMSG) for a single object chunk (raw bytes body on the NUID subject). */
    private function directChunkReply(string $payload, int $sid): string
    {
        $hdrs = "NATS/1.0\r\nNats-Stream: OBJ_x\r\nNats-Sequence: 3\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s%s\r\n", $sid, $h, $h + strlen($payload), $hdrs, $payload);
    }

    private function connect(FakeTransport $transport): NatsClient
    {
        // A small request timeout keeps a genuinely mis-wired reply from hanging on the 10 s default.
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 2000), $transport);
        $client->connect()->await();

        return $client;
    }

    /**
     * Installs a dynamic mux-inbox responder (#118). Post-#118 every Object Store operation that awaits a
     * reply - Direct Get (info/single-chunk get), JS publish acks (put/putStream chunk+meta), and
     * CONSUMER.CREATE/DELETE - funnels through request(), so their replies land on the ONE shared wildcard
     * inbox "_INBOX.<base>.*" rather than a per-request inbox. This learns the mux base from the wildcard
     * SUB and, for each request PUB/HPUB (a publish whose reply-to lives under that base), pops the next
     * reply frame FIFO and re-emits it on the CAPTURED reply-to with the mux sid (1), so a pre-built reply
     * reaches its request unchanged in content - only its addressing is corrected.
     *
     * Only request replies belong here; subscription deliveries (the pull fetch inbox used by the
     * multi-chunk download path) are NOT mux replies - enqueue those on their SUB via
     * FakeTransport::$enqueueOnWriteContaining so they arrive with the sid the server assigned.
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
                // A plain publish, or a subscription's own inbox (the fetch inbox) - not a mux request.
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

    // ------------------------------------------------------------------------------------------
    // putStream() final meta record (lines 345, 351, 352)
    // ------------------------------------------------------------------------------------------

    /**
     * Pins the putStream() meta record fields: the 'bucket' key, deleted=false, and the
     * options.max_chunk_size entry. Drives a 'hello' producer block re-chunked at size 3 (2 chunks).
     */
    public function testPutStreamMetaRecordCarriesBucketDeletedFalseAndChunkSize(): void
    {
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
        ]);
        // All four awaited replies are mux request replies, popped FIFO in write order: the concurrent
        // previous-revision lookup issues its Direct Get first, then the two chunk publish acks, then the
        // meta publish ack.
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()), // lookup
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),     // chunk1
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),     // chunk2
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),     // meta
        ]);

        $client = $this->connect($transport);

        $store = new ObjectStoreBucket($client, $client->jetStream(), 'assets', 3);
        $blocks = ['hello'];
        $index = 0;
        $stored = $store->putStream('big.txt', static function () use (&$index, $blocks): ?string {
            return $blocks[$index++] ?? null;
        })->await();

        $writes = implode('||', $transport->writes);
        // Locate the meta HPUB (it is the only frame carrying the encoded meta subject + JSON body).
        self::assertStringContainsString('HPUB $O.assets.M.' . $this->encodeName('big.txt') . ' ', $writes);

        // kills ArrayItem @ 345 - the 'bucket' key must be present in the meta record.
        self::assertStringContainsString('"bucket":"assets"', $writes);

        // kills FalseValue @ 351 - a freshly stored object is NOT a tombstone.
        self::assertStringContainsString('"deleted":false', $writes);
        self::assertStringNotContainsString('"deleted":true', $writes);
        self::assertFalse($stored->deleted);

        // kills ArrayItem @ 352 and ArrayItemRemoval @ 352 - options carries max_chunk_size.
        self::assertStringContainsString('"options":{"max_chunk_size":3}', $writes);
    }

    // ------------------------------------------------------------------------------------------
    // get() link-hop depth and boundary (lines 374, 386, 398)
    // ------------------------------------------------------------------------------------------

    /**
     * get() must start at depth 0, increment by exactly 1 per hop, and treat MAX_LINK_HOPS (8) as an
     * inclusive ceiling: a chain of exactly 8 links to a real object (depths 0..8) resolves. Any of
     * the three mutants pushes the real object past the ceiling and throws instead.
     */
    public function testGetResolvesExactlyEightLinkHopsToRealObject(): void
    {
        $nuid = 'nuidhops008';

        // 8 links: l0 -> l1 -> ... -> l7 -> obj. One Direct Get info() reply per hop, then the real
        // object, then its single chunk - all mux request replies, popped FIFO in hop order.
        $frames = [];
        $sid = 1;
        for ($i = 0; $i < 8; $i++) {
            $target = $i === 7 ? 'obj' : 'l' . ($i + 1);
            $frames[] = $this->directMetaReply('assets', 'l' . $i, ['options' => ['link' => ['bucket' => 'assets', 'name' => $target]]], $sid++);
        }
        // Real single-chunk object reached at depth 8.
        $frames[] = $this->directMetaReply('assets', 'obj', ['nuid' => $nuid, 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')], $sid++);
        $frames[] = $this->directChunkReply('hello', $sid++);

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $this->muxReplies($transport, $frames);
        $client = $this->connect($transport);

        // kills IncrementInteger @ 374 (start depth 0, not 1), GreaterThan @ 386 (> not >=),
        // IncrementInteger @ 398 (depth + 1, not + 2): all three would throw before reaching depth 8.
        $fetched = $client->jetStream()->objectStore('assets')->get('l0')->await();

        self::assertInstanceOf(ObjectData::class, $fetched);
        self::assertSame('hello', $fetched->data);
        self::assertSame('obj', $fetched->info->name);
    }

    /**
     * get() throws the exact link-hop message (depth 9 chain) - pins the message text/operand order.
     */
    public function testGetTooManyHopsMessageIsExact(): void
    {
        $linkMeta = ['options' => ['link' => ['bucket' => 'assets', 'name' => 'loop.txt']]];
        $frames = [];
        for ($i = 1; $i <= 9; $i++) {
            $frames[] = $this->directMetaReply('assets', 'loop.txt', $linkMeta, $i);
        }

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $this->muxReplies($transport, $frames);
        $client = $this->connect($transport);

        // kills Concat/ConcatOperandRemoval @ 387 (x4) - exact message, name interpolated in place.
        try {
            $client->jetStream()->objectStore('assets')->get('loop.txt')->await();
            self::fail('expected JetStreamException');
        } catch (JetStreamException $e) {
            self::assertSame('Too many Object Store link hops resolving "loop.txt"', $e->getMessage());
        }
    }

    // ------------------------------------------------------------------------------------------
    // linkTargetBucket() resolution (lines 426, 429, 431)
    // ------------------------------------------------------------------------------------------

    /**
     * A cross-bucket object link must resolve its target in the OTHER bucket: targetBucket comes from
     * the link (Coalesce @ 429), and the ternary returns objectStore(target) for the differing bucket
     * (Ternary @ 431). Both mutants would instead resolve in the current ('assets') bucket.
     */
    public function testGetFollowsCrossBucketObjectLinkToOtherBucket(): void
    {
        $nuid = 'nuidxbucket';
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            // info('shortcut') in assets -> object link pointing at bucket 'other', name 'doc.txt'.
            $this->directMetaReply('assets', 'shortcut', ['options' => ['link' => ['bucket' => 'other', 'name' => 'doc.txt']]], 1),
            // info('doc.txt') resolved in the OTHER bucket.
            $this->directMetaReply('other', 'doc.txt', ['nuid' => $nuid, 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')], 2),
            // single chunk Direct Get.
            $this->directChunkReply('hello', 3),
        ]);

        $client = $this->connect($transport);

        $fetched = $client->jetStream()->objectStore('assets')->get('shortcut')->await();

        self::assertInstanceOf(ObjectData::class, $fetched);
        self::assertSame('hello', $fetched->data);

        $writes = implode('||', $transport->writes);
        // kills Coalesce @ 429 and Ternary @ 431 - the target lookup hits OBJ_other, not OBJ_assets.
        self::assertStringContainsString('$JS.API.DIRECT.GET.OBJ_other', $writes);
        self::assertStringContainsString('$O.other.M.' . $this->encodeName('doc.txt'), $writes);
        self::assertStringContainsString('$O.other.C.' . $nuid, $writes);
    }

    /**
     * A bucket link (no 'name' key) is rejected with the exact message - pins line 426 concat order.
     */
    public function testGetBucketLinkMessageIsExact(): void
    {
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $this->directMetaReply('assets', 'bucket-link', ['options' => ['link' => ['bucket' => 'other-bucket']]], 1),
        ]);

        $client = $this->connect($transport);

        // kills Concat/ConcatOperandRemoval @ 426 (x4) - exact message, name interpolated in place.
        try {
            $client->jetStream()->objectStore('assets')->get('bucket-link')->await();
            self::fail('expected JetStreamException');
        } catch (JetStreamException $e) {
            self::assertSame(
                'Cannot get() the bucket link "bucket-link": it points to a bucket, not an object',
                $e->getMessage(),
            );
        }
    }

    // ------------------------------------------------------------------------------------------
    // getToCallback() link-hop depth and boundary (lines 446, 459, 460, 471)
    // ------------------------------------------------------------------------------------------

    /**
     * getToCallback() must start at depth 0, increment by 1, and treat 8 as inclusive - same boundary
     * proof as get() but for the streaming path (lines 446, 459, 471).
     */
    public function testGetToCallbackResolvesExactlyEightLinkHops(): void
    {
        $frames = [];
        $sid = 1;
        for ($i = 0; $i < 8; $i++) {
            $target = $i === 7 ? 'obj' : 'l' . ($i + 1);
            $frames[] = $this->directMetaReply('assets', 'l' . $i, ['options' => ['link' => ['bucket' => 'assets', 'name' => $target]]], $sid++);
        }
        $frames[] = $this->directMetaReply('assets', 'obj', ['nuid' => 'nuidcbhops8', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')], $sid++);
        $frames[] = $this->directChunkReply('hello', $sid++);

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $this->muxReplies($transport, $frames);
        $client = $this->connect($transport);

        $captured = '';
        // kills IncrementInteger @ 446, GreaterThan @ 459, IncrementInteger @ 471.
        $info = $client->jetStream()->objectStore('assets')->getToCallback(
            'l0',
            static function (string $chunk) use (&$captured): void {
                $captured .= $chunk;
            },
        )->await();

        self::assertSame('hello', $captured);
        self::assertNotNull($info);
        self::assertSame('obj', $info->name);
    }

    /**
     * getToCallback() throws the exact link-hop message - pins line 460 concat order/operands.
     */
    public function testGetToCallbackTooManyHopsMessageIsExact(): void
    {
        $linkMeta = ['options' => ['link' => ['bucket' => 'assets', 'name' => 'loop.txt']]];
        $frames = [];
        for ($i = 1; $i <= 9; $i++) {
            $frames[] = $this->directMetaReply('assets', 'loop.txt', $linkMeta, $i);
        }

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $this->muxReplies($transport, $frames);
        $client = $this->connect($transport);

        // kills Concat/ConcatOperandRemoval @ 460 (x4).
        try {
            $client->jetStream()->objectStore('assets')->getToCallback('loop.txt', static function (string $c): void {})->await();
            self::fail('expected JetStreamException');
        } catch (JetStreamException $e) {
            self::assertSame('Too many Object Store link hops resolving "loop.txt"', $e->getMessage());
        }
    }

    /**
     * getToCallback() must still verify the digest after streaming (line 482). A multi-chunk object
     * with the correct chunk count but corrupted content passes the completeness gate, so only the
     * verifyDigest() call can catch the mismatch; removing it would return the info() silently.
     */
    public function testGetToCallbackVerifiesDigestAfterStreaming(): void
    {
        $consumer = '{"stream_name":"OBJ_assets","name":"EPHCBV","config":{"ack_policy":"none"}}';
        $deleteConsumer = '{"success":true}';
        // Metadata digest is for the clean content, but the delivered chunks are corrupted.
        $meta = $this->directMetaReply('assets', 'doc.txt', ['nuid' => 'nuidcbverify', 'size' => 6, 'chunks' => 2, 'digest' => $this->digestOf('abcdef')], 1);

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
        ]);
        // The two chunks arrive on the pull fetch inbox (sid 2: mux SUB=1, the ephemeral CONSUMER.CREATE
        // is a mux request, fetch SUB=2), not the mux inbox - enqueue on the fetch SUB. Bodies corrupted.
        $transport->enqueueOnWriteContaining = [
            '_INBOX.JS.FETCH' => [
                "MSG _INBOX.JS.FETCH.c 2 3\r\nXXX\r\n",
                "MSG _INBOX.JS.FETCH.c 2 3\r\nYYY\r\n",
            ],
        ];
        $this->muxReplies($transport, [
            $meta,                                                                          // info() Direct Get
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),           // CONSUMER.CREATE
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer), // CONSUMER.DELETE
        ]);

        $client = $this->connect($transport);

        // kills MethodCallRemoval @ 482 - without verifyDigest() this returns info silently.
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Object digest mismatch');
        $client->jetStream()->objectStore('assets')->getToCallback('doc.txt', static function (string $c): void {})->await();
    }

    // ------------------------------------------------------------------------------------------
    // streamChunks() empty-object short-circuit (line 508)
    // ------------------------------------------------------------------------------------------

    /**
     * An object with chunks=0 must short-circuit to the empty-content digest WITHOUT creating an
     * ephemeral consumer. With '<= 0' -> '< 0' the zero case would fall through and create a consumer.
     */
    public function testGetEmptyObjectShortCircuitsWithoutEphemeralConsumer(): void
    {
        // chunks=0, not deleted, digest of the empty string so verifyDigest passes.
        $meta = $this->directMetaReply('assets', 'empty.txt', ['nuid' => 'nuidempty01', 'size' => 0, 'chunks' => 0, 'digest' => $this->digestOf('')], 1);

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
        ]);
        // Only the info() Direct Get is answered; the real code must not pull anything for a 0-chunk object.
        $this->muxReplies($transport, [
            $meta,
        ]);

        $client = $this->connect($transport);

        $fetched = $client->jetStream()->objectStore('assets')->get('empty.txt')->await();

        self::assertInstanceOf(ObjectData::class, $fetched);
        self::assertSame('', $fetched->data);

        // kills LessThanOrEqualTo @ 508 - the zero-chunk case never touches the consumer API.
        $writes = implode('||', $transport->writes);
        self::assertStringNotContainsString('$JS.API.CONSUMER.CREATE', $writes);
        self::assertStringNotContainsString('$JS.API.CONSUMER.MSG.NEXT', $writes);
    }

    // ------------------------------------------------------------------------------------------
    // Ephemeral-consumer download: config + cleanup (lines 549, 585, 587, 543)
    // ------------------------------------------------------------------------------------------

    /**
     * A successful multi-chunk download creates the consumer with deliver_policy=all (line 549) and
     * deletes the consumer afterwards (lines 585 guard true-branch, 587 the delete call itself).
     */
    public function testMultiChunkDownloadSetsDeliverPolicyAndDeletesConsumer(): void
    {
        $chunks = ['abc', 'def'];
        $assembled = implode('', $chunks);
        $meta = $this->directMetaReply('assets', 'doc.txt', ['nuid' => 'nuidmc01', 'size' => 6, 'chunks' => 2, 'digest' => $this->digestOf($assembled)], 1);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPHMC","config":{"ack_policy":"none"}}';
        $deleteConsumer = '{"success":true}';

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
        ]);
        // The two chunks arrive on the pull fetch inbox (sid 2: mux SUB=1, CONSUMER.CREATE is a mux
        // request, fetch SUB=2), not the mux inbox - enqueue them on the fetch SUB.
        $transport->enqueueOnWriteContaining = [
            '_INBOX.JS.FETCH' => [
                "MSG _INBOX.JS.FETCH.c 2 3\r\nabc\r\n",
                "MSG _INBOX.JS.FETCH.c 2 3\r\ndef\r\n",
            ],
        ];
        $this->muxReplies($transport, [
            $meta,                                                                          // info() Direct Get
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),           // CONSUMER.CREATE
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer), // CONSUMER.DELETE
        ]);

        $client = $this->connect($transport);

        $fetched = $client->jetStream()->objectStore('assets')->get('doc.txt')->await();

        self::assertInstanceOf(ObjectData::class, $fetched);
        self::assertSame($assembled, $fetched->data);

        $writes = implode('||', $transport->writes);
        // kills ArrayItemRemoval @ 549 - the consumer config keeps deliver_policy:all.
        self::assertStringContainsString('"deliver_policy":"all"', $writes);
        // kills NotIdentical @ 585 (x2), LogicalAndNegation @ 585, LogicalAndAllSubExprNegation @ 585,
        // and MethodCallRemoval @ 587 - the non-empty consumer name IS deleted.
        self::assertStringContainsString('$JS.API.CONSUMER.DELETE.OBJ_assets.EPHMC', $writes);
    }

    /**
     * When the consumer create FAILS, $consumerName stays null at the finally, so the guard
     * ($consumerName !== null && $consumerName !== '') is false and NO delete is attempted - the
     * original create error propagates as a JetStreamException. The LogicalAnd mutant ('||') makes the
     * guard true for a null name and calls deleteConsumer(stream, null), which is a TypeError under
     * strict_types - a different (non-JetStream) exception type, so the JetStreamException expectation
     * fails and the mutant is killed.
     */
    public function testNullConsumerNameSkipsDeletionOnCreateFailure(): void
    {
        $meta = $this->directMetaReply('assets', 'doc.txt', ['nuid' => 'nuidmc02', 'size' => 6, 'chunks' => 2, 'digest' => $this->digestOf('abcdef')], 1);
        $createError = '{"error":{"code":500,"description":"create boom"}}';

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $meta,                                                                      // info() Direct Get
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($createError), $createError), // CONSUMER.CREATE -> error
            // No delete reply queued: the real code never deletes a null-named consumer.
        ]);

        $client = $this->connect($transport);

        // kills LogicalAnd @ 585 ('&&' -> '||'): the real guard is false (null name), so the create
        // error surfaces as a JetStreamException; the mutant would instead deleteConsumer(stream, null)
        // and raise a TypeError.
        try {
            $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
            self::fail('expected JetStreamException');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('create boom', $e->getMessage());
        }

        // The null-named consumer must NOT trigger a delete request.
        $writes = implode('||', $transport->writes);
        self::assertStringNotContainsString('$JS.API.CONSUMER.DELETE', $writes);
    }

    /**
     * A non-408 fetch error must propagate AND still clean up the consumer via the finally block.
     * Unwrapping the finally (line 543) would let the error escape before the inline cleanup runs, so
     * no CONSUMER.DELETE would be written.
     */
    public function testNonTimeoutFetchErrorStillDeletesConsumerViaFinally(): void
    {
        $meta = $this->directMetaReply('assets', 'doc.txt', ['nuid' => 'nuiderr543', 'size' => 6, 'chunks' => 2, 'digest' => $this->digestOf('abcdef')], 1);
        $consumer = '{"stream_name":"OBJ_assets","name":"EPHFIN","config":{"ack_policy":"none"}}';
        $deleteConsumer = '{"success":true}';
        $status = "NATS/1.0 409 Consumer Deleted\r\nStatus: 409\r\nDescription: Consumer Deleted\r\n\r\n";
        $hb = strlen($status);

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
        ]);
        // The 409 terminal status arrives on the pull fetch inbox (sid 2: mux SUB=1, CONSUMER.CREATE is a
        // mux request, fetch SUB=2), not the mux inbox - enqueue it on the fetch SUB.
        $transport->enqueueOnWriteContaining = [
            '_INBOX.JS.FETCH' => [
                sprintf("HMSG _INBOX.JS.FETCH.c 2 %d %d\r\n%s\r\n", $hb, $hb, $status), // fetch -> 409 (rethrow)
            ],
        ];
        $this->muxReplies($transport, [
            $meta,                                                                          // info() Direct Get
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),           // CONSUMER.CREATE
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer), // CONSUMER.DELETE in finally
        ]);

        $client = $this->connect($transport);

        $threw = false;
        try {
            $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
        } catch (JetStreamException $e) {
            $threw = true;
            self::assertStringContainsString('409', $e->getMessage());
        }

        self::assertTrue($threw, 'expected the 409 fetch error to propagate');

        // kills UnwrapFinally @ 543 - the finally still deletes the consumer even though fetch threw.
        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('$JS.API.CONSUMER.DELETE.OBJ_assets.EPHFIN', $writes);
    }

    // ------------------------------------------------------------------------------------------
    // verifyDigest() mismatch message (line 625)
    // ------------------------------------------------------------------------------------------

    /**
     * The digest-mismatch message must read exactly "expected <stored>, got <computed>" with both
     * operands in place - pins all six concat mutants on line 625.
     */
    public function testDigestMismatchMessageIsExact(): void
    {
        $nuid = 'nuidmsg625';
        $storedDigest = $this->digestOf('hello world'); // metadata claims this...
        $body = 'CORRUPTED!!';                            // ...but the chunk body is this (11 bytes).
        $computedDigest = $this->digestOf($body);
        // Single-chunk fast path: info() reply, then the chunk Direct Get.
        $meta = $this->directMetaReply('assets', 'doc.txt', ['nuid' => $nuid, 'size' => 11, 'chunks' => 1, 'digest' => $storedDigest], 1);

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
        ]);
        $this->muxReplies($transport, [
            $meta,
            $this->directChunkReply($body, 2),
        ]);

        $client = $this->connect($transport);

        // kills Concat/ConcatOperandRemoval @ 625 (x6) - full message text + operand order.
        try {
            $client->jetStream()->objectStore('assets')->get('doc.txt')->await();
            self::fail('expected JetStreamException');
        } catch (JetStreamException $e) {
            self::assertSame(
                'Object digest mismatch: expected ' . $storedDigest . ', got ' . $computedDigest,
                $e->getMessage(),
            );
        }
    }
}
