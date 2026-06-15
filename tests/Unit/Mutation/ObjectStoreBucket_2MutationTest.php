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
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        return $client;
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
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()), // lookup (sid 1)
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),     // chunk1 (sid 2)
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),     // chunk2 (sid 3)
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),     // meta  (sid 4)
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
        $queue = [self::INFO, "PONG\r\n"];

        // 8 links: l0 -> l1 -> ... -> l7 -> obj. info() reply per hop (sids 1..8), then the real object.
        $sid = 1;
        for ($i = 0; $i < 8; $i++) {
            $target = $i === 7 ? 'obj' : 'l' . ($i + 1);
            $queue[] = $this->directMetaReply('assets', 'l' . $i, ['options' => ['link' => ['bucket' => 'assets', 'name' => $target]]], $sid++);
        }
        // Real single-chunk object reached at depth 8.
        $queue[] = $this->directMetaReply('assets', 'obj', ['nuid' => $nuid, 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')], $sid++);
        $queue[] = $this->directChunkReply('hello', $sid++);

        $client = $this->connect(new FakeTransport($queue));

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
        $queue = [self::INFO, "PONG\r\n"];
        for ($i = 1; $i <= 9; $i++) {
            $queue[] = $this->directMetaReply('assets', 'loop.txt', $linkMeta, $i);
        }

        $client = $this->connect(new FakeTransport($queue));

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
            // info('shortcut') in assets -> object link pointing at bucket 'other', name 'doc.txt' (sid 1).
            $this->directMetaReply('assets', 'shortcut', ['options' => ['link' => ['bucket' => 'other', 'name' => 'doc.txt']]], 1),
            // info('doc.txt') resolved in the OTHER bucket (sid 2).
            $this->directMetaReply('other', 'doc.txt', ['nuid' => $nuid, 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')], 2),
            // single chunk Direct Get (sid 3).
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
        $queue = [self::INFO, "PONG\r\n"];
        $sid = 1;
        for ($i = 0; $i < 8; $i++) {
            $target = $i === 7 ? 'obj' : 'l' . ($i + 1);
            $queue[] = $this->directMetaReply('assets', 'l' . $i, ['options' => ['link' => ['bucket' => 'assets', 'name' => $target]]], $sid++);
        }
        $queue[] = $this->directMetaReply('assets', 'obj', ['nuid' => 'nuidcbhops8', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')], $sid++);
        $queue[] = $this->directChunkReply('hello', $sid++);

        $client = $this->connect(new FakeTransport($queue));

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
        $queue = [self::INFO, "PONG\r\n"];
        for ($i = 1; $i <= 9; $i++) {
            $queue[] = $this->directMetaReply('assets', 'loop.txt', $linkMeta, $i);
        }

        $client = $this->connect(new FakeTransport($queue));

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
            $meta,                                                                          // info() (sid 1)
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),           // create consumer (sid 2)
            "MSG _INBOX.JS.FETCH.c 3 3\r\nXXX\r\n",                                         // corrupted chunk 1
            "MSG _INBOX.JS.FETCH.c 3 3\r\nYYY\r\n",                                         // corrupted chunk 2
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer), // delete consumer (sid 4)
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
            $meta, // info() (sid 1) - no further frames; the real code must not pull anything.
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
            $meta,                                                                          // info() (sid 1)
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),           // create consumer (sid 2)
            "MSG _INBOX.JS.FETCH.c 3 3\r\nabc\r\n",
            "MSG _INBOX.JS.FETCH.c 3 3\r\ndef\r\n",
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer), // delete consumer (sid 4)
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
            $meta,                                                                      // info() (sid 1)
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($createError), $createError), // create consumer -> error (sid 2)
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
            $meta,                                                                          // info() (sid 1)
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($consumer), $consumer),           // create consumer (sid 2)
            sprintf("HMSG _INBOX.JS.FETCH.c 3 %d %d\r\n%s\r\n", $hb, $hb, $status),          // fetch -> 409 (rethrow)
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($deleteConsumer), $deleteConsumer), // delete consumer in finally (sid 4)
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
