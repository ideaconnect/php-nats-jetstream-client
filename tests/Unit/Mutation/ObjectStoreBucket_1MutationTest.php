<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\ObjectStore\ObjectStoreBucket;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing unit tests for {@see ObjectStoreBucket} (chunk 1). Each test pins the exact
 * observable behaviour a surviving Infection mutant would change (a written wire frame, an
 * ObjectInfo field, a thrown exception, a boundary). Source must NOT be modified.
 */
final class ObjectStoreBucket_1MutationTest extends TestCase
{
    private const INFO_LINE = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    private function digestOf(string $data): string
    {
        return 'SHA-256=' . strtr(base64_encode(hash('sha256', $data, true)), '+/', '-_');
    }

    /** Direct Get reply carrying a live stored-object meta record for the link target check. */
    private function liveTargetMeta(string $name, string $bucket, int $sid): string
    {
        $body = (string) json_encode([
            'name' => $name,
            'bucket' => $bucket,
            'nuid' => 'ntgt',
            'size' => 3,
            'chunks' => 1,
            'mtime' => '2030-01-01T00:00:00Z',
            'deleted' => false,
        ], JSON_THROW_ON_ERROR);
        $enc = strtr(base64_encode($name), '+/', '-_');
        $hdrs = "NATS/1.0\r\nNats-Stream: OBJ_{$bucket}\r\nNats-Subject: \$O.{$bucket}.M.{$enc}\r\nNats-Sequence: 2\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s%s\r\n", $sid, $h, $h + strlen($body), $hdrs, $body);
    }

    private function notFound(): string
    {
        return '{"error":{"code":404,"description":"message not found"}}';
    }

    private function pubAck(int $seq): string
    {
        return sprintf('{"stream":"OBJ_assets","seq":%d,"duplicate":false}', $seq);
    }

    /** @param list<string> $reads */
    private function client(array $reads): NatsClient
    {
        // Post-#118 the request inbox is MUXED: request()/requestWithHeaders() no longer subscribe a
        // fresh inbox per call; instead ONE long-lived wildcard "<base>.*" (sid 1) serves every reply,
        // and each request publishes reply-to "<base>.<token>" and routes by that token. Every Object
        // Store operation that awaits a reply (lookup STREAM.MSG.GET, chunk/meta JS publish acks,
        // STREAM.CREATE/UPDATE) funnels through request(), so a pre-seeded fixed-subject "MSG _INBOX.x
        // <sid>" no longer reaches the waiting request and it would time out. Deliver the same reply
        // frames DYNAMICALLY on the captured mux reply-to instead of pre-seeding them (see muxReplies()).
        $transport = new FakeTransport([self::INFO_LINE, "PONG\r\n"]);
        $this->muxReplies($transport, $reads);
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();
        $this->lastTransport = $transport;

        return $client;
    }

    private FakeTransport $lastTransport;

    /**
     * Installs a dynamic responder that mirrors a real mux server: it learns the mux base from the
     * wildcard SUB "<base>.* <sid>" and, for each request PUB/HPUB (a publish whose reply-to lives under
     * that base), pops the next reply frame FIFO and re-emits it on the CAPTURED reply-to with the mux
     * sid (1). Frames are delivered in the order requests are written - the same request/SID order these
     * tests already encoded when they pre-seeded MSG _INBOX.a/b/c with sids 1/2/3 (put() runs its
     * previous-revision lookup eagerly first, then the chunk publish(es), then the meta publish).
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
                // A plain publish, or a subscription's own inbox - not a mux request.
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

    private function writes(): string
    {
        return implode('||', $this->lastTransport->writes);
    }

    /**
     * create(): the default stream config (description / allow_direct / subjects array) is serialized
     * onto the STREAM.CREATE request.
     */
    public function testCreateSerializesDefaultDescriptionAndDirectAndSubjects(): void
    {
        $client = $this->client([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen('{"config":{"name":"OBJ_assets"}}'), '{"config":{"name":"OBJ_assets"}}'),
        ]);

        $client->jetStream()->objectStore('assets')->create()->await();
        $w = $this->writes();

        // kills ArrayItemRemoval @ 79, Concat/ConcatOperandRemoval @ 80 - exact description string.
        self::assertStringContainsString('"description":"Object Store bucket assets"', $w);
        self::assertStringNotContainsString('"description":"assetsObject Store bucket "', $w);
        self::assertStringNotContainsString('"description":"assets"', $w);
        self::assertStringNotContainsString('"description":"Object Store bucket "', $w);

        // kills TrueValue @ 81 - allow_direct defaults to true, never false.
        self::assertStringContainsString('"allow_direct":true', $w);
        self::assertStringNotContainsString('"allow_direct":false', $w);

        // kills Concat / ConcatOperandRemoval / ArrayItemRemoval @ 88 - both subjects present, each
        // suffixed with '>' in the correct order, no operand swap or dropped item.
        self::assertStringContainsString('"subjects":["$O.assets.C.>","$O.assets.M.>"]', $w);
        self::assertStringNotContainsString('">$O.assets.C."', $w);
    }

    /**
     * seal(): array_filter() must DROP empty-array config fields (which would re-encode as a JSON
     * array the server rejects) while preserving scalar config on the STREAM.UPDATE.
     */
    public function testSealDropsEmptyArrayConfigFields(): void
    {
        // consumer_limits {} decodes to PHP [] ; max_bytes is a scalar that must survive.
        $info = '{"config":{"name":"OBJ_assets","max_bytes":1000,"consumer_limits":{}}}';
        $updated = '{"config":{"name":"OBJ_assets","sealed":true}}';

        $client = $this->client([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($info), $info),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($updated), $updated),
        ]);

        self::assertTrue($client->jetStream()->objectStore('assets')->seal()->await());
        $w = $this->writes();

        // kills UnwrapArrayFilter @ 110 - the empty-array field is removed; without array_filter it
        // would be re-sent (as an empty JSON array) on the UPDATE.
        self::assertStringContainsString('"sealed":true', $w);
        self::assertStringContainsString('"max_bytes":1000', $w);
        self::assertStringNotContainsString('"consumer_limits"', $w);
    }

    /**
     * addLink(): an empty name is rejected (assertValidName side effect) and the explicit target
     * bucket - not this bucket - is recorded in the link.
     */
    public function testAddLinkValidatesNameAndUsesExplicitTargetBucket(): void
    {
        $client = $this->client([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),  // name-free check
            $this->liveTargetMeta('real.bin', 'other', 2),                                      // target lives in OBJ_other
            sprintf("MSG _INBOX.b 3 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),
        ]);
        $bucket = $client->jetStream()->objectStore('assets');

        // kills Coalesce @ 131 - targetBucket ('other') is used, not $this->bucket ('assets').
        $link = $bucket->addLink('shortcut', 'real.bin', 'other')->await();
        self::assertSame(['bucket' => 'other', 'name' => 'real.bin'], $link->link);
        self::assertStringContainsString('"link":{"bucket":"other","name":"real.bin"}', $this->writes());
    }

    public function testAddLinkRejectsEmptyName(): void
    {
        $client = $this->client([]);

        // kills MethodCallRemoval @ 129 - without assertValidName the empty name would be accepted.
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid object name');
        $client->jetStream()->objectStore('assets')->addLink('', 'real.bin')->await();
    }

    public function testAddBucketLinkRejectsEmptyName(): void
    {
        $client = $this->client([]);

        // kills MethodCallRemoval @ 147 - addBucketLink must validate the name too.
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid object name');
        $client->jetStream()->objectStore('assets')->addBucketLink('', 'target-bucket')->await();
    }

    /**
     * linkMeta(): the link record carries the object name, this bucket, size=0, chunks=0 and
     * deleted=false (a live link, not a tombstone).
     */
    public function testLinkMetaFieldsExactValues(): void
    {
        $client = $this->client([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),  // name-free check
            $this->liveTargetMeta('real.bin', 'assets', 2),                                     // live target
            sprintf("MSG _INBOX.b 3 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),
        ]);

        $link = $client->jetStream()->objectStore('assets')->addLink('shortcut', 'real.bin')->await();
        $w = $this->writes();

        // kills ArrayItemRemoval @ 164 - the 'name' key must be present (ObjectInfo->name resolves it).
        self::assertSame('shortcut', $link->name);
        self::assertStringContainsString('"name":"shortcut"', $w);

        // kills ArrayItem @ 166 - 'bucket' => $this->bucket stays a real keyed array entry. The
        // mutation ('bucket' > $this->bucket) drops the key and inserts "0":true at the top level
        // (the inner options.link.bucket would still read "bucket":"assets", so we pin the top-level
        // key directly after "name" and reject the numeric-index artifact).
        self::assertSame('assets', $link->bucket);
        self::assertStringContainsString('"name":"shortcut","bucket":"assets"', $w);
        self::assertStringNotContainsString('"0":true', $w);

        // kills Increment/DecrementInteger @ 168 - size is exactly 0 for a link (no content).
        self::assertSame(0, $link->size);
        self::assertStringContainsString('"size":0', $w);
        self::assertStringNotContainsString('"size":1', $w);
        self::assertStringNotContainsString('"size":-1', $w);

        // kills Increment/DecrementInteger @ 169 - chunks is exactly 0 for a link.
        self::assertSame(0, $link->chunks);
        self::assertStringContainsString('"chunks":0', $w);
        self::assertStringNotContainsString('"chunks":1', $w);
        self::assertStringNotContainsString('"chunks":-1', $w);

        // kills FalseValue @ 172 - a freshly created link is NOT deleted.
        self::assertFalse($link->deleted);
        self::assertStringContainsString('"deleted":false', $w);
        self::assertStringNotContainsString('"deleted":true', $w);
    }

    /**
     * put() small payload (<= chunkSize): writes exactly one chunk PUB, records bucket, deleted=false,
     * the options.max_chunk_size, and (with a non-empty description) the description field.
     */
    public function testPutSmallObjectInfoFields(): void
    {
        $client = $this->client([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),
        ]);

        // chunkSize 5, payload 'hello' is exactly 5 bytes -> the <= boundary single-publish path.
        $store = new ObjectStoreBucket($client, $client->jetStream(), 'assets', 5);
        $stored = $store->put('logo.txt', 'hello')->await();
        $w = $this->writes();

        self::assertSame(1, $stored->chunks);
        self::assertSame(5, $stored->size);

        // boundary @ 213 (LessThanOrEqualTo) is EQUIVALENT under a fake transport: both the <= single
        // -publish branch and the < else/pipeline branch produce chunks=1 and one identical PUB frame.
        // The assertion below still pins the correct single-chunk behaviour.
        self::assertSame(1, substr_count($w, 'PUB $O.assets.C.' . $stored->nuid . ' '));

        // kills ArrayItem @ 242 - 'bucket' stays a real keyed entry directly after "name"
        // (the comparison mutation drops the key and inserts a numeric-index boolean).
        self::assertSame('assets', $stored->bucket);
        self::assertStringContainsString('"name":"logo.txt","bucket":"assets"', $w);
        self::assertStringNotContainsString('"0":', $w);

        // kills FalseValue @ 248 - a stored object is not deleted.
        self::assertFalse($stored->deleted);
        self::assertStringContainsString('"deleted":false', $w);

        // kills ArrayItem / ArrayItemRemoval @ 249 - options.max_chunk_size present with the value
        // (ArrayItem -> 'max_chunk_size' > chunkSize would drop the key; ArrayItemRemoval -> []).
        self::assertStringContainsString('"options":{"max_chunk_size":5}', $w);
    }

    public function testPutBoundaryExactChunkSizeIsSingleChunk(): void
    {
        // Pins the exact boundary (size == chunkSize -> chunks=1, one publish). NOTE: the @213
        // LessThanOrEqualTo->LessThan mutant is EQUIVALENT here - the < else branch also yields
        // chunks=1 with the same single PUB frame; this test documents the intended behaviour.
        $client = $this->client([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),
        ]);

        $store = new ObjectStoreBucket($client, $client->jetStream(), 'assets', 4);
        $stored = $store->put('exact.bin', 'abcd')->await(); // 4 bytes == chunkSize 4

        self::assertSame(4, $stored->size);
        self::assertSame(1, $stored->chunks);
        self::assertSame(1, substr_count($this->writes(), 'PUB $O.assets.C.' . $stored->nuid . ' '));
    }

    public function testPutEmptyDescriptionIsOmitted(): void
    {
        $client = $this->client([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),
        ]);

        // kills LogicalAnd @ 252 - with '||' an empty-string description ('' !== null is true) would
        // still be added as "description":""; with the real '&&' it must be omitted.
        $client->jetStream()->objectStore('assets')->put('logo.txt', 'hi', [], '')->await();
        self::assertStringNotContainsString('"description"', $this->writes());
    }

    public function testPutNonEmptyDescriptionIsIncluded(): void
    {
        $client = $this->client([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),
        ]);

        // kills LogicalAnd @ 252 - a real description (both clauses true) IS added.
        $client->jetStream()->objectStore('assets')->put('logo.txt', 'hi', [], 'hello')->await();
        self::assertStringContainsString('"description":"hello"', $this->writes());
    }

    public function testPutStreamRejectsEmptyName(): void
    {
        $client = $this->client([]);

        // kills MethodCallRemoval @ 283 - putStream must validate the name.
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid object name');
        $client->jetStream()->objectStore('assets')->putStream('', static fn(): ?string => null)->await();
    }

    /**
     * putStream() accumulates totalSize and the buffer across multiple producer blocks (so a multi-block
     * object is sized and chunked from the concatenation, not just the last block).
     */
    public function testPutStreamAccumulatesSizeAndBufferAcrossBlocks(): void
    {
        $client = $this->client([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),   // lookup
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),       // chunk
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),       // meta
        ]);

        // chunkSize 100 so the two small blocks accumulate into one tail chunk at end-of-stream.
        $store = new ObjectStoreBucket($client, $client->jetStream(), 'assets', 100);
        $blocks = ['abc', 'de'];
        $i = 0;
        $stored = $store->putStream('s.bin', static function () use (&$i, $blocks): ?string {
            return $blocks[$i++] ?? null;
        })->await();

        // kills Assignment @ 318 - totalSize must be 5 (3+2), not 2 (last block only).
        self::assertSame(5, $stored->size);

        $w = $this->writes();
        self::assertSame(1, $stored->chunks);
        self::assertSame(1, substr_count($w, 'PUB $O.assets.C.' . $stored->nuid . ' '));

        // kills Assignment @ 319 - the buffer ACCUMULATES, so the single tail chunk's published body
        // is the whole "abcde" (5 bytes). With '$buffer = $block' it would be only the last block "de".
        // (The digest is computed from per-block hash_update, so it is unaffected by this mutation -
        // the published chunk bytes are the discriminating signal.)
        self::assertStringContainsString(" 5\r\nabcde\r\n", $w);
        self::assertStringNotContainsString(" 2\r\nde\r\n", $w);
    }

    public function testPutStreamInfoCarriesNameField(): void
    {
        $client = $this->client([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),
        ]);

        $store = new ObjectStoreBucket($client, $client->jetStream(), 'assets', 100);
        $blocks = ['hello'];
        $i = 0;
        $stored = $store->putStream('named.bin', static function () use (&$i, $blocks): ?string {
            return $blocks[$i++] ?? null;
        })->await();

        // kills ArrayItemRemoval @ 343 - the 'name' key must be present in putStream's info record.
        self::assertSame('named.bin', $stored->name);
        self::assertStringContainsString('"name":"named.bin"', $this->writes());
    }

    /**
     * putStream() emits whole chunks via substr at the offset, and the inner/outer length gates are
     * inclusive at the exact chunkSize boundary.
     */
    public function testPutStreamChunkBoundaryAndSubstr(): void
    {
        $client = $this->client([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),   // lookup (sid 1)
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),       // chunk1 (sid 2)
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),       // chunk2 (sid 3)
            sprintf("MSG _INBOX.d 4 %d\r\n%s\r\n", strlen($this->pubAck(3)), $this->pubAck(3)),       // chunk3 (sid 4)
            sprintf("MSG _INBOX.e 5 %d\r\n%s\r\n", strlen($this->pubAck(4)), $this->pubAck(4)),       // meta  (sid 5)
        ]);

        // chunkSize 3; a single 9-byte block (exactly 3*chunkSize) must split into 3 full chunks of 3.
        $store = new ObjectStoreBucket($client, $client->jetStream(), 'assets', 3);
        $blocks = ['abcdefghi'];
        $i = 0;
        $stored = $store->putStream('exact3.bin', static function () use (&$i, $blocks): ?string {
            return $blocks[$i++] ?? null;
        })->await();

        // kills UnwrapSubstr @ 327 (each chunk is exactly 3 bytes from the offset, not the whole
        // buffer). The @324/@326 GreaterThanOrEqualTo->GreaterThan boundary mutants are EQUIVALENT:
        // the trailing tail-flush re-emits whatever the loop leaves, so chunk byte-ranges, count,
        // order and digest are unchanged either way.
        self::assertSame(9, $stored->size);
        self::assertSame(3, $stored->chunks);
        self::assertSame($this->digestOf('abcdefghi'), $stored->digest);

        $w = $this->writes();
        self::assertSame(3, substr_count($w, 'PUB $O.assets.C.' . $stored->nuid . ' '));
        // Each published chunk body is exactly 3 bytes in stream order - the frame ends " <n>\r\n<body>".
        // UnwrapSubstr @ 327 would publish the whole 9-byte buffer ("abcdefghi") instead.
        self::assertStringContainsString(" 3\r\nabc\r\n", $w);
        self::assertStringContainsString(" 3\r\ndef\r\n", $w);
        self::assertStringContainsString(" 3\r\nghi\r\n", $w);
        self::assertStringNotContainsString('abcdefghi', $w);
    }

    /**
     * put() pipeline window (line 229): with exactly UPLOAD_PIPELINE_DEPTH (16) chunks no chunk is
     * dropped; chunks/size/digest stay correct. NOTE: the @229 GreaterThanOrEqualTo mutants are
     * EQUIVALENT under a fake transport - the in-loop flush only changes await/batch timing; the
     * trailing flush guarantees every chunk is published regardless, so results are identical.
     */
    public function testPutPipelineFlushesAtDepthBoundary(): void
    {
        // 16 chunks of 1 byte each -> at the 16th the (count >= 16) gate flushes the window.
        $reads = [sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound())];
        for ($i = 0; $i < 16; $i++) {
            $reads[] = sprintf("MSG _INBOX.c%d %d %d\r\n%s\r\n", $i, $i + 2, strlen($this->pubAck($i + 1)), $this->pubAck($i + 1));
        }
        $reads[] = sprintf("MSG _INBOX.m 18 %d\r\n%s\r\n", strlen($this->pubAck(99)), $this->pubAck(99)); // meta
        $client = $this->client($reads);

        $store = new ObjectStoreBucket($client, $client->jetStream(), 'assets', 1);
        $data = str_repeat('x', 16);
        $stored = $store->put('many.bin', $data)->await();

        // Pins that all 16 chunks publish with the full-payload size and digest (boundary mutants @229
        // are equivalent here - see the method docblock).
        self::assertSame(16, $stored->size);
        self::assertSame(16, $stored->chunks);
        self::assertSame($this->digestOf($data), $stored->digest);
        self::assertSame(16, substr_count($this->writes(), 'PUB $O.assets.C.' . $stored->nuid . ' '));
    }

    /**
     * putStream() pipeline window (line 301): same depth-boundary contract via the streaming path.
     * NOTE: the @301 GreaterThanOrEqualTo mutants are EQUIVALENT under a fake transport for the same
     * reason as @229 - the trailing flush still publishes every chunk; only await timing changes.
     */
    public function testPutStreamPipelineFlushesAtDepthBoundary(): void
    {
        $reads = [sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound())];
        for ($i = 0; $i < 16; $i++) {
            $reads[] = sprintf("MSG _INBOX.c%d %d %d\r\n%s\r\n", $i, $i + 2, strlen($this->pubAck($i + 1)), $this->pubAck($i + 1));
        }
        $reads[] = sprintf("MSG _INBOX.m 18 %d\r\n%s\r\n", strlen($this->pubAck(99)), $this->pubAck(99)); // meta
        $client = $this->client($reads);

        $store = new ObjectStoreBucket($client, $client->jetStream(), 'assets', 1);
        $data = str_repeat('y', 16);
        $blocks = [$data];
        $i = 0;
        $stored = $store->putStream('many.bin', static function () use (&$i, $blocks): ?string {
            return $blocks[$i++] ?? null;
        })->await();

        // Pins full-payload chunks/size/digest (boundary mutants @301 are equivalent - see docblock).
        self::assertSame(16, $stored->size);
        self::assertSame(16, $stored->chunks);
        self::assertSame($this->digestOf($data), $stored->digest);
        self::assertSame(16, substr_count($this->writes(), 'PUB $O.assets.C.' . $stored->nuid . ' '));
    }
}
