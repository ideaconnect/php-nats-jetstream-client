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
 * Mutation-killing tests for src/JetStream/ObjectStore/ObjectStoreBucket.php (chunk 4).
 *
 * Each test pins an exact observable behavior (return value, thrown type/code/message, the bytes
 * written to FakeTransport, a count, or a boundary) that a specific surviving mutant would change.
 * Frames are fed deterministically through FakeTransport against the in-process Amp loop - no
 * sockets, no sleeps, no wall-clock timing.
 */
final class ObjectStoreBucket_4MutationTest extends TestCase
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

    private function connectedClient(FakeTransport $transport): NatsClient
    {
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        return $client;
    }

    /**
     * Direct Get reply (HMSG) whose body is the raw meta JSON, with a Nats-Sequence header. list()
     * reads the revision off that header; the body carries the object metadata.
     *
     * @param array<string,mixed> $extra
     */
    private function directMetaReply(string $name, array $extra, int $sid, int $seq = 2): string
    {
        $meta = array_merge([
            'name' => $name,
            'bucket' => 'assets',
            'mtime' => '2030-01-01T00:00:00Z',
            'deleted' => false,
        ], $extra);
        $body = (string) json_encode($meta, JSON_THROW_ON_ERROR);
        $hdrs = "NATS/1.0\r\nNats-Stream: OBJ_assets\r\nNats-Subject: \$O.assets.M." . $this->encodeName($name) . "\r\nNats-Sequence: {$seq}\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s%s\r\n", $sid, $h, $h + strlen($body), $hdrs, $body);
    }

    /** Direct Get status-only reply (HMSG), e.g. a 404 miss or a 503 no-responders. */
    private function directStatusReply(int $sid, int $code, string $description): string
    {
        $hdrs = "NATS/1.0 {$code} {$description}\r\nStatus: {$code}\r\nDescription: {$description}\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s\r\n", $sid, $h, $h, $hdrs);
    }

    /**
     * Builds a STREAM.INFO reply containing a single-page subjects map plus a terminator empty page.
     * Returned as a two-element array of MSG frames (sids assigned by caller).
     *
     * @param array<string,int> $subjects
     * @return array{0:string,1:string}
     */
    private function metaSubjectsPages(array $subjects, int $page1Sid, int $emptySid): array
    {
        $page1 = (string) json_encode(['state' => ['subjects' => $subjects]], JSON_THROW_ON_ERROR);
        $empty = '{"state":{"subjects":{}}}';

        return [
            sprintf("MSG _INBOX.p%d %d %d\r\n%s\r\n", $page1Sid, $page1Sid, strlen($page1), $page1),
            sprintf("MSG _INBOX.p%d %d %d\r\n%s\r\n", $emptySid, $emptySid, strlen($empty), $empty),
        ];
    }

    // ---------------------------------------------------------------------------------------------
    // list() - line 937 Ternary, line 954 Continue_
    // ---------------------------------------------------------------------------------------------

    /**
     * The list() lookup must surface the meta record's stream sequence (Nats-Sequence header) as the
     * ObjectInfo revision. The Ternary mutant swaps the branches so a PRESENT header yields null.
     */
    public function testListSurfacesRevisionFromNatsSequenceHeader(): void
    {
        $enc = $this->encodeName('doc.txt');
        [$p1, $p2] = $this->metaSubjectsPages(['$O.assets.M.' . $enc => 1], 1, 2);

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            $p1,
            $p2,
            // Direct Get for the one meta subject: Nats-Sequence: 11 is present -> revision must be 11.
            $this->directMetaReply('doc.txt', ['nuid' => 'n1', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')], 3, 11),
        ]);

        $objects = $this->connectedClient($transport)->jetStream()->objectStore('assets')->list()->await();

        // kills Ternary @ 937: real -> revision 11; mutated (branches swapped) -> null.
        self::assertCount(1, $objects);
        self::assertSame(11, $objects[0]->revision);
    }

    /**
     * list() must SKIP a deleted record (continue) and keep iterating the rest, not abort the loop
     * (break). With the deleted record positioned first, a `break` would drop the live record that
     * follows it, shrinking the result.
     */
    public function testListContinuesPastDeletedRecordToReturnLaterLiveOnes(): void
    {
        $goneEnc = $this->encodeName('gone.txt');
        $liveEnc = $this->encodeName('live.txt');
        // Order in the subjects map drives the lookup/result order: deleted first, live second.
        [$p1, $p2] = $this->metaSubjectsPages([
            '$O.assets.M.' . $goneEnc => 1,
            '$O.assets.M.' . $liveEnc => 1,
        ], 1, 2);

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            $p1,
            $p2,
            // Concurrent Direct Gets, one per subject, in subjects-map order (sid 3 then sid 4).
            $this->directMetaReply('gone.txt', ['nuid' => '', 'size' => 0, 'chunks' => 0, 'digest' => '', 'deleted' => true], 3),
            $this->directMetaReply('live.txt', ['nuid' => 'n2', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')], 4),
        ]);

        $objects = $this->connectedClient($transport)->jetStream()->objectStore('assets')->list()->await();

        // kills Continue_ @ 954: real -> skips gone.txt, keeps live.txt (count 1, name live.txt);
        // mutated `break` would stop at the deleted record and drop the later live one (count 0).
        self::assertCount(1, $objects);
        self::assertSame('live.txt', $objects[0]->name);
    }

    // ---------------------------------------------------------------------------------------------
    // getStatus() - lines 979/980/981 CastInt
    // ---------------------------------------------------------------------------------------------

    /**
     * getStatus() must coerce the numeric state counters to int. When the server reports them as
     * JSON strings, dropping any of the three (int) casts leaves a string in that field, which
     * assertSame(int, ...) rejects.
     */
    public function testGetStatusCastsCountersToInt(): void
    {
        // messages / last_seq / bytes delivered as STRINGS so the (int) casts are observable.
        $streamReply = (string) json_encode([
            'config' => ['name' => 'OBJ_assets'],
            'state' => [
                'messages' => '42',
                'last_seq' => '100',
                'bytes' => '8192',
                'subjects' => ['$O.assets.M.abc' => 1],
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($streamReply), $streamReply),
        ]);

        $status = $this->connectedClient($transport)->jetStream()->objectStore('assets')->getStatus()->await();

        // kills CastInt @ 979 / 980 / 981: each field must be a true int, not the wire string.
        self::assertSame(42, $status['messages']);
        self::assertSame(100, $status['last_sequence']);
        self::assertSame(8192, $status['bytes']);
    }

    // ---------------------------------------------------------------------------------------------
    // publishMeta() error handling - line 1073 (Decrement/Increment/Coalesce)
    // ---------------------------------------------------------------------------------------------

    /**
     * When the meta-publish ack carries an error object with NO code field, publishMeta must throw a
     * JetStreamException whose code defaults to exactly 0. Decrement(-1)/Increment(1) change the
     * default; Coalesce (0 ?? code) also yields 0 here, so the present-code test below kills it.
     */
    public function testPublishMetaErrorWithoutCodeDefaultsToZero(): void
    {
        // put() of an empty object: meta publish is issued first, then the concurrent lookup.
        $publishError = '{"error":{"description":"publish rejected"}}'; // no "code"
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($publishError), $publishError),  // meta publish -> error
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()), // lookup -> 404
        ]);

        try {
            $this->connectedClient($transport)->jetStream()->objectStore('assets')->put('empty.txt', '')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills DecrementInteger / IncrementInteger @ 1073: default code is 0, not -1 or 1.
            self::assertSame(0, $e->getCode());
            self::assertSame('publish rejected', $e->getMessage());
        }
    }

    /**
     * When the error object DOES carry a code, publishMeta must use it (code ?? 0), not discard it.
     * The Coalesce mutant (0 ?? code) would always yield 0, ignoring the real code.
     */
    public function testPublishMetaErrorUsesProvidedCode(): void
    {
        $publishError = '{"error":{"code":400,"description":"bad request"}}';
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($publishError), $publishError),  // meta publish -> error with code
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),
        ]);

        try {
            $this->connectedClient($transport)->jetStream()->objectStore('assets')->put('empty.txt', '')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills Coalesce @ 1073: the provided code 400 must be used, not flattened to 0.
            self::assertSame(400, $e->getCode());
        }
    }

    // ---------------------------------------------------------------------------------------------
    // metaSubjects() - line 1100 ConcatOperandRemoval, line 1124 (Dec/Inc/Coalesce), 1143 boundary
    // ---------------------------------------------------------------------------------------------

    /**
     * metaSubjects() (driven by list()) must request the subjects map on the STREAM.INFO API subject
     * ($JS.API.STREAM.INFO.OBJ_assets). Dropping the STREAM_INFO_PREFIX concat would request a bare
     * "OBJ_assets" subject instead.
     */
    public function testListRequestsStreamInfoApiSubject(): void
    {
        [$p1, $p2] = $this->metaSubjectsPages([], 1, 2);

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            // First page already empty -> list() returns [] after the single enumeration request.
            $p1,
        ]);

        $this->connectedClient($transport)->jetStream()->objectStore('assets')->list()->await();

        $writes = implode('||', $transport->writes);
        // kills ConcatOperandRemoval @ 1100: the enumeration must target the full STREAM.INFO subject.
        self::assertStringContainsString('$JS.API.STREAM.INFO.OBJ_assets', $writes);
    }

    /**
     * The metaSubjects() enumeration error must carry the provided code, defaulting to exactly 0 when
     * absent. Here the error supplies code 503; list() must rethrow it verbatim.
     */
    public function testMetaSubjectsEnumerationErrorPropagatesProvidedCode(): void
    {
        $error = '{"error":{"code":503,"description":"info unavailable"}}';
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($error), $error),
        ]);

        try {
            $this->connectedClient($transport)->jetStream()->objectStore('assets')->list()->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills Coalesce @ 1124: provided code 503 must be used, not flattened to 0.
            self::assertSame(503, $e->getCode());
            self::assertSame('info unavailable', $e->getMessage());
        }
    }

    /**
     * The metaSubjects() error code defaults to exactly 0 when the error object omits the code.
     */
    public function testMetaSubjectsEnumerationErrorWithoutCodeDefaultsToZero(): void
    {
        $error = '{"error":{"description":"info unavailable"}}'; // no code
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($error), $error),
        ]);

        try {
            $this->connectedClient($transport)->jetStream()->objectStore('assets')->list()->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills DecrementInteger / IncrementInteger @ 1124: default code is 0, not -1 or 1.
            self::assertSame(0, $e->getCode());
        }
    }

    /**
     * The pagination loop must terminate when a (non-empty) page contributes NO new subjects. A page
     * that only repeats already-collected subjects gives newCount == 0; with the real `> 0` guard the
     * loop stops, so exactly TWO enumeration requests are issued (the seed page + the all-duplicate
     * page). The GreaterThan mutant (>= 0) and the LogicalAnd mutant (|| instead of &&) would both
     * keep looping and consume a third request frame that is not queued.
     */
    public function testPaginationStopsWhenPageAddsNoNewSubjects(): void
    {
        $enc = $this->encodeName('a.txt');
        $page = (string) json_encode(['state' => ['subjects' => ['$O.assets.M.' . $enc => 1]]], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            // Page 1 (sid 1): seeds a.txt (newCount 1).
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($page), $page),
            // Page 2 (sid 2): SAME subject again -> newCount 0 -> loop must stop here.
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($page), $page),
            // Direct Get for the one unique subject (sid 3).
            $this->directMetaReply('a.txt', ['nuid' => 'n1', 'size' => 5, 'chunks' => 1, 'digest' => $this->digestOf('hello')], 3),
        ]);

        $objects = $this->connectedClient($transport)->jetStream()->objectStore('assets')->list()->await();

        self::assertCount(1, $objects);
        self::assertSame('a.txt', $objects[0]->name);

        // kills GreaterThan @ 1143 and LogicalAnd @ 1143: exactly two enumeration requests were made
        // (a third loop iteration would have written another STREAM.INFO request - and blocked on a
        // missing frame). Count the enumeration writes precisely.
        $writes = implode('||', $transport->writes);
        self::assertSame(2, substr_count($writes, '$JS.API.STREAM.INFO.OBJ_assets'));
    }

    // ---------------------------------------------------------------------------------------------
    // requestStreamMessage() - line 1154 ArrayItemRemoval, 1163 CastString, 1164 (Dec/Inc/Coalesce/CastInt)
    // Reached via info() 503 fallback -> fetchInfo(name, false) -> requestStreamMessage().
    // ---------------------------------------------------------------------------------------------

    /**
     * requestStreamMessage() must send a "last_by_subj" filter for the meta subject (so the server
     * returns the latest meta record). Removing the array item would send an empty body and ask for
     * the wrong (or no) message.
     */
    public function testRequestStreamMessageSendsLastBySubjFilter(): void
    {
        $enc = $this->encodeName('doc.txt');
        $meta = (string) json_encode([
            'message' => [
                'subject' => '$O.assets.M.' . $enc,
                'seq' => 7,
                'data' => base64_encode((string) json_encode([
                    'name' => 'doc.txt', 'bucket' => 'assets', 'nuid' => 'n1', 'size' => 5,
                    'chunks' => 1, 'digest' => $this->digestOf('hello'), 'deleted' => false,
                ], JSON_THROW_ON_ERROR)),
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            $this->directStatusReply(1, 503, 'No Responders'),                  // info() Direct Get -> 503 fallback
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($meta), $meta),       // STREAM.MSG.GET reply
        ]);

        $info = $this->connectedClient($transport)->jetStream()->objectStore('assets')->info('doc.txt')->await();

        self::assertNotNull($info);
        self::assertSame('doc.txt', $info->name);

        $writes = implode('||', $transport->writes);
        // kills ArrayItemRemoval @ 1154: the STREAM.MSG.GET body must carry the last_by_subj filter
        // for the encoded meta subject.
        self::assertStringContainsString('"last_by_subj":"$O.assets.M.' . $enc . '"', $writes);
    }

    /**
     * requestStreamMessage() error: the description must be cast to string and the code to int. When
     * the server reports a NON-STRING description and a STRING code, the casts are load-bearing - the
     * JetStreamException constructor is strictly typed (string $message, int $code), so dropping a
     * cast would raise a TypeError instead of the expected JetStreamException, and the code must equal
     * the parsed integer (not -1/1 and not flattened to 0).
     */
    public function testRequestStreamMessageErrorCastsDescriptionAndCode(): void
    {
        // description is a number, code is a string -> both casts must run for the throw to succeed.
        $error = '{"error":{"description":1234,"code":"500"}}';
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            $this->directStatusReply(1, 503, 'No Responders'),               // info() Direct Get -> 503 fallback
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($error), $error),  // STREAM.MSG.GET -> error
        ]);

        try {
            $this->connectedClient($transport)->jetStream()->objectStore('assets')->info('doc.txt')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills CastString @ 1163: description cast to string (else TypeError, not this exception).
            self::assertSame('1234', $e->getMessage());
            // kills CastInt @ 1164: "500" cast to int 500; kills Coalesce (would be 0) and +/-1.
            self::assertSame(500, $e->getCode());
        }
    }

    /**
     * requestStreamMessage() error code defaults to exactly 0 when the error omits the code (and the
     * description is present). Pins the (int)(... ?? 0) default against -1/+1.
     */
    public function testRequestStreamMessageErrorWithoutCodeDefaultsToZero(): void
    {
        $error = '{"error":{"description":"api boom"}}'; // no code
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            $this->directStatusReply(1, 503, 'No Responders'),
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($error), $error),
        ]);

        try {
            $this->connectedClient($transport)->jetStream()->objectStore('assets')->info('doc.txt')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills DecrementInteger / IncrementInteger @ 1164: default code is 0, not -1 or 1.
            self::assertSame(0, $e->getCode());
            self::assertSame('api boom', $e->getMessage());
        }
    }

    /**
     * requestStreamMessage() must return the WHOLE decoded response so fetchInfo can read its
     * "message" key. The ArrayOneItem mutant slices the array to its first key only; with "message"
     * placed AFTER another top-level key, that slice drops "message" and fetchInfo would return null.
     */
    public function testRequestStreamMessageReturnsFullResponseNotFirstItemOnly(): void
    {
        $enc = $this->encodeName('doc.txt');
        $metaJson = (string) json_encode([
            'name' => 'doc.txt', 'bucket' => 'assets', 'nuid' => 'n1', 'size' => 5,
            'chunks' => 1, 'digest' => $this->digestOf('hello'), 'deleted' => false,
        ], JSON_THROW_ON_ERROR);
        // "type" deliberately precedes "message" so array_slice(...,0,1) would keep "type" and lose
        // "message", which is what fetchInfo() actually consumes.
        $response = (string) json_encode([
            'type' => 'io.nats.jetstream.api.v1.stream_msg_get_response',
            'message' => [
                'subject' => '$O.assets.M.' . $enc,
                'seq' => 9,
                'data' => base64_encode($metaJson),
            ],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            $this->directStatusReply(1, 503, 'No Responders'),                  // info() Direct Get -> 503 fallback
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($response), $response),
        ]);

        $info = $this->connectedClient($transport)->jetStream()->objectStore('assets')->info('doc.txt')->await();

        // kills ArrayOneItem @ 1168: real returns the full response (message present) -> object found;
        // mutated keeps only the first key ("type") -> message lost -> info() would be null.
        self::assertNotNull($info);
        self::assertSame('doc.txt', $info->name);
        self::assertSame(9, $info->revision);
    }

    // ---------------------------------------------------------------------------------------------
    // nuid() - line 1222 (Decrement/Increment)
    // ---------------------------------------------------------------------------------------------

    /**
     * nuid() must be bin2hex(random_bytes(11)) == 22 hex chars. random_bytes(10) -> 20 chars,
     * random_bytes(12) -> 24 chars. The NUID is surfaced as ObjectInfo->nuid after a put().
     */
    public function testNuidIs22HexChars(): void
    {
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($this->notFound()), $this->notFound()),  // lookup -> 404
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($this->pubAck(1)), $this->pubAck(1)),      // chunk ack
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($this->pubAck(2)), $this->pubAck(2)),      // meta ack
        ]);

        $stored = $this->connectedClient($transport)->jetStream()->objectStore('assets')->put('logo.txt', 'hello')->await();

        // kills DecrementInteger / IncrementInteger @ 1222: 11 bytes -> exactly 22 lowercase hex chars.
        self::assertSame(22, strlen($stored->nuid));
        self::assertSame(1, preg_match('/^[0-9a-f]{22}$/', $stored->nuid));
    }
}
