<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing tests for src/JetStream/KeyValue/KeyValueBucket.php (chunk 3).
 *
 * Each test pins the exact observable behavior a surviving mutant would change: a thrown
 * exception's code/message, the offset bytes written across STREAM.INFO pages, the page-loop
 * termination, the json_decode depth boundary, and the string/int coercions feeding the
 * \Exception constructor (which throws a TypeError under strict_types when the coercion is removed).
 */
final class KeyValueBucket_3MutationTest extends TestCase
{
    /** A minimal connected client driven by a pre-seeded FakeTransport (no real sockets). */
    private function connected(FakeTransport $transport): NatsClient
    {
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        return $client;
    }

    /**
     * Builds a transport read queue: the INFO + PONG handshake followed by the supplied reply frames.
     *
     * @return list<string>
     */
    private function queue(string ...$frames): array
    {
        return array_values([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            ...$frames,
        ]);
    }

    /** Builds a plain MSG reply frame on the given sid. */
    private function msg(int $sid, string $payload): string
    {
        return sprintf("MSG _INBOX.x %d %d\r\n%s\r\n", $sid, strlen($payload), $payload);
    }

    /** Builds a Direct Get status-only HMSG reply (e.g. a 404 miss). */
    private function directStatus(int $sid, int $code, string $description): string
    {
        $hdrs = "NATS/1.0 {$code} {$description}\r\nStatus: {$code}\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s\r\n", $sid, $h, $h, $hdrs);
    }

    // ─── line 698: STREAM.INFO error code default + coalesce ─────────────────

    /**
     * A STREAM.INFO error with no `code` field must produce a JetStreamException whose code is
     * exactly 0 (the coalesce default), not -1 or 1.
     */
    public function testStreamInfoErrorCodeDefaultsToZeroWhenCodeAbsent(): void
    {
        // kills DecrementInteger @ 698 (0 -> -1), IncrementInteger @ 698 (0 -> 1)
        $error = '{"error":{"description":"stream info failed"}}';

        $transport = new FakeTransport($this->queue(
            $this->msg(1, $error),
        ));

        try {
            $this->connected($transport)->jetStream()->keyValue('cfg')->getAll()->await();
            self::fail('Expected JetStreamException for STREAM.INFO error');
        } catch (JetStreamException $e) {
            self::assertSame('stream info failed', $e->getMessage());
            self::assertSame(0, $e->getCode());
        }
    }

    /**
     * When the STREAM.INFO error carries a `code`, it must be used (left operand of `??`), not
     * discarded by a flipped coalesce that would always yield 0.
     */
    public function testStreamInfoErrorUsesProvidedCodeNotZero(): void
    {
        // kills Coalesce @ 698 ($error['code'] ?? 0  ->  0 ?? $error['code'])
        $error = '{"error":{"code":500,"description":"boom"}}';

        $transport = new FakeTransport($this->queue(
            $this->msg(1, $error),
        ));

        try {
            $this->connected($transport)->jetStream()->keyValue('cfg')->getAll()->await();
            self::fail('Expected JetStreamException for STREAM.INFO error');
        } catch (JetStreamException $e) {
            self::assertSame(500, $e->getCode());
        }
    }

    // ─── line 717: offset accumulation across pages ─────────────────────────

    /**
     * The paginated STREAM.INFO loop must ADD count($subjects) to the running offset each page, so
     * the second request carries offset=1 and the third carries offset=2. `+=` -> `=` keeps the
     * third request at offset=1; `+=` -> `-=` makes the second request offset=-1.
     */
    public function testPaginationOffsetAccumulatesAcrossPages(): void
    {
        // kills Assignment @ 717 (+= -> =) and PlusEqual @ 717 (+= -> -=)
        // Non-KV-prefixed subjects still drive the pagination offset/newCount but yield no Direct Get
        // lookups in getAll() (keyFromSubject() returns null), keeping the frame sequence simple.
        $page1 = '{"state":{"subjects":{"other.a":1}}}';            // offset 0, +1 new
        $page2 = '{"state":{"subjects":{"other.b":1}}}';            // offset 1, +1 new
        $page3 = '{"state":{"subjects":{"other.a":1}}}';            // offset 2, duplicate -> 0 new -> stop

        $transport = new FakeTransport($this->queue(
            $this->msg(1, $page1),
            $this->msg(2, $page2),
            $this->msg(3, $page3),
        ));

        $all = $this->connected($transport)->jetStream()->keyValue('cfg')->getAll()->await();
        self::assertSame([], $all);

        $infoWrites = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_contains($w, '$JS.API.STREAM.INFO.KV_cfg'),
        ));

        self::assertCount(3, $infoWrites);
        self::assertStringContainsString('"offset":0', $infoWrites[0]);
        self::assertStringContainsString('"offset":1', $infoWrites[1]);
        self::assertStringContainsString('"offset":2', $infoWrites[2]);
    }

    // ─── line 718: page-loop termination guard ──────────────────────────────

    /**
     * The page loop must STOP once a page adds no new subjects (newCount == 0) even though that page
     * is non-empty: `>` -> `>=` would treat 0 as "keep going", and `&&` -> `||` would keep going on
     * the still-non-empty page. Both make an extra STREAM.INFO request.
     */
    public function testPaginationStopsWhenPageAddsNoNewSubjects(): void
    {
        // kills GreaterThan @ 718 (> -> >=) and LogicalAnd @ 718 (&& -> ||)
        // Non-KV-prefixed subjects drive pagination without triggering Direct Get lookups, so the
        // only frames in play are the STREAM.INFO pages - letting us count requests precisely.
        $page1 = '{"state":{"subjects":{"other.a":1}}}';     // +1 new
        $page2 = '{"state":{"subjects":{"other.b":1}}}';     // +1 new
        $page3 = '{"state":{"subjects":{"other.a":1}}}';     // duplicate, newCount 0, non-empty -> real STOPS here
        $page4 = '{"state":{"subjects":{}}}';                // safety page a mutant would consume before stopping

        $transport = new FakeTransport($this->queue(
            $this->msg(1, $page1),
            $this->msg(2, $page2),
            $this->msg(3, $page3),
            $this->msg(4, $page4),
        ));

        $all = $this->connected($transport)->jetStream()->keyValue('cfg')->getAll()->await();
        self::assertSame([], $all);

        $infoCount = count(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_contains($w, '$JS.API.STREAM.INFO.KV_cfg'),
        ));

        // Real code issues exactly 3 STREAM.INFO requests; either mutant consumes page 4 -> 4 requests.
        self::assertSame(3, $infoCount);
    }

    // ─── line 733: json_decode depth boundary ───────────────────────────────

    /**
     * decodeReply must accept JSON nested right at the depth-512 limit. Lowering the limit to 511
     * (DecrementInteger) would reject it as malformed; a valid (if oddly-shaped) reply must decode.
     */
    public function testDecodeReplyAcceptsJsonAtDepthLimit(): void
    {
        // kills DecrementInteger @ 733 (512 -> 511)
        // Nesting level 511 decodes at depth 512/513 but throws at depth 511.
        $deep = str_repeat('[', 511) . str_repeat(']', 511);

        $transport = new FakeTransport($this->queue(
            $this->msg(1, $deep),
        ));

        // The decoded value is a nested list (no 'error'/'state' keys) -> empty subjects -> [].
        $all = $this->connected($transport)->jetStream()->keyValue('cfg')->getAll()->await();
        self::assertSame([], $all);
    }

    /**
     * decodeReply must REJECT JSON nested beyond the depth-512 limit as a malformed reply. Raising
     * the limit to 513 (IncrementInteger) would silently accept it instead of throwing.
     */
    public function testDecodeReplyRejectsJsonBeyondDepthLimit(): void
    {
        // kills IncrementInteger @ 733 (512 -> 513)
        // Nesting level 512 throws at depth 511/512 but decodes at depth 513.
        $tooDeep = str_repeat('[', 512) . str_repeat(']', 512);

        $transport = new FakeTransport($this->queue(
            $this->msg(1, $tooDeep),
        ));

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Malformed JetStream reply');
        $this->connected($transport)->jetStream()->keyValue('cfg')->getAll()->await();
    }

    // ─── line 735: malformed-reply exception message + code ──────────────────

    /**
     * The malformed-reply exception message must be the prefix followed by the underlying cause:
     * a swapped concat would put the cause first (no longer starts with the prefix), and dropping
     * the cause operand would lose the "Syntax error" detail entirely.
     */
    public function testMalformedReplyMessageIsPrefixThenCause(): void
    {
        // kills Concat @ 735 (prefix . cause -> cause . prefix)
        // kills ConcatOperandRemoval @ 735 (drops the cause)
        $transport = new FakeTransport($this->queue(
            $this->msg(1, 'not-json'),
        ));

        try {
            $this->connected($transport)->jetStream()->keyValue('cfg')->delete('theme')->await();
            self::fail('Expected JetStreamException for malformed reply');
        } catch (JetStreamException $e) {
            self::assertStringStartsWith('Malformed JetStream reply: ', $e->getMessage());
            self::assertStringContainsString('Syntax error', $e->getMessage());
        }
    }

    /**
     * The malformed-reply exception code must be exactly 0, not -1 or 1.
     */
    public function testMalformedReplyExceptionCodeIsZero(): void
    {
        // kills DecrementInteger @ 735 (0 -> -1), IncrementInteger @ 735 (0 -> 1)
        $transport = new FakeTransport($this->queue(
            $this->msg(1, 'not-json'),
        ));

        try {
            $this->connected($transport)->jetStream()->keyValue('cfg')->delete('theme')->await();
            self::fail('Expected JetStreamException for malformed reply');
        } catch (JetStreamException $e) {
            self::assertSame(0, $e->getCode());
        }
    }

    // ─── line 778: wrong-last-sequence error code match ──────────────────────

    /**
     * createKey() detects an existing key via the server's "wrong last sequence" error code 10071.
     * Here the error carries code 10071 but a description WITHOUT the "wrong last sequence" phrase,
     * so only the `=== 10071` branch can recognise it. Real code proceeds to get()/recreate and
     * succeeds; shifting the constant to 10070/10072 makes createKey re-throw instead.
     */
    public function testCreateKeyRecognisesWrongLastSequenceByErrCode10071(): void
    {
        // kills DecrementInteger + IncrementInteger on the 10071 constant (10070/10072 would rethrow)
        $firstPutErr = '{"error":{"code":400,"err_code":10071,"description":"precondition not met"}}'; // no "wrong last sequence" text (#154)
        $secondPutAck = '{"stream":"KV_cfg","seq":8,"duplicate":false}';

        $transport = new FakeTransport($this->queue(
            $this->msg(1, $firstPutErr),                          // exclusive create rejected with code 10071
            $this->directStatus(2, 404, 'Message Not Found'),     // get() -> key gone -> revision 0
            $this->msg(3, $secondPutAck),                          // recreate against revision 0 -> success
        ));

        $ack = $this->connected($transport)->jetStream()->keyValue('cfg')->createKey('theme', 'blue')->await();

        self::assertSame(8, $ack->seq);
    }

    // ─── line 795: publish-error description string coercion ─────────────────

    /**
     * publishWithHeadersAck must coerce a non-string `error.description` to string before handing it
     * to the JetStreamException (\Exception) constructor. Without the (string) cast a numeric
     * description is passed as an int and the constructor raises a TypeError under strict_types.
     */
    public function testPublishErrorDescriptionIsCoercedToString(): void
    {
        // kills CastString @ 795 ((string)(...) removed -> int reaches \Exception::__construct)
        $publishErr = '{"error":{"code":1,"description":12345}}'; // description is a JSON number

        $transport = new FakeTransport($this->queue(
            $this->msg(1, $publishErr),
        ));

        try {
            $this->connected($transport)->jetStream()->keyValue('cfg')->delete('theme')->await();
            self::fail('Expected JetStreamException carrying the coerced description');
        } catch (JetStreamException $e) {
            // Real code coerces 12345 -> "12345"; the mutant would have thrown a TypeError instead.
            self::assertSame('12345', $e->getMessage());
        }
    }

    // ─── line 796: publish-error code default / coalesce / int coercion ──────

    /**
     * A publish error with no `code` must yield exception code exactly 0, not -1 or 1.
     */
    public function testPublishErrorCodeDefaultsToZeroWhenAbsent(): void
    {
        // kills DecrementInteger @ 796 (0 -> -1), IncrementInteger @ 796 (0 -> 1)
        $publishErr = '{"error":{"description":"publish rejected"}}';

        $transport = new FakeTransport($this->queue(
            $this->msg(1, $publishErr),
        ));

        try {
            $this->connected($transport)->jetStream()->keyValue('cfg')->delete('theme')->await();
            self::fail('Expected JetStreamException for publish error');
        } catch (JetStreamException $e) {
            self::assertSame(0, $e->getCode());
        }
    }

    /**
     * When the publish error carries a `code`, it must be used (left operand of `??`), not replaced
     * by a flipped coalesce that always yields 0.
     */
    public function testPublishErrorUsesProvidedCodeNotZero(): void
    {
        // kills Coalesce @ 796 ($error['code'] ?? 0  ->  0 ?? $error['code'])
        $publishErr = '{"error":{"code":503,"description":"publish rejected"}}';

        $transport = new FakeTransport($this->queue(
            $this->msg(1, $publishErr),
        ));

        try {
            $this->connected($transport)->jetStream()->keyValue('cfg')->delete('theme')->await();
            self::fail('Expected JetStreamException for publish error');
        } catch (JetStreamException $e) {
            self::assertSame(503, $e->getCode());
        }
    }

    /**
     * publishWithHeadersAck must coerce a non-int `error.code` to int before the JetStreamException
     * (\Exception) constructor, which requires an int $code. A string "503" left uncast would raise
     * a TypeError under strict_types; the real code yields exception code 503.
     */
    public function testPublishErrorCodeIsCoercedToInt(): void
    {
        // kills CastInt @ 796 ((int)(...) removed -> string reaches \Exception::__construct)
        $publishErr = '{"error":{"code":"503","description":"publish rejected"}}'; // code is a JSON string

        $transport = new FakeTransport($this->queue(
            $this->msg(1, $publishErr),
        ));

        try {
            $this->connected($transport)->jetStream()->keyValue('cfg')->delete('theme')->await();
            self::fail('Expected JetStreamException for publish error');
        } catch (JetStreamException $e) {
            self::assertSame(503, $e->getCode());
        }
    }
}
