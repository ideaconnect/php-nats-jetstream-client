<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\KeyValue\KeyValueEntry;
use IDCT\NATS\JetStream\KeyValue\KeyWatchOptions;
use IDCT\NATS\Tests\Support\FakeTransport;

/**
 * Targeted mutation-killing tests for src/JetStream/KeyValue/KeyValueBucket.php (chunk 1).
 *
 * Each test pins the exact observable behavior a surviving mutant would change: the precise stream
 * config bytes written to the FakeTransport, a thrown exception type/message, an entry's typed
 * fields, or whether a watch handler / onCaughtUp signal fired.
 */
final class KeyValueBucket_1MutationTest extends \PHPUnit\Framework\TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /** Direct Get reply (HMSG): stored value as body with Nats-* (+ optional KV-Operation) headers. */
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

    /** Direct Get status-only reply (HMSG) — e.g. a 503 no-responders that triggers the MSG.GET fallback. */
    private function kvDirectStatus(int $sid, int $code, string $description): string
    {
        $hdrs = "NATS/1.0 {$code} {$description}\r\nStatus: {$code}\r\n\r\n";
        $h = strlen($hdrs);

        return sprintf("HMSG _INBOX.x %d %d %d\r\n%s\r\n", $sid, $h, $h, $hdrs);
    }

    private function connect(FakeTransport $transport): NatsClient
    {
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        return $client;
    }

    // ─── create() default stream config (lines 53-57, 64, 79) ────────────────

    /**
     * Pins every default field create() injects into the KV stream config, plus the subjects entry.
     * The options passed deliberately do NOT overlap any default, so array_merge($defaults, $mapped)
     * must still carry all defaults.
     */
    public function testCreateEmitsAllDefaultStreamConfigFields(): void
    {
        $reply = '{"config":{"name":"KV_cfg","subjects":["$KV.cfg.>"]}}';

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        // 'max_bytes' is unrelated to any default field -> defaults must survive array_merge.
        $this->connect($transport)->jetStream()->keyValue('cfg')->create(['max_bytes' => 4096])->await();

        $create = $transport->writes[3];

        // kills ArrayItemRemoval @ 53, Concat @ 54, ConcatOperandRemoval @ 54 (x2)
        self::assertStringContainsString('"description":"KV bucket cfg"', $create);
        // kills DecrementInteger @ 55, IncrementInteger @ 55
        self::assertStringContainsString('"max_msgs_per_subject":1', $create);
        // kills TrueValue @ 56
        self::assertStringContainsString('"allow_direct":true', $create);
        // kills TrueValue @ 57
        self::assertStringContainsString('"allow_rollup_hdrs":true', $create);
        // kills Concat @ 64, ConcatOperandRemoval @ 64 (x2)
        self::assertStringContainsString('"subjects":["$KV.cfg.>"]', $create);
        // kills UnwrapArrayMerge @ 79 (defaults would vanish if $mapped replaced array_merge)
        self::assertStringContainsString('"max_bytes":4096', $create);
    }

    /**
     * array_values() around the mapped sources must reindex to a JSON array. Sources supplied under
     * non-sequential string keys would, without array_values(), serialize as a JSON object.
     */
    public function testCreateSourcesAreReindexedToJsonArray(): void
    {
        $reply = '{"config":{"name":"KV_agg"}}';

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($reply), $reply),
        ]);

        // String keys force array_values() to matter: with it the result is a JSON array `[{...}]`;
        // without it json_encode emits an object `{"x":{...},...}`.
        $this->connect($transport)->jetStream()->keyValue('agg')->create([
            'sources' => ['x' => 'alpha', 'y' => 'beta'],
        ])->await();

        $create = $transport->writes[3];

        // kills UnwrapArrayValues @ 70
        self::assertStringContainsString('"sources":[{"name":"KV_alpha"},{"name":"KV_beta"}]', $create);
    }

    // ─── assertValidKey() guards removed (lines 140, 165, 191, 218, 259) ─────

    /**
     * update() must validate the key before checking the revision. With an invalid key AND a
     * non-positive revision, the real code reports the key error; removing assertValidKey() would let
     * it fall through to the revision error instead.
     */
    public function testUpdateValidatesKeyBeforeRevision(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $kv = $this->connect($transport)->jetStream()->keyValue('cfg');

        // kills MethodCallRemoval @ 140
        try {
            $kv->update('a b', 'v', 0)->await();
            self::fail('Expected a JetStreamException for the invalid key');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('Invalid KV key', $e->getMessage());
        }
    }

    /**
     * delete() must reject an invalid key up front. Without the guard it would attempt to publish to a
     * malformed subject (a different, non-"Invalid KV key" failure).
     */
    public function testDeleteValidatesKey(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $kv = $this->connect($transport)->jetStream()->keyValue('cfg');

        // kills MethodCallRemoval @ 165
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid KV key');
        $kv->delete('a b')->await();
    }

    /**
     * purge() must reject an invalid key up front.
     */
    public function testPurgeValidatesKey(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $kv = $this->connect($transport)->jetStream()->keyValue('cfg');

        // kills MethodCallRemoval @ 191
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Invalid KV key');
        $kv->purge('a b')->await();
    }

    /**
     * get() must reject an invalid key up front, before issuing any Direct Get request. A valid Direct
     * Get reply is queued so that if the guard is removed the mutant completes (returning an entry)
     * instead of hanging — turning a missing "Invalid KV key" rejection into an observable failure.
     */
    public function testGetValidatesKey(): void
    {
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            // Only reached by the mutant (guard removed): a valid Direct Get hit for the would-be request.
            $this->kvDirectReply('$KV.cfg.x', 'blue', 1, 1),
        ]);
        $kv = $this->connect($transport)->jetStream()->keyValue('cfg');

        // kills MethodCallRemoval @ 218
        try {
            $kv->get('a b')->await();
            self::fail('Expected get() to reject the invalid key before any Direct Get request');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('Invalid KV key', $e->getMessage());
        }
    }

    /**
     * getRevision() must validate the key before checking the revision. With an invalid key AND a
     * non-positive revision, removing the guard would surface the revision error instead.
     */
    public function testGetRevisionValidatesKeyBeforeRevision(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $kv = $this->connect($transport)->jetStream()->keyValue('cfg');

        // kills MethodCallRemoval @ 259
        try {
            $kv->getRevision('a b', 0)->await();
            self::fail('Expected a JetStreamException for the invalid key');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('Invalid KV key', $e->getMessage());
        }
    }

    // ─── operationFromHeaders() uppercasing (line 298) ───────────────────────

    /**
     * A lowercase KV-Operation must be upper-cased so 'del' resolves to the DEL tombstone (null value).
     * Without strtoupper() it would not match the 'DEL'/'PURGE' checks and a live empty value leaks.
     */
    public function testOperationFromHeadersUppercasesKvOperation(): void
    {
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            $this->kvDirectReply('$KV.cfg.theme', 'ignored', 3, 1, 'del'),
        ]);

        $entry = $this->connect($transport)->jetStream()->keyValue('cfg')->get('theme')->await();

        self::assertNotNull($entry);
        // kills UnwrapStrToUpper @ 298
        self::assertSame('DEL', $entry->operation);
        self::assertNull($entry->value);
    }

    // ─── getViaStreamMessage() request payload (line 319) ────────────────────

    /**
     * The leader MSG.GET fallback must request the latest record by subject via a `last_by_subj` key.
     * Mutating the array key (`=>` to `>`) or removing the item changes the request payload.
     */
    public function testStreamMessageFallbackRequestsLastBySubject(): void
    {
        $envelope = sprintf(
            '{"message":{"subject":"$KV.cfg.theme","seq":9,"data":"%s"}}',
            base64_encode('blue'),
        );

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            $this->kvDirectStatus(1, 503, 'No Responders'),                        // Direct Get -> fallback
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($envelope), $envelope),  // STREAM.MSG.GET
        ]);

        $entry = $this->connect($transport)->jetStream()->keyValue('cfg')->get('theme')->await();
        self::assertNotNull($entry);
        self::assertSame('blue', $entry->value);

        // Isolate the STREAM.MSG.GET request (the L319 payload). The unmutated Direct Get attempt also
        // carries a `last_by_subj`, so asserting on the conflated write stream would pass spuriously.
        $msgGetWrites = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_contains($w, '$JS.API.STREAM.MSG.GET.KV_cfg'),
        ));
        self::assertCount(1, $msgGetWrites);

        // kills ArrayItem @ 319 (key '=>' -> '>' yields payload [true]),
        //       ArrayItemRemoval @ 319 (payload becomes [])
        self::assertStringContainsString('"last_by_subj":"$KV.cfg.theme"', $msgGetWrites[0]);
    }

    // ─── getViaStreamMessage() error code default (line 325) ─────────────────

    /**
     * When a fallback error carries no `code`, it defaults to 0 and surfaces as a code-0
     * JetStreamException. Decrement/Increment of the default change the propagated code.
     */
    public function testStreamMessageFallbackErrorWithoutCodeUsesZero(): void
    {
        $errorReply = '{"error":{"description":"boom"}}';

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            $this->kvDirectStatus(1, 503, 'No Responders'),
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($errorReply), $errorReply),
        ]);

        $kv = $this->connect($transport)->jetStream()->keyValue('cfg');

        try {
            $kv->get('theme')->await();
            self::fail('Expected a JetStreamException');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('boom', $e->getMessage());
            // kills DecrementInteger @ 325 (default -1), IncrementInteger @ 325 (default 1)
            self::assertSame(0, $e->getCode());
        }
    }

    /**
     * A fallback error whose `code` is the string "404" must be (int)-cast before the strict `=== 404`
     * comparison, so the key reads as missing (null). Without the cast, "404" !== 404 and the error
     * would be thrown instead.
     */
    public function testStreamMessageFallbackCastsStringCodeBeforeComparing(): void
    {
        $errorReply = '{"error":{"code":"404","description":"Message Not Found"}}';

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            $this->kvDirectStatus(1, 503, 'No Responders'),
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($errorReply), $errorReply),
        ]);

        $entry = $this->connect($transport)->jetStream()->keyValue('cfg')->get('theme')->await();

        // kills CastInt @ 325
        self::assertNull($entry);
    }

    // ─── getViaStreamMessage() seq cast (line 355) ───────────────────────────

    /**
     * A fallback message's `seq` must be (int)-cast so the entry revision is a real int. With a string
     * seq and no cast, buildEntry()'s `?int $revision` parameter would receive a string (TypeError).
     */
    public function testStreamMessageFallbackCastsSeqToInt(): void
    {
        $envelope = sprintf(
            '{"message":{"subject":"$KV.cfg.theme","seq":"4","data":"%s"}}',
            base64_encode('blue'),
        );

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            $this->kvDirectStatus(1, 503, 'No Responders'),
            sprintf("MSG _INBOX.y 2 %d\r\n%s\r\n", strlen($envelope), $envelope),
        ]);

        $entry = $this->connect($transport)->jetStream()->keyValue('cfg')->get('theme')->await();

        self::assertNotNull($entry);
        // kills CastInt @ 355 (real: int 4; mutant: string "4" -> TypeError in buildEntry)
        self::assertSame(4, $entry->revision);
    }

    // ─── watch() filter subject (line 374) ───────────────────────────────────

    /**
     * The watch filter subject must be the bucket prefix concatenated with the pattern, in that order.
     */
    public function testWatchFilterSubjectIsPrefixThenPattern(): void
    {
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $this->connect($transport)->jetStream()->keyValue('cfg')->watch(
            static function (KeyValueEntry $entry): void {},
            'theme',
        )->await();

        $createRequest = implode('', $transport->writes);
        // kills Concat @ 374, ConcatOperandRemoval @ 374 (x2)
        self::assertStringContainsString('"filter_subject":"$KV.cfg.theme"', $createRequest);
    }

    // ─── watch() ignoreDeletes suppression (line 400) ────────────────────────

    /**
     * With ignoreDeletes set, a DEL delivery must NOT reach the handler. The mutated condition
     * (`$ignoreDeletes && !$isDelete`) would deliver deletes and suppress live values instead.
     */
    public function testWatchIgnoreDeletesSuppressesDeleteDelivery(): void
    {
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","num_pending":1,"config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';
        $delHdrs = "NATS/1.0\r\nKV-Operation: DEL\r\n\r\n";
        $dh = strlen($delHdrs);

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            // DEL delivery on the KV subject, carrying a parseable $JS.ACK reply (sid 2).
            sprintf("HMSG \$KV.cfg.theme 2 \$JS.ACK.KV_cfg.KVWATCH.1.7.1.0.1 %d %d\r\n%s\r\n", $dh, $dh, $delHdrs),
        ]);

        $client = $this->connect($transport);

        $called = false;
        $client->jetStream()->keyValue('cfg')->watch(
            static function (KeyValueEntry $entry) use (&$called): void {
                $called = true;
            },
            '>',
            new KeyWatchOptions(ignoreDeletes: true),
        )->await();

        $client->processIncoming()->await();

        // kills LogicalAndSingleSubExprNegation @ 400
        self::assertFalse($called, 'A DEL delivery must be suppressed when ignoreDeletes is set');
    }

    // ─── watch() in-handler caught-up signal (lines 412, 417) ────────────────

    /**
     * A delivery reporting num_pending=0 in its $JS.ACK reply must fire onCaughtUp exactly once. The
     * consumer starts with num_pending=1 so the immediate (onConsumerCreated) path does not fire — the
     * signal is driven purely by the in-handler caught-up check.
     */
    public function testWatchFiresCaughtUpFromDeliveryWhenPendingZero(): void
    {
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","num_pending":1,"config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none"}}';

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
            // PUT delivery whose ACK reports pending=0 (last token) -> end of initial data.
            "MSG \$KV.cfg.theme 2 \$JS.ACK.KV_cfg.KVWATCH.1.7.1.0.0 4\r\nblue\r\n",
        ]);

        $client = $this->connect($transport);

        $seen = null;
        $caughtUpCount = 0;
        $client->jetStream()->keyValue('cfg')->watch(
            static function (KeyValueEntry $entry) use (&$seen): void {
                $seen = $entry;
            },
            '>',
            new KeyWatchOptions(onCaughtUp: static function () use (&$caughtUpCount): void {
                $caughtUpCount++;
            }),
        )->await();

        // Immediate path must NOT fire (num_pending=1 at creation).
        self::assertSame(0, $caughtUpCount);

        $client->processIncoming()->await();

        self::assertInstanceOf(KeyValueEntry::class, $seen);
        // kills LogicalNot @ 412, LogicalAndAllSubExprNegation @ 412,
        //       DecrementInteger @ 417, Identical @ 417
        self::assertSame(1, $caughtUpCount, 'onCaughtUp must fire once when a delivery reports pending=0');
    }

    // ─── watch() immediate caught-up on empty bucket (line 429) ──────────────

    /**
     * When the created consumer reports no pending (num_pending field absent -> default 0), onCaughtUp
     * must fire immediately, before any delivery. Mutating the default to -1/1 would never fire it.
     */
    public function testWatchFiresCaughtUpImmediatelyWhenNumPendingFieldAbsent(): void
    {
        // num_pending is deliberately OMITTED so the `?? 0` default is exercised.
        $createReply = '{"stream_name":"KV_cfg","name":"KVWATCH","config":{"deliver_subject":"_INBOX.JS.PUSH.x","ack_policy":"none","deliver_policy":"last_per_subject"}}';

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply), $createReply),
        ]);

        $caughtUp = false;
        // No deliveries queued: the signal must fire purely from the absent-num_pending default.
        $this->connect($transport)->jetStream()->keyValue('cfg')->watch(
            static function (KeyValueEntry $entry): void {},
            '>',
            new KeyWatchOptions(onCaughtUp: static function () use (&$caughtUp): void {
                $caughtUp = true;
            }),
        )->await();

        // kills DecrementInteger @ 429, IncrementInteger @ 429
        self::assertTrue($caughtUp, 'onCaughtUp must fire when num_pending defaults to 0');
    }
}
