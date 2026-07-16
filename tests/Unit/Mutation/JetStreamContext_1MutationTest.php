<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\Tests\Support\FakeTransport;

/**
 * Mutation-killing tests for src/JetStream/JetStreamContext.php (chunk 1).
 *
 * Each method pins the exact observable behavior a surviving mutant would change: a thrown
 * message/boundary, the exact bytes written to the FakeTransport, a returned count/value, or the
 * pagination offset sequence. Doubles are driven in-process against FakeTransport (no real sockets).
 */
final class JetStreamContext_1MutationTest extends \PHPUnit\Framework\TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /**
     * Connects a client and arms the muxed request-inbox responder (#118) with the given reply frames.
     *
     * Post-#118 request()/requestMany() no longer subscribe a fresh inbox per call: ONE long-lived
     * wildcard "<base>.*" serves every reply and each request publishes reply-to "<base>.<token>". A
     * pre-seeded fixed-subject "MSG _INBOX.a <sid> ..." therefore never reaches the waiting request and
     * it times out. So instead of seeding the reply frames into the read queue, they are handed to
     * {@see muxReplies()}, which rewrites each frame's subject+sid onto the actual captured reply-to and
     * delivers them FIFO in request order (mirroring a real server).
     *
     * @param list<string> $muxFrames Pre-built MSG/HMSG reply frames; only their subject+sid get rewritten.
     *
     * @return array{NatsClient, FakeTransport}
     */
    private function connect(array $muxFrames): array
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();
        $this->muxReplies($transport, $muxFrames);

        return [$client, $transport];
    }

    private static function msg(string $json, string $inbox = '_INBOX.a', int $sid = 1): string
    {
        return sprintf("MSG %s %d %d\r\n%s\r\n", $inbox, $sid, strlen($json), $json);
    }

    /**
     * Installs a dynamic responder that mirrors a real NATS server against the muxed request inbox
     * (#118). It learns the mux base and sid from the wildcard "SUB <base>.* <sid>" frame; then, for
     * each request PUB/HPUB whose reply-to lives under that base, it pops the next queued reply frame
     * and re-emits it on the CAPTURED reply-to with the mux sid. Frames are delivered FIFO in the order
     * requests are written, so a method that issues several requests (retry loop, create->update,
     * pagination) still gets its replies in sequence. Surplus frames stay unconsumed - a trap frame that
     * a correct control flow must never reach is simply never delivered.
     *
     * @param list<string> $frames
     */
    private function muxReplies(FakeTransport $transport, array $frames): void
    {
        $queue = $frames;
        $base = null;
        $muxSid = 1;

        $transport->onWrite = static function (string $bytes) use (&$queue, &$base, &$muxSid): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            if (str_starts_with($head, 'SUB ')) {
                // Learn the mux base+sid from the wildcard registration "SUB <base>.* <sid>".
                $parts = explode(' ', $head);
                $subject = $parts[1] ?? '';
                if (str_starts_with($subject, '_INBOX.') && str_ends_with($subject, '.*')) {
                    $base = substr($subject, 0, -1); // strip trailing "*" -> "_INBOX.<hex>."
                    $muxSid = (int) ($parts[2] ?? 1);
                }

                return [];
            }

            if ($base === null || (!str_starts_with($head, 'PUB ') && !str_starts_with($head, 'HPUB '))) {
                return [];
            }

            // PUB <subject> <replyTo> <len>  |  HPUB <subject> <replyTo> <hdrLen> <totLen>
            $replyTo = explode(' ', $head)[2] ?? '';
            if (!str_starts_with($replyTo, $base) || $queue === []) {
                return [];
            }

            return [self::rewriteReplyFrame(array_shift($queue), $replyTo, $muxSid)];
        };
    }

    /**
     * Rewrites a pre-built MSG/HMSG reply frame's subject and sid to the request's captured mux reply-to
     * and the mux sid, preserving the payload/header bytes and declared lengths verbatim so the reply's
     * content is delivered unchanged - only its addressing is corrected for the mux inbox.
     */
    private static function rewriteReplyFrame(string $frame, string $replyTo, int $sid): string
    {
        $pos = strpos($frame, "\r\n");
        self::assertNotFalse($pos, 'reply frame must have a head line');

        $tokens = explode(' ', substr($frame, 0, $pos));
        $tokens[1] = $replyTo; // subject
        $tokens[2] = (string) $sid; // mux sid

        return implode(' ', $tokens) . substr($frame, $pos);
    }

    // ── Constructor default: publishRetryAttempts == 3 (lines 54) ─────────────────────────────────

    /**
     * With the DEFAULT publishRetryAttempts (3), two transient 503s followed by an ack must succeed.
     * kills DecrementInteger @ 54 (default 2 would throw on the 2nd 503 before the success arrives).
     */
    public function testDefaultRetryAttemptsAllowsTwoRetriesBeforeSuccess(): void
    {
        $status = "NATS/1.0 503\r\n\r\n";
        $ack = '{"stream":"ORDERS","seq":50,"duplicate":false}';

        [$client, $transport] = $this->connect([
            'HMSG _INBOX.a 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
            'HMSG _INBOX.b 2 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
            self::msg($ack, '_INBOX.c', 3),
        ]);

        // Default attempts (no explicit constructor arg); tiny wait via NatsOptions is irrelevant here.
        $js = new JetStreamContext($client, publishRetryWaitMs: 1);

        $result = $js->publish('orders.created', '{"id":1}')->await();
        self::assertSame(50, $result->seq);
    }

    /**
     * With the DEFAULT publishRetryAttempts (3), a THIRD consecutive 503 (the 3rd attempt) must throw
     * - retries are exhausted.
     * kills IncrementInteger @ 54 (default 4 would make a 3rd 503 followed by an ack succeed instead).
     */
    public function testDefaultRetryAttemptsStopsAfterThreeAttempts(): void
    {
        $status = "NATS/1.0 503\r\n\r\n";
        $ack = '{"stream":"ORDERS","seq":99,"duplicate":false}';

        [$client] = $this->connect([
            'HMSG _INBOX.a 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
            'HMSG _INBOX.b 2 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
            'HMSG _INBOX.c 3 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
            // A 4th frame is an ack - only reached if attempts were (incorrectly) 4.
            self::msg($ack, '_INBOX.d', 4),
        ]);

        $js = new JetStreamContext($client, publishRetryWaitMs: 1);

        $this->expectException(JetStreamException::class);
        $this->expectExceptionCode(503);
        $js->publish('orders.created', '{"id":1}')->await();
    }

    // ── batch() id-length boundary + generated-id length (lines 75, 79) ───────────────────────────

    /**
     * A 64-character batch id is the inclusive upper bound and must be ACCEPTED.
     * kills GreaterThan @ 75 (>= would reject exactly 64).
     */
    public function testBatchIdOf64CharsIsAccepted(): void
    {
        [$client] = $this->connect([]);
        $js = $client->jetStream();

        $id = str_repeat('a', 64);
        $publisher = $js->batch($id);
        self::assertSame($id, $publisher->batchId());
    }

    /**
     * A 65-character batch id exceeds the bound and must be rejected (pins the > boundary from above).
     * kills GreaterThan @ 75 (and the message guard).
     */
    public function testBatchIdOf65CharsIsRejected(): void
    {
        [$client] = $this->connect([]);
        $js = $client->jetStream();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Batch id must be between 1 and 64 characters');
        $js->batch(str_repeat('a', 65));
    }

    /**
     * A generated batch id is bin2hex(random_bytes(16)) = exactly 32 hex chars.
     * kills DecrementInteger @ 79 (15 -> 30 chars) and IncrementInteger @ 79 (17 -> 34 chars).
     */
    public function testGeneratedBatchIdIs32HexChars(): void
    {
        [$client] = $this->connect([]);
        $js = $client->jetStream();

        $id = $js->batch()->batchId();
        self::assertSame(32, strlen($id));
        self::assertMatchesRegularExpression('/^[0-9a-f]{32}$/', $id);
    }

    // ── assertValidBucket() error message concatenation (line 174) ───────────────────────────────

    /**
     * The invalid-bucket message is exactly `Invalid bucket name "<bucket>": only letters, ...` - the
     * anchored match pins operand order and presence of every concat operand.
     * kills Concat @ 174 (leading bucket), Concat @ 174 (trailing bucket),
     *       ConcatOperandRemoval @ 174 (dropped $bucket) and ConcatOperandRemoval @ 174 (dropped suffix).
     */
    public function testInvalidBucketMessageHasExactConcatenation(): void
    {
        [$client] = $this->connect([]);
        $js = $client->jetStream();

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessageMatches(
            '/^Invalid bucket name "bad\.name": only letters, digits, "-" and "_" are allowed$/',
        );
        $js->keyValue('bad.name');
    }

    // ── createStream payload includes name (line 198) ────────────────────────────────────────────

    /**
     * createStream merges the stream name into the request body.
     * kills ArrayItemRemoval @ 198 ('name' => $name dropped from the payload).
     */
    public function testCreateStreamPayloadCarriesName(): void
    {
        $reply = '{"config":{"name":"ORDERS","subjects":["orders.*"]}}';
        [$client, $transport] = $this->connect([self::msg($reply)]);

        $client->jetStream()->createStream('ORDERS', ['orders.*'])->await();

        self::assertStringContainsString('"name":"ORDERS"', $transport->writes[3]);
        self::assertStringContainsString('"subjects":["orders.*"]', $transport->writes[3]);
    }

    // ── updateStream payload includes name (line 233) ────────────────────────────────────────────

    /**
     * updateStream merges the stream name into the request body.
     * kills ArrayItemRemoval @ 233 and UnwrapArrayMerge @ 233 (both drop 'name' => $name).
     */
    public function testUpdateStreamPayloadCarriesName(): void
    {
        $reply = '{"config":{"name":"ORDERS","subjects":["orders.*"]}}';
        [$client, $transport] = $this->connect([self::msg($reply)]);

        $client->jetStream()->updateStream('ORDERS', ['max_age' => 123])->await();

        self::assertStringContainsString('$JS.API.STREAM.UPDATE.ORDERS', $transport->writes[3]);
        self::assertStringContainsString('"name":"ORDERS"', $transport->writes[3]);
        // The supplied config field must still be present (proves the merge keeps both operands).
        self::assertStringContainsString('"max_age":123', $transport->writes[3]);
    }

    // ── createOrUpdateStream fallback merges options + subjects into the UPDATE (line 260) ────────

    /**
     * On the create->update fallback, the UPDATE body must carry BOTH the original options and the
     * subjects (array_merge($options, ['subjects' => $subjects])).
     * kills ArrayItemRemoval @ 260 (subjects dropped), UnwrapArrayMerge @ 260 => $options (subjects
     *       dropped) and UnwrapArrayMerge @ 260 => ['subjects'=>$subjects] (options dropped).
     */
    public function testCreateOrUpdateFallbackMergesOptionsAndSubjects(): void
    {
        $createErr = '{"error":{"code":400,"err_code":10058,"description":"stream name already in use"}}';
        $updateOk = '{"config":{"name":"ORDERS","subjects":["orders.*"]}}';

        [$client, $transport] = $this->connect([
            self::msg($createErr, '_INBOX.a', 1),
            self::msg($updateOk, '_INBOX.b', 2),
        ]);

        $client->jetStream()->createOrUpdateStream('ORDERS', ['orders.*'], ['max_age' => 777])->await();

        // Post-#118 there is ONE mux SUB (writes[2]) and NO per-request SUB/UNSUB, so the two request
        // PUBs are writes[3] (create) and writes[4] (update) - the update is no longer at writes[6].
        $update = $transport->writes[4];
        self::assertStringContainsString('$JS.API.STREAM.UPDATE.ORDERS', $update);
        self::assertStringContainsString('"subjects":["orders.*"]', $update); // kills subjects-drop mutants
        self::assertStringContainsString('"max_age":777', $update);           // kills options-drop mutant
    }

    // ── streamNames(): initial offset == 0 and offset key present (lines 276, 280) ────────────────

    /**
     * The first STREAM.NAMES request starts at offset 0 and the offset key is part of the body.
     * kills DecrementInteger @ 276 ($offset = -1 => "offset":-1) and
     *       ArrayItemRemoval @ 280 ([] + $body drops the offset key entirely).
     */
    public function testStreamNamesFirstRequestUsesOffsetZero(): void
    {
        $reply = '{"streams":["ORDERS","EVENTS"],"total":2}';
        [$client, $transport] = $this->connect([self::msg($reply)]);

        $client->jetStream()->streamNames()->await();

        self::assertStringContainsString('"offset":0', $transport->writes[3]);
    }

    // ── streamNames(): subject-filter ternary (line 277) ─────────────────────────────────────────

    /**
     * A concrete non-empty subject filter MUST be sent as `subject` in the body.
     * kills NotIdentical @ 277 (=== null / === ''), LogicalAndAllSubExprNegation @ 277,
     *       LogicalAndNegation @ 277, ArrayItemRemoval @ 277 (=> []) and Ternary @ 277 (branch swap).
     */
    public function testStreamNamesIncludesSubjectFilterWhenProvided(): void
    {
        $reply = '{"streams":["ORDERS"],"total":1}';
        [$client, $transport] = $this->connect([self::msg($reply)]);

        $client->jetStream()->streamNames('orders.>')->await();

        self::assertStringContainsString('"subject":"orders.>"', $transport->writes[3]);
    }

    /**
     * With NO subject filter the body must NOT carry a `subject` key.
     * kills LogicalAnd @ 277 (&& -> ||, which would include the (null) subject) and the Ternary swap.
     */
    public function testStreamNamesOmitsSubjectFilterWhenNull(): void
    {
        $reply = '{"streams":["ORDERS"],"total":1}';
        [$client, $transport] = $this->connect([self::msg($reply)]);

        $client->jetStream()->streamNames()->await();

        self::assertStringNotContainsString('"subject"', $transport->writes[3]);
    }

    /**
     * An EMPTY-string subject filter must also omit the `subject` key (treated like "no filter").
     * kills NotIdentical @ 277 (!== '' -> === ''), which would include an empty subject.
     */
    public function testStreamNamesOmitsSubjectFilterWhenEmptyString(): void
    {
        $reply = '{"streams":["ORDERS"],"total":1}';
        [$client, $transport] = $this->connect([self::msg($reply)]);

        $client->jetStream()->streamNames('')->await();

        self::assertStringNotContainsString('"subject"', $transport->writes[3]);
    }

    // ── streamNames(): pagination loop + filtering (lines 279, 283, 290, 291, 292) ───────────────

    /**
     * Two pages are fetched and concatenated: the loop continues while pages are non-empty and the
     * running count is below the server-reported total.
     * kills DoWhile @ 279 (while(false) => only page 1),
     *       Ternary @ 291 (total = count(names) => stop after page 1),
     *       NotIdentical @ 292 ($page === [] => stop after page 1).
     */
    public function testStreamNamesPaginatesAcrossTwoPages(): void
    {
        $page1 = '{"total":3,"offset":0,"streams":["S1","S2"]}';
        $page2 = '{"total":3,"offset":2,"streams":["S3"]}';

        [$client] = $this->connect([
            self::msg($page1, '_INBOX.a', 1),
            self::msg($page2, '_INBOX.b', 2),
        ]);

        $names = $client->jetStream()->streamNames()->await();

        self::assertSame(['S1', 'S2', 'S3'], $names);
    }

    /**
     * Across THREE pages the offset advances cumulatively (0, 2, 4) - it accumulates, not resets.
     * kills Assignment @ 290 ($offset = count($page) never reaches 4) and
     *       PlusEqual @ 290 ($offset -= count($page) goes negative, never reaches 4).
     */
    public function testStreamNamesAdvancesOffsetCumulatively(): void
    {
        $page1 = '{"total":5,"offset":0,"streams":["S1","S2"]}';
        $page2 = '{"total":5,"offset":2,"streams":["S3","S4"]}';
        $page3 = '{"total":5,"offset":4,"streams":["S5"]}';

        [$client, $transport] = $this->connect([
            self::msg($page1, '_INBOX.a', 1),
            self::msg($page2, '_INBOX.b', 2),
            self::msg($page3, '_INBOX.c', 3),
        ]);

        $names = $client->jetStream()->streamNames()->await();

        self::assertSame(['S1', 'S2', 'S3', 'S4', 'S5'], $names);
        $writes = implode('||', $transport->writes);
        self::assertStringContainsString('"offset":2', $writes);
        self::assertStringContainsString('"offset":4', $writes); // only reachable with cumulative +=
    }

    /**
     * Non-string entries in the `streams` array are filtered out (array_filter(..., 'is_string')).
     * kills UnwrapArrayFilter @ 283 (without the filter, the integer entry would leak into the result).
     */
    public function testStreamNamesFiltersNonStringEntries(): void
    {
        // 123 is a non-string entry the server should never produce; the client must drop it.
        $reply = '{"streams":["A",123,"B"],"total":2}';
        [$client] = $this->connect([self::msg($reply)]);

        $names = $client->jetStream()->streamNames()->await();

        self::assertSame(['A', 'B'], $names);
    }

    // ── deleteStream success coercion (line 322) ─────────────────────────────────────────────────

    /**
     * A delete response WITHOUT a `success` key defaults to false.
     * kills FalseValue @ 322 (?? false -> ?? true would report a phantom success).
     */
    public function testDeleteStreamDefaultsToFalseWhenSuccessAbsent(): void
    {
        $reply = '{}'; // no "success" key
        [$client] = $this->connect([self::msg($reply)]);

        self::assertFalse($client->jetStream()->deleteStream('ORDERS')->await());
    }

    // ── purgeStream purged coercion (line 337) ───────────────────────────────────────────────────

    /**
     * A purge response WITHOUT a `purged` key reports 0 purged.
     * kills DecrementInteger @ 337 (?? -1) and IncrementInteger @ 337 (?? 1).
     */
    public function testPurgeStreamDefaultsToZeroWhenPurgedAbsent(): void
    {
        $reply = '{}'; // no "purged" key
        [$client] = $this->connect([self::msg($reply)]);

        $result = $client->jetStream()->purgeStream('ORDERS')->await();
        self::assertSame(['purged' => 0], $result);
    }

    /**
     * A non-int `purged` value is cast to int (the array return type does NOT re-coerce inner values).
     * kills CastInt @ 337 ((int) removed would leave the string "42" in the returned array).
     */
    public function testPurgeStreamCastsPurgedToInt(): void
    {
        $reply = '{"purged":"42"}'; // string, not int
        [$client] = $this->connect([self::msg($reply)]);

        $result = $client->jetStream()->purgeStream('ORDERS')->await();
        self::assertSame(42, $result['purged']);
    }

    // ── listStreams cumulative offset (line 362) ─────────────────────────────────────────────────

    /**
     * listStreams advances the page offset cumulatively across three pages (0, 2, 4).
     * kills Assignment @ 362 ($offset = count($page) never reaches 4).
     */
    public function testListStreamsAdvancesOffsetCumulatively(): void
    {
        $mk = static fn(array $names, int $offset): string => json_encode([
            'total' => 5,
            'offset' => $offset,
            'streams' => array_map(static fn(string $n): array => ['config' => ['name' => $n, 'subjects' => ['s.>']]], $names),
        ], JSON_THROW_ON_ERROR);

        [$client, $transport] = $this->connect([
            self::msg($mk(['S1', 'S2'], 0), '_INBOX.a', 1),
            self::msg($mk(['S3', 'S4'], 2), '_INBOX.b', 2),
            self::msg($mk(['S5'], 4), '_INBOX.c', 3),
        ]);

        $streams = $client->jetStream()->listStreams()->await();

        self::assertCount(5, $streams);
        self::assertSame(['S1', 'S2', 'S3', 'S4', 'S5'], array_map(static fn($s): string => $s->name, $streams));
        self::assertStringContainsString('"offset":4', implode('||', $transport->writes));
    }

    // ── listConsumers initial offset == 0 (line 381) ─────────────────────────────────────────────

    /**
     * The first CONSUMER.LIST request starts at offset 0.
     * kills DecrementInteger @ 381 ($offset = -1 => "offset":-1).
     */
    public function testListConsumersFirstRequestUsesOffsetZero(): void
    {
        $reply = '{"consumers":[{"stream_name":"ORDERS","name":"A","config":{"durable_name":"A"}}],"total":1}';
        [$client, $transport] = $this->connect([self::msg($reply)]);

        $client->jetStream()->listConsumers('ORDERS')->await();

        self::assertStringContainsString('$JS.API.CONSUMER.LIST.ORDERS', $transport->writes[3]);
        self::assertStringContainsString('"offset":0', $transport->writes[3]);
    }
}
