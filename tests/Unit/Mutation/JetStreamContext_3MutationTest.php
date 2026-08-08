<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsHeaders;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\Tests\Support\FakeTransport;

/**
 * Mutation-killing tests for src/JetStream/JetStreamContext.php (chunk 3).
 *
 * Each test pins the exact observable behaviour a surviving mutant would change: a wire payload,
 * a return value, a thrown message, a collected-message count/ordering, or a boundary on the
 * ordered-consumer recreate logic. Frames are fed via FakeTransport and pumped with processIncoming(),
 * mirroring the existing JetStreamContextTest patterns - no real sockets, no sleeps.
 */
final class JetStreamContext_3MutationTest extends \PHPUnit\Framework\TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    private function jsOk(string $json): string
    {
        return sprintf("MSG _INBOX.any 1 %d\r\n%s\r\n", strlen($json), $json);
    }

    private function connected(FakeTransport $transport): NatsClient
    {
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        return $client;
    }

    /**
     * Post-#118 the request inbox is MUXED: request()/requestWithHeaders() no longer subscribe a fresh
     * inbox per call; instead ONE long-lived wildcard "<base>.*" (sid 1) serves every reply, and each
     * request publishes reply-to "<base>.<token>" and routes by that token. A pre-seeded
     * "MSG _INBOX.x <sid>" therefore no longer reaches the waiting request (its subject is not the
     * request's random "<base>.<token>", so dispatchMuxReply drops it and the request times out).
     *
     * This installs a dynamic responder that mirrors a real server: it learns the mux base from the
     * wildcard SUB, and for each request PUB/HPUB (a publish whose reply-to lives under that base)
     * pops the next reply frame and re-emits it on the CAPTURED reply-to with the mux sid (1). Frames
     * are delivered FIFO in the order requests are written, so a method that issues several requests
     * (e.g. paginated consumerNames) still gets its replies in sequence.
     *
     * Only request replies belong here; subscription deliveries (a push consumer's deliver subject,
     * an ordered consumer's rotated deliver inbox) are NOT mux replies - use orderedConsumerServer().
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
     * Extracts the mux reply-to (the 3rd token) from a written PUB/HPUB request frame, or '' when the
     * frame is not a request publish. Lets a dynamic onWrite responder echo a reply on the request's
     * actual reply-to (the mux "<base>.<token>") instead of a guessed inbox subject (#118).
     */
    private static function requestReplyTo(string $bytes): string
    {
        $head = strtok($bytes, "\r\n");
        if ($head === false || (!str_starts_with($head, 'PUB ') && !str_starts_with($head, 'HPUB '))) {
            return '';
        }

        return explode(' ', $head)[2] ?? '';
    }

    /**
     * Builds a mux reply frame (MSG on the given reply-to, mux sid 1) carrying $payload - the wire form
     * the server sends back on the shared request inbox (#118).
     */
    private static function muxMsg(string $replyTo, string $payload): string
    {
        return sprintf("MSG %s 1 %d\r\n%s\r\n", $replyTo, strlen($payload), $payload);
    }

    /**
     * Installs a mux-aware server for ordered consumer recreate tests. Pre-#118 each request owned a
     * per-request SUB, so a CONSUMER.CREATE/DELETE reply routed by a fixed sid and the rotated deliver
     * inbox landed at a sid shifted by those per-request subs (2,4,5,...). Post-#118 all requests share
     * the mux inbox (no per-request SUB), so: (a) CREATE/DELETE replies must be echoed on the request's
     * captured reply-to, and (b) the rotated deliver sid collapses to the next free sid (2,3,4,...). This
     * responder removes both hazards by construction: request replies go on the captured reply-to, and
     * each deliver epoch's frames are emitted on the deliver SUB using the sid the server ACTUALLY
     * assigned (captured live), so tests never hard-code a sid.
     *
     * @param list<callable(string):list<string>> $onCreate FIFO, one per CONSUMER.CREATE PUB; gets the reply-to.
     * @param list<callable(string):list<string>> $onDelete FIFO, one per CONSUMER.DELETE PUB; gets the reply-to.
     * @param list<callable(int):list<string>> $deliverEpochs FIFO, one per deliver SUB; gets the captured sid.
     */
    private function orderedConsumerServer(
        FakeTransport $transport,
        array $onCreate,
        array $onDelete = [],
        array $deliverEpochs = [],
    ): void {
        // The callback arrays are captured BY VALUE (so PHPStan keeps their callable return types); FIFO
        // consumption is tracked by the by-reference cursors below.
        $createIdx = 0;
        $deleteIdx = 0;
        $deliverIdx = 0;
        $transport->onWrite = static function (string $bytes) use (
            $onCreate,
            $onDelete,
            $deliverEpochs,
            &$createIdx,
            &$deleteIdx,
            &$deliverIdx,
        ): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            if (str_starts_with($head, 'SUB ')) {
                $parts = explode(' ', $head);
                $subject = $parts[1] ?? '';
                // The mux wildcard SUB ends with ".*"; every other SUB here is a (rotated) deliver inbox.
                $epoch = $deliverEpochs[$deliverIdx] ?? null;
                if ($epoch !== null && !str_ends_with($subject, '.*')) {
                    $deliverIdx++;

                    return $epoch((int) ($parts[2] ?? 0));
                }

                return [];
            }

            $replyTo = self::requestReplyTo($bytes);
            if ($replyTo === '') {
                return [];
            }

            $create = $onCreate[$createIdx] ?? null;
            if ($create !== null && str_contains($bytes, '$JS.API.CONSUMER.CREATE.')) {
                $createIdx++;

                return $create($replyTo);
            }

            $delete = $onDelete[$deleteIdx] ?? null;
            if ($delete !== null && str_contains($bytes, '$JS.API.CONSUMER.DELETE.')) {
                $deleteIdx++;

                return $delete($replyTo);
            }

            return [];
        };
    }

    // ─── directGetBatch: collected message shape, unsubscribe, error description ──────────

    /**
     * directGetBatch builds each collected message with sid 0.
     */
    public function testDirectGetBatchCollectedMessageHasSidZero(): void
    {
        $h1 = "NATS/1.0\r\nNats-Stream: ORDERS\r\nNats-Subject: orders.a\r\nNats-Sequence: 5\r\n\r\n";
        $b1 = 'aaa';
        $eob = "NATS/1.0 204 EOB\r\n\r\n";

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s%s\r\n", strlen($h1), strlen($h1) + strlen($b1), $h1, $b1),
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s\r\n", strlen($eob), strlen($eob), $eob),
        ]);

        $client = $this->connected($transport);

        $messages = $client->jetStream()->directGetBatch('ORDERS', ['batch' => 10])->await();

        self::assertCount(1, $messages);
        // kills IncrementInteger @ 679 (sid: 0 -> 1)
        self::assertSame(0, $messages[0]->sid);
    }

    /**
     * directGetBatch unsubscribes from its private inbox once the batch completes (an UNSUB is written).
     */
    public function testDirectGetBatchUnsubscribesAfterCompletion(): void
    {
        $eob = "NATS/1.0 204 EOB\r\n\r\n";

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s\r\n", strlen($eob), strlen($eob), $eob),
        ]);

        $client = $this->connected($transport);

        $client->jetStream()->directGetBatch('ORDERS', ['batch' => 10])->await();

        // kills MethodCallRemoval @ 707 (unsubscribe($sid)->await())
        self::assertStringContainsString('UNSUB ', implode('', $transport->writes));
    }

    /**
     * directGetBatch surfaces the server-provided Description on an error frame, not the fallback text.
     */
    public function testDirectGetBatchSurfacesServerErrorDescription(): void
    {
        // 500 status with an explicit reason that becomes the Description header.
        $err = "NATS/1.0 500 detailed-server-error\r\n\r\n";

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("HMSG _INBOX.JS.DGET.x 1 %d %d\r\n%s\r\n", strlen($err), strlen($err), $err),
        ]);

        $client = $this->connected($transport);

        try {
            $client->jetStream()->directGetBatch('ORDERS', ['batch' => 10])->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills Ternary @ 712 (would swap to the generic fallback message)
            self::assertSame('detailed-server-error', $e->getMessage());
            self::assertSame(500, $e->getCode());
        }
    }

    // ─── Consumer create payloads carry stream_name ──────────────────────────────────────

    /**
     * createConsumer includes stream_name alongside config in the CREATE request body.
     */
    public function testCreateConsumerSendsStreamName(): void
    {
        $reply = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC"}}';
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $this->muxReplies($transport, [$this->jsOk($reply)]);
        $client = $this->connected($transport);

        $client->jetStream()->createConsumer('ORDERS', 'PROC')->await();

        // kills ArrayItemRemoval @ 740
        self::assertStringContainsString('"stream_name":"ORDERS"', $transport->writes[3]);
    }

    /**
     * addConsumer includes stream_name alongside config in the CREATE request body.
     */
    public function testAddConsumerSendsStreamName(): void
    {
        $reply = '{"stream_name":"ORDERS","name":"worker","config":{"durable_name":"worker"}}';
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $this->muxReplies($transport, [$this->jsOk($reply)]);
        $client = $this->connected($transport);

        $config = \IDCT\NATS\JetStream\Configuration\ConsumerConfiguration::create()->durable('worker');
        $client->jetStream()->addConsumer('ORDERS', $config)->await();

        // kills ArrayItemRemoval @ 760
        self::assertStringContainsString('"stream_name":"ORDERS"', $transport->writes[3]);
    }

    /**
     * createEphemeralConsumer includes stream_name in the CREATE request body.
     */
    public function testCreateEphemeralConsumerSendsStreamName(): void
    {
        $reply = '{"stream_name":"ORDERS","name":"E1","config":{}}';
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $this->muxReplies($transport, [$this->jsOk($reply)]);
        $client = $this->connected($transport);

        $client->jetStream()->createEphemeralConsumer('ORDERS')->await();

        // kills ArrayItemRemoval @ 825
        self::assertStringContainsString('"stream_name":"ORDERS"', $transport->writes[3]);
    }

    /**
     * createPushConsumer includes stream_name in the CREATE request body.
     */
    public function testCreatePushConsumerSendsStreamName(): void
    {
        $reply = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC"}}';
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $this->muxReplies($transport, [$this->jsOk($reply)]);
        $client = $this->connected($transport);

        $client->jetStream()->createPushConsumer('ORDERS', 'PROC', '_INBOX.deliver')->await();

        // kills ArrayItemRemoval @ 853
        self::assertStringContainsString('"stream_name":"ORDERS"', $transport->writes[3]);
    }

    /**
     * createEphemeralPushConsumer includes stream_name in the CREATE request body.
     */
    public function testCreateEphemeralPushConsumerSendsStreamName(): void
    {
        $reply = '{"stream_name":"ORDERS","name":"E1","config":{"deliver_subject":"d"}}';
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $this->muxReplies($transport, [$this->jsOk($reply)]);
        $client = $this->connected($transport);

        $client->jetStream()->createEphemeralPushConsumer('ORDERS', '_INBOX.deliver')->await();

        // kills ArrayItemRemoval @ 887
        self::assertStringContainsString('"stream_name":"ORDERS"', $transport->writes[3]);
    }

    /**
     * createEphemeralConsumer still validates priority config before dispatch (assert is not removed).
     */
    public function testCreateEphemeralConsumerValidatesPriorityConfig(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $client = $this->connected($transport);

        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('priority_policy must be one of');

        try {
            // kills MethodCallRemoval @ 821 (assertValidPriorityConfig)
            $client->jetStream()->createEphemeralConsumer('ORDERS', null, ['priority_policy' => 'bogus'])->await();
        } finally {
            // No CREATE request was dispatched (only INFO + PONG were written).
            self::assertCount(2, $transport->writes);
        }
    }

    // ─── consumerNames: pagination, offset, filtering ────────────────────────────────────

    /**
     * consumerNames issues the first page with offset 0 and includes the offset key in the body.
     */
    public function testConsumerNamesSendsZeroOffsetOnFirstRequest(): void
    {
        $reply = '{"consumers":["a"],"total":1}';
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $this->muxReplies($transport, [$this->jsOk($reply)]);
        $client = $this->connected($transport);

        $client->jetStream()->consumerNames('ORDERS')->await();

        // kills DecrementInteger @ 789 (offset 0 -> -1) and ArrayItemRemoval @ 792 (drop offset key)
        self::assertStringContainsString('"offset":0', $transport->writes[3]);
    }

    /**
     * consumerNames paginates across pages, advancing the offset by the page size each round.
     */
    public function testConsumerNamesPaginatesAndAccumulatesOffset(): void
    {
        // Three pages of two names each (total 6). The accumulated offset must reach 4 on the third
        // request - a plain assignment (offset = count(page)) would re-request offset 2 instead.
        $page1 = '{"consumers":["c1","c2"],"total":6,"offset":0}';
        $page2 = '{"consumers":["c3","c4"],"total":6,"offset":2}';
        $page3 = '{"consumers":["c5","c6"],"total":6,"offset":4}';

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        // Post-#118 all three paginated requests share the mux inbox; replies are echoed FIFO on each
        // request's captured reply-to (mux sid 1) as the pages are fetched in sequence.
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($page1), $page1),
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($page2), $page2),
            sprintf("MSG _INBOX.c 3 %d\r\n%s\r\n", strlen($page3), $page3),
        ]);

        $client = $this->connected($transport);

        $names = $client->jetStream()->consumerNames('ORDERS')->await();

        // kills DoWhile @ 791 (would stop after one page), NotIdentical @ 804 (loop condition),
        // Ternary @ 803 (total), Assignment/PlusEqual @ 802 (offset accumulation).
        self::assertSame(['c1', 'c2', 'c3', 'c4', 'c5', 'c6'], $names);

        $writes = implode('||', $transport->writes);
        // Successive pages advance the offset by the page size: 0 -> 2 -> 4 (offset += count(page)).
        self::assertStringContainsString('"offset":2', $writes);
        self::assertStringContainsString('"offset":4', $writes);
    }

    /**
     * consumerNames filters out non-string entries and returns a clean reindexed list.
     */
    public function testConsumerNamesFiltersNonStringsAndReindexes(): void
    {
        // Mixed list: a non-string (42) sits between two valid names.
        $reply = '{"consumers":["c1",42,"c3"],"total":2}';
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $this->muxReplies($transport, [$this->jsOk($reply)]);
        $client = $this->connected($transport);

        $names = $client->jetStream()->consumerNames('ORDERS')->await();

        // kills UnwrapArrayFilter @ 795 (would keep the non-string 42 in the page).
        // (UnwrapArrayValues @ 795 is equivalent: the page is re-appended value-by-value into $names,
        //  which discards the array keys, so dropping array_values() changes nothing observable.)
        self::assertSame(['c1', 'c3'], $names);
    }

    // ─── subscribePushConsumer: deliver-subject coalesce ─────────────────────────────────

    /**
     * subscribePushConsumer uses the caller-supplied deliver subject (not a freshly generated inbox).
     */
    public function testSubscribePushConsumerUsesProvidedDeliverSubject(): void
    {
        $reply = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC"}}';
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        // The CREATE request routes through the mux inbox; its reply is echoed on the captured reply-to.
        // The deliver subscription (my.deliver.subject) is set up after and delivers nothing here.
        $this->muxReplies($transport, [$this->jsOk($reply)]);
        $client = $this->connected($transport);

        $client->jetStream()->subscribePushConsumer('ORDERS', 'PROC', static function (): void {}, 'my.deliver.subject')->await();

        $written = implode('', $transport->writes);
        // kills Coalesce @ 910 ($deliverSubject ?? generate -> generate ?? $deliverSubject):
        // the mutant would always generate an _INBOX.JS.PUSH inbox instead of honouring the argument.
        self::assertStringContainsString('"deliver_subject":"my.deliver.subject"', $written);
        self::assertStringContainsString('SUB my.deliver.subject', $written);
        self::assertStringNotContainsString('_INBOX.JS.PUSH', $written);
    }

    // ─── Ordered consumer config constants ───────────────────────────────────────────────

    /**
     * The ordered ephemeral consumer is created with max_deliver = 1.
     */
    public function testSubscribeOrderedConsumerSetsMaxDeliverOne(): void
    {
        $reply = json_encode([
            'stream_name' => 'ORDERS',
            'name' => 'ORD1',
            'config' => ['ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        // The ordered consumer's CREATE routes through the mux inbox; echo its reply on the captured
        // reply-to. No deliveries are needed to observe the CREATE body.
        $this->muxReplies($transport, [$this->jsOk((string) $reply)]);
        $client = $this->connected($transport);

        $client->jetStream()->subscribeOrderedConsumer('ORDERS', static function (): void {})->await();

        // kills DecrementInteger @ 985 (1 -> 0) and IncrementInteger @ 985 (1 -> 2)
        self::assertStringContainsString('"max_deliver":1', $transport->writes[3]);
    }

    // ─── Ordered consumer: recreate restart point + reset + tail-gap boundary ────────────

    /**
     * A gap on the very FIRST delivery (before any in-order message advances lastStreamSeq) recreates
     * from opt_start_seq = lastStreamSeq(0) + 1 = 1.
     */
    public function testOrderedConsumerRecreatesFromSequenceOneWhenGapOnFirstDelivery(): void
    {
        $createReply = static fn(string $name): string => (string) json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        // Mux inbox (#118): CREATE/DELETE replies echo on the request's captured reply-to; the deliver
        // frame uses the sid the server actually assigned to the deliver SUB (no per-request SUB shift).
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD1'))],
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD2'))],
            ],
            onDelete: [
                static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)],
            ],
            deliverEpochs: [
                // First delivery is already out of order: consumer seq 5 (expected 1) -> immediate
                // recreate. lastStreamSeq is still 0, so opt_start_seq must be 0 + 1 = 1.
                static fn (int $sid): array => ["MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.5.9.5.0.0 4\r\nbad5\r\n"],
                // The rotated (second) epoch delivers nothing: server-side replay is exercised in the
                // integration suite, not against FakeTransport.
            ],
        );

        $client = $this->connected($transport);

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $m) use (&$received): void {
            $received[] = $m->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 6; $i++) {
            $client->processIncoming()->await();
        }

        self::assertSame([], $received);

        $written = implode('', $transport->writes);
        // A pre-first-delivery recreate (lastStreamSeq still 0) re-applies the INITIAL deliver
        // policy instead of a by_start_sequence restart: for this default consumer that is
        // semantically 'all' (identical to the old opt_start_seq 1), while for a
        // 'new'/'last_per_subject' initial policy (a KV/OS watch) a sequence-1 replay would
        // wrongly re-deliver the whole stream. Kills mutants that flip the lastStreamSeq > 0
        // guard: a mutated guard would emit by_start_sequence/opt_start_seq here.
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'), 'the gap must trigger exactly one recreate');
        self::assertStringNotContainsString('"by_start_sequence"', $written);
        self::assertStringNotContainsString('"opt_start_seq"', $written);
    }

    /**
     * After a recreate, the expected consumer sequence resets to 1 so the recreated consumer's first
     * delivery (consumer seq 1) is accepted and delivered.
     */
    public function testOrderedConsumerResetsExpectedSequenceToOneAfterRecreate(): void
    {
        $createReply = static fn(string $name): string => (string) json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        // Mux inbox (#118): CREATE/DELETE replies echo on the captured reply-to. ORD2's first delivery
        // must arrive AFTER its CREATE reply, because the recreate re-adopts the consumer name from the
        // reply (and resets expected seq to 1) only once the create await returns; a frame that arrived
        // before that would still be filtered under the previous (client-chosen) name. So "after" rides
        // the recreate CREATE reply on the rotated deliver sid captured live from its SUB.
        $rotatedSid = 0;
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD1'))],
                static function (string $rt) use ($createReply, &$rotatedSid): array {
                    return [
                        self::muxMsg($rt, $createReply('ORD2')),
                        // Recreated consumer ORD2 delivers consumer seq 1 on the ROTATED inbox -> must be
                        // accepted against the reset expected seq 1.
                        "MSG deliver.ord $rotatedSid \$JS.ACK.EVENTS.ORD2.1.2.1.0.0 5\r\nafter\r\n",
                    ];
                },
            ],
            onDelete: [
                static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)],
            ],
            deliverEpochs: [
                static fn (int $sid): array => [
                    // In-order msg1 (consumer seq 1) -> delivered, expected next 2.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    // Gap (consumer seq 3) -> recreate to ORD2; expected resets to 1.
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
                ],
                // The rotated deliver SUB: capture its live sid so ORD2's "after" frame (emitted with the
                // recreate CREATE reply above) is addressed to the inbox the client actually subscribed.
                static function (int $sid) use (&$rotatedSid): array {
                    $rotatedSid = $sid;

                    return [];
                },
            ],
        );

        $client = $this->connected($transport);

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $m) use (&$received): void {
            $received[] = $m->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 8; $i++) {
            $client->processIncoming()->await();
        }

        // kills IncrementInteger @ 1012 (expectedConsumerSeq reset 1 -> 2): with the mutant, ORD2's
        // first message (consumer seq 1) would mismatch expected 2, be discarded, and trigger another
        // recreate instead of being delivered.
        self::assertSame(['msg1', 'after'], $received);
    }

    /**
     * Tail-gap boundary: a heartbeat reporting last-consumer EXACTLY ONE AHEAD of what was processed
     * (lastDelivered = expectedConsumerSeq - 1 + 1 = 2 after one message) triggers a recreate, while a
     * heartbeat that is merely caught up (lastDelivered = 1 = expected - 1) does NOT.
     */
    public function testOrderedConsumerTailGapRecreatesWhenHeartbeatExactlyOneAhead(): void
    {
        $createReply = static fn(string $name): string => (string) json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        $deleteReply = '{"success":true}';
        // After msg1, expected = 2. Heartbeat reports last delivered consumer seq = 2 (> expected-1=1).
        $hb = NatsHeaders::toWireBlock([
            'Status' => '100',
            'Description' => 'Idle Heartbeat',
            'Nats-Last-Consumer' => '2',
        ]);

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        // Mux inbox (#118): CREATE/DELETE replies echo on the captured reply-to; msg1 + the tail-gap
        // heartbeat land on the sid the server actually assigned to the deliver SUB.
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD1'))],
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD2'))],
            ],
            onDelete: [
                static fn (string $rt): array => [self::muxMsg($rt, $deleteReply)],
            ],
            deliverEpochs: [
                static fn (int $sid): array => [
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    sprintf("HMSG deliver.ord $sid %d %d\r\n%s\r\n", strlen($hb), strlen($hb), $hb),
                ],
                // The rotated (second) epoch delivers nothing here.
            ],
        );

        $client = $this->connected($transport);

        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (): void {}, 'events.>')->await();

        for ($i = 0; $i < 6; $i++) {
            $client->processIncoming()->await();
        }

        $written = implode('', $transport->writes);
        // kills DecrementInteger @ 1025 (expectedConsumerSeq - 1 -> - 0): mutant threshold becomes
        // "> 2", so lastDelivered = 2 would NOT recreate. Real recreates exactly once.
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS.ORD1'));
        self::assertSame(2, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
    }

    /**
     * Tail-gap lower boundary: a heartbeat reporting last-consumer EQUAL to the highest processed
     * sequence (caught up, lastDelivered = 1 = expected - 1 after one message) must NOT recreate.
     */
    public function testOrderedConsumerTailGapNoRecreateWhenHeartbeatCaughtUp(): void
    {
        $createReply = static fn(string $name): string => (string) json_encode([
            'stream_name' => 'EVENTS',
            'name' => $name,
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);
        // After msg1, expected = 2; processed consumer seq high-water = 1. Heartbeat says 1 (caught up).
        $hb = NatsHeaders::toWireBlock([
            'Status' => '100',
            'Description' => 'Idle Heartbeat',
            'Nats-Last-Consumer' => '1',
        ]);

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        // Mux inbox (#118): the single CREATE reply echoes on the captured reply-to; msg1 + the
        // caught-up heartbeat land on the sid the server actually assigned to the deliver SUB. A
        // caught-up heartbeat must NOT recreate, so no DELETE/second CREATE is ever issued.
        $this->orderedConsumerServer(
            $transport,
            onCreate: [
                static fn (string $rt): array => [self::muxMsg($rt, $createReply('ORD1'))],
            ],
            onDelete: [],
            deliverEpochs: [
                static fn (int $sid): array => [
                    "MSG deliver.ord $sid \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
                    sprintf("HMSG deliver.ord $sid %d %d\r\n%s\r\n", strlen($hb), strlen($hb), $hb),
                ],
            ],
        );

        $client = $this->connected($transport);

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $m) use (&$received): void {
            $received[] = $m->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 4; $i++) {
            $client->processIncoming()->await();
        }

        self::assertSame(['msg1'], $received);

        $written = implode('', $transport->writes);
        // kills IncrementInteger @ 1025 (- 1 -> - 2: threshold ">0" would recreate on caught-up 1),
        // GreaterThan @ 1025 (> -> >=: threshold ">=1" would recreate on equal 1), and
        // LogicalAnd @ 1025 (&& -> ||: a non-null lastDelivered would always recreate).
        self::assertSame(0, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS'));
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
    }

    // ─── deleteConsumer / unpinConsumer success defaults ─────────────────────────────────

    /**
     * deleteConsumer defaults to false when the server response omits "success".
     */
    public function testDeleteConsumerDefaultsToFalseWhenSuccessAbsent(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $this->muxReplies($transport, [$this->jsOk('{}')]);
        $client = $this->connected($transport);

        // kills FalseValue @ 1087 (?? false -> ?? true): with no "success" key the result must be false.
        self::assertFalse($client->jetStream()->deleteConsumer('ORDERS', 'PROC')->await());
    }

    /**
     * unpinConsumer honours an explicit success:false in the response (it does not default to true).
     */
    public function testUnpinConsumerReturnsFalseOnExplicitFailure(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $this->muxReplies($transport, [$this->jsOk('{"success":false}')]);
        $client = $this->connected($transport);

        // kills Coalesce @ 1136 (true ?? success -> always true): an explicit success:false must win.
        self::assertFalse($client->jetStream()->unpinConsumer('ORDERS', 'PROC', 'g1')->await());
    }

    // ─── jsRequest no-responder message (publish path) ───────────────────────────────────

    /**
     * A JetStream publish to a subject with no responder surfaces a 503 JetStreamException whose
     * message leads with the fixed prefix and embeds the target subject.
     */
    public function testNoResponderMessageHasPrefixThenSubject(): void
    {
        $status = "NATS/1.0 503\r\n\r\n";

        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        // Mux inbox (#118): the 503 no-responder frame is echoed as an HMSG on the publish request's
        // captured reply-to (mux sid 1).
        $this->muxReplies($transport, [
            'HMSG _INBOX.a 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
        ]);

        $client = $this->connected($transport);
        // Disable retry so the 503 surfaces immediately.
        $js = new JetStreamContext($client, publishRetryAttempts: 1);

        try {
            $js->publish('orders.created', '{"id":1}')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills Concat @ 1171 (order swap: would start with the subject) ...
            self::assertStringStartsWith('No JetStream responder for subject orders.created', $e->getMessage());
            // ... and ConcatOperandRemoval @ 1171 (drops the subject entirely).
            self::assertStringContainsString('orders.created', $e->getMessage());
            self::assertSame(503, $e->getCode());
        }
    }
}
