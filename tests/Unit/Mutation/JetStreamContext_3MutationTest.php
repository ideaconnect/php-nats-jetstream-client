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
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        return $client;
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
        $transport = new FakeTransport([self::INFO, "PONG\r\n", $this->jsOk($reply)]);
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
        $transport = new FakeTransport([self::INFO, "PONG\r\n", $this->jsOk($reply)]);
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
        $transport = new FakeTransport([self::INFO, "PONG\r\n", $this->jsOk($reply)]);
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
        $transport = new FakeTransport([self::INFO, "PONG\r\n", $this->jsOk($reply)]);
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
        $transport = new FakeTransport([self::INFO, "PONG\r\n", $this->jsOk($reply)]);
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
        $transport = new FakeTransport([self::INFO, "PONG\r\n", $this->jsOk($reply)]);
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

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
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
        $transport = new FakeTransport([self::INFO, "PONG\r\n", $this->jsOk($reply)]);
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
        $transport = new FakeTransport([self::INFO, "PONG\r\n", $this->jsOk($reply)]);
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

        $transport = new FakeTransport([self::INFO, "PONG\r\n", $this->jsOk((string) $reply)]);
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

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply('ORD1')), $createReply('ORD1')),
            // First delivery is already out of order: consumer seq 5 (expected 1) -> immediate recreate.
            // lastStreamSeq is still 0, so opt_start_seq must be 0 + 1 = 1.
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.5.9.5.0.0 4\r\nbad5\r\n",
            sprintf("MSG _INBOX.b 3 %d\r\n%s\r\n", strlen($deleteReply), $deleteReply),
            sprintf("MSG _INBOX.c 5 %d\r\n%s\r\n", strlen($createReply('ORD2')), $createReply('ORD2')),
        ]);

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
        // kills DecrementInteger @ 979 (lastStreamSeq 0 -> -1 would make opt_start_seq 0).
        self::assertStringContainsString('"opt_start_seq":1', $written);
        self::assertStringNotContainsString('"opt_start_seq":0', $written);
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

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply('ORD1')), $createReply('ORD1')),
            // In-order msg1 (consumer seq 1) -> delivered, expected next 2.
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
            // Gap (consumer seq 3) -> recreate to ORD2; expected resets to 1.
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.3.4.3.0.0 4\r\nbad3\r\n",
            sprintf("MSG _INBOX.b 3 %d\r\n%s\r\n", strlen($deleteReply), $deleteReply),
            sprintf("MSG _INBOX.c 5 %d\r\n%s\r\n", strlen($createReply('ORD2')), $createReply('ORD2')),
            // Recreated consumer ORD2 delivers consumer seq 1 on the ROTATED inbox (sid 4) -> must be
            // accepted against expected 1.
            "MSG deliver.ord 4 \$JS.ACK.EVENTS.ORD2.1.2.1.0.0 5\r\nafter\r\n",
        ]);

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

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply('ORD1')), $createReply('ORD1')),
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
            sprintf("HMSG deliver.ord 2 %d %d\r\n%s\r\n", strlen($hb), strlen($hb), $hb),
            sprintf("MSG _INBOX.b 3 %d\r\n%s\r\n", strlen($deleteReply), $deleteReply),
            sprintf("MSG _INBOX.c 5 %d\r\n%s\r\n", strlen($createReply('ORD2')), $createReply('ORD2')),
        ]);

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

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createReply('ORD1')), $createReply('ORD1')),
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
            sprintf("HMSG deliver.ord 2 %d %d\r\n%s\r\n", strlen($hb), strlen($hb), $hb),
        ]);

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
        $transport = new FakeTransport([self::INFO, "PONG\r\n", $this->jsOk('{}')]);
        $client = $this->connected($transport);

        // kills FalseValue @ 1087 (?? false -> ?? true): with no "success" key the result must be false.
        self::assertFalse($client->jetStream()->deleteConsumer('ORDERS', 'PROC')->await());
    }

    /**
     * unpinConsumer honours an explicit success:false in the response (it does not default to true).
     */
    public function testUnpinConsumerReturnsFalseOnExplicitFailure(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n", $this->jsOk('{"success":false}')]);
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

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
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
