<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\JetStream\Schedule;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

/**
 * Mutation-killing tests for src/JetStream/JetStreamContext.php (chunk 4).
 *
 * Each test pins the exact observable behavior a specific surviving mutant would change: a wire
 * payload, an exception code/message, or a header presence/absence. Doubles are the in-process
 * FakeTransport (no sockets, no Docker), driven by pre-seeded NATS frames.
 */
final class JetStreamContext_4MutationTest extends TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    /**
     * A subscription's idle-heartbeat watchdog (#113) is a live EventLoop timer that outlives a test
     * whose client is never disconnect()ed. Cancel every callback left registered so the shared loop
     * starts each test clean and a leaked watchdog cannot fire against a lingering client.
     */
    protected function tearDown(): void
    {
        foreach (EventLoop::getIdentifiers() as $id) {
            EventLoop::cancel($id);
        }
    }

    /**
     * @param list<string> $frames
     * @param-out FakeTransport $transport
     */
    private function connectedClientWith(array $frames, ?FakeTransport &$transport = null): NatsClient
    {
        $transport = new FakeTransport(array_merge([self::INFO, "PONG\r\n"], $frames));
        $client = new NatsClient(new NatsOptions(requestTimeoutMs: 1000), $transport);
        $client->connect()->await();

        return $client;
    }

    /**
     * Post-#118 the request inbox is MUXED: request()/requestWithHeaders() no longer subscribe a fresh
     * inbox per call; instead ONE long-lived wildcard "<base>.*" (sid 1) serves every reply, and each
     * request publishes reply-to "<base>.<token>" and routes by that token. A pre-seeded
     * "MSG/HMSG _INBOX.x <sid>" therefore no longer reaches the waiting request (its subject is not the
     * request's random "<base>.<token>", so dispatchMuxReply drops it and the request times out).
     *
     * This installs a dynamic responder that mirrors a real server: it learns the mux base from the
     * wildcard SUB, and for each request PUB/HPUB (a publish whose reply-to lives under that base)
     * pops the next reply frame and re-emits it on the CAPTURED reply-to with the mux sid (1). Frames
     * are delivered FIFO in the order requests are written, so a method that issues several requests
     * (e.g. a 503 retry then the success ack) still gets its replies in sequence.
     *
     * Only request replies belong here; subscription deliveries (a pull fetch inbox) are NOT mux replies -
     * keep those pre-seeded on the read queue.
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

    // -----------------------------------------------------------------------------------------
    // jsRequest() no-responders message (line 1171)
    // -----------------------------------------------------------------------------------------

    /**
     * The no-responders message is "No JetStream responder for subject <subject> (the subject ...)".
     *
     * @return void
     */
    public function testNoResponderMessageKeepsSubjectFirstAndSuffix(): void
    {
        // A 503 no-responders status with NO retry exhausts immediately and surfaces the message.
        $status = "NATS/1.0 503\r\n\r\n";
        $transport = null;
        $client = $this->connectedClientWith([], $transport);
        $this->muxReplies($transport, [
            'HMSG _INBOX.a 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
        ]);

        $js = new JetStreamContext($client, publishRetryAttempts: 1, publishRetryWaitMs: 1);

        try {
            $js->publish('orders.created', '{"id":1}')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            $msg = $e->getMessage();
            // kills ConcatOperandRemoval @ 1171 - the parenthetical suffix must be present.
            self::assertStringContainsString('not bound to a stream', $msg);
            // kills Concat @ 1171 - order is "subject <subj> (the subject ...)", so the literal
            // "subject orders.created" appears BEFORE the parenthetical, not after it.
            self::assertStringContainsString('No JetStream responder for subject orders.created', $msg);
            self::assertStringEndsWith('enabled)', $msg);
        }
    }

    // -----------------------------------------------------------------------------------------
    // publishWithRetry attempt accounting (lines 1263, 1265)
    // -----------------------------------------------------------------------------------------

    /**
     * With publishRetryAttempts=1 the caller asked for NO retry: a single 503 must surface immediately.
     */
    public function testSingleAttemptDoesNotRetryOn503(): void
    {
        $status = "NATS/1.0 503\r\n\r\n";
        $ackPayload = '{"stream":"ORDERS","seq":7,"duplicate":false}';

        $transport = null;
        $client = $this->connectedClientWith([], $transport);
        $this->muxReplies($transport, [
            'HMSG _INBOX.a 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $js = new JetStreamContext($client, publishRetryAttempts: 1, publishRetryWaitMs: 1);

        // kills IncrementInteger @ 1263 (max(1,..)->max(2,..)) - a 2nd attempt would consume the
        // success ack and return; real code must throw on the single attempt.
        $this->expectException(JetStreamException::class);
        $this->expectExceptionCode(503);
        $js->publish('orders.created', '{"id":1}')->await();
    }

    /**
     * With publishRetryAttempts=2 the first 503 is retried and the retry succeeds.
     */
    public function testTwoAttemptsRetryOnceThenSucceed(): void
    {
        $status = "NATS/1.0 503\r\n\r\n";
        $ackPayload = '{"stream":"ORDERS","seq":51,"duplicate":false}';

        $transport = null;
        $client = $this->connectedClientWith([], $transport);
        $this->muxReplies($transport, [
            'HMSG _INBOX.a 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $js = new JetStreamContext($client, publishRetryAttempts: 2, publishRetryWaitMs: 1);

        // kills IncrementInteger @ 1265 (for $attempt=1 -> $attempt=2) - starting the counter at 2
        // makes the first iteration already satisfy attempt>=attempts and throw with NO retry.
        $ack = $js->publish('orders.created', '{"id":1}')->await();
        self::assertSame(51, $ack->seq);
    }

    // -----------------------------------------------------------------------------------------
    // publishScheduled rollup default + source guard (lines 1303, 1321)
    // -----------------------------------------------------------------------------------------

    /**
     * The $rollup parameter defaults to false: without it, no Nats-Schedule-Rollup header is emitted.
     */
    public function testScheduledPublishDefaultsRollupOff(): void
    {
        $ackPayload = '{"stream":"SCHED","seq":1,"duplicate":false}';
        $transport = null;
        $client = $this->connectedClientWith([], $transport);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client->jetStream()->publishScheduled(
            'schedules.x',
            'events.x',
            '{"e":1}',
            Schedule::every('1h'),
        )->await();

        // kills FalseValue @ 1303 (rollup default false->true) - defaulting to true would emit the
        // rollup header even though the caller never asked for it.
        self::assertStringNotContainsString('Nats-Schedule-Rollup', $transport->writes[3]);
    }

    /**
     * An empty source string must NOT emit a Nats-Schedule-Source header (both conditions required).
     */
    public function testScheduledPublishOmitsEmptySource(): void
    {
        $ackPayload = '{"stream":"SCHED","seq":2,"duplicate":false}';
        $transport = null;
        $client = $this->connectedClientWith([], $transport);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $client->jetStream()->publishScheduled(
            'schedules.x',
            'events.x',
            '{"e":1}',
            Schedule::every('1h'),
            source: '',
        )->await();

        // kills LogicalAnd @ 1321 (&& -> ||) - with source='' the OR form is true (because
        // '' !== null) and would emit an empty Nats-Schedule-Source header.
        self::assertStringNotContainsString('Nats-Schedule-Source', $transport->writes[3]);
    }

    // -----------------------------------------------------------------------------------------
    // parsePublishAck malformed-body wrapping (line 1347)
    // -----------------------------------------------------------------------------------------

    /**
     * A non-JSON publish ack is wrapped as a JetStreamException whose message starts with the literal
     * prefix, carries the JsonException detail, and uses code 0.
     */
    public function testMalformedPublishAckMessageAndCode(): void
    {
        $transport = null;
        $client = $this->connectedClientWith([], $transport);
        $this->muxReplies($transport, [
            "MSG _INBOX.a 1 7\r\nnotjson\r\n",
        ]);

        try {
            $client->jetStream()->publish('orders.created', '{"id":1}')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills Concat @ 1347 - prefix-first ordering.
            self::assertStringStartsWith('Malformed JetStream publish ack: ', $e->getMessage());
            // kills ConcatOperandRemoval @ 1347 - the JsonException detail must be appended.
            self::assertStringContainsString('Syntax error', $e->getMessage());
            // kills Increment/DecrementInteger @ 1347 - wrap code is exactly 0.
            self::assertSame(0, $e->getCode());
        }
    }

    // -----------------------------------------------------------------------------------------
    // parsePublishAck embedded API error code default (line 1355)
    // -----------------------------------------------------------------------------------------

    /**
     * A publish-ack API error WITHOUT a code field maps to JetStreamException code 0.
     */
    public function testPublishApiErrorWithoutCodeDefaultsToZero(): void
    {
        $errorPayload = '{"error":{"description":"publish failed"}}';
        $transport = null;
        $client = $this->connectedClientWith([], $transport);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        try {
            $client->jetStream()->publish('orders.created', '{"id":1}')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills Increment/DecrementInteger @ 1355 - absent code defaults to 0, not 1 or -1.
            self::assertSame(0, $e->getCode());
            self::assertSame('publish failed', $e->getMessage());
        }
    }

    /**
     * A publish-ack API error WITH a code field propagates that code (not a hardcoded 0).
     */
    public function testPublishApiErrorPropagatesProvidedCode(): void
    {
        $errorPayload = '{"error":{"code":500,"description":"publish failed"}}';
        $transport = null;
        $client = $this->connectedClientWith([], $transport);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        try {
            $client->jetStream()->publish('orders.created', '{"id":1}')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills Coalesce @ 1355 ($error['code'] ?? 0  ->  0 ?? $error['code']) - the mutant always
            // yields 0; real code must surface the provided 500.
            self::assertSame(500, $e->getCode());
        }
    }

    // -----------------------------------------------------------------------------------------
    // incrementCounter delta normalization + validation (lines 1375, 1376)
    // -----------------------------------------------------------------------------------------

    /**
     * The delta is trimmed before validation, so whitespace-padded integers are accepted and the
     * trimmed value is sent on the wire.
     */
    public function testCounterDeltaIsTrimmedBeforeValidation(): void
    {
        $ackPayload = '{"stream":"COUNTERS","seq":1,"val":"10"}';
        $transport = null;
        $client = $this->connectedClientWith([], $transport);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        // kills UnwrapTrim @ 1375 - without trim, ' 10 ' fails the /^[+-]?\d+$/ check and throws.
        $value = $client->jetStream()->incrementCounter('counters.visits', ' 10 ')->await();
        self::assertSame('10', $value);
        self::assertStringContainsString('Nats-Incr:10', $transport->writes[3]);
    }

    /**
     * The validation regex is anchored at both ends: garbage before the digits is rejected.
     */
    public function testCounterDeltaRejectsLeadingGarbage(): void
    {
        $transport = null;
        $client = $this->connectedClientWith([], $transport);

        // kills PregMatchRemoveCaret @ 1376 - dropping ^ would let 'x10' match (\d+ at the end) and
        // be dispatched; the anchored regex must reject it before any request.
        $this->expectException(JetStreamException::class);
        $this->expectExceptionMessage('Counter increment must be an integer string');

        try {
            $client->jetStream()->incrementCounter('counters.visits', 'x10')->await();
        } finally {
            self::assertCount(2, $transport->writes); // only INFO+PONG handshake, no PUB/HPUB
        }
    }

    // -----------------------------------------------------------------------------------------
    // parseCounterValue malformed-body wrapping (line 1421)
    // -----------------------------------------------------------------------------------------

    /**
     * A non-JSON counter response is wrapped as a JetStreamException whose message starts with the
     * literal prefix, carries the JsonException detail, and uses code 0.
     */
    public function testMalformedCounterResponseMessageAndCode(): void
    {
        $transport = null;
        $client = $this->connectedClientWith([], $transport);
        $this->muxReplies($transport, [
            "MSG _INBOX.a 1 7\r\nnotjson\r\n",
        ]);

        try {
            $client->jetStream()->incrementCounter('counters.visits', '+1')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills Concat @ 1421 - prefix-first ordering.
            self::assertStringStartsWith('Malformed counter response: ', $e->getMessage());
            // kills ConcatOperandRemoval @ 1421 - the JsonException detail must be appended.
            self::assertStringContainsString('Syntax error', $e->getMessage());
            // kills Increment/DecrementInteger @ 1421 - wrap code is exactly 0.
            self::assertSame(0, $e->getCode());
        }
    }

    // -----------------------------------------------------------------------------------------
    // parseCounterValue embedded API error code default (line 1429)
    // -----------------------------------------------------------------------------------------

    /**
     * A counter API error WITHOUT a code field maps to JetStreamException code 0.
     */
    public function testCounterApiErrorWithoutCodeDefaultsToZero(): void
    {
        $errorPayload = '{"error":{"description":"counter failed"}}';
        $transport = null;
        $client = $this->connectedClientWith([], $transport);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        try {
            $client->jetStream()->incrementCounter('counters.visits', '+1')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills Increment/DecrementInteger @ 1429 - absent code defaults to 0, not 1 or -1.
            self::assertSame(0, $e->getCode());
            self::assertSame('counter failed', $e->getMessage());
        }
    }

    /**
     * A counter API error WITH a code field propagates that code (not a hardcoded 0).
     */
    public function testCounterApiErrorPropagatesProvidedCode(): void
    {
        $errorPayload = '{"error":{"code":503,"description":"counter failed"}}';
        $transport = null;
        $client = $this->connectedClientWith([], $transport);
        $this->muxReplies($transport, [
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        try {
            $client->jetStream()->incrementCounter('counters.visits', '+1')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills Coalesce @ 1429 ($error['code'] ?? 0  ->  0 ?? $error['code']) - mutant always
            // yields 0; real code must surface the provided 503.
            self::assertSame(503, $e->getCode());
        }
    }

    // -----------------------------------------------------------------------------------------
    // fetchNext / fetchBatch default expiry + fetchNext fixed batch (lines 1451, 1454, 1484)
    // -----------------------------------------------------------------------------------------

    /**
     * fetchNext defaults expiresMs to 3000 (=> "expires":3000000000) and pulls a batch of exactly 1.
     */
    public function testFetchNextDefaultExpiryAndBatchOfOne(): void
    {
        $deliveryPayload = '{"event":"created"}';
        $transport = null;
        $client = $this->connectedClientWith([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($deliveryPayload), $deliveryPayload),
        ], $transport);

        $client->jetStream()->fetchNext('ORDERS', 'PROC')->await();

        $written = $transport->writes[3];
        // kills Increment/DecrementInteger @ 1451 - default expiresMs is exactly 3000 ms.
        self::assertStringContainsString('"expires":3000000000', $written);
        // kills IncrementInteger @ 1454 - fetchNext always pulls a batch of 1.
        self::assertStringContainsString('"batch":1', $written);
    }

    /**
     * fetchBatch defaults expiresMs to 3000 (=> "expires":3000000000) when not supplied.
     */
    public function testFetchBatchDefaultExpiry(): void
    {
        $deliveryPayload = '{"event":"created"}';
        $transport = null;
        $client = $this->connectedClientWith([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($deliveryPayload), $deliveryPayload),
        ], $transport);

        $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1)->await();

        // kills Increment/DecrementInteger @ 1484 - default expiresMs is exactly 3000 ms.
        self::assertStringContainsString('"expires":3000000000', $transport->writes[3]);
    }
}
