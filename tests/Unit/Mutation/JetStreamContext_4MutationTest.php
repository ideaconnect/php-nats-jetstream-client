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
     * @param list<string> $frames
     */
    private function connectedClient(array $frames): NatsClient
    {
        $transport = new FakeTransport(array_merge([self::INFO, "PONG\r\n"], $frames));
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        return $client;
    }

    /**
     * @param list<string> $frames
     * @param-out FakeTransport $transport
     */
    private function connectedClientWith(array $frames, ?FakeTransport &$transport = null): NatsClient
    {
        $transport = new FakeTransport(array_merge([self::INFO, "PONG\r\n"], $frames));
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        return $client;
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
        $client = $this->connectedClientWith([
            'HMSG _INBOX.a 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
        ], $transport);

        $js = new JetStreamContext($client, publishRetryAttempts: 1, publishRetryWaitMs: 1);

        try {
            $js->publish('orders.created', '{"id":1}')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            $msg = $e->getMessage();
            // kills ConcatOperandRemoval @ 1171 — the parenthetical suffix must be present.
            self::assertStringContainsString('not bound to a stream', $msg);
            // kills Concat @ 1171 — order is "subject <subj> (the subject ...)", so the literal
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

        $client = $this->connectedClient([
            'HMSG _INBOX.a 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $js = new JetStreamContext($client, publishRetryAttempts: 1, publishRetryWaitMs: 1);

        // kills IncrementInteger @ 1263 (max(1,..)->max(2,..)) — a 2nd attempt would consume the
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

        $client = $this->connectedClient([
            'HMSG _INBOX.a 1 ' . strlen($status) . ' ' . strlen($status) . "\r\n" . $status . "\r\n",
            sprintf("MSG _INBOX.b 2 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ]);

        $js = new JetStreamContext($client, publishRetryAttempts: 2, publishRetryWaitMs: 1);

        // kills IncrementInteger @ 1265 (for $attempt=1 -> $attempt=2) — starting the counter at 2
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
        $client = $this->connectedClientWith([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ], $transport);

        $client->jetStream()->publishScheduled(
            'schedules.x',
            'events.x',
            '{"e":1}',
            Schedule::every('1h'),
        )->await();

        // kills FalseValue @ 1303 (rollup default false->true) — defaulting to true would emit the
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
        $client = $this->connectedClientWith([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ], $transport);

        $client->jetStream()->publishScheduled(
            'schedules.x',
            'events.x',
            '{"e":1}',
            Schedule::every('1h'),
            source: '',
        )->await();

        // kills LogicalAnd @ 1321 (&& -> ||) — with source='' the OR form is true (because
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
        $client = $this->connectedClient([
            "MSG _INBOX.a 1 7\r\nnotjson\r\n",
        ]);

        try {
            $client->jetStream()->publish('orders.created', '{"id":1}')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills Concat @ 1347 — prefix-first ordering.
            self::assertStringStartsWith('Malformed JetStream publish ack: ', $e->getMessage());
            // kills ConcatOperandRemoval @ 1347 — the JsonException detail must be appended.
            self::assertStringContainsString('Syntax error', $e->getMessage());
            // kills Increment/DecrementInteger @ 1347 — wrap code is exactly 0.
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
        $client = $this->connectedClient([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        try {
            $client->jetStream()->publish('orders.created', '{"id":1}')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills Increment/DecrementInteger @ 1355 — absent code defaults to 0, not 1 or -1.
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
        $client = $this->connectedClient([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        try {
            $client->jetStream()->publish('orders.created', '{"id":1}')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills Coalesce @ 1355 ($error['code'] ?? 0  ->  0 ?? $error['code']) — the mutant always
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
        $client = $this->connectedClientWith([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ackPayload), $ackPayload),
        ], $transport);

        // kills UnwrapTrim @ 1375 — without trim, ' 10 ' fails the /^[+-]?\d+$/ check and throws.
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

        // kills PregMatchRemoveCaret @ 1376 — dropping ^ would let 'x10' match (\d+ at the end) and
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
        $client = $this->connectedClient([
            "MSG _INBOX.a 1 7\r\nnotjson\r\n",
        ]);

        try {
            $client->jetStream()->incrementCounter('counters.visits', '+1')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills Concat @ 1421 — prefix-first ordering.
            self::assertStringStartsWith('Malformed counter response: ', $e->getMessage());
            // kills ConcatOperandRemoval @ 1421 — the JsonException detail must be appended.
            self::assertStringContainsString('Syntax error', $e->getMessage());
            // kills Increment/DecrementInteger @ 1421 — wrap code is exactly 0.
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
        $client = $this->connectedClient([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        try {
            $client->jetStream()->incrementCounter('counters.visits', '+1')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills Increment/DecrementInteger @ 1429 — absent code defaults to 0, not 1 or -1.
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
        $client = $this->connectedClient([
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($errorPayload), $errorPayload),
        ]);

        try {
            $client->jetStream()->incrementCounter('counters.visits', '+1')->await();
            self::fail('Expected JetStreamException');
        } catch (JetStreamException $e) {
            // kills Coalesce @ 1429 ($error['code'] ?? 0  ->  0 ?? $error['code']) — mutant always
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
        // kills Increment/DecrementInteger @ 1451 — default expiresMs is exactly 3000 ms.
        self::assertStringContainsString('"expires":3000000000', $written);
        // kills IncrementInteger @ 1454 — fetchNext always pulls a batch of 1.
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

        // kills Increment/DecrementInteger @ 1484 — default expiresMs is exactly 3000 ms.
        self::assertStringContainsString('"expires":3000000000', $transport->writes[3]);
    }
}
