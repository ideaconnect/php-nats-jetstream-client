<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsHeaders;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Tests\Support\FakeTransport;

/**
 * Targeted unit tests that kill surviving Infection mutants in
 * src/JetStream/JetStreamContext.php (chunk 5, lines ~1509-1918).
 *
 * Each test pins the EXACT observable behaviour a specific mutation would break.
 */
final class JetStreamContext_5MutationTest extends \PHPUnit\Framework\TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    // ─── fetchBatch: terminal status boundary (>= 400) ──────────────────────

    /**
     * A status of EXACTLY 400 must be treated as terminal (>= 400), not collected as a message.
     */
    public function testFetchBatchTreats400AsTerminal(): void
    {
        // kills GreaterThanOrEqualTo @ 1515
        $status = "NATS/1.0 400 Bad Request\r\nStatus: 400\r\nDescription: Bad Request\r\n\r\n";
        $bytes = strlen($status);

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("HMSG _INBOX.JS.FETCH.a 1 %d %d\r\n%s\r\n", $bytes, $bytes, $status),
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500)->await();
            self::fail('Expected a terminal-status JetStreamException for status 400.');
        } catch (JetStreamException $e) {
            // Real code: 400 is terminal -> thrown with code 400.
            // Mutant (> 400): 400 collected as a message -> no exception (would return 1 message).
            self::assertSame(400, $e->getCode());
            self::assertStringContainsString('status 400', $e->getMessage());
        }
    }

    /**
     * The fetch subscription must be removed (UNSUB) once the pull completes — it is issued from the
     * `finally` block.
     */
    public function testFetchBatchUnsubscribesAfterCompletion(): void
    {
        // kills MethodCallRemoval @ 1544
        $msg = '{"event":"x"}';

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg), $msg),
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500)->await();

        // Real code unsubscribes in finally; the mutant removes that call entirely.
        self::assertStringContainsString('UNSUB', implode('', $transport->writes));
    }

    // ─── schedule validation: trim + anchored regexes ──────────────────────

    /**
     * The schedule string is trimmed before validation; a surrounding-whitespace alias must validate.
     */
    public function testScheduleIsTrimmedBeforeValidation(): void
    {
        // kills UnwrapTrim @ 1658
        $ack = '{"stream":"SCHED","seq":1,"duplicate":false}';

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ack), $ack),
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // Real: trim('  @daily  ') -> '@daily' -> alias matches -> publishes.
        // Mutant (no trim): '  @daily  ' fails alias regex and is not 6 cron fields -> throws.
        $ackResult = $client->jetStream()->publishScheduled(
            'schedules.report',
            'events.report',
            '{"event":"daily"}',
            '  @daily  ',
        )->await();

        self::assertSame('SCHED', $ackResult->stream);
        self::assertStringContainsString('Nats-Schedule:@daily', implode('', $transport->writes));
    }

    /**
     * The @at regex is anchored at both ends: junk before/after a valid timestamp must be rejected.
     */
    public function testScheduleAtRegexIsAnchored(): void
    {
        // kills PregMatchRemoveCaret @ 1662 (leading junk) and PregMatchRemoveDollar @ 1662 (trailing junk)
        foreach (['xxx@at 2030-01-01T00:00:00Z', '@at 2030-01-01T00:00:00Z trailing'] as $bad) {
            $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
            $client = new NatsClient(new NatsOptions(), $transport);
            $client->connect()->await();

            try {
                $client->jetStream()->publishScheduled('s.subj', 'e.subj', 'p', $bad)->await();
                self::fail('Expected unanchored @at schedule "' . $bad . '" to be rejected.');
            } catch (JetStreamException $e) {
                self::assertStringContainsString('Unsupported schedule expression', $e->getMessage());
            }
            // No request dispatched (only the CONNECT + PING handshake writes).
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * The @every regex is anchored at the start: a leading non-@every token must be rejected.
     */
    public function testScheduleEveryRegexIsAnchored(): void
    {
        // kills PregMatchRemoveCaret @ 1667
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        try {
            // 2 whitespace-separated tokens -> not 6 cron fields; '^@every' must not match mid-string.
            $client->jetStream()->publishScheduled('s.subj', 'e.subj', 'p', 'pre@every 1h')->await();
            self::fail('Expected unanchored @every schedule to be rejected.');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('Unsupported schedule expression', $e->getMessage());
        }
        self::assertCount(2, $transport->writes);
    }

    /**
     * The predefined-alias regex is anchored at both ends.
     */
    public function testSchedulePredefinedAliasRegexIsAnchored(): void
    {
        // kills PregMatchRemoveCaret @ 1672 (leading junk) and PregMatchRemoveDollar @ 1672 (trailing junk)
        foreach (['x@daily', '@dailyz'] as $bad) {
            $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
            $client = new NatsClient(new NatsOptions(), $transport);
            $client->connect()->await();

            try {
                $client->jetStream()->publishScheduled('s.subj', 'e.subj', 'p', $bad)->await();
                self::fail('Expected unanchored alias "' . $bad . '" to be rejected.');
            } catch (JetStreamException $e) {
                self::assertStringContainsString('Unsupported schedule expression', $e->getMessage());
            }
            self::assertCount(2, $transport->writes);
        }
    }

    /**
     * The "unsupported schedule" exception message is built by concatenating the schedule value into a
     * fixed template, in a fixed order. Asserting the EXACT message kills every Concat / ConcatOperandRemoval
     * mutant on that statement.
     */
    public function testUnsupportedScheduleExceptionMessageExact(): void
    {
        // kills Concat @ 1683 (3 variants) and ConcatOperandRemoval @ 1683 (3 variants)
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->publishScheduled('s.subj', 'e.subj', 'p', 'not-a-schedule')->await();
            self::fail('Expected unsupported schedule to throw.');
        } catch (JetStreamException $e) {
            self::assertSame(
                'Unsupported schedule expression "not-a-schedule": expected @at <RFC3339>, @every <duration>,'
                . ' a predefined alias (@daily, @hourly, ...), or a 6-field cron expression',
                $e->getMessage(),
            );
        }
        self::assertCount(2, $transport->writes);
    }

    // ─── isCronSchedule: trim + anchored regex (time-zone gating) ───────────

    /**
     * isCronSchedule trims before testing, so a leading-space "@every" is still classified as @every
     * (NOT cron) — and a time zone supplied with it is rejected.
     */
    public function testIsCronScheduleTrimsBeforeClassifying(): void
    {
        // kills UnwrapTrim @ 1696
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        try {
            // assertSupportedSchedulePattern trims first so this passes validation; isCronSchedule must
            // still see '@every ...' (trimmed) and classify it as NON-cron -> reject the time zone.
            $client->jetStream()->publishScheduled('s.subj', 'e.subj', 'p', '  @every 1h', timeZone: 'Europe/Warsaw')->await();
            self::fail('Expected a time zone with a (trimmed) @every schedule to be rejected.');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('only valid for cron', $e->getMessage());
        }
        self::assertCount(2, $transport->writes);
    }

    /**
     * isCronSchedule's @at/@every regex is anchored at the start: a 6-field cron whose 2nd field is
     * "@at" is cron-class (time zone allowed) only because the anchor prevents a mid-string match.
     */
    public function testIsCronScheduleRegexIsAnchored(): void
    {
        // kills PregMatchRemoveCaret @ 1696
        $ack = '{"stream":"SCHED","seq":2,"duplicate":false}';
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($ack), $ack),
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // 'a @at b c d e' is a valid 6-field cron expression. With the start anchor it is cron-class,
        // so the time zone is accepted and the request is dispatched. Without the anchor the mutant
        // matches the inner '@at ' and classifies it as @at (non-cron) -> rejects the time zone.
        $result = $client->jetStream()->publishScheduled(
            'schedules.cron',
            'events.cron',
            'p',
            'a @at b c d e',
            timeZone: 'Europe/Warsaw',
        )->await();

        self::assertSame('SCHED', $result->stream);
        $written = implode('', $transport->writes);
        self::assertStringContainsString('Nats-Schedule:a @at b c d e', $written);
        self::assertStringContainsString('Nats-Schedule-Time-Zone:Europe/Warsaw', $written);
    }

    // ─── ordered consumer: heartbeatLastConsumerSeq && guard ────────────────

    /**
     * A non-numeric Nats-Last-Consumer (e.g. "9x") must yield null (no tail-gap recreate). The `&&`
     * guard requires BOTH non-empty AND all-digits; an `||` would accept "9x" and (int)-cast it to 9,
     * spuriously triggering a recreate.
     */
    public function testHeartbeatLastConsumerRequiresAllDigits(): void
    {
        // kills LogicalAnd @ 1766
        $createReply = json_encode([
            'stream_name' => 'EVENTS',
            'name' => 'ORD1',
            'config' => ['deliver_subject' => 'deliver.ord', 'ack_policy' => 'none'],
        ], JSON_THROW_ON_ERROR);

        // Heartbeat carrying a NON-numeric last-consumer value: (int)"9x" === 9 would be > processed.
        $hbHeaders = NatsHeaders::toWireBlock([
            'Status' => '100',
            'Description' => 'Idle Heartbeat',
            'Nats-Last-Consumer' => '9x',
        ]);

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen((string) $createReply), (string) $createReply),
            // In-order msg1 -> expectedConsumerSeq advances to 2.
            "MSG deliver.ord 2 \$JS.ACK.EVENTS.ORD1.1.1.1.0.0 4\r\nmsg1\r\n",
            // Heartbeat with non-numeric last-consumer "9x".
            sprintf("HMSG deliver.ord 2 %d %d\r\n%s\r\n", strlen($hbHeaders), strlen($hbHeaders), $hbHeaders),
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $received = [];
        $client->jetStream()->subscribeOrderedConsumer('EVENTS', static function (NatsMessage $m) use (&$received): void {
            $received[] = $m->payload;
        }, 'events.>')->await();

        for ($i = 0; $i < 4; $i++) {
            $client->processIncoming()->await();
        }

        self::assertSame(['msg1'], $received);

        $written = implode('', $transport->writes);
        // Real: "9x" -> null -> NO tail-gap recreate (exactly one CREATE, no DELETE).
        // Mutant (||): "9x" accepted, (int)9 > 1 -> spurious recreate (DELETE + second CREATE).
        self::assertSame(1, substr_count($written, '$JS.API.CONSUMER.CREATE.EVENTS'));
        self::assertSame(0, substr_count($written, '$JS.API.CONSUMER.DELETE.EVENTS'));
    }

    // ─── applyFilterSubjects: array_values + return integrity ───────────────

    /**
     * filter_subjects is reindexed with array_values so a gap-indexed input still encodes as a JSON
     * array, not an object.
     */
    public function testFilterSubjectsAreReindexedToArray(): void
    {
        // kills UnwrapArrayValues @ 1799
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC"}}';

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // A non-zero-indexed array: array_values() reindexes to [0=>...]; without it the JSON encoder
        // emits an object {"3":...}.
        $client->jetStream()->createConsumer('ORDERS', 'PROC', null, [
            'filter_subjects' => [3 => 'orders.eu.>'],
        ])->await();

        $written = $transport->writes[3];
        self::assertStringContainsString('"filter_subjects":["orders.eu.>"]', $written);
        self::assertStringNotContainsString('"filter_subjects":{', $written);
    }

    /**
     * applyFilterSubjects returns the FULL config (all keys), not a one-key slice.
     */
    public function testApplyFilterSubjectsReturnsFullConfig(): void
    {
        // kills ArrayOneItem @ 1801
        $createPayload = '{"stream_name":"ORDERS","name":"PROC","config":{"durable_name":"PROC"}}';

        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen($createPayload), $createPayload),
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // config carries ack_policy (default), durable_name, and filter_subjects: a one-key slice would
        // drop two of these.
        $client->jetStream()->createConsumer('ORDERS', 'PROC', null, [
            'filter_subjects' => ['orders.eu.>'],
        ])->await();

        $written = $transport->writes[3];
        self::assertStringContainsString('"durable_name":"PROC"', $written);
        self::assertStringContainsString('"ack_policy":"explicit"', $written);
        self::assertStringContainsString('"filter_subjects":["orders.eu.>"]', $written);
    }

    // ─── buildPullRequest: group / priority validation ──────────────────────

    /**
     * The pull-group regex is anchored at the start: a leading invalid char must be rejected.
     */
    public function testPullGroupRegexIsAnchored(): void
    {
        // kills PregMatchRemoveCaret @ 1834
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        try {
            // '!abc' has a valid 16-char tail but an invalid leading char: the start anchor rejects it.
            $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500, ['group' => '!abc'])->await();
            self::fail('Expected an invalid pull group to be rejected.');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('Pull group must be', $e->getMessage());
        }
        // Rejected before dispatch: only the handshake writes exist.
        self::assertCount(2, $transport->writes);
    }

    /**
     * Pull priority 0 is the inclusive lower bound and must be accepted (`< 0`, not `<= 0`).
     */
    public function testPullPriorityZeroIsAccepted(): void
    {
        // kills LessThan @ 1841
        $msg = '{"event":"x"}';
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg), $msg),
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // Real: 0 < 0 is false -> valid -> request dispatched.
        // Mutant (<= 0): 0 rejected -> throws before dispatch.
        $messages = $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500, ['priority' => 0])->await();

        self::assertCount(1, $messages);
        self::assertStringContainsString('"priority":0', $transport->writes[3]);
    }

    /**
     * Pull priority 9 is the inclusive upper bound and must be accepted (`> 9`, not `>= 9`).
     */
    public function testPullPriorityNineIsAccepted(): void
    {
        // kills GreaterThan @ 1841
        $msg = '{"event":"x"}';
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.JS.FETCH.a 1 %d\r\n%s\r\n", strlen($msg), $msg),
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // Real: 9 > 9 is false -> valid. Mutant (>= 9): 9 rejected -> throws.
        $messages = $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500, ['priority' => 9])->await();

        self::assertCount(1, $messages);
        self::assertStringContainsString('"priority":9', $transport->writes[3]);
    }

    /**
     * A NON-integer priority must be rejected by the `!is_int(...) || ...` guard. The `&&`-precedence
     * mutant would let a numeric string slip through.
     */
    public function testPullPriorityRejectsNonInteger(): void
    {
        // kills LogicalOr @ 1841
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        try {
            // Real: !is_int('5') is true -> rejected.
            // Mutant (&&): (!is_int && '5'<0) is false, '5'>9 is false -> NOT rejected -> dispatched.
            $client->jetStream()->fetchBatch('ORDERS', 'PROC', 1, 2500, ['priority' => '5'])->await();
            self::fail('Expected a non-integer pull priority to be rejected.');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('Pull priority must be an integer', $e->getMessage());
        }
        self::assertCount(2, $transport->writes);
    }

    // ─── assertValidPriorityConfig: priority_groups regex ───────────────────

    /**
     * The priority_groups name regex is anchored at the start.
     */
    public function testPriorityGroupsRegexIsAnchored(): void
    {
        // kills PregMatchRemoveCaret @ 1879
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        try {
            $client->jetStream()->createConsumer('ORDERS', 'PROC', null, [
                'priority_groups' => ['!abc'],
            ])->await();
            self::fail('Expected an invalid priority_groups name to be rejected.');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('priority_groups names must be', $e->getMessage());
        }
        // Rejected before dispatch.
        self::assertCount(2, $transport->writes);
    }

    // ─── requestJson: empty body encodes as a JSON object ───────────────────

    /**
     * An empty request body must be encoded as a JSON object `{}` (cast to object), not an array `[]`.
     */
    public function testEmptyRequestBodyEncodesAsObject(): void
    {
        // kills CastObject @ 1918
        $transport = new FakeTransport([
            self::INFO,
            "PONG\r\n",
            sprintf("MSG _INBOX.a 1 %d\r\n%s\r\n", strlen('{"paused":false}'), '{"paused":false}'),
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // resumeConsumer issues a requestJson with an empty ([]) body.
        $client->jetStream()->resumeConsumer('ORDERS', 'PROC')->await();

        $written = $transport->writes[3];
        // Real: (object)[] -> "{}". Mutant: [] -> "[]".
        self::assertStringContainsString("\r\n{}\r\n", $written);
        self::assertStringNotContainsString("\r\n[]\r\n", $written);
    }
}
