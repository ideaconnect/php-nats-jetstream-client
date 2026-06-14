<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing tests for {@see \IDCT\NATS\JetStream\Consumers\PullConsumerIterator}.
 *
 * Every test pins a single observable behavior that a surviving Infection mutant would change:
 * an exact pull count, the bytes of the issued pull request, or the captured/retained pin id.
 */
final class PullConsumerIteratorMutationTest extends TestCase
{
    /** @return list<string> */
    private function infoAndPong(): array
    {
        return [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
    }

    private function jsMsg(string $payload, string $sid, string $replyTo): string
    {
        return sprintf("MSG _INBOX.JS.FETCH.any %s %s %d\r\n%s\r\n", $sid, $replyTo, strlen($payload), $payload);
    }

    private function statusFrame(int $code, string $reason, string $sid): string
    {
        $headers = sprintf("NATS/1.0 %d %s\r\nStatus: %d\r\n\r\n", $code, $reason, $code);
        $len = strlen($headers);

        return sprintf("HMSG _INBOX.JS.FETCH.any %s %d %d\r\n%s\r\n", $sid, $len, $len, $headers);
    }

    /**
     * Message carrying a Nats-Pin-Id header (and an ACK reply-to) so pinIdOf() returns a non-null id.
     */
    private function pinnedMsg(string $payload, string $sid, string $pinId): string
    {
        $hdrs = sprintf("NATS/1.0\r\nNats-Pin-Id: %s\r\n\r\n", $pinId);

        return sprintf(
            "HMSG _INBOX.JS.FETCH.any %s \$JS.ACK.S.C.1.1.1.0.0 %d %d\r\n%s%s\r\n",
            $sid,
            strlen($hdrs),
            strlen($hdrs) + strlen($payload),
            $hdrs,
            $payload,
        );
    }

    /** @return list<string> The CONSUMER.MSG.NEXT pull requests recorded on the transport, in order. */
    private function pullWrites(FakeTransport $transport): array
    {
        return array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_contains($w, 'CONSUMER.MSG.NEXT'),
        ));
    }

    /**
     * Pins the finite-mode iteration boundary. With iterations=1 the loop must run EXACTLY once: a
     * single pull is issued, the one message is processed, and NO second pull is ever attempted.
     *
     * Kills four loop-control mutants at once, each of which over-runs the loop and issues a 2nd pull:
     *  - DecrementInteger @ 262 ($iteration = 0 -> -1)
     *  - LessThan @ 264 ($iteration < $this->iterations -> <=)
     *  - LogicalOrAllSubExprNegation @ 264 (whole OR sub-expr negated -> iteration cap ignored)
     *  - Increment @ 268 (++$iteration -> --$iteration -> counter never reaches the cap)
     */
    public function testFiniteIterationsRunsLoopExactlyOnce(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // iter 1 (sid 1): one real message.
            $this->jsMsg('only-one', '1', '$JS.ACK.S.C.1.1.1.0.0'),
            // Safety net: any (buggy) 2nd pull immediately gets a 404 so the test stays bounded/fast.
            $this->statusFrame(404, 'No Messages', '2'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $processed = [];
        $total = $client->jetStream()
            ->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setIterations(1)
            ->handle(function (NatsMessage $msg) use (&$processed): void {
                $processed[] = $msg->payload;
            })->await();

        // kills DecrementInteger@262, LessThan@264, LogicalOrAllSubExprNegation@264, Increment@268
        self::assertCount(1, $this->pullWrites($transport), 'iterations=1 must issue exactly one pull request');
        self::assertSame(1, $total);
        self::assertSame(['only-one'], $processed);
    }

    /**
     * Reinforces the iteration counter for iterations=2: the loop must run EXACTLY twice (two pulls,
     * two messages) and stop — not once, not three times. Provides a clearer failure for the
     * off-by-one / counter-direction mutants on the loop.
     *
     * Kills (corroborates): DecrementInteger@262, LessThan@264, Increment@268.
     */
    public function testFiniteIterationsRunsLoopExactlyTwice(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            $this->jsMsg('a', '1', '$JS.ACK.S.C.1.1.1.0.0'),
            $this->jsMsg('b', '2', '$JS.ACK.S.C.1.2.2.0.0'),
            // Safety net for an over-running 3rd pull.
            $this->statusFrame(404, 'No Messages', '3'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $processed = [];
        $total = $client->jetStream()
            ->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setIterations(2)
            ->handle(function (NatsMessage $msg) use (&$processed): void {
                $processed[] = $msg->payload;
            })->await();

        // kills DecrementInteger@262, LessThan@264, Increment@268
        self::assertCount(2, $this->pullWrites($transport), 'iterations=2 must issue exactly two pull requests');
        self::assertSame(2, $total);
        self::assertSame(['a', 'b'], $processed);
    }

    /**
     * Pins the no_wait default. setNoWait() with NO argument must default to true and therefore emit
     * "no_wait":true in the pull request. The mutant flips the default to false, omitting the field.
     *
     * Kills: TrueValue @ 180 (setNoWait(bool $noWait = true) -> = false).
     */
    public function testSetNoWaitDefaultEmitsNoWaitTrueInPull(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            $this->jsMsg('m', '1', '$JS.ACK.S.C.1.1.1.0.0'),
            $this->statusFrame(404, 'No Messages', '2'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()
            ->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setIterations(1)
            ->setNoWait() // default argument — must be true
            ->handle(static function (): void {})->await();

        $pulls = $this->pullWrites($transport);
        self::assertNotEmpty($pulls);

        // kills TrueValue@180
        self::assertStringContainsString('"no_wait":true', $pulls[0], 'setNoWait() default must enable no_wait');
    }

    /**
     * Pins infinite-mode handling of a 408 (request timeout): it is a ROUTINE empty window, so the
     * loop must keep polling and ultimately deliver the message that arrives on the next pull.
     *
     * Both mutants at line 296 drop 408 from the routine list ([404,408] -> [404,407] / [404,409]),
     * which would make a 408 terminal and stop the loop before the message — total would be 0.
     *
     * Kills: DecrementInteger @ 296, IncrementInteger @ 296.
     */
    public function testInfiniteModeContinuesPast408Timeout(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // iter 1 (sid 1): routine 408 timeout — infinite mode must keep polling, not stop.
            $this->statusFrame(408, 'Request Timeout', '1'),
            // iter 2 (sid 2): the message arrives on the next pull.
            $this->jsMsg('after-timeout', '2', '$JS.ACK.S.C.1.1.1.0.0'),
            // iter 3 (sid 3): a terminal 409 (consumer deleted) stops the loop.
            $this->statusFrame(409, 'Consumer Deleted', '3'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $processed = [];
        $total = $client->jetStream()
            ->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setIterations(null) // infinite
            ->handle(function (NatsMessage $msg) use (&$processed): void {
                $processed[] = $msg->payload;
            })->await();

        // kills DecrementInteger@296, IncrementInteger@296
        self::assertSame(1, $total, 'a 408 must be routine in infinite mode: the next message is delivered');
        self::assertSame(['after-timeout'], $processed);
    }

    /**
     * Pins the IncrementInteger@296 mutant specifically: [404,408] -> [404,409] would also make a
     * TERMINAL 409 "Consumer Deleted" be treated as routine in infinite mode and keep polling. The
     * real code must stop on a terminal 409 (it is NOT in the routine list and isTransientPullStatus()
     * is false for "Consumer Deleted"). We prove the loop stops: no message is delivered after it.
     *
     * Kills: IncrementInteger @ 296 (corroborates).
     */
    public function testInfiniteModeStopsOnTerminal409(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // iter 1 (sid 1): terminal 409 must stop the loop immediately.
            $this->statusFrame(409, 'Consumer Deleted', '1'),
            // If the loop (wrongly) kept polling, it would deliver this — it must NOT be reached.
            $this->jsMsg('should-not-arrive', '2', '$JS.ACK.S.C.1.1.1.0.0'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $delivered = false;
        $total = $client->jetStream()
            ->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setIterations(null) // infinite
            ->handle(function () use (&$delivered): void {
                $delivered = true;
            })->await();

        // kills IncrementInteger@296 (terminal 409 must NOT be treated as routine)
        self::assertSame(0, $total, 'a terminal 409 must stop the infinite loop');
        self::assertFalse($delivered, 'no message must be delivered after a terminal 409');
        self::assertCount(1, $this->pullWrites($transport), 'the loop must not pull again after a terminal 409');
    }

    /**
     * Pins the first AND at line 314: the pin id is captured ONLY when a group is configured. With no
     * group set, even if a delivered message carries a Nats-Pin-Id header, the iterator must NOT
     * capture it, so the next pull must NOT include an "id" field.
     *
     * Mutant: ($group !== null && $pinId === null) -> ($group !== null || $pinId === null). With no
     * group + null pinId + a non-empty batch the mutated condition becomes true and captures the pin,
     * leaking "id":"leaked-pin" into the second pull.
     *
     * Kills: LogicalAnd @ 314 (&& -> || on the first conjunction).
     */
    public function testPinNotCapturedWhenNoGroupConfigured(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // iter 1 (sid 1): a message that DOES carry a pin id, but no group is configured.
            $this->pinnedMsg('p1', '1', 'leaked-pin'),
            // iter 2 (sid 2): a plain message; its pull must NOT carry an "id" under the real code.
            $this->jsMsg('p2', '2', '$JS.ACK.S.C.1.2.2.0.0'),
            $this->statusFrame(404, 'No Messages', '3'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()
            ->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setIterations(3)
            // NO setGroup(): pin capture must stay disabled.
            ->handle(static function (): void {})->await();

        $pulls = $this->pullWrites($transport);
        self::assertGreaterThanOrEqual(2, count($pulls));

        // kills LogicalAnd@314 (first conjunction)
        self::assertStringNotContainsString('"id"', $pulls[1], 'no group => pin id must never be captured/sent');
    }

    /**
     * Pins the second AND at line 314: once a pin id is captured for a group, a later message in a
     * subsequent batch must NOT overwrite it (the guard $pinId === null prevents re-capture). The real
     * code retains the original pin id across iterations.
     *
     * Mutant: ($group !== null && $pinId === null && $messages !== []) ->
     *         ($group !== null && $pinId === null || $messages !== []). With a non-empty batch the
     * mutated condition is always true, so it re-captures the pin from each batch's first message and
     * would switch the sent id from "pin-1" to "pin-2".
     *
     * Kills: LogicalAnd @ 314 (second conjunction).
     */
    public function testCapturedPinIdIsRetainedAcrossIterations(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // iter 1 (sid 1): first delivery pins the group with "pin-1".
            $this->pinnedMsg('m1', '1', 'pin-1'),
            // iter 2 (sid 2): a message advertising a DIFFERENT pin "pin-2"; real code must ignore it.
            $this->pinnedMsg('m2', '2', 'pin-2'),
            // iter 3 (sid 3): a plain message; its pull must still carry the ORIGINAL "pin-1".
            $this->jsMsg('m3', '3', '$JS.ACK.S.C.1.3.3.0.0'),
            $this->statusFrame(404, 'No Messages', '4'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()
            ->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setGroup('g1')
            ->setIterations(4)
            ->handle(static function (): void {})->await();

        $pulls = $this->pullWrites($transport);
        self::assertGreaterThanOrEqual(3, count($pulls));

        // iter 2 pull carries the pin captured in iter 1.
        self::assertStringContainsString('"id":"pin-1"', $pulls[1]);
        // kills LogicalAnd@314 (second conjunction): pin must NOT be re-captured to "pin-2".
        self::assertStringContainsString('"id":"pin-1"', $pulls[2], 'captured pin id must be retained, not overwritten');
        self::assertStringNotContainsString('"id":"pin-2"', $pulls[2]);
    }
}
