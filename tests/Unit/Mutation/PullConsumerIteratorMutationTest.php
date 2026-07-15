<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\PullServerTrait;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing tests for {@see \IDCT\NATS\JetStream\Consumers\PullConsumerIterator} over the #120
 * pipelined pull engine ({@see \IDCT\NATS\JetStream\JetStreamContext::consumePipelined()}).
 *
 * Every test pins a single observable behavior that a surviving Infection mutant would change: an exact
 * pull count, the bytes of the issued pull request, or the captured/retained pin id. Replies are
 * delivered by {@see PullServerTrait::pullServer()} on each pull's captured reply-to (token-routed);
 * where per-pull sequencing is the point (an infinite run), the iterator pins setDepth(1) so the run
 * stays serial and each pull maps 1:1 to a scripted response. Finite runs are always strictly serial.
 */
final class PullConsumerIteratorMutationTest extends TestCase
{
    use PullServerTrait;

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
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            // pull 1: one real message.
            [['msg' => 'only-one']],
            // Safety net: any (buggy) 2nd pull immediately gets a 404 so the test stays bounded/fast.
            [['status' => 404, 'desc' => 'No Messages']],
        ]);
        $js = $this->context($transport);

        $processed = [];
        $total = $js
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
     * two messages) and stop - not once, not three times. Provides a clearer failure for the
     * off-by-one / counter-direction mutants on the loop.
     *
     * Kills (corroborates): DecrementInteger@262, LessThan@264, Increment@268.
     */
    public function testFiniteIterationsRunsLoopExactlyTwice(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'a']],
            [['msg' => 'b']],
            // Safety net for an over-running 3rd pull.
            [['status' => 404, 'desc' => 'No Messages']],
        ]);
        $js = $this->context($transport);

        $processed = [];
        $total = $js
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
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'm']],
            [['status' => 404, 'desc' => 'No Messages']],
        ]);
        $js = $this->context($transport);

        $js
            ->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setIterations(1)
            ->setNoWait() // default argument - must be true
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
     * which would make a 408 terminal and stop the loop before the message - total would be 0.
     *
     * Kills: DecrementInteger @ 296, IncrementInteger @ 296.
     */
    public function testInfiniteModeContinuesPast408Timeout(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            // pull 1: routine 408 timeout - infinite mode must keep polling, not stop.
            [['status' => 408, 'desc' => 'Request Timeout']],
            // pull 2: the message arrives on the next pull.
            [['msg' => 'after-timeout']],
            // pull 3: a terminal 409 (consumer deleted) stops the loop.
            [['status' => 409, 'desc' => 'Consumer Deleted']],
        ]);
        $js = $this->context($transport);

        $processed = [];
        $total = $js
            ->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setDepth(1) // serial: one pull per scripted response (per-pull sequencing under test)
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
     * real code must stop on a terminal 409 (it is NOT in the routine list and isNonTerminalPullStatus()
     * is false for "Consumer Deleted"). We prove the loop stops: no message is delivered after it.
     *
     * setDepth(1) keeps the run serial so a stop on the very first pull means exactly one pull was
     * issued (a depth>1 run would fan out extra pulls before retiring the terminal head).
     *
     * Kills: IncrementInteger @ 296 (corroborates).
     */
    public function testInfiniteModeStopsOnTerminal409(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            // pull 1: terminal 409 must stop the loop immediately.
            [['status' => 409, 'desc' => 'Consumer Deleted']],
            // If the loop (wrongly) kept polling, it would deliver this - it must NOT be reached.
            [['msg' => 'should-not-arrive']],
        ]);
        $js = $this->context($transport);

        $delivered = false;
        $total = $js
            ->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setDepth(1)
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
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            // pull 1: a message that DOES carry a pin id, but no group is configured.
            [['msg' => 'p1', 'pin' => 'leaked-pin']],
            // pull 2: a plain message; its pull must NOT carry an "id" under the real code.
            [['msg' => 'p2']],
            [['status' => 404, 'desc' => 'No Messages']],
        ]);
        $js = $this->context($transport);

        $js
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
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            // pull 1: first delivery pins the group with "pin-1".
            [['msg' => 'm1', 'pin' => 'pin-1']],
            // pull 2: a message advertising a DIFFERENT pin "pin-2"; real code must ignore it.
            [['msg' => 'm2', 'pin' => 'pin-2']],
            // pull 3: a plain message; its pull must still carry the ORIGINAL "pin-1".
            [['msg' => 'm3']],
            [['status' => 404, 'desc' => 'No Messages']],
        ]);
        $js = $this->context($transport);

        $js
            ->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setGroup('g1')
            ->setIterations(4)
            ->handle(static function (): void {})->await();

        $pulls = $this->pullWrites($transport);
        self::assertGreaterThanOrEqual(3, count($pulls));

        // pull 2 carries the pin captured in pull 1.
        self::assertStringContainsString('"id":"pin-1"', $pulls[1]);
        // kills LogicalAnd@314 (second conjunction): pin must NOT be re-captured to "pin-2".
        self::assertStringContainsString('"id":"pin-1"', $pulls[2], 'captured pin id must be retained, not overwritten');
        self::assertStringNotContainsString('"id":"pin-2"', $pulls[2]);
    }
}
