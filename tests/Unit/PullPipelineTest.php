<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\PullServerTrait;
use PHPUnit\Framework\TestCase;

/**
 * End-to-end tests for the #120 pipelined pull engine (JetStreamContext::consumePipelined via
 * PullConsumerIterator::handle()). Uses the shared {@see PullServerTrait} harness, which delivers
 * frames exactly as a real server does: DATA on the message's original subject with a $JS.ACK reply
 * (no token), STATUS on the pull's reply token - so token-less data is attributed FIFO to the oldest
 * open pull and statuses correlate by token.
 */
final class PullPipelineTest extends TestCase
{
    use PullServerTrait;

    public function testSinglePullDeliversItsBatch(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'a'], ['msg' => 'b']],
        ]);
        $js = $this->context($transport);

        $received = [];
        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(2)
            ->setIterations(1)
            ->handle(function (NatsMessage $m) use (&$received): void {
                $received[] = $m->payload;
            })->await();

        self::assertSame(2, $processed);
        self::assertSame(['a', 'b'], $received);

        // One long-lived pull inbox SUB, and exactly one UNSUB at teardown (no per-pull churn).
        $subs = array_filter($transport->writes, static fn (string $w): bool => str_starts_with($w, 'SUB _INBOX.JS.PULL.'));
        $unsubs = array_filter($transport->writes, static fn (string $w): bool => str_starts_with($w, 'UNSUB '));
        self::assertCount(1, $subs);
        self::assertCount(1, $unsubs);
    }

    /** @return int count of pull-request PUBs written so far */
    private function pullPubCount(FakeTransport $transport): int
    {
        return count(array_filter(
            $transport->writes,
            static fn (string $w): bool => str_starts_with($w, 'PUB $JS.API.CONSUMER.MSG.NEXT.'),
        ));
    }

    public function testOverlapIssuesSecondPullBeforeDrainingTheFirst(): void
    {
        // Infinite, depth=2: two pulls each deliver one message, then a terminal 409 stops the run.
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'a']],
            [['msg' => 'b']],
            [['status' => 409, 'desc' => 'Consumer Deleted']],
        ]);
        $js = $this->context($transport);

        $pubsAtFirstDelivery = null;
        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(2)
            ->handle(function (NatsMessage $m) use (&$pubsAtFirstDelivery, $transport): void {
                // batch=1 must STILL pipeline: with depth=2 both pulls are issued before the first is
                // drained, so by the first delivery there are already 2 pull PUBs on the wire.
                $pubsAtFirstDelivery ??= $this->pullPubCount($transport);
            })->await();

        self::assertSame(2, $processed);
        self::assertSame(2, $pubsAtFirstDelivery, 'depth=2 must issue pull #2 before draining pull #1 (overlap)');
    }

    public function testFiniteModeIssuesExactlyNPullsAndStopsOnFirstEmpty(): void
    {
        // Finite N=3 is serial (effDepth=1). Pull #1 delivers, pull #2 is a 404 -> finite stops there:
        // exactly 2 pulls on the wire, total = pull #1's messages, no 3rd pull.
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'a'], ['msg' => 'b']],
            [['status' => 404, 'desc' => 'No Messages']],
        ]);
        $js = $this->context($transport);

        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(2)
            ->setIterations(3)
            ->handle(static function (): void {})->await();

        self::assertSame(2, $processed);
        self::assertSame(2, $this->pullPubCount($transport), 'finite mode must stop on the first empty (404), not issue N=3');
    }

    public function testTerminalStatusStopsAndReportsOnErrorExactlyOnce(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['status' => 409, 'desc' => 'Consumer Deleted']],
        ]);
        $js = $this->context($transport);

        $errors = [];
        $processed = $js->pullConsumer('S', 'C')
            ->setOnError(static function (\Throwable $e) use (&$errors): void {
                $errors[] = $e;
            })
            ->handle(static function (): void {})->await();

        self::assertSame(0, $processed);
        self::assertCount(1, $errors);
        self::assertInstanceOf(JetStreamException::class, $errors[0]);
        self::assertStringContainsString('Consumer Deleted', $errors[0]->getMessage());
    }

    /**
     * A 503 (no JS API responder - server restart window, JetStream still wiring up after a
     * reconnect, a leader election) is TRANSIENT: an infinite run must poll through it like the
     * sibling "409 Leadership Change" (and like publish()'s ADR-21 retry), not die on it. The first
     * 503 of a streak fires onError once as an operator signal; delivery re-arms the signal, and a
     * genuinely terminal status afterwards still stops the run. Pre-fix the 503 was terminal.
     */
    public function testInfiniteRunPollsThroughTransientNoResponders(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['status' => 503, 'desc' => 'No Responders Available For Request']],
            [['msg' => 'a']],
            [['status' => 409, 'desc' => 'Consumer Deleted']],
        ]);
        $js = $this->context($transport);

        $errors = [];
        $received = [];
        $processed = $js->pullConsumer('S', 'C')
            ->setOnError(static function (\Throwable $e) use (&$errors): void {
                $errors[] = $e;
            })
            ->handle(static function (NatsMessage $m) use (&$received): void {
                $received[] = $m->payload;
            })->await();

        self::assertSame(1, $processed, 'the run must survive the transient 503 and process the next pull');
        self::assertSame(['a'], $received);
        self::assertCount(2, $errors, 'exactly one 503 signal plus the terminal 409 report');
        self::assertInstanceOf(JetStreamException::class, $errors[0]);
        self::assertSame(503, $errors[0]->getCode());
        self::assertInstanceOf(JetStreamException::class, $errors[1]);
        self::assertStringContainsString('Consumer Deleted', $errors[1]->getMessage());
    }

    /**
     * The headline pre-fix failure: with NO onError configured, an infinite worker hitting a
     * momentary 503 terminated permanently and handle() resolved NORMALLY with the processed count -
     * indistinguishable from a clean drain, zero error signal, messages piling up in the stream. It
     * must keep consuming instead.
     */
    public function testInfiniteRunWithoutOnErrorSurvivesTransientNoResponders(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['status' => 503, 'desc' => 'No Responders Available For Request']],
            [['msg' => 'a']],
            [['status' => 409, 'desc' => 'Consumer Deleted']],
        ]);
        $js = $this->context($transport);

        $processed = $js->pullConsumer('S', 'C')
            ->handle(static function (): void {})->await();

        self::assertSame(1, $processed, 'pre-fix this resolved 0: the worker died silently on the 503');
    }

    /**
     * The one-shot no-responders signal re-arms on any routine NON-503 retire (404/408/...), not
     * only on a delivery: a 404 proves the JS API answers again, so a LATER outage on an idle
     * stream (which may never deliver between outages) must be reported too - delivery-only
     * re-arm left every outage after the first invisible for exactly the consumers least able to
     * notice one any other way.
     */
    public function testNoRespondersSignalReArmsAfterRoutineNonEmptyGap(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['status' => 503, 'desc' => 'No Responders Available For Request']],
            [['status' => 404, 'desc' => 'No Messages']],
            [['status' => 503, 'desc' => 'No Responders Available For Request']],
            [['status' => 409, 'desc' => 'Consumer Deleted']],
        ]);
        $js = $this->context($transport);

        $errors = [];
        $processed = $js->pullConsumer('S', 'C')
            ->setOnError(static function (\Throwable $e) use (&$errors): void {
                $errors[] = $e;
            })
            ->handle(static function (): void {})->await();

        self::assertSame(0, $processed);
        self::assertCount(3, $errors, 'both 503 outages plus the terminal 409 must be reported');
        self::assertSame(503, $errors[0]->getCode());
        self::assertSame(503, $errors[1]->getCode(), 'the second outage must be reported after the 404 re-armed the signal');
        self::assertStringContainsString('Consumer Deleted', $errors[2]->getMessage());
    }

    /**
     * A permission-rejected pull reply inbox can never deliver a reply: every pull retires as a
     * silent client-side deadline (classified routine), so pre-fix the engine spun FOREVER with
     * zero signal on any channel - handle() never returned and onError never fired. It must fail
     * fast with a clear, catchable configuration error (#167's pull twin).
     */
    public function testPermissionRejectedPullInboxFailsFastInsteadOfSpinning(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', []);

        // Chain onto the harness responder: answer the pull-inbox SUB with the server's async
        // permissions -ERR naming the exact subject, as a real server does.
        $inner = $transport->onWrite;
        $transport->onWrite = static function (string $bytes) use ($inner): array {
            $frames = $inner !== null ? $inner($bytes) : [];
            $head = strtok($bytes, "\r\n");
            if (is_string($head) && str_starts_with($head, 'SUB _INBOX.JS.PULL.')) {
                $subject = explode(' ', $head)[1] ?? '';
                $frames[] = "-ERR 'Permissions Violation for Subscription to \"" . $subject . "\"'\r\n";
            }

            return $frames;
        };

        $js = $this->context($transport);

        $startedNs = hrtime(true);
        try {
            $js->pullConsumer('S', 'C')
                ->setExpiresMs(300)
                ->handle(static function (): void {})->await(new \Amp\TimeoutCancellation(10.0));
            self::fail('a permission-rejected pull inbox must fail the run');
        } catch (JetStreamException $e) {
            self::assertStringContainsString('rejected by server permissions', $e->getMessage());
            self::assertStringContainsString('_INBOX.JS.PULL.>', $e->getMessage(), 'the error must name the wildcard to grant');
        }

        self::assertLessThan(8.0, (hrtime(true) - $startedNs) / 1e9, 'the failure must be prompt, not an eventual timeout');
    }

    public function testStopAbandonsRemainingBatch(): void
    {
        // One pull of batch 3; the handler stops after the first message -> the rest are abandoned.
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'a'], ['msg' => 'b'], ['msg' => 'c']],
        ]);
        $js = $this->context($transport);

        $iter = $js->pullConsumer('S', 'C')->setBatching(3);
        $received = [];
        $processed = $iter->handle(function (NatsMessage $m) use (&$received, $iter): void {
            $received[] = $m->payload;
            $iter->stop();
        })->await();

        self::assertSame(1, $processed);
        self::assertSame(['a'], $received);
    }

    public function testGroupedUnpinnedRunFansOutAfterFirstDelivery(): void
    {
        // Review finding #1 (#120): a priority group under the overflow/prioritized policy never emits a
        // Nats-Pin-Id, so its pin stays null for the whole run. The engine must pull serially ONLY until
        // its first (bootstrap) delivery - not forever - then fan out to setDepth(). gen1 (serial): pull#0
        // delivers 'a'; gen2 (fanned to depth 2): pull#1 delivers 'b' and pull#2 is a terminal 409, so BOTH
        // gen2 pulls are already on the wire by the time 'b' is delivered (3 pull PUBs). If depth stayed
        // clamped to 1 (the bug), only 2 pulls would be on the wire at 'b'.
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'a']],
            [['msg' => 'b']],
            [['status' => 409, 'desc' => 'Consumer Deleted']],
        ]);
        $js = $this->context($transport);

        $pubsAtB = null;
        $received = [];
        $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(2)
            ->setGroup('g')
            ->handle(function (NatsMessage $m) use (&$pubsAtB, &$received, $transport): void {
                $received[] = $m->payload;
                if ($m->payload === 'b') {
                    $pubsAtB = $this->pullPubCount($transport);
                }
            })->await();

        self::assertSame(['a', 'b'], $received);
        self::assertSame(3, $pubsAtB, 'an un-pinned group must fan out to setDepth() after its first delivery, not stay serial');
    }

    public function testPinnedGroupReserializesPinRecaptureAfterStalePin(): void
    {
        // #170: a pinned_client group (its deliveries carry a Nats-Pin-Id) captures a pin and fans out to
        // setDepth(). If a later 423 CLEARS the pin, the group must RE-SERIALIZE - pull one at a time to
        // re-capture the pin cleanly - rather than racing pin-less pulls at full depth again. gen1 (serial):
        // pull#0 delivers 'a' + pin p1 -> pinned, everPinned latched. gen2 (fanned to 2): pull#1 & pull#2
        // both 423 -> the pin is cleared. gen3 MUST be serial (everPinned && pin===null): pull#3 alone
        // re-captures with 'b' + p2. So exactly 4 pull PUBs are on the wire when 'b' is delivered. Without
        // the #170 re-serialization, gen3 would fan out (anyDelivered && pin===null), issuing pull#3 AND
        // pull#4 before 'b' -> 5 PUBs at 'b'.
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'a', 'pin' => 'p1']],                        // pull#0: bootstrap (serial), captures p1
            [['status' => 423, 'desc' => 'Nats-Pin-Id Mismatch']], // pull#1: stale pin -> clears pin
            [['status' => 423, 'desc' => 'Nats-Pin-Id Mismatch']], // pull#2: stale pin (fanned sibling)
            [['msg' => 'b', 'pin' => 'p2']],                       // pull#3: re-capture (must be serial)
            [['status' => 409, 'desc' => 'Consumer Deleted']],     // terminal
            [['status' => 409, 'desc' => 'Consumer Deleted']],     // terminal (spare, in case of fan-out)
        ]);
        $js = $this->context($transport);

        $pubsAtB = null;
        $received = [];
        $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(2)
            ->setGroup('g')
            ->handle(function (NatsMessage $m) use (&$pubsAtB, &$received, $transport): void {
                $received[] = $m->payload;
                if ($m->payload === 'b') {
                    $pubsAtB = $this->pullPubCount($transport);
                }
            })->await();

        self::assertSame(['a', 'b'], $received);
        self::assertSame(4, $pubsAtB, 'after a 423 pin loss, a pinned group must re-serialize its pin re-capture, not fan out');
    }

    public function testDeliveringPipelineIsNotClampedByInterleavedNoWaitEmpties(): void
    {
        // #169 repro: no_wait + depth 2 with steady traffic below depth*batch, so tail pulls race an
        // already-drained stream and 404 immediately while head pulls keep delivering. A single tail
        // empty must NOT latch the idle drain (streak 1 < depth 2): the pipeline keeps refilling at
        // full depth with no backoff pause and no serial clamp. Pre-#169, each interleaved 404 latched
        // the drain and forced a backoff + serial generation: only 5 pulls would reach the wire.
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'a']],                                   // pull#0: delivers (streak 0, refills #2)
            [['status' => 404, 'desc' => 'No Messages']],       // pull#1: streak 1 < 2 -> NO latch, refills #3
            [['msg' => 'b']],                                   // pull#2: delivers (streak 0, refills #4)
            [['status' => 404, 'desc' => 'No Messages']],       // pull#3: streak 1 -> NO latch, refills #5
            [['status' => 409, 'desc' => 'Consumer Deleted']],  // pull#4: terminal stops the run
            [['status' => 409, 'desc' => 'Consumer Deleted']],  // pull#5: already on the wire at full depth
        ]);
        $js = $this->context($transport);

        $received = [];
        $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(2)
            ->setNoWait(true)
            ->setOnError(static function (): void {})
            ->handle(function (NatsMessage $m) use (&$received): void {
                $received[] = $m->payload;
            })->await();

        self::assertSame(['a', 'b'], $received);
        self::assertSame(6, $this->pullPubCount($transport), 'interleaved no_wait empties must not clamp a delivering pipeline to serial');
    }

    public function testProductiveThenIdleStreamStillBacksOff(): void
    {
        // #169 soundness (the trap that sank the first fix attempt): the streak must survive the
        // engine's CONTINUOUS REFILL, so a stream that delivers and then goes idle still accumulates
        // consecutive empty retires to the latch and backs off - a per-generation "delivered" flag
        // would never reset (inflight rarely drains) and would disable the idle backoff forever.
        // Here: one delivery, then three no_wait 404s (streak 1 refill, 2 latch, 3 drain-out) ->
        // warranted boundary (>=10ms pacing) -> serial terminal generation: exactly 5 pulls.
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'a']],                                   // pull#0: delivers (streak resets)
            [['status' => 404, 'desc' => 'No Messages']],       // pull#1: streak 1 (refills #3)
            [['status' => 404, 'desc' => 'No Messages']],       // pull#2: streak 2 -> latch
            [['status' => 404, 'desc' => 'No Messages']],       // pull#3: drains out -> backoff boundary
            [['status' => 409, 'desc' => 'Consumer Deleted']],  // pull#4: serial terminal
        ]);
        $js = $this->context($transport);

        $startNs = hrtime(true);
        $received = [];
        $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(2)
            ->setNoWait(true)
            ->setOnError(static function (): void {})
            ->handle(function (NatsMessage $m) use (&$received): void {
                $received[] = $m->payload;
            })->await();
        $elapsedNs = hrtime(true) - $startNs;

        self::assertSame(['a'], $received);
        self::assertSame(5, $this->pullPubCount($transport), 'the post-delivery idle streak must clamp the next generation serial');
        // The warranted boundary paced the relaunch (first backoff step is 10ms); a fix that disabled
        // the idle backoff after any delivery would relaunch instantly at full depth.
        self::assertGreaterThanOrEqual(9_000_000, $elapsedNs, 'a productive-then-idle no_wait stream must still be paced');
    }

    public function testDrainFinishesInFlightBatchThenStops(): void
    {
        // One pull of batch 3; the handler drains after the first message -> the whole in-flight batch
        // is delivered, but no further pull is issued.
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'a'], ['msg' => 'b'], ['msg' => 'c']],
        ]);
        $js = $this->context($transport);

        $iter = $js->pullConsumer('S', 'C')->setBatching(3)->setDepth(1);
        $received = [];
        $processed = $iter->handle(function (NatsMessage $m) use (&$received, $iter): void {
            $received[] = $m->payload;
            $iter->drain();
        })->await();

        self::assertSame(3, $processed);
        self::assertSame(['a', 'b', 'c'], $received);
        self::assertSame(1, $this->pullPubCount($transport), 'drain must not issue a further pull');
    }
}
