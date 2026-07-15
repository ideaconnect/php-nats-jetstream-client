<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\PullServerTrait;
use PHPUnit\Framework\TestCase;

/**
 * Mutation-killing tests for the #120 pipelined pull engine in JetStreamContext::consumePipelined()
 * (chunk 9). Each test pins a specific observable behavior - the exact number of pull requests that
 * reach the wire, whether a message is delivered, or whether a pin is dropped - that one surviving
 * mutation would flip.
 *
 * The idle-streak mutants (@ 2313/2315/2351) are observed through effectivePullDepth(): a warranted
 * backoff increments $consecutiveEmptyPulls, which forces the NEXT generation strictly serial
 * (depth 1); a missing/spurious backoff leaves it at the configured depth (2). So the count of pull
 * PUBs a bounded, self-terminating script emits differs by exactly one between the real code and the
 * mutation.
 */
final class JetStreamContext_9MutationTest extends TestCase
{
    use PullServerTrait;

    private static function msgFrame(string $replyTo, int $sid, string $payload): string
    {
        // DATA is delivered on the ORIGINAL subject with a $JS.ACK reply (not the pull reply token);
        // $replyTo is accepted for call-site symmetry with statusFrame() but deliberately unused.
        unset($replyTo);

        return sprintf("MSG evt.s %d \$JS.ACK.S.C.1.1.1.0.0 %d\r\n%s\r\n", $sid, strlen($payload), $payload);
    }

    private static function statusFrame(string $replyTo, int $sid, int $code, string $desc): string
    {
        $hdr = sprintf("NATS/1.0 %d %s\r\n\r\n", $code, $desc);

        return sprintf("HMSG %s %d %d %d\r\n%s\r\n", $replyTo, $sid, strlen($hdr), strlen($hdr), $hdr);
    }

    /**
     * Installs a responder that buffers a whole depth-2 generation together in ONE read chunk: the first
     * pull PUB's reply is held, and the second pull PUB emits $onSecond(headReply, tailReply, sid) whose
     * frames the test packs into a single chunk - so both pulls are buffered before the engine's retire
     * pass runs (the only way to observe drain-all-then-issue ordering in-process). Data frames land on
     * the oldest open pull FIFO (head first) regardless of which reply token is passed; the head/tail
     * tokens matter only for STATUS frames. Any third+ pull PUB gets a terminal 409 so the run stops.
     *
     * @param \Closure(string, string, int): list<string> $onSecond
     */
    private function packedGenerationResponder(FakeTransport $transport, string $stream, string $consumer, \Closure $onSecond): void
    {
        $pullSubject = '$JS.API.CONSUMER.MSG.NEXT.' . $stream . '.' . $consumer;
        $sid = 1;
        /** @var list<string> $replyTos */
        $replyTos = [];
        $transport->onWrite = static function (string $bytes) use ($pullSubject, &$sid, &$replyTos, $onSecond): array {
            $head = strtok($bytes, "\r\n");
            if ($head === false) {
                return [];
            }

            if (str_starts_with($head, 'SUB _INBOX.JS.PULL.') && str_contains($head, '.* ')) {
                $sid = (int) (explode(' ', $head)[2] ?? 1);

                return [];
            }

            if (!str_starts_with($head, 'PUB ' . $pullSubject . ' ')) {
                return [];
            }

            $replyTo = explode(' ', $head)[2] ?? '';
            $replyTos[] = $replyTo;
            $count = count($replyTos);

            if ($count === 1) {
                return []; // hold the head pull's reply so the tail can complete first
            }

            if ($count === 2) {
                return $onSecond($replyTos[0], $replyTos[1], $sid);
            }

            return [self::statusFrame($replyTo, $sid, 409, 'Consumer Deleted')];
        };
    }

    /**
     * A routine 409 empty (non-terminal, e.g. "Leadership Change") in infinite mode DOES warrant an
     * idle backoff, so $consecutiveEmptyPulls increments and the next generation is issued serially
     * (depth 1). Generation 1 is two empty pulls (depth 2), then one terminal pull -> exactly 3 PUBs.
     */
    public function testRoutine409EmptyWarrantsBackoffSoNextGenerationIsSerial(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['status' => 409, 'desc' => 'Leadership Change']], // pull#0 (gen1, depth 2)
            [['status' => 409, 'desc' => 'Leadership Change']], // pull#1 (gen1)
            [['status' => 409, 'desc' => 'Consumer Deleted']],  // pull#2 (gen2, serial) - terminal
            [['status' => 409, 'desc' => 'Consumer Deleted']],  // pull#3 - only a mutant that skipped backoff reaches it
        ]);
        $js = $this->context($transport);

        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(2)
            ->setExpiresMs(200)
            ->handle(static function (): void {})->await();

        self::assertSame(0, $processed);
        // kills LessThan(409->408) @ 2351, IncrementInteger(409->410) @ 2351, Identical(===->!==) @ 2351:
        // each stops a routine 409 from warranting a backoff, leaving generation 2 at depth 2 -> 4 PUBs.
        self::assertCount(3, $this->pullWrites($transport));
    }

    /**
     * A plain WAITING 404 empty (noWait=false) must NOT warrant a backoff (it already spent its expires
     * window server-side), so $consecutiveEmptyPulls stays 0 and the next generation stays at depth 2.
     * Generation 1 is two 404 pulls, then two terminal pulls (depth 2) -> exactly 4 PUBs.
     */
    public function testWaiting404EmptyDoesNotWarrantBackoffSoNextGenerationStaysParallel(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['status' => 404, 'desc' => 'No Messages']],       // pull#0 (gen1, depth 2)
            [['status' => 404, 'desc' => 'No Messages']],       // pull#1 (gen1)
            [['status' => 409, 'desc' => 'Consumer Deleted']],  // pull#2 (gen2) - terminal (head)
            [['status' => 409, 'desc' => 'Consumer Deleted']],  // pull#3 (gen2) - issued only when gen2 stays depth 2
        ]);
        $js = $this->context($transport);

        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(2)
            ->setExpiresMs(200)
            ->handle(static function (): void {})->await();

        self::assertSame(0, $processed);
        // kills LogicalOrAllSubExprNegation(!noWait || !(code===409)) @ 2351 (and the Identical mutant):
        // a mutant that DOES warrant a backoff for a waiting 404 forces generation 2 serial -> 3 PUBs.
        self::assertCount(4, $this->pullWrites($transport));
    }

    /**
     * A terminal error retires the whole in-flight generation: once it fires onError and breaks, no
     * further already-buffered pull in the generation is delivered. Here the head pull (pull#0) turns
     * out terminal while the tail pull (pull#1) already holds a message; that message must be abandoned
     * (processed stays 0), which the break at the terminal-retire guarantees.
     *
     * The head's 409 is packed BEFORE the tail's data in one chunk, so the router sees the head done
     * first and then attributes the token-less data frame to the (still open) tail - reproducing "head
     * terminal, tail buffered" without relying on out-of-order completion (impossible under FIFO).
     */
    public function testTerminalErrorAbandonsAlreadyBufferedTailOfGeneration(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->packedGenerationResponder($transport, 'S', 'C', static fn (string $head, string $tail, int $sid): array => [
            self::statusFrame($head, $sid, 409, 'Consumer Deleted') . self::msgFrame($tail, $sid, 'x'),
        ]);
        $js = $this->context($transport);

        $received = [];
        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(2)
            ->setExpiresMs(200)
            ->handle(function (NatsMessage $m) use (&$received): void {
                $received[] = $m->payload;
            })->await();

        // kills Break_->Continue_ @ 2367: a mutant that continues past the terminal head would go on to
        // deliver the tail pull's buffered 'x' instead of abandoning it.
        self::assertSame(0, $processed);
        self::assertSame([], $received);
    }

    /**
     * A successful delivery drains EVERY ready pull in the current retire pass before the engine issues
     * any fresh pull (continue at 2317), so at the moment the second buffered message is handled no new
     * pull has yet reached the wire. With out-of-order completion both pulls are ready together: the
     * count of pull PUBs seen when 'b' is delivered pins the ordering.
     */
    public function testDeliveryDrainsAllReadyPullsBeforeIssuingMore(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        // One packed chunk holds both pulls' batches: FIFO gives 'a' to the head (pull#0) and 'b' to
        // the tail (pull#1), so both are ready together when the retire pass runs.
        $this->packedGenerationResponder($transport, 'S', 'C', static fn (string $head, string $tail, int $sid): array => [
            self::msgFrame($head, $sid, 'a') . self::msgFrame($tail, $sid, 'b'),
        ]);
        $js = $this->context($transport);

        $pubsAtB = null;
        $order = [];
        $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(2)
            ->setExpiresMs(200)
            ->handle(function (NatsMessage $m) use (&$pubsAtB, &$order, $transport): void {
                $order[] = $m->payload;
                if ($m->payload === 'b') {
                    $pubsAtB = count($this->pullWrites($transport));
                }
            })->await();

        self::assertSame(['a', 'b'], $order);
        // kills Continue_->Break_ @ 2317: a mutant that breaks after delivering 'a' issues a refill pull
        // before draining 'b', so 3 pull PUBs would be on the wire when 'b' is handled, not 2.
        self::assertSame(2, $pubsAtB);
    }

    /**
     * A 423 stale-pin retire drops the pin (setPinId(null) @ 2327) so the next pull is re-issued
     * WITHOUT the dead `id`. A grouped consumer captures a pin from its first message, carries it on
     * the following pull, hits a 423, and the pull after that must no longer carry the pin.
     */
    public function testStalePin423DropsPinBeforeRepulling(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'm', 'pin' => 'PIN1']],                  // pull#0: pin message -> captures PIN1
            [['status' => 423, 'desc' => 'stale pin']],         // pull#1: carries id=PIN1, gets a 423
            [['status' => 409, 'desc' => 'Consumer Deleted']],  // pull#2: must NOT carry id after the drop
        ]);
        $js = $this->context($transport);

        $processed = $js->pullConsumer('S', 'C')
            ->setGroup('g')
            ->setBatching(1)
            ->setDepth(1)
            ->setExpiresMs(200)
            ->handle(static function (): void {})->await();

        self::assertSame(1, $processed);
        $pulls = $this->pullWrites($transport);
        self::assertCount(3, $pulls);
        // Sanity: the pin WAS captured and carried on the pull issued after the pin message.
        self::assertStringContainsString('"id":"PIN1"', $pulls[1]);
        // kills MethodCallRemoval(setPinId(null)) @ 2327: without the drop the stale PIN1 rides the
        // re-pull, so the third pull request would still carry "id":"PIN1".
        self::assertStringNotContainsString('"id"', $pulls[2]);
    }

    /**
     * A delivery resets the idle streak to exactly 0 (not -1) @ 2313. After the reset, one warranted
     * (routine 409) empty generation increments it to 1, forcing the following generation serial. A
     * mutant that reset to -1 would need TWO increments to go serial, leaving one extra parallel
     * generation -> one extra pull PUB.
     */
    public function testDeliveryResetsIdleStreakToZeroNotNegative(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'a']],                                   // pull#0: delivers -> resets the streak
            [['msg' => 'b']],                                   // pull#1: delivers (refill during delivery)
            [['status' => 409, 'desc' => 'Leadership Change']], // pull#2: routine empty (refill)
            [['status' => 409, 'desc' => 'Leadership Change']], // pull#3: routine empty -> backoff, streak=1
            [['status' => 409, 'desc' => 'Consumer Deleted']],  // pull#4: serial terminal (real)
            [['status' => 409, 'desc' => 'Consumer Deleted']],  // pull#5: only a -1-resetting mutant reaches it
        ]);
        $js = $this->context($transport);

        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(2)
            ->setExpiresMs(200)
            ->handle(static function (): void {})->await();

        self::assertSame(2, $processed);
        // kills DecrementInteger(0->-1) @ 2313: resetting to -1 needs an extra empty generation to reach
        // the serial threshold, so generation after the idle wave stays depth 2 -> 6 PUBs, not 5.
        self::assertCount(5, $this->pullWrites($transport));
    }

    /**
     * A delivery clears the backoff-warranted latch to false @ 2315, so a subsequent NON-warranted
     * (waiting 404) empty generation does not spuriously back off: the streak stays 0 and the next
     * generation stays depth 2. A mutant that latched true would back off, go serial, and issue one
     * FEWER terminal pull.
     */
    public function testDeliveryClearsBackoffWarrantedLatch(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'a']],                                   // pull#0: delivers -> clears the latch
            [['msg' => 'b']],                                   // pull#1: delivers (refill)
            [['status' => 404, 'desc' => 'No Messages']],       // pull#2: waiting empty (no backoff warranted)
            [['status' => 404, 'desc' => 'No Messages']],       // pull#3: waiting empty
            [['status' => 409, 'desc' => 'Consumer Deleted']],  // pull#4: terminal (head of depth-2 gen)
            [['status' => 409, 'desc' => 'Consumer Deleted']],  // pull#5: issued only while the gen stays depth 2
        ]);
        $js = $this->context($transport);

        $processed = $js->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setDepth(2)
            ->setExpiresMs(200)
            ->handle(static function (): void {})->await();

        self::assertSame(2, $processed);
        // kills FalseValue(false->true) @ 2315: a latched-true backoffWarranted makes the waiting-404
        // generation back off and go serial, so only 5 pull PUBs would reach the wire, not 6.
        self::assertCount(6, $this->pullWrites($transport));
    }
}
