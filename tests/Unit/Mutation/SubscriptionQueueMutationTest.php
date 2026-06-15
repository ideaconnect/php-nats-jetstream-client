<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Core\SubscriptionQueue;
use IDCT\NATS\Exception\NatsException;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

use function Amp\async;

final class SubscriptionQueueMutationTest extends TestCase
{
    private function makeConnectedClient(FakeTransport $transport): NatsClient
    {
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        return $client;
    }

    /** @return list<string> */
    private function infoAndPong(): array
    {
        return [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
    }

    private function msg(string $payload): NatsMessage
    {
        return new NatsMessage('events', 99, null, $payload);
    }

    // ------------------------------------------------------------------
    // Line 42: default $maxPending = 1024
    // ------------------------------------------------------------------

    /**
     * Pins the DEFAULT maxPending at exactly 1024. Using DropOldest, after enqueuing 1024 messages
     * the queue is full; the 1025th drops the oldest, so the buffer count stays at 1024 and the
     * surviving head is the 2nd-enqueued message.
     *
     * - DecrementInteger (default 1023): cap reached at 1023, an extra drop happens earlier so the
     *   head after 1025 enqueues would differ / count would be 1023.
     * - IncrementInteger (default 1025): cap is 1025, so the 1025th enqueue does NOT drop and count
     *   would be 1025 with the original head intact.
     */
    public function testDefaultMaxPendingIsExactly1024(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = $this->makeConnectedClient($transport);
        // No maxPending argument -> exercises the default.
        $queue = new SubscriptionQueue($client, 99);

        for ($i = 0; $i < 1025; $i++) {
            $queue->enqueue($this->msg('m' . $i));
        }

        $messages = $queue->fetchAll();

        // kills DecrementInteger/IncrementInteger @ 42: count must be exactly 1024.
        self::assertCount(1024, $messages);
        // With cap 1024 the very first ('m0') was dropped, so head is 'm1'.
        self::assertSame('m1', $messages[0]->payload);
        self::assertSame('m1024', $messages[1023]->payload);
    }

    // ------------------------------------------------------------------
    // Line 58: $limit = max(1, $this->maxPending)
    // ------------------------------------------------------------------

    /**
     * With maxPending = 1 and the Error policy, the SECOND enqueue must throw (count 1 >= limit 1).
     *
     * - IncrementInteger (max(2, ...)): limit becomes 2, so the second enqueue would NOT throw.
     * - Logical/identity: original max(1,1) = 1.
     */
    public function testMaxOfOneFloorThrowsOnSecondEnqueueWithErrorPolicy(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = $this->makeConnectedClient($transport);
        $queue = new SubscriptionQueue($client, 99, 1, SlowConsumerPolicy::Error);

        $queue->enqueue($this->msg('a')); // count 0 >= 1? no -> stored

        $this->expectException(NatsException::class);
        // kills IncrementInteger @ 58: second enqueue (count 1 >= limit 1) must overflow.
        $queue->enqueue($this->msg('b'));
    }

    /**
     * With maxPending = 0 the floor of max(1, 0) = 1 must allow the FIRST enqueue to succeed.
     *
     * - DecrementInteger (max(0, 0) = 0): limit 0 means the very first enqueue sees count 0 >= 0 and
     *   would throw immediately (Error policy) / dequeue an empty queue.
     */
    public function testMaxFloorClampsZeroToOneAllowingFirstEnqueue(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = $this->makeConnectedClient($transport);
        $queue = new SubscriptionQueue($client, 99, 0, SlowConsumerPolicy::Error);

        // kills DecrementInteger @ 58: real limit is 1, so the first enqueue must NOT throw.
        $queue->enqueue($this->msg('a'));

        $messages = $queue->fetchAll();
        self::assertCount(1, $messages);
        self::assertSame('a', $messages[0]->payload);
    }

    // ------------------------------------------------------------------
    // Line 122 / 147: fetch()/next() return the buffered dequeue
    // ------------------------------------------------------------------

    /**
     * fetch() must RETURN the already-buffered message via the early return, WITHOUT starting any
     * processIncoming read. A blockWhenEmpty transport makes the difference observable: the real code
     * returns 'first' with zero reads; the mutant falls through into a (blocking) read cycle.
     *
     * - ReturnRemoval @ 122: removing the return makes fetch() skip the buffered short-circuit and
     *   start a fresh processIncoming cycle that parks on the idle socket (startedReads > 0).
     */
    public function testFetchReturnsBufferedMessageWithoutReading(): void
    {
        $transport = new FakeTransport($this->infoAndPong(), blockWhenEmpty: true);
        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('events')->await();

        // Buffer one message directly (no further frames are queued; the transport blocks on read).
        $queue->enqueue($this->msg('first'));

        $msg = \Amp\Future\await(
            [async(static fn(): ?NatsMessage => $queue->fetch())],
            new TimeoutCancellation(2.0),
        )[0];

        // kills ReturnRemoval @ 122: returns the buffered 'first' via the early return, no read started.
        self::assertNotNull($msg);
        self::assertSame('first', $msg->payload);
        self::assertSame(0, $transport->startedReads);
    }

    /**
     * next() must RETURN the already-buffered message via its early return, WITHOUT starting a read.
     *
     * - ReturnRemoval @ 147: removing the return drops the early short-circuit and runs the
     *   timeout/non-blocking branch, which parks on the idle (blocking) transport (startedReads > 0).
     */
    public function testNextReturnsBufferedMessageWithoutReading(): void
    {
        $transport = new FakeTransport($this->infoAndPong(), blockWhenEmpty: true);
        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('events')->await();
        $queue->setTimeout(0.1);

        $queue->enqueue($this->msg('first'));

        $msg = \Amp\Future\await(
            [async(static fn(): ?NatsMessage => $queue->next())],
            new TimeoutCancellation(2.0),
        )[0];

        // kills ReturnRemoval @ 147: returns buffered 'first' via early return, no read started.
        self::assertNotNull($msg);
        self::assertSame('first', $msg->payload);
        self::assertSame(0, $transport->startedReads);
    }

    // ------------------------------------------------------------------
    // Line 150: if ($this->timeout <= 0)
    // ------------------------------------------------------------------

    /**
     * timeout == 0 must take the non-blocking single-cycle branch (NON_BLOCKING_TIMEOUT), which reads
     * one cycle and surfaces a deliverable message.
     *
     * - LessThanOrEqualTo (< 0): with timeout exactly 0, `0 < 0` is false, so the mutant falls into
     *   the blocking branch using TimeoutCancellation(0). A 0s cancellation fires before the read can
     *   surface the frame, so the mutant returns null instead of the message.
     */
    public function testTimeoutZeroNonBlockingBranchSurfacesMessage(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG events 1 3\r\nxyz\r\n",
        ]);
        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('events')->await();
        // timeout stays 0 -> exactly the boundary of the <= 0 branch.

        $msg = $queue->next();

        // kills LessThanOrEqualTo @ 150: the timeout==0 non-blocking branch must read the message.
        self::assertNotNull($msg);
        self::assertSame('xyz', $msg->payload);
    }

    // ------------------------------------------------------------------
    // Line 166 / 169 / 172 / 173 / 176 / 177: next() blocking wait loop
    // ------------------------------------------------------------------

    /**
     * With a positive timeout and a message that arrives only after the first empty read, next() must
     * keep looping until the message appears, then return it.
     *
     * - Plus @ 166 (deadline = now - timeout): deadline is in the past, so the do/while condition is
     *   immediately false; the loop would run a single iteration. Combined with a second-cycle
     *   delivery, the real code (deadline in the future) loops again and surfaces 'late', the mutant
     *   would not.
     * - DoWhile @ 169 (while(false)): same single-iteration collapse.
     * - LessThan/LessThanNegotiation @ 177: flipping the deadline comparison changes loop continuation.
     * - FunctionCallRemoval @ 176 (drop delay): the delay yields cooperatively; without it the second
     *   frame may not be pumped. With it the message is collected.
     */
    public function testNextLoopsUntilMessageArrivesOnLaterCycle(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            '',                            // first processIncoming cycle: 0 frames
            "MSG events 1 4\r\nlate\r\n", // second cycle: the message arrives
        ]);
        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('events')->await();
        $queue->setTimeout(0.5);

        $result = \Amp\Future\await(
            [async(static fn(): ?NatsMessage => $queue->next())],
            new TimeoutCancellation(2.0),
        )[0];

        // kills Plus@166 / DoWhile@169 / LessThan@177 / LessThanNegotiation@177 / FunctionCallRemoval@176:
        // the loop must iterate past the first empty read and return 'late'.
        self::assertNotNull($result);
        self::assertSame('late', $result->payload);
    }

    /**
     * The break inside next()'s loop fires as soon as a message is buffered: the FIRST queued message
     * is returned, and the second remains buffered for a subsequent call.
     *
     * - GreaterThan @ 172 (count >= 0): would break on the very first iteration regardless of whether
     *   a message arrived, returning null when the buffer is still empty.
     * - GreaterThanNegotiation @ 172 (count <= 0): would break only while empty, never on a delivery,
     *   so it would run to the timeout instead of returning promptly.
     */
    public function testNextBreaksOnFirstBufferedMessageLeavingRestBuffered(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG events 1 3\r\none\r\n",
            "MSG events 1 3\r\ntwo\r\n",
        ]);
        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('events')->await();
        $queue->setTimeout(0.2);

        $first = $queue->next();
        // kills GreaterThan@172 / GreaterThanNegotiation@172: must return the first delivered message.
        self::assertNotNull($first);
        self::assertSame('one', $first->payload);

        // The second message must still be retrievable (not consumed/dropped by a runaway loop).
        $second = $queue->next();
        self::assertNotNull($second);
        self::assertSame('two', $second->payload);
    }

    /**
     * Once a message is buffered, next() must BREAK out of the wait loop immediately - it must not
     * keep pumping the socket. With a blocking transport that supplies one frame then parks, the real
     * code reads exactly the queued chunk and breaks (no blocking read is ever started); the mutant
     * keeps looping and starts a blocking processIncoming read.
     *
     * - Break_ @ 173 (break -> continue): after the message is buffered the loop continues, so a
     *   further processIncoming starts a blocking read on the now-empty transport (startedReads > 0)
     *   until the deadline, instead of returning at once.
     */
    public function testNextBreakStopsLoopWithoutFurtherReads(): void
    {
        // One frame queued, then the transport blocks on reads (models an otherwise-idle socket).
        $transport = new FakeTransport([...$this->infoAndPong(), "MSG events 1 1\r\na\r\n"], blockWhenEmpty: true);
        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('events')->await();
        $queue->setTimeout(0.1);

        $msg = \Amp\Future\await(
            [async(static fn(): ?NatsMessage => $queue->next())],
            new TimeoutCancellation(2.0),
        )[0];

        // kills Break_ @ 173: the queued chunk is consumed and the loop breaks before any blocking
        // read is started.
        self::assertNotNull($msg);
        self::assertSame('a', $msg->payload);
        self::assertSame(0, $transport->startedReads);
    }

    // ------------------------------------------------------------------
    // Line 201: first drain loop in fetchAll()
    // ------------------------------------------------------------------

    /**
     * The buffered-drain loop must respect a finite limit AND drain when limit is null.
     *
     * Pre-buffer 3 messages, request limit 2: exactly 2 are returned in order.
     *
     * - Identical @ 201 ($limit !== null): with 3 buffered and limit 2 the condition logic flips so
     *   the loop would not stop at 2.
     * - LessThan @ 201 (<= ): would drain 3 (count 2 <= 2 still true).
     * - LessThanNegotiation @ 201 (>=): loop body would never run -> empty result on the drain.
     * - LogicalOr @ 201 (&&): with limit set the (null || ...) becomes (false && ...) -> never drains.
     * - LogicalOrAllSubExprNegation / LogicalOrNegation / LogicalAndAllSubExprNegation / While_ @ 201:
     *   all change which/whether items are drained from the pre-buffered queue.
     */
    public function testFetchAllBufferedDrainRespectsLimitTwoOfThree(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = $this->makeConnectedClient($transport);
        // High cap so all three are stored.
        $queue = new SubscriptionQueue($client, 99, 10, SlowConsumerPolicy::DropOldest);

        $queue->enqueue($this->msg('a'));
        $queue->enqueue($this->msg('b'));
        $queue->enqueue($this->msg('c'));

        $messages = $queue->fetchAll(2);

        // kills Identical/LessThan/LessThanNegotiation/LogicalOr/LogicalOrAllSubExprNegation/
        // LogicalOrNegation/LogicalAndAllSubExprNegation/While_ @ 201.
        self::assertCount(2, $messages);
        self::assertSame('a', $messages[0]->payload);
        self::assertSame('b', $messages[1]->payload);
    }

    /**
     * With no limit (null), the buffered drain must collect ALL buffered messages.
     *
     * Distinguishes the negation mutants at 201 that only break when $limit === null: e.g.
     * LogicalOrNegation (!( null || ... )) would yield false and drain nothing.
     */
    public function testFetchAllNullLimitDrainsEntireBuffer(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = $this->makeConnectedClient($transport);
        $queue = new SubscriptionQueue($client, 99, 10, SlowConsumerPolicy::DropOldest);

        $queue->enqueue($this->msg('a'));
        $queue->enqueue($this->msg('b'));
        $queue->enqueue($this->msg('c'));

        // No timeout, null limit: the buffered drain alone must return all three.
        $messages = $queue->fetchAll();

        // kills LogicalOrNegation/LogicalOrAllSubExprNegation/While_/LogicalAndAllSubExprNegation @ 201.
        self::assertCount(3, $messages);
        self::assertSame('a', $messages[0]->payload);
        self::assertSame('b', $messages[1]->payload);
        self::assertSame('c', $messages[2]->payload);
    }

    // ------------------------------------------------------------------
    // Line 205 / 206: early return when buffered already meets the limit
    // ------------------------------------------------------------------

    /**
     * When the buffered drain already satisfies the limit, fetchAll() returns immediately WITHOUT a
     * processIncoming cycle. Using a blocking transport proves no read happens (otherwise it would
     * park / take the timeout).
     *
     * - GreaterThanOrEqualTo @ 205 (count > limit): equal-to-limit would NOT early-return, so the
     *   code would fall into the read loop and block on the idle transport.
     * - LogicalAndAllSubExprNegation @ 205: inverts the predicate so the early return is skipped.
     * - ReturnRemoval @ 206: removes the early return, again forcing the blocking read path.
     * - ArrayOneItem @ 206: would slice the returned array down to 1 item.
     */
    public function testFetchAllEarlyReturnsExactlyAtLimitWithoutReading(): void
    {
        $transport = new FakeTransport($this->infoAndPong(), blockWhenEmpty: true);
        $client = $this->makeConnectedClient($transport);
        $queue = new SubscriptionQueue($client, 99, 10, SlowConsumerPolicy::DropOldest);

        $queue->enqueue($this->msg('a'));
        $queue->enqueue($this->msg('b'));

        // limit == buffered count (2). Real code returns instantly; mutants fall into a blocking read.
        $result = \Amp\Future\await(
            [async(static fn(): array => $queue->fetchAll(2))],
            new TimeoutCancellation(2.0),
        )[0];

        // kills GreaterThanOrEqualTo@205 / LogicalAndAllSubExprNegation@205 / ReturnRemoval@206 / ArrayOneItem@206
        self::assertCount(2, $result);
        self::assertSame('a', $result[0]->payload);
        self::assertSame('b', $result[1]->payload);
        // No processIncoming read should have started (early return before the read loop).
        self::assertSame(0, $transport->startedReads);
    }

    // ------------------------------------------------------------------
    // Line 211: $hasTimeout = $this->timeout > 0
    // ------------------------------------------------------------------

    /**
     * With timeout == 0 and an empty buffer, fetchAll() must treat it as NO timeout: a single
     * best-effort cycle that returns [] without blocking.
     *
     * - GreaterThan @ 211 (timeout >= 0): timeout 0 would be considered "has timeout", so the
     *   single-cycle break (line 222-225 needs !$hasTimeout) would not fire and the loop would keep
     *   waiting/delaying against the blocking transport until the outer cancellation.
     */
    public function testFetchAllTimeoutZeroIsTreatedAsNoTimeoutSingleCycle(): void
    {
        $transport = new FakeTransport($this->infoAndPong(), blockWhenEmpty: true);
        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('idle')->await();
        // timeout stays 0.

        $result = \Amp\Future\await(
            [async(static fn(): array => $queue->fetchAll())],
            new TimeoutCancellation(2.0),
        )[0];

        // kills GreaterThan @ 211: timeout 0 means !hasTimeout, so a single empty cycle returns [].
        self::assertSame([], $result);
    }

    // ------------------------------------------------------------------
    // Line 218: inner drain loop inside fetchAll() read loop
    // ------------------------------------------------------------------

    /**
     * The inner drain inside the read loop must stop EXACTLY at the limit even when a single
     * processIncoming cycle buffers more messages than the limit. All three frames are delivered in
     * one read chunk so the inner drain sees a,b,c buffered simultaneously; with limit 2 it must take
     * only a,b and leave c.
     *
     * - LessThan @ 218 (<=): the inner drain would over-collect (count 2 <= 2 still true) -> 3 returned.
     * - LogicalOrAllSubExprNegation @ 218: changes which items the inner drain takes (would over- or
     *   under-collect from the buffer).
     */
    public function testFetchAllInnerDrainStopsAtLimitWithinOneCycle(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // Three frames in ONE chunk: a single processIncoming buffers a,b,c together, so the inner
            // drain (line 218) - not the pre-buffer drain (201) - is what must honor the limit.
            "MSG events 1 1\r\na\r\nMSG events 1 1\r\nb\r\nMSG events 1 1\r\nc\r\n",
        ]);
        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('events')->await();
        $queue->setTimeout(0.2);

        $messages = $queue->fetchAll(2);

        // kills LessThan@218 / LogicalOrAllSubExprNegation@218: exactly 2 (a,b), c stays buffered.
        self::assertCount(2, $messages);
        self::assertSame('a', $messages[0]->payload);
        self::assertSame('b', $messages[1]->payload);
    }

    // ------------------------------------------------------------------
    // Line 239: final drain loop in fetchAll()
    // ------------------------------------------------------------------

    /**
     * The final-drain loop (after the read loop ends) must respect the limit. We enqueue more than the
     * limit concurrently so that, when the timeout window closes, surplus is buffered; the final drain
     * must stop exactly at the limit.
     *
     * - LessThan @ 239 (<=): would drain one extra (count == limit still <= limit).
     * - LessThanNegotiation @ 239 (>=): the loop body would never run for the limited case, dropping
     *   a legitimately-collected message.
     * - LogicalOrAllSubExprNegation @ 239: changes the final-drain predicate.
     */
    public function testFetchAllFinalDrainRespectsLimit(): void
    {
        $transport = new FakeTransport($this->infoAndPong(), blockWhenEmpty: true);
        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('events')->await();
        $queue->setTimeout(0.1);

        // Enqueue 3 directly while fetchAll() is blocked in processIncoming; after the timeout fires
        // the final drain runs against all 3 buffered, but limit 2 must cap it.
        async(static function () use ($queue): void {
            \Amp\delay(0.02);
            $queue->enqueue(new NatsMessage('events', 1, null, 'x'));
            $queue->enqueue(new NatsMessage('events', 1, null, 'y'));
            $queue->enqueue(new NatsMessage('events', 1, null, 'z'));
        });

        $messages = $queue->fetchAll(2);

        // kills LessThan@239 / LessThanNegotiation@239 / LogicalOrAllSubExprNegation@239.
        self::assertCount(2, $messages);
        self::assertSame('x', $messages[0]->payload);
        self::assertSame('y', $messages[1]->payload);
    }
}
