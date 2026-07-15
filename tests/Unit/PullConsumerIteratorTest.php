<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\Consumers\PullConsumerIterator;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\PullServerTrait;
use PHPUnit\Framework\TestCase;

/**
 * Behavioral tests for {@see PullConsumerIterator} over the #120 pipelined pull engine
 * ({@see JetStreamContext::consumePipelined()}). Replies are delivered dynamically by
 * {@see PullServerTrait::pullServer()} on each pull's captured reply-to (token-routed), not on a fixed
 * fetch inbox. Infinite tests whose intent is per-pull sequencing pin {@see PullConsumerIterator::setDepth()}
 * to 1 to keep the classic serial one-pull-at-a-time semantics (overlap itself is covered by
 * {@see PullPipelineTest}); finite tests are always strictly serial.
 */
final class PullConsumerIteratorTest extends TestCase
{
    use PullServerTrait;

    public function testFluentBuilderSetsProperties(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $iter = $js->pullConsumer('ORDERS', 'PROC')
            ->setBatching(10)
            ->setExpiresMs(5000)
            ->setIterations(3);

        self::assertInstanceOf(PullConsumerIterator::class, $iter);
        self::assertSame(10, $iter->getBatching());
        self::assertSame(5000, $iter->getExpiresMs());
        self::assertSame(3, $iter->getIterations());
    }

    public function testDefaultValues(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        // #120: the default batch is now 100 (was 1) - a single pull fetches a full batch and the engine
        // pipelines across pulls rather than one message per round-trip.
        self::assertSame(100, $iter->getBatching());
        self::assertSame(3000, $iter->getExpiresMs());
        self::assertNull($iter->getIterations());
    }

    public function testSetBatchingRejectsZero(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        $this->expectException(JetStreamException::class);
        $iter->setBatching(0);
    }

    public function testSetExpiresMsRejectsZero(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        $this->expectException(JetStreamException::class);
        $iter->setExpiresMs(0);
    }

    public function testSetIterationsRejectsZero(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        $this->expectException(JetStreamException::class);
        $iter->setIterations(0);
    }

    public function testSetIterationsAcceptsNull(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C')
            ->setIterations(5)
            ->setIterations(null);

        self::assertNull($iter->getIterations());
    }

    public function testSetDepthRejectsZero(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        $this->expectException(JetStreamException::class);
        $iter->setDepth(0);
    }

    public function testHandleProcessesOneIteration(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        // Finite (serial) run: pull #1 delivers one message; pull #2 gets a terminal 404 that stops it.
        $this->pullServer($transport, 'ORDERS', 'PROC', [
            [['msg' => 'order-1']],
            [['status' => 404, 'desc' => 'No Messages']],
        ]);
        $js = $this->context($transport);

        $processed = [];
        $total = $js
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(500)
            ->setIterations(2)
            ->handle(function (NatsMessage $msg, JetStreamContext $js) use (&$processed): void {
                $processed[] = $msg->payload;
            })->await();

        self::assertSame(1, $total);
        self::assertSame(['order-1'], $processed);
    }

    /**
     * Verifies stop() ends the consume loop promptly, abandoning the rest of the in-flight batch (#32).
     */
    public function testStopAbandonsRestOfBatch(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        // A single pull delivers a full batch of 3; the handler stops after the first message.
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'm1'], ['msg' => 'm2'], ['msg' => 'm3']],
        ]);
        $js = $this->context($transport);

        $iter = $js->pullConsumer('S', 'C')->setBatching(3)->setExpiresMs(200);
        $processed = [];
        $total = $iter->handle(function (NatsMessage $msg) use (&$processed, $iter): void {
            $processed[] = $msg->payload;
            $iter->stop();
        })->await();

        self::assertSame(1, $total);
        self::assertSame(['m1'], $processed);
    }

    /**
     * Verifies drain() finishes the in-flight batch but issues no further pull (#32).
     */
    public function testDrainFinishesBatchThenStops(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        // A single pull delivers a full batch of 3; the handler drains after the first message.
        $this->pullServer($transport, 'S', 'C', [
            [['msg' => 'm1'], ['msg' => 'm2'], ['msg' => 'm3']],
        ]);
        $js = $this->context($transport);

        // setDepth(1) keeps the run serial so exactly one pull is in flight (drain's "finish this batch,
        // issue no further pull" contract, mirroring the classic loop).
        $iter = $js->pullConsumer('S', 'C')->setBatching(3)->setExpiresMs(200)->setDepth(1);
        $processed = [];
        $total = $iter->handle(function (NatsMessage $msg) use (&$processed, $iter): void {
            $processed[] = $msg->payload;
            $iter->drain();
        })->await();

        // The whole in-flight batch finished (drain is graceful), but no second pull was issued.
        self::assertSame(3, $total);
        self::assertSame(['m1', 'm2', 'm3'], $processed);
        self::assertCount(1, $this->pullWrites($transport), 'drain must not issue a further pull');
    }

    /**
     * Verifies onError surfaces a terminal consume error (e.g. Consumer Deleted) and stops the loop (#63).
     */
    public function testOnErrorFiresOnTerminalStatus(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'STREAM', 'CONS', [
            [['status' => 409, 'desc' => 'Consumer Deleted']],
        ]);
        $js = $this->context($transport);

        $error = null;
        $total = $js
            ->pullConsumer('STREAM', 'CONS')
            ->setBatching(1)
            ->setExpiresMs(200)
            ->setIterations(1)
            ->setOnError(static function (JetStreamException $e) use (&$error): void {
                $error = $e;
            })
            ->handle(static function (): void {
                self::fail('No message should be delivered on a terminal status');
            })->await();

        self::assertSame(0, $total);
        self::assertInstanceOf(JetStreamException::class, $error);
        self::assertSame(409, $error->getCode());
        self::assertStringContainsString('Consumer Deleted', $error->getMessage());
    }

    /**
     * Verifies a routine empty window (404) does NOT trigger onError (#63).
     */
    public function testOnErrorNotFiredOnRoutineEmptyWindow(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'STREAM', 'CONS', [
            [['status' => 404, 'desc' => 'No Messages']],
        ]);
        $js = $this->context($transport);

        $fired = false;
        $js
            ->pullConsumer('STREAM', 'CONS')
            ->setBatching(1)
            ->setExpiresMs(200)
            ->setIterations(1)
            ->setOnError(static function (JetStreamException $e) use (&$fired): void {
                $fired = true;
            })
            ->handle(static function (): void {})->await();

        self::assertFalse($fired, 'A routine 404 empty window must not trigger onError');
    }

    public function testHandleStopsOnNoMessages(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        // Immediately returns a terminal 404 - no messages; finite mode stops on the first empty.
        $this->pullServer($transport, 'STREAM', 'CONS', [
            [['status' => 404, 'desc' => 'No Messages']],
        ]);
        $js = $this->context($transport);

        $total = $js
            ->pullConsumer('STREAM', 'CONS')
            ->setBatching(5)
            ->setIterations(10)
            ->setExpiresMs(100)
            ->handle(function (): void {
                self::fail('Handler should not be called when no messages are available');
            })->await();

        self::assertSame(0, $total);
    }

    public function testHandleInfiniteModeContinuesPastEmptyWindow(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'ORDERS', 'PROC', [
            // pull 1: empty window (404) - infinite mode must keep polling, not stop.
            [['status' => 404, 'desc' => 'No Messages']],
            // pull 2: a message arrives after the idle gap.
            [['msg' => 'order-1']],
            // pull 3: a terminal error (consumer deleted) stops the loop.
            [['status' => 409, 'desc' => 'Consumer Deleted']],
        ]);
        $js = $this->context($transport);

        $processed = [];
        $total = $js
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setDepth(1) // serial: one pull per scripted response (per-pull sequencing under test)
            ->setIterations(null) // infinite
            ->handle(function (NatsMessage $msg, JetStreamContext $js) use (&$processed): void {
                $processed[] = $msg->payload;
            })->await();

        // The message after the empty window is delivered (old code stopped on the first 404).
        self::assertSame(1, $total);
        self::assertSame(['order-1'], $processed);
    }

    public function testHandleInfiniteModeContinuesPastTransient409(): void
    {
        // A 409 can be transient (backpressure/failover/shutdown) or terminal (Consumer Deleted).
        // The status-line reason flows into the exception message via NatsHeaders::fromWireBlock.
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'ORDERS', 'PROC', [
            // pull 1: transient 409 (backpressure) - infinite mode must keep polling.
            [['status' => 409, 'desc' => 'Exceeded MaxAckPending']],
            // pull 2: a message arrives once backpressure clears.
            [['msg' => 'job-7']],
            // pull 3: a terminal 409 (consumer deleted) stops the loop.
            [['status' => 409, 'desc' => 'Consumer Deleted']],
        ]);
        $js = $this->context($transport);

        $processed = [];
        $total = $js
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setDepth(1)
            ->setIterations(null) // infinite
            ->handle(function (NatsMessage $msg, JetStreamContext $js) use (&$processed): void {
                $processed[] = $msg->payload;
            })->await();

        // Old code treated the transient 409 as terminal and stopped before the message.
        self::assertSame(1, $total);
        self::assertSame(['job-7'], $processed);
    }

    /**
     * Verifies a 409 "Message Size Exceeds MaxBytes" is a pull-COMPLETION status, not a terminal
     * error: an infinite consume loop with setMaxBytes() must survive an oversized pending head
     * message and keep pulling (nats.go excludes ErrMaxBytesExceeded from terminal handling) (#153).
     */
    public function testHandleInfiniteModeContinuesPastMaxBytes409(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'ORDERS', 'PROC', [
            // pull 1: the pending head message exceeds max_bytes - the pull completes empty.
            [['status' => 409, 'desc' => 'Message Size Exceeds MaxBytes']],
            // pull 2: a message that fits arrives on the next pull.
            [['msg' => 'fits-now']],
            // pull 3: a terminal 409 (consumer deleted) stops the loop.
            [['status' => 409, 'desc' => 'Consumer Deleted']],
        ]);
        $js = $this->context($transport);

        $processed = [];
        $total = $js
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setMaxBytes(1024)
            ->setDepth(1)
            ->setIterations(null) // infinite
            ->handle(function (NatsMessage $msg) use (&$processed): void {
                $processed[] = $msg->payload;
            })->await();

        // Old code treated the MaxBytes 409 as terminal: the worker stopped permanently with total 0.
        self::assertSame(1, $total);
        self::assertSame(['fits-now'], $processed);
    }

    /**
     * Verifies a 409 "Batch Completed" is a pull-COMPLETION status, not a terminal error: the
     * infinite loop re-pulls instead of stopping (nats.go excludes ErrBatchCompleted from terminal
     * handling) (#153).
     */
    public function testHandleInfiniteModeContinuesPastBatchCompleted409(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'ORDERS', 'PROC', [
            // pull 1: the server closes the pull early with "Batch Completed" and no message.
            [['status' => 409, 'desc' => 'Batch Completed']],
            // pull 2: the next pull delivers a message.
            [['msg' => 'next-batch']],
            // pull 3: a terminal 409 (consumer deleted) stops the loop.
            [['status' => 409, 'desc' => 'Consumer Deleted']],
        ]);
        $js = $this->context($transport);

        $processed = [];
        $total = $js
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setDepth(1)
            ->setIterations(null) // infinite
            ->handle(function (NatsMessage $msg) use (&$processed): void {
                $processed[] = $msg->payload;
            })->await();

        // Old code treated the Batch Completed 409 as terminal: total would be 0.
        self::assertSame(1, $total);
        self::assertSame(['next-batch'], $processed);
    }

    /**
     * Verifies no_wait infinite mode paces consecutive empty pulls instead of busy-polling: an
     * empty consumer answers each no_wait pull with an immediate 404, and the iterator must apply
     * its escalating idle backoff (10ms doubling, capped at 500ms) between them so an idle stream
     * is not hammered with an unthrottled re-pull storm (#153). Event-driven: the scripted 404s
     * answer instantly, so any elapsed time is exactly the iterator's own pacing. setDepth(1) keeps
     * the pacing serial - one pull per scripted response - so the backoff sequence is deterministic.
     */
    public function testNoWaitInfiniteModePacesConsecutiveEmptyPulls(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'ORDERS', 'PROC', [
            // pulls 1-3: the empty consumer answers each no_wait pull with an immediate 404.
            [['status' => 404, 'desc' => 'No Messages']],
            [['status' => 404, 'desc' => 'No Messages']],
            [['status' => 404, 'desc' => 'No Messages']],
            // pull 4: a message finally arrives.
            [['msg' => 'queued']],
            // pull 5: a terminal 409 (consumer deleted) stops the loop.
            [['status' => 409, 'desc' => 'Consumer Deleted']],
        ]);
        $js = $this->context($transport);

        $startNs = hrtime(true);
        $processed = [];
        $total = $js
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setNoWait(true)
            ->setDepth(1)
            ->setIterations(null) // infinite
            ->handle(function (NatsMessage $msg) use (&$processed): void {
                $processed[] = $msg->payload;
            })->await();
        $elapsedNs = hrtime(true) - $startNs;

        // The loop kept pulling through the empty windows and delivered the message...
        self::assertSame(1, $total);
        self::assertSame(['queued'], $processed);

        // ...issuing exactly one pull per scripted response (5 responses -> 5 pulls, no storm)...
        self::assertCount(5, $this->pullWrites($transport));

        // ...and the three consecutive empty pulls were paced by the escalating idle backoff
        // (10ms + 20ms + 40ms = 70ms minimum; the old code re-pulled instantly, elapsed ~0ms).
        self::assertGreaterThanOrEqual(65_000_000, $elapsedNs, 'consecutive empty no_wait pulls must be paced');
    }

    // ── setGroup / setPriority / setMinPending / setMinAckPending / setMaxBytes / setNoWait ──────

    /**
     * Verifies setGroup() throws on an invalid group name.
     */
    public function testSetGroupRejectsInvalidName(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        $this->expectException(JetStreamException::class);
        $iter->setGroup('this-name-is-way-too-long-to-be-valid');
    }

    /**
     * Verifies setGroup() accepts null (clearing the group).
     */
    public function testSetGroupAcceptsNull(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C')->setGroup('g1')->setGroup(null);

        // No exception; setGroup returns $this so we can still call methods.
        self::assertInstanceOf(PullConsumerIterator::class, $iter);
    }

    /**
     * Verifies setPriority() throws when priority < 0.
     */
    public function testSetPriorityRejectsNegative(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        $this->expectException(JetStreamException::class);
        $iter->setPriority(-1);
    }

    /**
     * Verifies setPriority() throws when priority > 9.
     */
    public function testSetPriorityRejectsAboveNine(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        $this->expectException(JetStreamException::class);
        $iter->setPriority(10);
    }

    /**
     * Verifies setPriority() sets the priority and returns $this.
     */
    public function testSetPriorityAcceptsValidValues(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        $result = $iter->setPriority(5);
        self::assertSame($iter, $result);

        // Boundary checks: 0 and 9 are both valid.
        $iter->setPriority(0);
        $iter->setPriority(9);
    }

    /**
     * Verifies setPriority(null) clears the priority.
     */
    public function testSetPriorityAcceptsNull(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C')->setPriority(3)->setPriority(null);
        self::assertInstanceOf(PullConsumerIterator::class, $iter);
    }

    /**
     * Verifies setMinPending() stores the value and returns $this.
     */
    public function testSetMinPendingStoresValue(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        $result = $iter->setMinPending(42);
        self::assertSame($iter, $result);
    }

    /**
     * Verifies setMinPending(null) clears the threshold.
     */
    public function testSetMinPendingAcceptsNull(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C')->setMinPending(10)->setMinPending(null);
        self::assertInstanceOf(PullConsumerIterator::class, $iter);
    }

    /**
     * Verifies setMinAckPending() stores the value and returns $this.
     */
    public function testSetMinAckPendingStoresValue(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        $result = $iter->setMinAckPending(7);
        self::assertSame($iter, $result);
    }

    /**
     * Verifies setMinAckPending(null) clears the threshold.
     */
    public function testSetMinAckPendingAcceptsNull(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C')->setMinAckPending(5)->setMinAckPending(null);
        self::assertInstanceOf(PullConsumerIterator::class, $iter);
    }

    /**
     * Verifies setMaxBytes() stores the value and returns $this.
     */
    public function testSetMaxBytesStoresValue(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        $result = $iter->setMaxBytes(1024);
        self::assertSame($iter, $result);
    }

    /**
     * Verifies setMaxBytes(null) clears the cap.
     */
    public function testSetMaxBytesAcceptsNull(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C')->setMaxBytes(512)->setMaxBytes(null);
        self::assertInstanceOf(PullConsumerIterator::class, $iter);
    }

    /**
     * Verifies setNoWait() sets the flag and returns $this.
     */
    public function testSetNoWaitStoresValue(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        $result = $iter->setNoWait(true);
        self::assertSame($iter, $result);

        // Calling with false clears the flag.
        $iter->setNoWait(false);
    }

    /**
     * Verifies setNoWait() defaults to true when called with no argument.
     */
    public function testSetNoWaitDefaultsToTrue(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        $result = $iter->setNoWait();
        self::assertSame($iter, $result);
    }

    // ── buildPull() optional-field branches via actual pull wire output ──────────

    /**
     * Covers the priority, min_pending, min_ack_pending, max_bytes, and no_wait branches of
     * buildPull(): verifies all optional pull fields appear in the NATS PUB payload when set.
     */
    public function testBuildPullIncludesAllOptionalFields(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        // One message so the loop runs one iteration, then a 404 to stop.
        $this->pullServer($transport, 'ORDERS', 'PROC', [
            [['msg' => 'order-x']],
            [['status' => 404, 'desc' => 'No Messages']],
        ]);
        $js = $this->context($transport);

        $js
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(500)
            ->setIterations(2)
            ->setPriority(3)
            ->setMinPending(10)
            ->setMinAckPending(5)
            ->setMaxBytes(65536)
            ->setNoWait(true)
            ->handle(static function (): void {})->await();

        $pullWrites = $this->pullWrites($transport);

        self::assertNotEmpty($pullWrites, 'At least one pull request must have been issued');
        $firstPull = $pullWrites[0];

        // Each buildPull() branch must appear in the JSON payload.
        self::assertStringContainsString('"priority":3', $firstPull);
        self::assertStringContainsString('"min_pending":10', $firstPull);
        self::assertStringContainsString('"min_ack_pending":5', $firstPull);
        self::assertStringContainsString('"max_bytes":65536', $firstPull);
        self::assertStringContainsString('"no_wait":true', $firstPull);
    }

    /**
     * Covers the buildPull() optional-field branches' negative path: when the optional fields are NOT set,
     * they must be absent from the pull payload.
     */
    public function testBuildPullOmitsUnsetOptionalFields(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'ORDERS', 'PROC', [
            [['msg' => 'order-y']],
            [['status' => 404, 'desc' => 'No Messages']],
        ]);
        $js = $this->context($transport);

        // No optional fields set.
        $js
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(500)
            ->setIterations(2)
            ->handle(static function (): void {})->await();

        $pullWrites = $this->pullWrites($transport);

        self::assertNotEmpty($pullWrites);
        $firstPull = $pullWrites[0];

        self::assertStringNotContainsString('"priority"', $firstPull);
        self::assertStringNotContainsString('"min_pending"', $firstPull);
        self::assertStringNotContainsString('"min_ack_pending"', $firstPull);
        self::assertStringNotContainsString('"max_bytes"', $firstPull);
        self::assertStringNotContainsString('"no_wait"', $firstPull);
    }

    // ── stop() / drain() / resetLifecycle() re-use tests ─────────────────────────────────────────

    /**
     * Covers resetLifecycle(): a reused iterator whose stop() was called in the first run must
     * NOT be pre-stopped during a second handle() call. Each handle() run opens its own pull inbox
     * subscription, so the pull server relearns the sid per run and routes replies by reply-token.
     */
    public function testReusedIteratorAfterStopStartsFresh(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            // First run: pull delivers a batch of 2; the handler stops after the first (m2 abandoned).
            [['msg' => 'm1'], ['msg' => 'm2']],
            // Second run: one message then a 404 completes the (batch=2) pull.
            [['msg' => 'fresh-msg'], ['status' => 404, 'desc' => 'No Messages']],
        ]);
        $js = $this->context($transport);

        $iter = $js
            ->pullConsumer('S', 'C')
            ->setBatching(2)
            ->setExpiresMs(200)
            ->setIterations(1);

        // First run: stop inside handler; only 1 message processed (m2 abandoned).
        $firstRun = [];
        $iter->handle(function (NatsMessage $msg) use (&$firstRun, $iter): void {
            $firstRun[] = $msg->payload;
            $iter->stop();
        })->await();
        self::assertSame(['m1'], $firstRun);

        // Second run: stop flag must be cleared by resetLifecycle(). 1 message + 404 stop.
        $secondRun = [];
        $iter->handle(function (NatsMessage $msg) use (&$secondRun): void {
            $secondRun[] = $msg->payload;
        })->await();
        self::assertSame(['fresh-msg'], $secondRun);
    }

    /**
     * Covers resetLifecycle(): a reused iterator whose drain() was called in the first run must
     * NOT be pre-drained during a second handle() call.
     */
    public function testReusedIteratorAfterDrainStartsFresh(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'S', 'C', [
            // First run: one message; handler drains, batch finishes, no second pull.
            [['msg' => 'run1']],
            // Second run: one message (batch=1 completes on it).
            [['msg' => 'run2']],
        ]);
        $js = $this->context($transport);

        $iter = $js
            ->pullConsumer('S', 'C')
            ->setBatching(1)
            ->setExpiresMs(200)
            ->setIterations(1);

        // First run: drain during the only message.
        $firstRun = [];
        $iter->handle(function (NatsMessage $msg) use (&$firstRun, $iter): void {
            $firstRun[] = $msg->payload;
            $iter->drain();
        })->await();
        self::assertSame(['run1'], $firstRun);

        // Second run: drain flag must be reset; the iterator must poll again.
        $secondRun = [];
        $iter->handle(function (NatsMessage $msg) use (&$secondRun): void {
            $secondRun[] = $msg->payload;
        })->await();
        self::assertSame(['run2'], $secondRun);
    }

    /**
     * Covers the onError path with a 408 (request timeout): routine empty window, must NOT fire onError.
     */
    public function testOnErrorNotFiredOnRoutine408(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'STREAM', 'CONS', [
            [['status' => 408, 'desc' => 'Request Timeout']],
        ]);
        $js = $this->context($transport);

        $fired = false;
        $js
            ->pullConsumer('STREAM', 'CONS')
            ->setBatching(1)
            ->setExpiresMs(200)
            ->setIterations(1)
            ->setOnError(static function (JetStreamException $e) use (&$fired): void {
                $fired = true;
            })
            ->handle(static function (): void {})->await();

        self::assertFalse($fired, 'A routine 408 timeout must not trigger onError');
    }

    /**
     * Verifies a stale-pin (423) status drops the pin id and re-pulls without it, capturing the new
     * pin id from the next delivery (issue #7). Finite grouped mode is strictly serial, so each pull
     * maps 1:1 to a scripted response in order.
     */
    public function testHandleRePinsOnStalePin(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $this->pullServer($transport, 'ORDERS', 'PROC', [
            // pull 1: stale pin -> drop pin id and retry without it.
            [['status' => 423, 'desc' => 'Nats-Pin-Id Mismatch']],
            // pull 2: re-pinned, a message arrives carrying the new pin id.
            [['msg' => 'order-9', 'pin' => 'pin-new']],
            // pull 3: a plain message; the pull issued for it must carry the captured pin id.
            [['msg' => 'order-10']],
        ]);
        $js = $this->context($transport);

        $processed = [];
        $total = $js
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setGroup('g1')
            ->setIterations(3)
            ->handle(function (NatsMessage $msg, JetStreamContext $js) use (&$processed): void {
                $processed[] = $msg->payload;
            })->await();

        self::assertSame(2, $total);
        self::assertSame(['order-9', 'order-10'], $processed);

        $pullWrites = $this->pullWrites($transport);

        // pull 1: group present, no pin yet.
        self::assertStringContainsString('"group":"g1"', $pullWrites[0]);
        self::assertStringNotContainsString('"id"', $pullWrites[0]);
        // pull 2 (right after the 423): pin was cleared, so still no id.
        self::assertStringNotContainsString('"id"', $pullWrites[1]);
        // pull 3: the pin id captured from pull 2's delivery is now re-sent.
        self::assertStringContainsString('"id":"pin-new"', $pullWrites[2]);
    }
}
