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
use PHPUnit\Framework\TestCase;

final class PullConsumerIteratorTest extends TestCase
{
    /** @return list<string> */
    private function infoAndPong(): array
    {
        return [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
    }

    private function jsMsg(string $subject, string $payload, string $replyTo): string
    {
        return sprintf("MSG %s 1 %s %d\r\n%s\r\n", $subject, $replyTo, strlen($payload), $payload);
    }

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

        self::assertSame(1, $iter->getBatching());
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

    public function testHandleProcessesOneIteration(): void
    {
        $statusHeaders = "NATS/1.0 404 No Messages\r\nStatus: 404\r\n\r\n";
        $hdrLen = strlen($statusHeaders);

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // First iteration: 1 message delivered.
            $this->jsMsg('_INBOX.JS.FETCH.any', 'order-1', '$JS.ACK.ORDERS.PROC.1.1.1.123.0'),
            // Second iteration: terminal 404 status -> JetStreamException breaks loop.
            sprintf("HMSG _INBOX.JS.FETCH.any 2 %d %d\r\n%s\r\n", $hdrLen, $hdrLen, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $processed = [];
        $total = $client->jetStream()
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
        $statusHeaders = "NATS/1.0 404 No Messages\r\nStatus: 404\r\n\r\n";
        $hdrLen = strlen($statusHeaders);

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // One fetch (sid 1) delivers a full batch of 3.
            $this->jsMsg('_INBOX.JS.FETCH.any', 'm1', '$JS.ACK.S.C.1.1.1.123.0')
                . $this->jsMsg('_INBOX.JS.FETCH.any', 'm2', '$JS.ACK.S.C.1.2.2.123.0')
                . $this->jsMsg('_INBOX.JS.FETCH.any', 'm3', '$JS.ACK.S.C.1.3.3.123.0'),
            // Safety terminator if a (buggy) second pull were issued.
            sprintf("HMSG _INBOX.JS.FETCH.any 2 %d %d\r\n%s\r\n", $hdrLen, $hdrLen, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $iter = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setExpiresMs(200);
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
        $statusHeaders = "NATS/1.0 404 No Messages\r\nStatus: 404\r\n\r\n";
        $hdrLen = strlen($statusHeaders);

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            $this->jsMsg('_INBOX.JS.FETCH.any', 'm1', '$JS.ACK.S.C.1.1.1.123.0')
                . $this->jsMsg('_INBOX.JS.FETCH.any', 'm2', '$JS.ACK.S.C.1.2.2.123.0')
                . $this->jsMsg('_INBOX.JS.FETCH.any', 'm3', '$JS.ACK.S.C.1.3.3.123.0'),
            sprintf("HMSG _INBOX.JS.FETCH.any 2 %d %d\r\n%s\r\n", $hdrLen, $hdrLen, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $iter = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setExpiresMs(200);
        $processed = [];
        $total = $iter->handle(function (NatsMessage $msg) use (&$processed, $iter): void {
            $processed[] = $msg->payload;
            $iter->drain();
        })->await();

        // The whole in-flight batch finished (drain is graceful), but no second pull was issued.
        self::assertSame(3, $total);
        self::assertSame(['m1', 'm2', 'm3'], $processed);
    }

    /**
     * Verifies onError surfaces a terminal consume error (e.g. Consumer Deleted) and stops the loop (#63).
     */
    public function testOnErrorFiresOnTerminalStatus(): void
    {
        $statusHeaders = "NATS/1.0 409 Consumer Deleted\r\nStatus: 409\r\nDescription: Consumer Deleted\r\n\r\n";
        $hdrLen = strlen($statusHeaders);

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            sprintf("HMSG _INBOX.JS.FETCH.any 1 %d %d\r\n%s\r\n", $hdrLen, $hdrLen, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $error = null;
        $total = $client->jetStream()
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
        $statusHeaders = "NATS/1.0 404 No Messages\r\nStatus: 404\r\n\r\n";
        $hdrLen = strlen($statusHeaders);

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            sprintf("HMSG _INBOX.JS.FETCH.any 1 %d %d\r\n%s\r\n", $hdrLen, $hdrLen, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $fired = false;
        $client->jetStream()
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
        $statusHeaders = "NATS/1.0 404 No Messages\r\nStatus: 404\r\n\r\n";
        $hdrLen = strlen($statusHeaders);

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // Immediately returns terminal 404 status - no messages.
            sprintf("HMSG _INBOX.JS.FETCH.any 1 %d %d\r\n%s\r\n", $hdrLen, $hdrLen, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $total = $client->jetStream()
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
        $noMessages = "NATS/1.0 404 No Messages\r\nStatus: 404\r\n\r\n";
        $deleted = "NATS/1.0 409 Consumer Deleted\r\nStatus: 409\r\n\r\n";
        $h404 = strlen($noMessages);
        $h409 = strlen($deleted);
        $body = 'order-1';

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // iter 1 (sid 1): empty window (404) - infinite mode must keep polling, not stop.
            sprintf("HMSG _INBOX.JS.FETCH.any 1 %d %d\r\n%s\r\n", $h404, $h404, $noMessages),
            // iter 2 (sid 2): a message arrives after the idle gap.
            sprintf("MSG _INBOX.JS.FETCH.any 2 \$JS.ACK.ORDERS.PROC.1.1.1.123.0 %d\r\n%s\r\n", strlen($body), $body),
            // iter 3 (sid 3): a terminal error (consumer deleted) stops the loop.
            sprintf("HMSG _INBOX.JS.FETCH.any 3 %d %d\r\n%s\r\n", $h409, $h409, $deleted),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $processed = [];
        $total = $client->jetStream()
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(100)
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
        $maxAck = "NATS/1.0 409 Exceeded MaxAckPending\r\nStatus: 409\r\n\r\n"; // transient -> keep polling
        $deleted = "NATS/1.0 409 Consumer Deleted\r\nStatus: 409\r\n\r\n";      // terminal  -> stop
        $hMax = strlen($maxAck);
        $hDel = strlen($deleted);
        $body = 'job-7';

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // iter 1 (sid 1): transient 409 (backpressure) - infinite mode must keep polling.
            sprintf("HMSG _INBOX.JS.FETCH.any 1 %d %d\r\n%s\r\n", $hMax, $hMax, $maxAck),
            // iter 2 (sid 2): a message arrives once backpressure clears.
            sprintf("MSG _INBOX.JS.FETCH.any 2 \$JS.ACK.ORDERS.PROC.1.1.1.123.0 %d\r\n%s\r\n", strlen($body), $body),
            // iter 3 (sid 3): a terminal 409 (consumer deleted) stops the loop.
            sprintf("HMSG _INBOX.JS.FETCH.any 3 %d %d\r\n%s\r\n", $hDel, $hDel, $deleted),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $processed = [];
        $total = $client->jetStream()
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(100)
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
        $maxBytes = "NATS/1.0 409 Message Size Exceeds MaxBytes\r\nStatus: 409\r\n\r\n"; // completion -> keep polling
        $deleted = "NATS/1.0 409 Consumer Deleted\r\nStatus: 409\r\n\r\n";                // terminal   -> stop
        $hMax = strlen($maxBytes);
        $hDel = strlen($deleted);
        $body = 'fits-now';

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // iter 1 (sid 1): the pending head message exceeds max_bytes - the pull completes empty.
            sprintf("HMSG _INBOX.JS.FETCH.any 1 %d %d\r\n%s\r\n", $hMax, $hMax, $maxBytes),
            // iter 2 (sid 2): a message that fits arrives on the next pull.
            sprintf("MSG _INBOX.JS.FETCH.any 2 \$JS.ACK.ORDERS.PROC.1.1.1.123.0 %d\r\n%s\r\n", strlen($body), $body),
            // iter 3 (sid 3): a terminal 409 (consumer deleted) stops the loop.
            sprintf("HMSG _INBOX.JS.FETCH.any 3 %d %d\r\n%s\r\n", $hDel, $hDel, $deleted),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $processed = [];
        $total = $client->jetStream()
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setMaxBytes(1024)
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
        $completed = "NATS/1.0 409 Batch Completed\r\nStatus: 409\r\n\r\n"; // completion -> keep polling
        $deleted = "NATS/1.0 409 Consumer Deleted\r\nStatus: 409\r\n\r\n";  // terminal   -> stop
        $hCom = strlen($completed);
        $hDel = strlen($deleted);
        $body = 'next-batch';

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // iter 1 (sid 1): the server closes the pull early with "Batch Completed" and no message.
            sprintf("HMSG _INBOX.JS.FETCH.any 1 %d %d\r\n%s\r\n", $hCom, $hCom, $completed),
            // iter 2 (sid 2): the next pull delivers a message.
            sprintf("MSG _INBOX.JS.FETCH.any 2 \$JS.ACK.ORDERS.PROC.1.1.1.123.0 %d\r\n%s\r\n", strlen($body), $body),
            // iter 3 (sid 3): a terminal 409 (consumer deleted) stops the loop.
            sprintf("HMSG _INBOX.JS.FETCH.any 3 %d %d\r\n%s\r\n", $hDel, $hDel, $deleted),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $processed = [];
        $total = $client->jetStream()
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(100)
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
     * answer instantly, so any elapsed time is exactly the iterator's own pacing.
     */
    public function testNoWaitInfiniteModePacesConsecutiveEmptyPulls(): void
    {
        $noMessages = "NATS/1.0 404 No Messages\r\nStatus: 404\r\n\r\n";
        $deleted = "NATS/1.0 409 Consumer Deleted\r\nStatus: 409\r\n\r\n";
        $h404 = strlen($noMessages);
        $h409 = strlen($deleted);
        $body = 'queued';

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // iters 1-3 (sids 1-3): the empty consumer answers each no_wait pull with an immediate 404.
            sprintf("HMSG _INBOX.JS.FETCH.any 1 %d %d\r\n%s\r\n", $h404, $h404, $noMessages),
            sprintf("HMSG _INBOX.JS.FETCH.any 2 %d %d\r\n%s\r\n", $h404, $h404, $noMessages),
            sprintf("HMSG _INBOX.JS.FETCH.any 3 %d %d\r\n%s\r\n", $h404, $h404, $noMessages),
            // iter 4 (sid 4): a message finally arrives.
            sprintf("MSG _INBOX.JS.FETCH.any 4 \$JS.ACK.ORDERS.PROC.1.1.1.123.0 %d\r\n%s\r\n", strlen($body), $body),
            // iter 5 (sid 5): a terminal 409 (consumer deleted) stops the loop.
            sprintf("HMSG _INBOX.JS.FETCH.any 5 %d %d\r\n%s\r\n", $h409, $h409, $deleted),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $startNs = hrtime(true);
        $processed = [];
        $total = $client->jetStream()
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(100)
            ->setNoWait(true)
            ->setIterations(null) // infinite
            ->handle(function (NatsMessage $msg) use (&$processed): void {
                $processed[] = $msg->payload;
            })->await();
        $elapsedNs = hrtime(true) - $startNs;

        // The loop kept pulling through the empty windows and delivered the message...
        self::assertSame(1, $total);
        self::assertSame(['queued'], $processed);

        // ...issuing exactly one pull per scripted response (5 responses -> 5 pulls, no storm)...
        $pullWrites = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_contains($w, 'CONSUMER.MSG.NEXT'),
        ));
        self::assertCount(5, $pullWrites);

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
        $body = 'order-x';
        $statusHeaders = "NATS/1.0 404 No Messages\r\nStatus: 404\r\n\r\n";
        $hdrLen = strlen($statusHeaders);

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // One message so the loop runs one iteration, then a 404 to stop.
            $this->jsMsg('_INBOX.JS.FETCH.any', $body, '$JS.ACK.ORDERS.PROC.1.1.1.123.0'),
            sprintf("HMSG _INBOX.JS.FETCH.any 2 %d %d\r\n%s\r\n", $hdrLen, $hdrLen, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->jetStream()
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

        $pullWrites = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_contains($w, 'CONSUMER.MSG.NEXT'),
        ));

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
        $body = 'order-y';
        $statusHeaders = "NATS/1.0 404 No Messages\r\nStatus: 404\r\n\r\n";
        $hdrLen = strlen($statusHeaders);

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            $this->jsMsg('_INBOX.JS.FETCH.any', $body, '$JS.ACK.ORDERS.PROC.1.1.1.123.0'),
            sprintf("HMSG _INBOX.JS.FETCH.any 2 %d %d\r\n%s\r\n", $hdrLen, $hdrLen, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // No optional fields set.
        $client->jetStream()
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(500)
            ->setIterations(2)
            ->handle(static function (): void {})->await();

        $pullWrites = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_contains($w, 'CONSUMER.MSG.NEXT'),
        ));

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
     * NOT be pre-stopped during a second handle() call.
     *
     * SID note: each fetchBatch() subscribe() increments nextSid. First handle() = SID 1, second = SID 2.
     */
    public function testReusedIteratorAfterStopStartsFresh(): void
    {
        $body = 'fresh-msg';
        $statusHeaders = "NATS/1.0 404 No Messages\r\nStatus: 404\r\n\r\n";
        $hdrLen = strlen($statusHeaders);

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // First run (SID 1): deliver 2 msgs concatenated; handler stops after the first.
            $this->jsMsg('_INBOX.JS.FETCH.any', 'm1', '$JS.ACK.S.C.1.1.1.0.0')
                . $this->jsMsg('_INBOX.JS.FETCH.any', 'm2', '$JS.ACK.S.C.1.2.2.0.0'),
            // Second run (SID 2): deliver 1 msg, then 404 to stop.
            sprintf("MSG _INBOX.JS.FETCH.any 2 \$JS.ACK.S.C.2.1.1.0.0 %d\r\n%s\r\n", strlen($body), $body),
            sprintf("HMSG _INBOX.JS.FETCH.any 2 %d %d\r\n%s\r\n", $hdrLen, $hdrLen, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $iter = $client->jetStream()
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
        self::assertSame([$body], $secondRun);
    }

    /**
     * Covers resetLifecycle(): a reused iterator whose drain() was called in the first run must
     * NOT be pre-drained during a second handle() call.
     *
     * SID note: each fetchBatch() subscribe() increments nextSid. First handle() = SID 1, second = SID 2.
     */
    public function testReusedIteratorAfterDrainStartsFresh(): void
    {
        $statusHeaders = "NATS/1.0 404 No Messages\r\nStatus: 404\r\n\r\n";
        $hdrLen = strlen($statusHeaders);

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // First run (SID 1): one message; handler drains, batch finishes, no second pull.
            sprintf("MSG _INBOX.JS.FETCH.any 1 \$JS.ACK.S.C.1.1.1.0.0 %d\r\n%s\r\n", strlen('run1'), 'run1'),
            // Second run (SID 2): one message then 404 stop.
            sprintf("MSG _INBOX.JS.FETCH.any 2 \$JS.ACK.S.C.2.1.1.0.0 %d\r\n%s\r\n", strlen('run2'), 'run2'),
            sprintf("HMSG _INBOX.JS.FETCH.any 2 %d %d\r\n%s\r\n", $hdrLen, $hdrLen, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $iter = $client->jetStream()
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
        $statusHeaders = "NATS/1.0 408 Request Timeout\r\nStatus: 408\r\n\r\n";
        $hdrLen = strlen($statusHeaders);

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            sprintf("HMSG _INBOX.JS.FETCH.any 1 %d %d\r\n%s\r\n", $hdrLen, $hdrLen, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $fired = false;
        $client->jetStream()
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
     * pin id from the next delivery (issue #7).
     */
    public function testHandleRePinsOnStalePin(): void
    {
        $stalePin = "NATS/1.0 423 Nats-Pin-Id Mismatch\r\nStatus: 423\r\n\r\n";
        $hStale = strlen($stalePin);
        $pinHdrs = "NATS/1.0\r\nNats-Pin-Id: pin-new\r\n\r\n";

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // iter 1 (sid 1): stale pin -> drop pin id and retry without it.
            sprintf("HMSG _INBOX.JS.FETCH.any 1 %d %d\r\n%s\r\n", $hStale, $hStale, $stalePin),
            // iter 2 (sid 2): re-pinned, a message arrives carrying the new pin id.
            sprintf(
                "HMSG _INBOX.JS.FETCH.any 2 \$JS.ACK.ORDERS.PROC.1.1.1.123.0 %d %d\r\n%s%s\r\n",
                strlen($pinHdrs),
                strlen($pinHdrs) + strlen('order-9'),
                $pinHdrs,
                'order-9',
            ),
            // iter 3 (sid 3): a plain message; the pull issued for this iteration must carry the pin id.
            sprintf("MSG _INBOX.JS.FETCH.any 3 \$JS.ACK.ORDERS.PROC.2.2.2.123.0 %d\r\n%s\r\n", strlen('order-10'), 'order-10'),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $processed = [];
        $total = $client->jetStream()
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

        $pullWrites = array_values(array_filter(
            $transport->writes,
            static fn(string $w): bool => str_contains($w, 'CONSUMER.MSG.NEXT'),
        ));

        // iter 1 pull: group present, no pin yet.
        self::assertStringContainsString('"group":"g1"', $pullWrites[0]);
        self::assertStringNotContainsString('"id"', $pullWrites[0]);
        // iter 2 pull (right after the 423): pin was cleared, so still no id.
        self::assertStringNotContainsString('"id"', $pullWrites[1]);
        // iter 3 pull: the pin id captured from iter 2's delivery is now re-sent.
        self::assertStringContainsString('"id":"pin-new"', $pullWrites[2]);
    }
}
