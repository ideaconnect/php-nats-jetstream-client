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
