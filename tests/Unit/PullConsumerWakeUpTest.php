<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\TimeoutCancellation;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\JetStream\Consumers\PullConsumerIterator;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\delay;

/**
 * The wake-ups stop() and drain() give the pull engine (#181): a call from another fiber - a signal handler's timer,
 * a supervisor, a test - ends the engine's wait at once, whether it waits on the socket for the in-flight pulls or in
 * the #153 idle backoff, instead of being seen only when that wait ends on its own, at the earliest pull's deadline
 * (its expiry plus a second) or at the end of the backoff. stop() then ends the run; drain() stops issuing pulls and
 * goes on pumping the pulls in flight, which still complete or reach their deadline. Each wake-up is composed into a
 * wait only while it has not fired, so a latched drain never ends a later wait at once, and a run gets fresh wake-ups,
 * so one fired in an earlier run cannot end a wait of the next. A run a later handle() on the same iterator orphans
 * (its wake-ups replaced, and fired by Amp as they are destructed) goes on as before #181, with no spin, and a stop()
 * latched while a pull of a generation is written ends the generation there.
 *
 * Over the scripted server in tests/Support/ReconnectingTransport.php seen through tests/Support/WatchedTransport.php:
 * the server holds a pull (silence) or answers it at once with a 404 status, and the reads of the socket say where the
 * engine waits - a read under way is the pump read on the socket, none is the backoff - so the tests act on the order
 * of events, and the time bounds sit far from both the expected and the broken behaviour (milliseconds against
 * seconds, or against the 500 ms left of a backoff).
 */
final class PullConsumerWakeUpTest extends TestCase
{
    use ReconnectScenarios;

    private const PULL_SUBJECT = '$JS.API.CONSUMER.MSG.NEXT.S.C';
    private const ACK_SUBJECT = '$JS.ACK.S.C.1.1.1.0.0';

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /**
     * A stop() from another fiber while the pump read waits on a silent server ends the run at once: with a 3 s
     * expiry the engine used to return only at the pull's deadline, about 4 s after the pull. Nothing was delivered,
     * and the pull inbox is released once as on every exit.
     */
    public function testAStopFromAnotherFiberEndsThePumpReadAtOnce(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $this->pullServer($transport, static fn(): array => []);
        $client = $this->client($watched);

        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(3_000);
        $run = $iterator->handle(static function (): void {
            self::fail('the server holds the pull: nothing is delivered');
        });
        // The pump read is on the socket, with the pull's deadline 4 s away.
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);

        $stoppedAt = hrtime(true);
        $iterator->stop();
        $processed = $run->await(new TimeoutCancellation(6));
        $returnedAfter = $this->secondsSince($stoppedAt);

        self::assertSame(0, $processed);
        self::assertLessThan(0.5, $returnedAfter, sprintf('handle() returned %.3f s after the stop; it used to wait for the pull\'s deadline', $returnedAfter));
        self::assertSame(1, $this->pullCount($transport), 'no pull after the stop');
        self::assertCount(1, $transport->controlLinesStartingWith('UNSUB '), 'the pull inbox is released once');
    }

    /**
     * The same with depth 2 once a pull has been retired: the first pull is answered empty at once and retired, the
     * engine refills the depth and pumps for the two silent pulls left in flight, and a stop() from another fiber
     * ends that read at once rather than at the earliest of their deadlines.
     */
    public function testAStopFromAnotherFiberEndsThePumpReadWithAPullAlreadyRetired(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1
            ? [self::statusFrame($replyTo, $sid, 404, 'No Messages')]
            : []);
        $client = $this->client($watched);

        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(3_000);
        $run = $iterator->handle(static function (): void {
            self::fail('the server delivers nothing');
        });
        // Pulls 1 and 2 go out together; the 404 retires pull 1 (one empty is below the depth, so no idle latch), pull 3
        // refills the depth, and the pump read then waits for pulls 2 and 3.
        $this->waitUntil(fn(): bool => $this->pullCount($transport) === 3 && $watched->readsUnderWay === 1);

        $stoppedAt = hrtime(true);
        $iterator->stop();
        $processed = $run->await(new TimeoutCancellation(6));
        $returnedAfter = $this->secondsSince($stoppedAt);

        self::assertSame(0, $processed);
        self::assertLessThan(0.5, $returnedAfter, sprintf('handle() returned %.3f s after the stop', $returnedAfter));
        self::assertSame(3, $this->pullCount($transport), 'no pull after the stop');
        self::assertCount(1, $transport->controlLinesStartingWith('UNSUB '));
    }

    /**
     * A stop() latched while the issue phase awaits the pull's own write - the write's completion is the last
     * suspension before the pump read - has fired its wake-up before any wait could hold it. The engine checks the
     * flag once more before it reads, so the run ends without a read of the socket, not at the pull's deadline.
     */
    public function testAStopLatchedDuringThePullsWriteEndsTheRunWithoutAReadToTheDeadline(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $this->pullServer($transport, static fn(): array => []);
        $client = $this->client($watched);

        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(3_000);
        // The server's side of the write: the stop lands while the engine still awaits the write's future.
        $transport->afterWrite = static function (string $bytes) use ($iterator): void {
            if (str_contains($bytes, 'PUB ' . self::PULL_SUBJECT . ' ')) {
                $iterator->stop();
            }
        };
        $readsBefore = $watched->reads;

        $startedAt = hrtime(true);
        $processed = $iterator->handle(static function (): void {
            self::fail('the server holds the pull: nothing is delivered');
        })->await(new TimeoutCancellation(6));
        $elapsed = $this->secondsSince($startedAt);

        self::assertSame(0, $processed);
        self::assertLessThan(0.5, $elapsed, sprintf('handle() returned after %.3f s; it used to read to the pull\'s deadline', $elapsed));
        self::assertSame($readsBefore, $watched->reads, 'the stop was seen before the pump read: no read of the socket');
        self::assertSame(1, $this->pullCount($transport));
        self::assertCount(1, $transport->controlLinesStartingWith('UNSUB '));
    }

    /**
     * A stop() from a timer while the engine waits in its idle backoff ends the backoff at once. Seven no_wait pulls
     * answered empty at once back the engine off for 10, 20, 40, 80, 160, 320 and then 500 ms; the timer fires 20 ms
     * into the last one, with no read on the socket, which is how the test knows the engine was in the backoff. The
     * engine used to wait out the backoff and see the stop only then, about 480 ms later.
     */
    public function testAStopFromATimerEndsTheIdleBackoffAtOnce(): void
    {
        [$transport, $watched, $iterator, $signal] = $this->idleRunSignalledInItsSeventhBackoff(static fn(PullConsumerIterator $iterator) => $iterator->stop());

        $processed = $iterator->handle(static function (): void {
            self::fail('every pull is answered empty');
        })->await(new TimeoutCancellation(6));

        $signalledAt = $signal->at;
        if ($signalledAt === null) {
            self::fail('the timer never fired');
        }
        self::assertSame(0, $signal->readsUnderWay, 'the engine was in its backoff when the timer fired, not in a read');
        $returnedAfter = $this->secondsSince($signalledAt);
        self::assertLessThan(0.25, $returnedAfter, sprintf('handle() returned %.3f s after the stop; it used to wait out the 500 ms backoff', $returnedAfter));
        self::assertSame(0, $processed);
        self::assertSame(7, $this->pullCount($transport), 'no pull after the stop');
        self::assertSame(0, $watched->readsUnderWay);
    }

    /**
     * A drain() from a timer during the idle backoff ends it at once as well, and with nothing in flight the run ends
     * there: no further pull is issued, the engine used to issue none either, but only after the backoff ran out.
     */
    public function testADrainFromATimerEndsTheIdleBackoffAtOnce(): void
    {
        [$transport, $watched, $iterator, $signal] = $this->idleRunSignalledInItsSeventhBackoff(static fn(PullConsumerIterator $iterator) => $iterator->drain());

        $processed = $iterator->handle(static function (): void {
            self::fail('every pull is answered empty');
        })->await(new TimeoutCancellation(6));

        $signalledAt = $signal->at;
        if ($signalledAt === null) {
            self::fail('the timer never fired');
        }
        self::assertSame(0, $signal->readsUnderWay, 'the engine was in its backoff when the timer fired, not in a read');
        $returnedAfter = $this->secondsSince($signalledAt);
        self::assertLessThan(0.25, $returnedAfter, sprintf('handle() returned %.3f s after the drain; it used to wait out the 500 ms backoff', $returnedAfter));
        self::assertSame(0, $processed);
        self::assertSame(7, $this->pullCount($transport), 'a drain issues no further pull');
        self::assertSame(0, $watched->readsUnderWay);
    }

    /**
     * A drain() from another fiber while the pump read waits for a pull in flight wakes the engine once: the read on
     * the socket ends at once, the engine issues no further pull and reads again for the pull still in flight, and
     * when the server answers it the batch is delivered and the run ends. The engine used to stay in the first read
     * until the answer, and a drain while it waited had no visible effect before that.
     */
    public function testADrainFromAnotherFiberWakesThePumpAndStillDeliversTheInFlightPull(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $pull = new class {
            public ?int $sid = null;
        };
        $this->pullServer($transport, static function (string $replyTo, int $sid) use ($pull): array {
            $pull->sid = $sid;

            return [];
        });
        $client = $this->client($watched);

        $received = [];
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(3_000);
        $run = $iterator->handle(static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        });
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $readsBefore = $watched->reads;
        $cancelledBefore = $watched->cancelledReads;

        $iterator->drain();
        // The drain ended the read on the socket, and the engine, its pull still in flight, reads again.
        $this->waitUntil(static fn(): bool => $watched->cancelledReads === $cancelledBefore + 1 && $watched->readsUnderWay === 1);
        self::assertSame($readsBefore + 1, $watched->reads, 'one read ended by the drain, one more for the pull in flight');
        self::assertSame(1, $this->pullCount($transport), 'no pull after the drain');

        // The server answers the pull now.
        $sid = $pull->sid;
        if ($sid === null) {
            self::fail('the pull was never made');
        }
        $transport->pushFrame(ReconnectingTransport::msgFrame('evt.s', $sid, 'm1', self::ACK_SUBJECT));
        $processed = $run->await(new TimeoutCancellation(6));

        self::assertSame(1, $processed);
        self::assertSame(['m1'], $received, 'the pull in flight when the drain came is still delivered');
        self::assertSame(1, $this->pullCount($transport), 'a drain issues no further pull');
        self::assertSame($readsBefore + 1, $watched->reads, 'the read that got the batch was the last');
        self::assertCount(1, $transport->controlLinesStartingWith('UNSUB '));
    }

    /**
     * A drain() with a silent pull in flight still waits for that pull's deadline (its expiry plus a second), as a
     * drain from inside the handler does, and the latched drain does not spin: its fired wake-up is left out of the
     * later waits, so the engine reads the socket once more, to the deadline, rather than once per loop turn.
     */
    public function testADrainWithASilentPullInFlightWaitsForItsDeadlineWithOneMoreRead(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $pull = new class {
            public ?int $issuedAt = null;
        };
        $this->pullServer($transport, static function () use ($pull): array {
            $pull->issuedAt = hrtime(true);

            return [];
        });
        $client = $this->client($watched);

        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(500);
        $run = $iterator->handle(static function (): void {
            self::fail('the server holds the pull: nothing is delivered');
        });
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $readsBefore = $watched->reads;

        $iterator->drain();
        $processed = $run->await(new TimeoutCancellation(6));
        $issuedAt = $pull->issuedAt;
        if ($issuedAt === null) {
            self::fail('the pull was never made');
        }
        $pullLasted = $this->secondsSince($issuedAt);

        self::assertSame(0, $processed);
        self::assertGreaterThan(1.4, $pullLasted, 'the pull in flight reached its deadline, 500 ms of expiry plus a second, before the run ended');
        self::assertLessThan(3.0, $pullLasted);
        self::assertSame($readsBefore + 1, $watched->reads, 'the drain ended one read; exactly one more waited to the deadline, no spin');
        self::assertSame(1, $this->pullCount($transport));
    }

    /**
     * A stop() after a drain() still wakes the pump: the drain's fired wake-up is left out of the read that follows
     * it, the stop's is not, so the stop ends that read at once instead of at the pull's deadline.
     */
    public function testAStopAfterADrainStillWakesThePump(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $this->pullServer($transport, static fn(): array => []);
        $client = $this->client($watched);

        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(3_000);
        $run = $iterator->handle(static function (): void {
            self::fail('the server holds the pull: nothing is delivered');
        });
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $cancelledBefore = $watched->cancelledReads;

        $iterator->drain();
        // The read after the drain is on the socket, without the drain's wake-up.
        $this->waitUntil(static fn(): bool => $watched->cancelledReads === $cancelledBefore + 1 && $watched->readsUnderWay === 1);

        $stoppedAt = hrtime(true);
        $iterator->stop();
        $processed = $run->await(new TimeoutCancellation(6));
        $returnedAfter = $this->secondsSince($stoppedAt);

        self::assertSame(0, $processed);
        self::assertLessThan(0.5, $returnedAfter, sprintf('handle() returned %.3f s after the stop that followed a drain', $returnedAfter));
        self::assertSame(1, $this->pullCount($transport));
        self::assertCount(1, $transport->controlLinesStartingWith('UNSUB '));
    }

    /**
     * A run started again after a stop gets fresh wake-ups: the first run's fired stop wake-up does not end the next
     * run's first read, which stays on the socket until the server answers, and that run then delivers as usual.
     * A stale wake-up composed into the wait would end every read at once, a spin of reads until the deadline.
     */
    public function testARunStartedAgainAfterAStopGetsFreshWakeUps(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $pull = new class {
            public ?int $sid = null;
        };
        $this->pullServer($transport, static function (string $replyTo, int $sid) use ($pull): array {
            $pull->sid = $sid;

            return [];
        });
        $client = $this->client($watched);

        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(3_000);
        $first = $iterator->handle(static function (): void {
            self::fail('the first run is stopped before anything is delivered');
        });
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $stoppedAt = hrtime(true);
        $iterator->stop();
        self::assertSame(0, $first->await(new TimeoutCancellation(6)));
        self::assertLessThan(0.5, $this->secondsSince($stoppedAt), 'the first run ended at once on the stop');

        $received = [];
        $second = $iterator->handle(static function (NatsMessage $message) use (&$received, $iterator): void {
            $received[] = $message->payload;
            // From inside the handler, as before #181: the run ends before the next pull.
            $iterator->stop();
        });
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $readsAtStart = $watched->reads;
        delay(0.2);
        self::assertSame($readsAtStart, $watched->reads, 'the second run\'s first read is still on the socket: no stale wake-up ended it');
        self::assertSame(1, $watched->readsUnderWay);

        $sid = $pull->sid;
        if ($sid === null) {
            self::fail('the second run\'s pull was never made');
        }
        $transport->pushFrame(ReconnectingTransport::msgFrame('evt.s', $sid, 'm2', self::ACK_SUBJECT));
        $processed = $second->await(new TimeoutCancellation(6));

        self::assertSame(1, $processed);
        self::assertSame(['m2'], $received);
        self::assertSame(2, $this->pullCount($transport), 'one pull per run');
    }

    /**
     * A second handle() on the same iterator while the first run still waits on the socket replaces the first run's
     * wake-ups, and Amp fires a replaced one as it is destructed: the first run's read ends once, and the run then
     * waits as it did before #181, to its pull's deadline, still delivering that pull's batch when the server answers.
     * A fired wake-up composed into its waits would end every one of them at once: a spin of reads in queued callbacks
     * that runs ahead of every timer in the process, so the test brakes it at the fiftieth read, which the fixed engine
     * never reaches.
     */
    public function testASecondHandleWhileARunStillWaitsDoesNotMakeThatRunSpin(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $pull = new class {
            public ?int $sid = null;
        };
        // The first run's pull is held; every later pull is answered empty at once.
        $this->pullServer($transport, static function (string $replyTo, int $sid, int $pullNumber) use ($pull): array {
            if ($pullNumber === 1) {
                $pull->sid = $sid;

                return [];
            }

            return [self::statusFrame($replyTo, $sid, 404, 'No Messages')];
        });
        $client = $this->client($watched);

        $received = [];
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(3_000);
        $first = $iterator->handle(static function (NatsMessage $message) use (&$received, $iterator): void {
            $received[] = $message->payload;
            $iterator->stop();
        });
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $spin = $this->brakeOnASpinOfReads($watched, $iterator);

        // A finite second run, answered empty at once, so it ends by itself.
        $second = $iterator->setIterations(1)->handle(static function (): void {
            self::fail('the second run is answered empty');
        });
        self::assertSame(0, $second->await(new TimeoutCancellation(2)));
        delay(0.2);

        self::assertLessThan(10, $spin->reads, sprintf('the first run made %d reads of the socket after the second run started: a spin', $spin->reads));
        self::assertSame(1, $watched->readsUnderWay, 'the first run\'s read is back on the socket, waiting');
        self::assertFalse($first->isComplete(), 'the first run goes on');

        // The server answers the first run's pull now: the run delivers it and ends on the handler's stop().
        $sid = $pull->sid;
        if ($sid === null) {
            self::fail('the first run\'s pull was never made');
        }
        $transport->pushFrame(ReconnectingTransport::msgFrame('evt.s', $sid, 'm1', self::ACK_SUBJECT));
        self::assertSame(1, $first->await(new TimeoutCancellation(6)));
        self::assertSame(['m1'], $received, 'the orphaned run still delivers its pull');
        self::assertSame(2, $this->pullCount($transport), 'one pull per run');
        self::assertCount(2, $transport->controlLinesStartingWith('UNSUB '), 'each run released its pull inbox');
    }

    /**
     * The restart idiom, stop() and handle() again in one tick without awaiting the first run: the second handle()
     * clears the stop before the first run saw it (the runs share the flags, which is why a run is to be awaited before
     * the next starts) and replaces its wake-ups, fired by the stop and by Amp. The first run goes on as before #181,
     * without a spin on its fired wake-ups, and still delivers its pull. The brake at the fiftieth read is for the
     * engine before the fix, whose spin starved every timer.
     */
    public function testAStopThenHandleAgainInOneTickDoesNotMakeTheOrphanedRunSpin(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $pull = new class {
            public ?int $sid = null;
        };
        $this->pullServer($transport, static function (string $replyTo, int $sid, int $pullNumber) use ($pull): array {
            if ($pullNumber === 1) {
                $pull->sid = $sid;

                return [];
            }

            return [self::statusFrame($replyTo, $sid, 404, 'No Messages')];
        });
        $client = $this->client($watched);

        $received = [];
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(3_000);
        $first = $iterator->handle(static function (NatsMessage $message) use (&$received, $iterator): void {
            $received[] = $message->payload;
            $iterator->stop();
        });
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $spin = $this->brakeOnASpinOfReads($watched, $iterator);

        $iterator->stop();
        $second = $iterator->setIterations(1)->handle(static function (): void {
            self::fail('the second run is answered empty');
        });
        self::assertSame(0, $second->await(new TimeoutCancellation(2)));
        delay(0.2);

        self::assertLessThan(10, $spin->reads, sprintf('the first run made %d reads of the socket after stop() and handle(): a spin', $spin->reads));
        self::assertSame(1, $watched->readsUnderWay, 'the first run\'s read is back on the socket, waiting');
        self::assertFalse($first->isComplete(), 'the second handle() cleared the stop before the first run saw it');

        $sid = $pull->sid;
        if ($sid === null) {
            self::fail('the first run\'s pull was never made');
        }
        $transport->pushFrame(ReconnectingTransport::msgFrame('evt.s', $sid, 'm1', self::ACK_SUBJECT));
        self::assertSame(1, $first->await(new TimeoutCancellation(6)));
        self::assertSame(['m1'], $received, 'the orphaned run still delivers its pull');
        self::assertSame(2, $this->pullCount($transport), 'one pull per run');
        self::assertCount(2, $transport->controlLinesStartingWith('UNSUB '));
    }

    /**
     * A stop() latched while the first pull of a depth-2 generation is written ends the generation there: the issue
     * phase reads the flag before each pull, so the second pull is not written for a run about to end, which would
     * have left it pending on the server until its expiry. The run then ends without a read of the socket.
     */
    public function testAStopLatchedDuringTheFirstPullsWriteIssuesNoSecondPull(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $this->pullServer($transport, static fn(): array => []);
        $client = $this->client($watched);

        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(3_000);
        $transport->afterWrite = static function (string $bytes) use ($iterator): void {
            if (str_contains($bytes, 'PUB ' . self::PULL_SUBJECT . ' ')) {
                $iterator->stop();
            }
        };
        $readsBefore = $watched->reads;

        $startedAt = hrtime(true);
        $processed = $iterator->handle(static function (): void {
            self::fail('the server holds the pulls: nothing is delivered');
        })->await(new TimeoutCancellation(6));
        $elapsed = $this->secondsSince($startedAt);

        self::assertSame(0, $processed);
        self::assertLessThan(0.5, $elapsed, sprintf('handle() returned after %.3f s', $elapsed));
        self::assertSame(1, $this->pullCount($transport), 'the second pull of the generation is not written after the stop');
        self::assertSame($readsBefore, $watched->reads, 'no read of the socket');
        self::assertCount(1, $transport->controlLinesStartingWith('UNSUB '));
    }

    /**
     * A drain() latched while the first pull of a depth-2 generation is written ends the generation there as well,
     * and that first pull, in flight, is still pumped: the engine reads for it, writes nothing more, and delivers its
     * batch when the server answers.
     */
    public function testADrainLatchedDuringTheFirstPullsWriteIssuesNoSecondPullAndStillPumpsTheFirst(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $pull = new class {
            public ?int $sid = null;
        };
        $this->pullServer($transport, static function (string $replyTo, int $sid) use ($pull): array {
            $pull->sid = $sid;

            return [];
        });
        $client = $this->client($watched);

        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(3_000);
        $transport->afterWrite = static function (string $bytes) use ($iterator): void {
            if (str_contains($bytes, 'PUB ' . self::PULL_SUBJECT . ' ')) {
                $iterator->drain();
            }
        };
        $received = [];
        $run = $iterator->handle(static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        });
        // The pump read for the pull in flight is on the socket: the issue phase is over, with one pull written.
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        self::assertSame(1, $this->pullCount($transport), 'the second pull of the generation is not written after the drain');
        self::assertFalse($run->isComplete(), 'the pull in flight is pumped');

        $sid = $pull->sid;
        if ($sid === null) {
            self::fail('the pull was never made');
        }
        $transport->pushFrame(ReconnectingTransport::msgFrame('evt.s', $sid, 'm1', self::ACK_SUBJECT));
        $processed = $run->await(new TimeoutCancellation(6));

        self::assertSame(1, $processed);
        self::assertSame(['m1'], $received, 'the pull in flight when the drain came is still delivered');
        self::assertSame(1, $this->pullCount($transport));
        self::assertCount(1, $transport->controlLinesStartingWith('UNSUB '));
    }

    /**
     * A finite run (setIterations) stopped from another fiber while its pull is pumped ends at once as well: the
     * wake-ups do not depend on the run's mode, and the finite run's exact pull count holds, one pull written.
     */
    public function testAFiniteRunStoppedFromAnotherFiberEndsAtOnce(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $this->pullServer($transport, static fn(): array => []);
        $client = $this->client($watched);

        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setIterations(3)->setExpiresMs(3_000);
        $run = $iterator->handle(static function (): void {
            self::fail('the server holds the pull: nothing is delivered');
        });
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);

        $stoppedAt = hrtime(true);
        $iterator->stop();
        $processed = $run->await(new TimeoutCancellation(6));
        $returnedAfter = $this->secondsSince($stoppedAt);

        self::assertSame(0, $processed);
        self::assertLessThan(0.5, $returnedAfter, sprintf('the finite run returned %.3f s after the stop', $returnedAfter));
        self::assertSame(1, $this->pullCount($transport), 'no pull after the stop');
        self::assertCount(1, $transport->controlLinesStartingWith('UNSUB '));
    }

    /**
     * An idle no_wait run whose seven pulls are all answered empty at once, with a timer that signals the iterator
     * 20 ms into the seventh backoff, the first 500 ms one, and records when it fired and whether a read was on the
     * socket then.
     *
     * @param \Closure(PullConsumerIterator): void $signal
     * @return array{ReconnectingTransport, WatchedTransport, PullConsumerIterator, object{at: ?int, readsUnderWay: ?int}}
     */
    private function idleRunSignalledInItsSeventhBackoff(\Closure $signal): array
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->client($watched);
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setNoWait()->setExpiresMs(3_000);
        $signalled = new class {
            /** When the timer fired, on the monotonic clock. */
            public ?int $at = null;
            /** Reads of the socket under way when it fired: 0 while the engine waits in its backoff. */
            public ?int $readsUnderWay = null;
        };
        $this->pullServer($transport, static function (string $replyTo, int $sid, int $pull) use ($watched, $iterator, $signal, $signalled): array {
            if ($pull === 7) {
                EventLoop::delay(0.02, static function () use ($watched, $iterator, $signal, $signalled): void {
                    $signalled->readsUnderWay = $watched->readsUnderWay;
                    $signalled->at = hrtime(true);
                    $signal($iterator);
                });
            }

            return [self::statusFrame($replyTo, $sid, 404, 'No Messages')];
        });

        return [$transport, $watched, $iterator, $signalled];
    }

    /**
     * The scripted server's side of the pull consumer: every pull to the consumer is answered by $onPull, given the
     * pull's reply subject, the sid the client subscribed for the pull inbox, and the pull's number (1 for the first).
     * No frames hold the pull, as a server with no message does until the pull expires.
     *
     * @param \Closure(string, int, int): list<string> $onPull
     */
    private function pullServer(ReconnectingTransport $transport, \Closure $onPull): void
    {
        $pulls = 0;
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport, $onPull, &$pulls): array {
            if ($replyTo === null || $subject !== self::PULL_SUBJECT) {
                return [];
            }

            $sid = $transport->sidFor($replyTo);
            if ($sid === null) {
                return [];
            }

            return $onPull($replyTo, $sid, ++$pulls);
        };
    }

    /** A pull status frame, on the pull's reply subject as the server sends it. */
    private static function statusFrame(string $replyTo, int $sid, int $code, string $description): string
    {
        return ReconnectingTransport::hmsgFrame($replyTo, $sid, sprintf("NATS/1.0 %d %s\r\n\r\n", $code, $description), '');
    }

    /** The pull requests the client has written so far. */
    private function pullCount(ReconnectingTransport $transport): int
    {
        return count($transport->controlLinesStartingWith('PUB ' . self::PULL_SUBJECT . ' '));
    }

    /**
     * Counts the reads of the socket from now on and stops the iterator at the fiftieth. A run that composes a fired
     * wake-up into its waits reads the socket once per queued callback, and queued callbacks run ahead of every timer,
     * so without the brake no timeout could end the test on an engine that spins; the fixed engine never reaches it.
     *
     * @return object{reads: int}
     */
    private function brakeOnASpinOfReads(WatchedTransport $watched, PullConsumerIterator $iterator): object
    {
        $spin = new class {
            /** Reads of the socket since the brake was set. */
            public int $reads = 0;
        };
        $watched->onRead = static function () use ($spin, $iterator): void {
            if (++$spin->reads === 50) {
                $iterator->stop();
            }
        };

        return $spin;
    }

    /** A connected client over the watched transport, closed in tearDown; no heartbeat, so only the engine reads. */
    private function client(WatchedTransport $transport): NatsClient
    {
        $client = new NatsClient($this->options(true, 2_000, 1_000, 0, 2, null, 5, 20, null), $transport);
        $this->opened[] = $client;
        $client->connect()->await();

        return $client;
    }
}
