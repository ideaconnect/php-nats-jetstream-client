<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\JetStream\Consumers\PullConsumerIterator;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
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
 * so one fired in an earlier run cannot end a wait of the next. A stop() latched while a pull of a generation is
 * written ends the generation there.
 *
 * Each run has its own flags and wake-ups (#189): stop() and drain() act on every run active when they are called, and
 * a later handle() on the same iterator leaves an earlier run alone. So the restart idiom, stop() or drain() and then
 * handle() in one tick without awaiting the earlier run, ends the earlier run on its own signal, also before its engine
 * has started and when its handler restarts the consumer while a pull is being retired; repeated restarts leave one run
 * active; and one stop() ends every active run at once, each woken by its own wake-up. The runs used to share the
 * iterator's two flags, which the second handle() cleared before the earlier run saw them: that run went on next to
 * the new one, its wake-ups replaced (and fired by Amp as it destructed them, which the engine leaves out of its waits,
 * so nothing spun), until a later stop() reached it at its pull's deadline.
 *
 * Over the scripted server in tests/Support/ReconnectingTransport.php seen through tests/Support/WatchedTransport.php:
 * the server holds a pull (silence) or answers it at once with a 404 status, and the reads of the socket say where the
 * engine waits - a read under way is the pump read on the socket, none is the backoff - so the tests act on the order
 * of events, and the time bounds sit far from both the expected and the broken behaviour (milliseconds against
 * seconds, or against the 500 ms left of a backoff; a run that should end on its signal ends in milliseconds, or not
 * before its 30 s pull expires, against the 5 s the test waits for it). Each #189 test fails on 2.24.6, except the
 * declared guard, which passes on both.
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
     * A second handle() on the same iterator while the first run still waits on the socket leaves the first run alone
     * (#189): its wake-ups are its own, so nothing fires them, and it goes on, its read on the socket ended only by the
     * second run's frames, which arrive on the same socket, and it still delivers its pull's batch when the server
     * answers, while the second run, answered empty, ends by itself. The second handle() used to replace the first
     * run's wake-ups, which Amp fired as it destructed them. A fired wake-up composed into the first run's waits would
     * end every one of them at once: a spin of reads in queued callbacks that runs ahead of every timer in the process,
     * so the test brakes it at the fiftieth read, which the engine never reaches.
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
        self::assertSame(['m1'], $received, 'the first run still delivers its pull');
        self::assertSame(2, $this->pullCount($transport), 'one pull per run');
        self::assertCount(2, $transport->controlLinesStartingWith('UNSUB '), 'each run released its pull inbox');
    }

    /**
     * The restart idiom, stop() and handle() again in one tick without awaiting the first run, as a supervisor that
     * restarts the consumer with a new batch size does (#189): the stop is the first run's, so that run ends on it, its
     * read ended by its wake-up, with nothing processed and its pull inbox released, while the second run, started
     * clean with the batch it was started with, is the only one that handles anything, ending on its handler's stop().
     * The server's answer to the first run's pull, which comes once both runs are over, reaches no handler. The runs
     * used to share the stop flag, which the second handle() cleared before the first run saw it: the first run went
     * on next to the second, on the same consumer with the same handler, until a later stop() reached it or the
     * connection closed. The brake at the fiftieth read stops a run that spins on a fired wake-up, a spin that would
     * starve the timer that bounds the wait for the first run.
     */
    public function testAStopThenHandleAgainInOneTickEndsTheFirstRunAndOnlyTheSecondHandles(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        // The first run's pull is held; the second run's, of two, is answered with m2 and m3.
        $server = $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull): array => $pull === 2
            ? [self::messages($sid, 'm2', 'm3')]
            : []);
        $client = $this->client($watched);

        $received = [];
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        $first = $iterator->handle(static function (NatsMessage $message) use (&$received): void {
            $received[] = 'first run: ' . $message->payload;
        });
        $this->waitUntil(static fn(): bool => count($server->pulls) === 1 && $watched->readsUnderWay === 1);
        $spin = $this->brakeOnASpinOfReads($watched, $iterator);

        $iterator->stop();
        $second = $iterator->setBatching(2)->handle(static function (NatsMessage $message) use (&$received, $iterator): void {
            $received[] = 'second run: ' . $message->payload;
            $iterator->stop();
        });
        // Should the test fail with the runs still going, the close in tearDown ends them: nothing is to report it.
        $second->ignore();

        $firstProcessed = $this->processedBy($first, $iterator, 'the first run did not end at its stop: the second handle() cleared the stop before the first run saw it');
        $secondProcessed = $second->await(new TimeoutCancellation(5));
        // The server answers the first run's pull only now; the flush's read takes it before its PONG.
        $transport->pushFrame(self::messages($server->pulls[0]['sid'], 'm1'));
        $client->flush()->await();

        self::assertLessThan(10, $spin->reads, sprintf('%d reads of the socket after stop() and handle(): a spin', $spin->reads));
        self::assertSame(0, $firstProcessed);
        self::assertSame(1, $secondProcessed, 'the second run ended on its handler\'s stop(), m3 left undelivered');
        self::assertSame(['second run: m2'], $received, 'only the second run handles anything');
        self::assertCount(2, $server->pulls, 'one pull per run');
        [$firstPull, $secondPull] = $server->pulls;
        self::assertSame(1, self::batchOf($firstPull['request']), 'the first run pulled with the batch it was started with');
        self::assertSame(2, self::batchOf($secondPull['request']), 'the second run pulls with its own');
        self::assertSame(
            ['UNSUB ' . $firstPull['sid'], 'UNSUB ' . $secondPull['sid']],
            $transport->controlLinesStartingWith('UNSUB '),
            'each run released its own pull inbox, once',
        );
    }

    /**
     * The drain twin of the restart idiom (#189): drain() and handle() again in one tick, without awaiting the first
     * run. The drain is the first run's: that run issues no further pull, delivers the pull it has in flight to its own
     * handler when the server answers it, after the second run has pulled, and then ends, its inbox released, while
     * the second run, started clean, goes on until a stop() ends it. The runs used to share the drain flag, which the
     * second handle() cleared: the first run pulled again right after delivering, next to the second. Whichever comes
     * first decides: the end of the first run, or a further pull on its inbox.
     */
    public function testADrainThenHandleAgainInOneTickDrainsTheFirstRun(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        /** @var DeferredFuture<string> $outcome */
        $outcome = new DeferredFuture();
        $firstInbox = new class {
            public ?int $sid = null;
        };
        // Every pull is held; one more on the first run's inbox is that run pulling again.
        $server = $this->pullServer($transport, static function (string $replyTo, int $sid, int $pull) use ($firstInbox, $outcome): array {
            $firstInbox->sid ??= $sid;
            if ($pull > 2 && $sid === $firstInbox->sid && !$outcome->isComplete()) {
                $outcome->complete('the first run pulled again');
            }

            return [];
        });
        $client = $this->client($watched);

        $received = [];
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        $first = $iterator->handle(static function (NatsMessage $message) use (&$received): void {
            $received[] = 'first run: ' . $message->payload;
        });
        $first->finally(static function () use ($outcome): void {
            if (!$outcome->isComplete()) {
                $outcome->complete('the first run ended');
            }
        })->ignore();
        $this->waitUntil(static fn(): bool => count($server->pulls) === 1 && $watched->readsUnderWay === 1);

        $iterator->drain();
        $second = $iterator->handle(static function (NatsMessage $message) use (&$received): void {
            $received[] = 'second run: ' . $message->payload;
        });
        // Should the test fail with the runs still going, the close in tearDown ends them: nothing is to report it.
        $second->ignore();
        // The server answers the first run's pull once the second run has pulled.
        $this->waitUntil(static fn(): bool => count($server->pulls) === 2);
        $transport->pushFrame(self::messages($server->pulls[0]['sid'], 'm1'));

        self::assertSame('the first run ended', $outcome->getFuture()->await(new TimeoutCancellation(5)));
        self::assertSame(1, $first->await(new TimeoutCancellation(1)), 'the first run delivered its pull in flight');
        self::assertSame(['first run: m1'], $received);
        self::assertCount(2, $server->pulls, 'the drained run pulled no more');
        self::assertNotSame($server->pulls[0]['sid'], $server->pulls[1]['sid'], 'the second pull is the second run\'s');
        self::assertSame(['UNSUB ' . $server->pulls[0]['sid']], $transport->controlLinesStartingWith('UNSUB '), 'the first run released its inbox');
        self::assertFalse($second->isComplete(), 'the second run goes on');

        $iterator->stop();
        self::assertSame(0, $second->await(new TimeoutCancellation(5)));
    }

    /**
     * The restart idiom before the first run's engine has started (#189): handle(), stop() and handle() again in one
     * tick. The stop is the first run's: once its inbox is subscribed, the run sees the stop at the top of its loop and
     * ends with nothing pulled, its inbox released, and only the second run pulls. The second handle() used to clear the
     * stop before the first run's engine had even started: the first run pulled next to the second and went on.
     */
    public function testAStopThenHandleAgainBeforeTheFirstRunStartedEndsItBeforeItPulls(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $server = $this->pullServer($transport, static fn(): array => []);
        $client = $this->client($watched);

        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        $first = $iterator->handle(static function (): void {
            self::fail('the server holds every pull: nothing is delivered');
        });
        $iterator->stop();
        $second = $iterator->handle(static function (): void {
            self::fail('the server holds every pull: nothing is delivered');
        });
        // Should the test fail with the runs still going, the close in tearDown ends them: nothing is to report it.
        $second->ignore();

        $firstProcessed = $this->processedBy($first, $iterator, 'the first run did not end at its stop: the second handle() cleared it before the run started');
        $this->waitUntil(static fn(): bool => count($server->pulls) === 1 && $watched->readsUnderWay === 1);

        self::assertSame(0, $firstProcessed);
        self::assertCount(2, $transport->controlLinesStartingWith('SUB _INBOX.JS.PULL.'), 'each run subscribed an inbox of its own');
        $released = $transport->controlLinesStartingWith('UNSUB ');
        self::assertCount(1, $released, 'the first run released its inbox');
        self::assertNotSame('UNSUB ' . $server->pulls[0]['sid'], $released[0], 'the one pull is the second run\'s: the first run never pulled');

        $iterator->stop();
        self::assertSame(0, $second->await(new TimeoutCancellation(5)));
        self::assertCount(1, $server->pulls);
    }

    /**
     * Repeated restarts leave one run active (#189): three times stop() and handle() again in one tick, each once the
     * newest run's pull is on the server. Each stopped run ends with nothing processed, its inbox released, and only
     * the newest run goes on, until one more stop() ends it too. The runs used to pile up: each handle() cleared the
     * stop just before it, so all four runs went on, each with its own inbox and a pull on the server.
     */
    public function testRepeatedRestartsLeaveOnlyTheNewestRunActive(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $server = $this->pullServer($transport, static fn(): array => []);
        $client = $this->client($watched);

        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        $runs = [];
        for ($run = 1; $run <= 4; ++$run) {
            if ($run > 1) {
                $this->waitUntil(static fn(): bool => count($server->pulls) === $run - 1 && $watched->readsUnderWay === 1);
                $iterator->stop();
            }
            $future = $iterator->handle(static function (): void {
                self::fail('the server holds every pull: nothing is delivered');
            });
            // Should the test fail with the runs still going, the close in tearDown ends them: nothing is to report it.
            $future->ignore();
            $runs[] = $future;
        }

        foreach (array_slice($runs, 0, 3) as $index => $stopped) {
            self::assertSame(0, $this->processedBy($stopped, $iterator, sprintf('run %d did not end at the stop before the next handle()', $index + 1)));
        }
        $this->waitUntil(static fn(): bool => count($server->pulls) === 4);
        self::assertFalse($runs[3]->isComplete(), 'the newest run goes on');
        self::assertCount(3, $transport->controlLinesStartingWith('UNSUB '), 'each stopped run released its inbox');
        self::assertCount(4, array_unique(array_column($server->pulls, 'sid')), 'one pull per run, each on its own inbox');

        $iterator->stop();
        self::assertSame(0, $runs[3]->await(new TimeoutCancellation(5)));
        self::assertCount(4, $transport->controlLinesStartingWith('UNSUB '));
        self::assertCount(4, $server->pulls, 'no pull after a stop');
    }

    /**
     * One stop() ends every run active on the iterator at once (#189): four runs started on it one after the other,
     * without a stop in between, each with its pull held on a silent server, all end on a single stop() from another
     * fiber, each woken by its own wake-up; no frame arrives on the socket to end an older run's wait instead. Each
     * handle() used to replace the wake-ups of the run before, so the stop woke only the newest run, and the older
     * ones saw the shared flag only once their waits ended, at their pulls' deadline.
     */
    public function testOneStopEndsEveryActiveRunAtOnce(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $server = $this->pullServer($transport, static fn(): array => []);
        $client = $this->client($watched);

        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        $runs = [];
        for ($run = 1; $run <= 4; ++$run) {
            $future = $iterator->handle(static function (): void {
                self::fail('the server holds every pull: nothing is delivered');
            });
            // Should the test fail with the runs still going, the close in tearDown ends them: nothing is to report it.
            $future->ignore();
            $runs[] = $future;
            $this->waitUntil(static fn(): bool => count($server->pulls) === $run && $watched->readsUnderWay === 1);
        }

        $iterator->stop();
        foreach ($runs as $index => $run) {
            self::assertSame(0, $this->processedBy($run, $iterator, sprintf('run %d did not end at the stop', $index + 1)));
        }

        self::assertCount(4, $server->pulls, 'no pull after the stop');
        self::assertCount(4, $transport->controlLinesStartingWith('UNSUB '), 'each run released its inbox');
    }

    /** @return iterable<string, array{?int}> */
    public static function runsRestartedFromTheirHandler(): iterable
    {
        yield 'an infinite run' => [null];
        yield 'a finite run' => [1];
    }

    /**
     * The restart idiom from inside the handler, on the first message of a pull being retired (#189): batch 2 and depth
     * 1, the first pull answered with m1 and m2, and the handler stops the run and starts another on m1. The first
     * handler gets m1 only, m2 left undelivered as after any stop(), and the first run returns 1 with its one pull,
     * its inbox released, while the second run pulls on its own inbox. The second handle() used to clear the stop
     * before the retire phase checked it again: the first handler got m2 as well, and an infinite first run went on
     * pulling next to the second, until a later stop() reached it.
     */
    #[DataProvider('runsRestartedFromTheirHandler')]
    public function testARestartFromTheHandlerWhileAPullIsRetiredLeavesTheRestOfItUndelivered(?int $iterations): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $server = $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1
            ? [self::messages($sid, 'm1', 'm2')]
            : []);
        $client = $this->client($watched);

        $handled = [];
        $restarted = new class {
            /** @var Future<int>|null The run the first run's handler started. */
            public ?Future $run = null;
        };
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(1)->setExpiresMs(30_000)->setIterations($iterations);
        $first = $iterator->handle(static function (NatsMessage $message) use (&$handled, $iterator, $restarted): void {
            $handled[] = 'first run: ' . $message->payload;
            if ($message->payload === 'm1') {
                $iterator->stop();
                $restarted->run = $iterator->handle(static function (NatsMessage $message) use (&$handled): void {
                    $handled[] = 'second run: ' . $message->payload;
                });
                // Should the test fail with the run still going, the close in tearDown ends it: nothing is to report it.
                $restarted->run->ignore();
            }
        });

        $firstProcessed = $this->processedBy($first, $iterator, 'the first run did not end at its handler\'s stop: the handle() after it cleared it');
        $this->waitUntil(static fn(): bool => count($server->pulls) === 2);
        $second = $restarted->run;
        if ($second === null) {
            self::fail('the handler never restarted the consumer');
        }

        self::assertSame(['first run: m1'], $handled, 'm2 is left undelivered, as after any stop()');
        self::assertSame(1, $firstProcessed);
        self::assertNotSame($server->pulls[0]['sid'], $server->pulls[1]['sid'], 'the second pull is the second run\'s');
        self::assertSame(['UNSUB ' . $server->pulls[0]['sid']], $transport->controlLinesStartingWith('UNSUB '), 'the first run released its inbox');

        $iterator->stop();
        self::assertSame(0, $second->await(new TimeoutCancellation(5)));
        self::assertCount(2, $server->pulls, 'one pull per run');
    }

    /**
     * Guard, passes on both: giving up on a run's future does not end the run (#189). An await of handle()'s future
     * that its own cancellation ends leaves the run going, still one the iterator reaches: a later stop() from another
     * fiber ends it at once, and the future then resolves with its count, its inbox released.
     */
    public function testAnAwaitGivenUpLeavesTheRunToALaterStop(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $server = $this->pullServer($transport, static fn(): array => []);
        $client = $this->client($watched);

        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        $run = $iterator->handle(static function (): void {
            self::fail('the server holds the pull: nothing is delivered');
        });
        $this->waitUntil(static fn(): bool => count($server->pulls) === 1 && $watched->readsUnderWay === 1);

        $givenUp = new DeferredCancellation();
        $givenUp->cancel();
        try {
            $run->await($givenUp->getCancellation());
            self::fail('the await was given up before the run could end');
        } catch (CancelledException) {
            // The application stopped waiting; the run itself goes on.
        }
        self::assertFalse($run->isComplete(), 'the run goes on');

        $iterator->stop();
        self::assertSame(0, $this->processedBy($run, $iterator, 'the stop did not reach the run whose await was given up'));
        self::assertCount(1, $server->pulls);
        self::assertSame(['UNSUB ' . $server->pulls[0]['sid']], $transport->controlLinesStartingWith('UNSUB '));
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
     * pull's reply subject, the sid the client subscribed for the pull inbox, and the pull's number (1 for the first),
     * and recorded, with that sid and its JSON request, in the object returned. No frames hold the pull, as a server
     * with no message does until the pull expires.
     *
     * @param \Closure(string, int, int): list<string> $onPull
     * @return object{pulls: list<array{sid: int, request: string}>}
     */
    private function pullServer(ReconnectingTransport $transport, \Closure $onPull): object
    {
        $server = new class {
            /** @var list<array{sid: int, request: string}> Each pull, in order: the sid of its run's inbox and its JSON request. */
            public array $pulls = [];
        };
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport, $onPull, $server): array {
            if ($replyTo === null || $subject !== self::PULL_SUBJECT) {
                return [];
            }

            $sid = $transport->sidFor($replyTo);
            if ($sid === null) {
                return [];
            }

            $server->pulls[] = ['sid' => $sid, 'request' => $payload];

            return $onPull($replyTo, $sid, count($server->pulls));
        };

        return $server;
    }

    /** One chunk of messages on a run's inbox, each as a real server delivers a pull's message. */
    private static function messages(int $sid, string ...$payloads): string
    {
        $chunk = '';
        foreach ($payloads as $payload) {
            $chunk .= ReconnectingTransport::msgFrame('evt.s', $sid, $payload, self::ACK_SUBJECT);
        }

        return $chunk;
    }

    /** The batch a pull's JSON request asked for. */
    private static function batchOf(string $request): int
    {
        $decoded = json_decode($request, true, 512, JSON_THROW_ON_ERROR);
        self::assertIsArray($decoded);
        self::assertIsInt($decoded['batch'] ?? null);

        return $decoded['batch'];
    }

    /**
     * What $run returned. It is to end at once, or it goes on until its 30 s pull expires: when it has not ended within
     * 5 s, the test fails with $notEnded, the iterator stopped, so that no run outlives the test (the close in tearDown
     * ends one the stop does not reach in time, and the future is ignored, so that nothing reports that).
     *
     * @param Future<int> $run
     */
    private function processedBy(Future $run, PullConsumerIterator $iterator, string $notEnded): int
    {
        try {
            return $run->await(new TimeoutCancellation(5));
        } catch (CancelledException) {
            $run->ignore();
            $iterator->stop();
            self::fail($notEnded);
        }
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
