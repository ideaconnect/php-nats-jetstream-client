<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\Tests\Support\DrainFlushWatcher;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\delay;

/**
 * The client's drain() hands over what a pull consumer run's pulls hold (#207). The pipelined engine behind
 * PullConsumerIterator::handle() buffers each message into the pull it belongs to and hands a pull's messages to the
 * handler only when the pull completes, and the client's drain() delivered only the connection's own queues: it closed
 * the connection under the run, whose read then failed and handed nothing over, so that a worker which drained the
 * client on SIGTERM lost what the pulls held (gone on a consumer without acks, redelivered after ack_wait otherwise),
 * and the handler of a pull being handed over ran on after the drain had returned, every ack failing. The run is now a
 * drain participant: once the drain's flush is done it hands every pull in flight over while the connection is
 * Draining, the drain waiting for that, the handler included, within its budget, and handle() resolves with the count,
 * as after the iterator's drain(); the run issues no pull while the connection is Draining, and a drain whose budget
 * runs out closes the connection, the rest discarded and named in its report. PullConsumerConnectionLossTest checks the
 * disconnect() side of the rule and the drain() of a run reading for more, and PullConsumerConnectionEndingFrameTest a
 * drain during the hand-over of an infinite run going on past a frame that ended the connection.
 *
 * Over the scripted server in tests/Support/ReconnectingTransport.php, seen through tests/Support/WatchedTransport.php,
 * which counts the reads of the socket, and tests/Support/DrainFlushWatcher.php, which tells when the drain has written
 * the PING of its flush and when it has read the PONG. The order of events decides every outcome: a handler holds a
 * message on a gate the test opens, and the server answers the drain's PING only when the test says so where the drain
 * must still be flushing. The time bounds keep a broken run from hanging the suite, except in the tests that show the
 * drain does not wait (for its budget, or for an idle backoff), whose bounds sit far from both outcomes (a quarter of a
 * second against the half second of a backoff, a second against the two seconds of a budget). Each test fails on
 * 2.23.0, where the drain asked no run, except the three declared guards, which pass on both.
 */
final class PullConsumerClientDrainTest extends TestCase
{
    use ReconnectScenarios;

    private const PULL_PREFIX = '$JS.API.CONSUMER.MSG.NEXT.S.';
    private const PULL_SUBJECT = '$JS.API.CONSUMER.MSG.NEXT.S.C';
    private const DEADLINE_REPORT = 'drain deadline exceeded: 1 buffered message(s) were not delivered before close';

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /**
     * A message that reaches the pull during the drain's flush is handed over with the others: the pull (batch 3, depth
     * 1, a 30 s expiry) holds m-1 and m-2 when the application drains the client, and the server sends m-3, which fills
     * the pull, right after the drain's UNSUB of the run's inbox, ahead of the PONG to the drain's PING, as a server
     * sends what it delivered before the UNSUB. The engine's read takes m-3, and the run hands the pull over, in its
     * retire phase or in the hand-over the drain asks for once it has read its PONG, whichever comes first. The
     * handler, which acks, gets all three while the connection is Draining, the three acks reach the server, handle()
     * returns 3, drain() resolves once that is done, and the run writes no pull after the drain's UNSUB. On 2.23.0 the
     * drain, done with its flush, resolved while the handler was at m-2, without waiting for the run: m-3 came on the
     * Closed connection, its ack failing, and handle() threw "Connection is not open".
     */
    public function testAMessageArrivingDuringTheDrainsFlushIsHandedOverWithTheRest(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport);
        $log = self::log();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000)
            ->handle(self::ackingHandler($log, $client));
        $this->waitUntil(static fn(): bool => isset($server->sids['C']) && $watched->readsUnderWay === 1);
        $sid = $server->sids['C'];
        $this->sendEachInAReadOfItsOwn($transport, $watched, $sid, 'C', ['m-1', 'm-2']);

        // The server's side of the drain's UNSUB of the inbox: m-3, which it delivered before it, comes right behind
        // it.
        $transport->afterWrite = static function (string $bytes) use ($transport, $sid): void {
            if ($bytes === 'UNSUB ' . $sid . "\r\n") {
                $transport->pushFrame(self::messages($sid, 'C', 'm-3'));
            }
        };
        $client->drain()->await(new TimeoutCancellation(5));
        $log[] = 'drain() resolved';
        [$processed, $error] = self::settle($run);

        self::assertSame([
            'handler m-1 (Draining)', 'ack of m-1 sent',
            'handler m-2 (Draining)', 'ack of m-2 sent',
            'handler m-3 (Draining)', 'ack of m-3 sent',
            'drain() resolved',
        ], $log->getArrayCopy());
        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(3, $processed);
        self::assertSame(3, self::acksOnTheWire($transport), 'every ack reached the server');
        self::assertCount(1, $transport->controlLinesStartingWith('PUB ' . self::PULL_SUBJECT . ' '), 'one pull, none after the drain\'s UNSUB');
        self::assertSame([], $recorder->errors, 'nothing discarded, nothing reported');
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * The drain asks the run only once its flush is done, so that what the server sent before the UNSUB of the inbox
     * reaches the pulls before they are handed over: batch 3, depth 1, the pull holding m-1 and m-2, and the handler
     * holding m-1 on a gate. The application drains the client, the server not answering the drain's PING yet; once the
     * drain has written its UNSUBs and its PING, the server's m-3, sent before the UNSUB, comes in: it fills the pull,
     * which the run hands over, the handler holding m-1. Then the gate opens and the PONG comes: the handler gets all
     * three while the connection is Draining, three acks, handle() returns 3. A drain that asked before its flush had
     * the run hand m-1 and m-2 over at once, the pull marked done, and m-3, read during the flush, was dropped as a
     * straggler: unacked, delivered again after the ack wait, lost on a consumer without acks. On 2.23.0 the drain did
     * not wait for the run: the ack of m-2 failed, and m-3 came on the Closed connection, its ack failing too.
     */
    public function testTheDrainAsksOnlyOnceItsFlushHasReadWhatTheServerSentBeforeTheUnsubscribe(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport);
        $flush = new DrainFlushWatcher($transport, $watched, $client);
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        $log = self::log();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000)
            ->handle(self::ackingHandler($log, $client, $gate));
        $this->waitUntil(static fn(): bool => isset($server->sids['C']) && $watched->readsUnderWay === 1);
        $sid = $server->sids['C'];
        $this->sendEachInAReadOfItsOwn($transport, $watched, $sid, 'C', ['m-1', 'm-2']);

        $transport->answerPings = false;
        $drain = $client->drain();
        $this->waitUntil(static fn(): bool => $flush->pingWritten());
        $reads = $watched->reads;
        $transport->pushFrame(self::messages($sid, 'C', 'm-3'));
        $this->waitUntil(static fn(): bool => $watched->reads > $reads && $log->getArrayCopy() !== []);
        $gate->complete();
        $transport->pushFrame("PONG\r\n");
        $drain->await(new TimeoutCancellation(5));
        [$processed, $error] = self::settle($run);

        self::assertSame([
            'handler m-1 (Draining)', 'ack of m-1 sent',
            'handler m-2 (Draining)', 'ack of m-2 sent',
            'handler m-3 (Draining)', 'ack of m-3 sent',
        ], $log->getArrayCopy());
        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(3, $processed);
        self::assertSame(3, self::acksOnTheWire($transport));
        self::assertSame([], $recorder->errors);
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * The hand-over the drain asks for goes through the pulls in flight in issue order: depth 2 and batch 2, the second
     * pull's write held up (a socket under backpressure) while the application drains the client, so that the engine
     * waits in its issue phase, not on the socket. The drain's flush reads m-1, m-2 and m-3 with its PONG: m-1 and m-2
     * fill the first pull and m-3 goes into the second, and the drain asks the run. Once the write is let through, the
     * run goes back to the top of its loop and hands both pulls over, the first one first: the handler gets m-1, m-2
     * and m-3, in that order, while the connection is Draining, and handle() returns 3. Handed over in reverse issue
     * order, m-3 came first. On 2.23.0 the drain asked no run, and the handler got nothing.
     */
    public function testTheDrainsHandOverGoesThroughThePullsInIssueOrder(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport, static function (string $consumer, string $replyTo, int $sid, int $pull) use ($transport): array {
            if ($pull === 1) {
                $transport->stallNextWriteContaining('PUB ' . self::PULL_SUBJECT . ' ', 10.0);
            }

            return [];
        });
        $flush = new DrainFlushWatcher($transport, $watched, $client);
        $log = self::log();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000)
            ->handle(self::ackingHandler($log, $client));
        $this->waitUntil(static fn(): bool => isset($server->sids['C']) && $transport->writesStalled() === 1);
        $sid = $server->sids['C'];

        $transport->answerPings = false;
        $drain = $client->drain();
        $this->waitUntil(static fn(): bool => $flush->pingWritten());
        $transport->pushFrame(self::messages($sid, 'C', 'm-1', 'm-2', 'm-3') . "PONG\r\n");
        $this->waitUntil(static fn(): bool => $flush->flushDone());
        $transport->releaseStalledWrites();
        $drain->await(new TimeoutCancellation(5));
        [$processed, $error] = self::settle($run);

        self::assertSame([
            'handler m-1 (Draining)', 'ack of m-1 sent',
            'handler m-2 (Draining)', 'ack of m-2 sent',
            'handler m-3 (Draining)', 'ack of m-3 sent',
        ], $log->getArrayCopy());
        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(3, $processed);
        self::assertSame([], $recorder->errors);
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * The drain waits for a handler that holds a message when it asks: batch 2, the server answers the pull with m-1
     * and m-2, which fill it, so the retire phase hands it over, and the handler holds m-1 on a gate. The application
     * drains the client meanwhile, from its own fiber: once the drain has read the PONG of its flush, which it reads
     * itself since the engine waits in the handler, drain() is still pending, and the test opens the gate. The handler
     * acks m-1 and gets m-2, both while the connection is Draining, both acks reach the server, handle() returns 2, and
     * drain() resolves only then. On 2.23.0 drain() had resolved by the time its PONG was read, without waiting for the
     * handler, and the handler got m-2 on the Closed connection, both acks failing.
     */
    public function testADrainFromAnotherFiberWaitsForTheHandlerAndTheRestOfThePull(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport, static fn(string $consumer, string $replyTo, int $sid, int $pull): array => $pull === 1 ? [self::messages($sid, 'C', 'm-1', 'm-2')] : []);
        $flush = new DrainFlushWatcher($transport, $watched, $client);
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        $log = self::log();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(1)->setExpiresMs(30_000)
            ->handle(self::ackingHandler($log, $client, $gate));
        $this->waitUntil(static fn(): bool => $log->getArrayCopy() === ['handler m-1 (Open)']);

        $drain = $client->drain();
        $this->waitUntil(static fn(): bool => $flush->flushDone() || $drain->isComplete());
        $log[] = $drain->isComplete() ? 'drain() resolved while the handler held m-1' : 'drain() waits while the handler holds m-1';
        $gate->complete();
        $drain->await(new TimeoutCancellation(5));
        $log[] = 'drain() resolved';
        [$processed, $error] = self::settle($run);

        self::assertSame([
            'handler m-1 (Open)',
            'drain() waits while the handler holds m-1',
            'ack of m-1 sent',
            'handler m-2 (Draining)', 'ack of m-2 sent',
            'drain() resolved',
        ], $log->getArrayCopy());
        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(2, $processed);
        self::assertSame(2, self::acksOnTheWire($transport));
        self::assertSame([0], $server->epochs['C'] ?? [], 'one pull, none after the drain began');
        self::assertSame([], $recorder->errors);
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * A drain that asks while the handler runs in the retire phase also gets the pulls still in flight: depth 2 and
     * batch 2, the server answers the first pull with m-1, m-2 and m-3 in one chunk, so that m-1 and m-2 fill the first
     * pull and m-3 goes into the second, which stays open. The retire phase hands the first pull over, and the handler
     * holds m-1 on a gate while the client drains; once the drain has read its own PONG, and so asked the run, the gate
     * opens. The handler gets m-2, and the run, rather than wait in its pump read for the second pull, whose wake-up
     * from the drain has fired already, goes back to the top of its loop and hands m-3 over too: all of it while the
     * connection is Draining, three acks, handle() returns 3, and drain() resolves without a deadline report. Waiting
     * in that read, the run would have held the drain to its budget, m-3 discarded and reported. On 2.23.0 drain() had
     * resolved while the handler held m-1, which then got m-2 on the Closed connection.
     */
    public function testADrainAskingWhileTheHandlerRunsAlsoGetsThePullsStillInFlight(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport, static fn(string $consumer, string $replyTo, int $sid, int $pull): array => $pull === 1 ? [self::messages($sid, 'C', 'm-1', 'm-2', 'm-3')] : []);
        $flush = new DrainFlushWatcher($transport, $watched, $client);
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        $log = self::log();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000)
            ->handle(self::ackingHandler($log, $client, $gate));
        $this->waitUntil(static fn(): bool => $log->getArrayCopy() === ['handler m-1 (Open)']);

        $drain = $client->drain();
        $this->waitUntil(static fn(): bool => $flush->flushDone() || $drain->isComplete());
        $gate->complete();
        $drain->await(new TimeoutCancellation(5));
        $log[] = 'drain() resolved';
        [$processed, $error] = self::settle($run);

        self::assertSame([
            'handler m-1 (Open)', 'ack of m-1 sent',
            'handler m-2 (Draining)', 'ack of m-2 sent',
            'handler m-3 (Draining)', 'ack of m-3 sent',
            'drain() resolved',
        ], $log->getArrayCopy());
        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(3, $processed);
        self::assertSame(3, self::acksOnTheWire($transport));
        self::assertSame([], $recorder->errors, 'no deadline report: the drain did not wait to its budget');
        self::assertSame([0, 0], $server->epochs['C'] ?? [], 'two pulls, none after the drain began');
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /** @return iterable<string, array{int, bool, string}> */
    public static function handOversOutlastingTheBudget(): iterable
    {
        yield 'a full pull, in the retire phase' => [2, true, 'handler m-1 (Open)'];
        yield 'a pull still open, in the hand-over the drain asks for' => [3, false, 'handler m-1 (Draining)'];
    }

    /**
     * A handler that outlasts the drain's budget has the rest of the pull discarded and named in the drain's report: a
     * budget of half a second (requestTimeoutMs 500), the pull holding m-1 and m-2, either full (batch 2, answered at
     * once, so that the retire phase hands it over, the handler holding m-1 until the drain is called) or still open
     * (batch 3, each message in a read of its own, so that the drain's hand-over hands it over). The handler acks m-1
     * once the drain has been called, and then holds it until drain() has resolved: the drain waits for it to the end
     * of its budget, reports "drain deadline exceeded: 1 buffered message(s) were not delivered before close", m-2
     * still held by the run, and closes the connection. m-2 is then not handed over, the close discarding it, and
     * handle() returns 1, what was handed over. On 2.23.0 drain() resolved without waiting, reporting nothing: in the
     * retire phase the handler then got m-2 on the Closed connection, and with the pull open it got nothing, and
     * handle() threw "Connection is not open" either way.
     */
    #[DataProvider('handOversOutlastingTheBudget')]
    public function testAHandlerOutlastingTheDrainsBudgetHasTheRestDiscardedAndReported(int $batch, bool $answeredAtOnce, string $firstEntry): void
    {
        [$transport, $watched, $client, $recorder] = $this->client(requestTimeoutMs: 500);
        $server = $this->pullServer($transport, $answeredAtOnce
            ? static fn(string $consumer, string $replyTo, int $sid, int $pull): array => $pull === 1 ? [self::messages($sid, 'C', 'm-1', 'm-2')] : []
            : null);
        $drainCall = new class {
            /** @var Future<void>|null The drain() the test made, once it made it. */
            public ?Future $drain = null;
        };
        $log = self::log();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching($batch)->setDepth(1)->setExpiresMs(30_000)
            ->handle(function (NatsMessage $message, JetStreamContext $js) use ($log, $client, $drainCall): void {
                $log[] = 'handler ' . $message->payload . ' (' . $client->state()->name . ')';
                $this->waitUntil(static fn(): bool => $drainCall->drain !== null);
                self::ack($js, $message, $log);
                if ($message->payload === 'm-1') {
                    $this->waitUntil(static fn(): bool => $drainCall->drain?->isComplete() ?? false, 5.0);
                }
            });
        $this->waitUntil(static fn(): bool => isset($server->sids['C']));
        $sid = $server->sids['C'];
        if ($answeredAtOnce) {
            $this->waitUntil(static fn(): bool => $log->getArrayCopy() === ['handler m-1 (Open)']);
        } else {
            $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
            $this->sendEachInAReadOfItsOwn($transport, $watched, $sid, 'C', ['m-1', 'm-2']);
        }

        $drainCall->drain = $client->drain();
        $drainCall->drain->await(new TimeoutCancellation(5));
        $log[] = 'drain() resolved';
        [$processed, $error] = self::settle($run);

        self::assertSame([$firstEntry, 'ack of m-1 sent', 'drain() resolved'], $log->getArrayCopy(), 'm-2 is discarded');
        self::assertSame([self::DEADLINE_REPORT], $recorder->errorsContaining('drain deadline exceeded'), 'the report names m-2, which the run still held');
        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(1, $processed, 'what was handed over');
        self::assertSame(1, self::acksOnTheWire($transport));
        self::assertSame([0], $server->epochs['C'] ?? []);
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /** @return iterable<string, array{array<string, list<string>>, int, int}> */
    public static function pullsHoldingMessagesWhenTheBudgetRunsOut(): iterable
    {
        yield 'two runs on one connection, each holding the rest of its pull' => [['C' => ['c-1', 'c-2'], 'D' => ['d-1', 'd-2']], 1, 2];
        yield 'one run of depth 2, holding the rest of one pull and all of the next' => [['C' => ['c-1', 'c-2', 'c-3', 'c-4']], 2, 3];
    }

    /**
     * The drain's report names the sum of what every run still holds when its budget runs out, and of what every pull
     * of a run holds: a budget of half a second (requestTimeoutMs 500), batch 2, the server answering the first pull of
     * each run with all its messages in one chunk, so that the first pull is full and the retire phase hands it over,
     * and each handler holding its first message on a gate that the test opens once drain() has resolved. In the first
     * data set two runs on one connection, of consumers C and D, hold the rest of their pulls, c-2 and d-2: the drain
     * asks both, waits for both to the end of its budget, and its report names 2. In the second, a run of depth 2 holds
     * c-2 in the pull being retired and c-3 and c-4 in the second pull, still in flight behind it: the report names 3.
     * Nothing the report named reaches a handler, and each handle() returns 1, what was handed over. Counting only the
     * last run the drain asked, the report named 1 in the first data set, and counting only the last pull of the run,
     * 2 in the second. On 2.23.0 the drain asked no run and resolved without waiting, reporting nothing: each handler
     * then got the rest on the Closed connection, and handle() threw "Connection is not open".
     *
     * @param array<string, list<string>> $answers What the server answers the first pull of each consumer's run with.
     */
    #[DataProvider('pullsHoldingMessagesWhenTheBudgetRunsOut')]
    public function testTheDrainsReportAddsUpWhatEveryRunAndEveryPullStillHolds(array $answers, int $depth, int $notDelivered): void
    {
        [$transport, , $client, $recorder] = $this->client(requestTimeoutMs: 500);
        $this->pullServer($transport, static fn(string $consumer, string $replyTo, int $sid, int $pull): array => $pull === 1 ? [self::messages($sid, $consumer, ...$answers[$consumer])] : []);
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        $logs = [];
        $runs = [];
        foreach ($answers as $consumer => $payloads) {
            $log = self::log();
            $first = $payloads[0];
            $logs[$consumer] = $log;
            $runs[$consumer] = $client->jetStream()->pullConsumer('S', $consumer)->setBatching(2)->setDepth($depth)->setExpiresMs(30_000)
                ->handle(static function (NatsMessage $message) use ($log, $client, $gate, $first): void {
                    $log[] = 'handler ' . $message->payload . ' (' . $client->state()->name . ')';
                    if ($message->payload === $first) {
                        $gate->getFuture()->await(new TimeoutCancellation(5));
                    }
                });
        }
        $holding = array_map(static fn(array $payloads): array => ['handler ' . $payloads[0] . ' (Open)'], $answers);
        $logged = static fn(): array => array_map(static fn(\ArrayObject $log): array => $log->getArrayCopy(), $logs);
        $this->waitUntil(static fn(): bool => $logged() === $holding);

        $client->drain()->await(new TimeoutCancellation(5));
        $gate->complete();
        $outcomes = array_map(self::settle(...), $runs);

        self::assertSame(
            [sprintf('drain deadline exceeded: %d buffered message(s) were not delivered before close', $notDelivered)],
            $recorder->errorsContaining('drain deadline exceeded'),
            'the report names what every run, and every pull of a run, still held',
        );
        self::assertSame($holding, $logged(), 'nothing the report named reached a handler');
        foreach ($outcomes as $consumer => [$processed, $error]) {
            self::assertNull($error, sprintf('%s\'s handle() threw %s', $consumer, self::describe($error)));
            self::assertSame(1, $processed, sprintf('%s\'s handle() returns what was handed over', $consumer));
        }
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * A drain whose budget runs out discards what the run still holds from then on, also while it closes the transport
     * (#207): a close that takes a while (the scripted server's closeDelay, as a TLS or WebSocket close does), a budget
     * of half a second, the pull full with m-1 and m-2, and the handler holding m-1 on a gate that the transport's close
     * opens. The drain waits to its deadline, reports m-2 as not delivered, and closes: the handler, resuming while the
     * close is under way, the connection still Draining, acks m-1, and m-2 is not handed over, handle() returning 1.
     * With the discard signal read from the state alone, which stays Draining while the close is awaited, m-2 went to
     * the handler during the close, its ack written, while the report named it as not delivered. On 2.23.0 the drain
     * neither waited for the run nor reported anything.
     */
    public function testWhatTheDrainsReportNamesIsNotHandedOverWhileTheDrainClosesTheTransport(): void
    {
        [$transport, , $client, $recorder] = $this->client(requestTimeoutMs: 500);
        $this->pullServer($transport, static fn(string $consumer, string $replyTo, int $sid, int $pull): array => $pull === 1 ? [self::messages($sid, 'C', 'm-1', 'm-2')] : []);
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        $transport->closeDelay = 0.2;
        $transport->beforeClose = static function () use ($gate): void {
            if (!$gate->isComplete()) {
                $gate->complete();
            }
        };
        $log = self::log();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(1)->setExpiresMs(30_000)
            ->handle(self::ackingHandler($log, $client, $gate));
        $this->waitUntil(static fn(): bool => $log->getArrayCopy() === ['handler m-1 (Open)']);

        $client->drain()->await(new TimeoutCancellation(5));
        $log[] = 'drain() resolved';
        [$processed, $error] = self::settle($run);

        self::assertSame([self::DEADLINE_REPORT], $recorder->errorsContaining('drain deadline exceeded'), 'the report names m-2');
        self::assertSame(['handler m-1 (Open)', 'ack of m-1 sent', 'drain() resolved'], $log->getArrayCopy(), 'm-2, named by the report, is not handed over');
        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(1, $processed);
        self::assertSame(1, self::acksOnTheWire($transport));
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /** @return iterable<string, array{float, bool, bool}> */
    public static function handlersGoingOnAsTheBudgetRunsOut(): iterable
    {
        yield 'a close at once, a handler that does not ack' => [0.0, false, false];
        yield 'a close that takes 100 ms, a handler that acks' => [0.1, true, false];
        yield 'an error listener that awaits after the report, a handler that acks' => [0.0, true, true];
    }

    /**
     * A handler that goes on right as the drain's budget runs out gets none of what the drain's report names: a budget
     * of half a second (requestTimeoutMs 500), the pull (batch 4) holding m-1, m-2 and m-3, each in a read of its own,
     * and the handler holding m-1 on a gate in the hand-over the drain asks for. The error listener that gets the
     * drain's "drain deadline exceeded" report opens the gate: it runs in the drain's fiber, right before the drain
     * releases the connection's state and awaits the transport's close with the connection still Draining, so that the
     * handler goes on while that close is under way. In the first data set the close takes one turn and the handler
     * does not ack (a consumer without acks); in the second the close takes 100 ms (a WebSocket close frame, say) and
     * the handler acks; in the third the listener awaits 50 ms after it has opened the gate, as a listener or logger
     * that writes somewhere may, so that the handler goes on while the report is still being made, before the drain
     * releases anything. Either way the handler gets nothing more, the report names m-2 and m-3, and handle() returns
     * 1; with the acks, only m-1's reaches the server, if it does. With the discard signal read from the state alone,
     * which stays Draining while the close is awaited, the handler got m-2 and m-3 right after the report had named
     * them as not delivered, with the slow close their acks went out too, and handle() returned 3. On 2.23.0 the drain
     * asked no run, and the handler got nothing.
     */
    #[DataProvider('handlersGoingOnAsTheBudgetRunsOut')]
    public function testAHandlerGoingOnAsTheBudgetRunsOutGetsNoneOfWhatTheReportNamed(float $closeDelay, bool $acks, bool $listenerAwaits): void
    {
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        [$transport, $watched, $client, $recorder] = $this->client(
            requestTimeoutMs: 500,
            onError: static function (\Throwable $error) use ($gate, $listenerAwaits): void {
                if (str_contains($error->getMessage(), 'drain deadline exceeded') && !$gate->isComplete()) {
                    $gate->complete();
                    if ($listenerAwaits) {
                        delay(0.05);
                    }
                }
            },
        );
        $transport->closeDelay = $closeDelay;
        $server = $this->pullServer($transport);
        $log = self::log();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(4)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message, JetStreamContext $js) use ($log, $client, $gate, $acks): void {
                $log[] = 'handler ' . $message->payload . ' (' . $client->state()->name . ')';
                if ($message->payload === 'm-1') {
                    $gate->getFuture()->await(new TimeoutCancellation(5));
                }
                if ($acks) {
                    self::ack($js, $message, $log);
                }
            });
        $this->waitUntil(static fn(): bool => isset($server->sids['C']) && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server->sids['C'], 'C', ['m-1', 'm-2', 'm-3']);

        $client->drain()->await(new TimeoutCancellation(5));
        [$processed, $error] = self::settle($run);

        self::assertSame(['handler m-1 (Draining)'], array_values(array_filter($log->getArrayCopy(), static fn(string $entry): bool => str_starts_with($entry, 'handler '))), 'nothing the report named reached the handler');
        self::assertSame(['drain deadline exceeded: 2 buffered message(s) were not delivered before close'], $recorder->errorsContaining('drain deadline exceeded'));
        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(1, $processed);
        self::assertLessThanOrEqual(1, self::acksOnTheWire($transport), 'no ack but m-1\'s');
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * A disconnect() that interrupts the hand-over the drain asks for ends it as it begins, as it ends any delivery of
     * the run: the pull (batch 4) holds m-1, m-2 and m-3, each in a read of its own, the drain asks the run, and the
     * handler holds m-1 on a gate when the application disconnects. The gate opens as the disconnect() starts closing
     * the transport, the connection still Draining until that close is over, so that the handler, which does not await
     * after its work, goes on while the disconnect() is under way. It gets nothing more, and handle() returns 1, what
     * was handed over. With the discard signal read from the state alone, the handler got m-2 and m-3 while the
     * disconnect() was closing the connection, and handle() returned 3. On 2.23.0 the drain asked no run, and the
     * handler got nothing.
     */
    public function testADisconnectInterruptingTheDrainsHandOverEndsItAsItBegins(): void
    {
        [$transport, $watched, $client] = $this->client();
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        $server = $this->pullServer($transport);
        $log = self::log();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(4)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($log, $client, $gate): void {
                $log[] = 'handler ' . $message->payload . ' (' . $client->state()->name . ')';
                if ($message->payload === 'm-1') {
                    $gate->getFuture()->await(new TimeoutCancellation(5));
                }
            });
        $this->waitUntil(static fn(): bool => isset($server->sids['C']) && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server->sids['C'], 'C', ['m-1', 'm-2', 'm-3']);

        $drain = $client->drain();
        $this->waitUntil(static fn(): bool => $log->getArrayCopy() === ['handler m-1 (Draining)'] || $drain->isComplete());
        $transport->beforeClose = static function () use ($gate, $log): void {
            if (!$gate->isComplete()) {
                $log[] = 'the disconnect() closes the transport';
                $gate->complete();
            }
        };
        $client->disconnect()->await(new TimeoutCancellation(5));
        [$processed, $error] = self::settle($run);
        $drain->await(new TimeoutCancellation(5));

        self::assertSame(['handler m-1 (Draining)', 'the disconnect() closes the transport'], $log->getArrayCopy(), 'the disconnect() under way ends the hand-over');
        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(1, $processed);
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * Guard, passes on both: a disconnect() that interrupts the drain's flush leaves the run to fail as after any
     * disconnect(), as on 2.23.0, where the drain asked no run at all. The pull (batch 3) holds m-1 and m-2, the
     * server does not answer the drain's PING, and the application disconnects once the PING is out. The drain, its
     * flush ended by the close, asks no run: the run's read meets the closed connection, the close discards what the
     * pull held, the handler gets nothing, and handle() throws "Connection is not open". A drain that asked the run
     * all the same, having found the connection open when it began, had the run discard what it held at the top of
     * its loop and return 0, as if it had held nothing.
     */
    public function testADisconnectThatInterruptsTheDrainsFlushLeavesTheRunToFail(): void
    {
        [$transport, $watched, $client] = $this->client();
        $flush = new DrainFlushWatcher($transport, $watched, $client);
        $server = $this->pullServer($transport);
        $log = self::log();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000)
            ->handle(self::ackingHandler($log, $client));
        $this->waitUntil(static fn(): bool => isset($server->sids['C']) && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server->sids['C'], 'C', ['m-1', 'm-2']);

        $transport->answerPings = false;
        $drain = $client->drain();
        $this->waitUntil(static fn(): bool => $flush->pingWritten());
        $client->disconnect()->await(new TimeoutCancellation(5));
        $drain->await(new TimeoutCancellation(5));
        [$processed, $error] = self::settle($run);

        self::assertNull($processed, 'the run fails');
        self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame('Connection is not open', $error->getMessage());
        self::assertSame([], $log->getArrayCopy(), 'the close discards what the pull held');
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * A run that goes on while the drain still flushes writes no pull after the drain's UNSUB of its inbox, where
     * nats-server would drop it, its reply subject without interest: batch 2, the pull full with m-1 and m-2, the
     * handler holding m-1 on a gate when the application drains the client, and the server not answering the drain's
     * PING yet. Once the drain has written its UNSUBs and its PING, the test opens the gate: the handler acks m-1 and
     * gets m-2, both while the connection is Draining, and the run, which the drain has not asked yet, issues nothing
     * while the connection is Draining and, with nothing left in flight, returns 2 at once. The drain then gets its
     * PONG, finds no run to ask, and closes. On 2.23.0 the run wrote its next pull after the UNSUB and waited on it
     * until the drain, its PONG never coming, had waited out its two-second budget and closed the connection: handle()
     * threw "Connection is not open".
     */
    public function testARunGoingOnWhileTheDrainFlushesWritesNoPullAfterTheUnsubscribe(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport, static fn(string $consumer, string $replyTo, int $sid, int $pull): array => $pull === 1 ? [self::messages($sid, 'C', 'm-1', 'm-2')] : []);
        $flush = new DrainFlushWatcher($transport, $watched, $client);
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        $log = self::log();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(1)->setExpiresMs(30_000)
            ->handle(self::ackingHandler($log, $client, $gate));
        $this->waitUntil(static fn(): bool => $log->getArrayCopy() === ['handler m-1 (Open)']);
        $sid = $server->sids['C'] ?? 0;

        $transport->answerPings = false;
        $drain = $client->drain();
        $this->waitUntil(static fn(): bool => $flush->pingWritten());
        $gate->complete();
        [$processed, $error] = self::settle($run);
        $stateWhenTheRunEnded = $client->state();
        $transport->pushFrame("PONG\r\n");
        $drain->await(new TimeoutCancellation(5));

        self::assertTrue($flush->unsubscribed($sid));
        self::assertSame([], $flush->pullsAfterTheUnsubscribeOf($sid, self::PULL_SUBJECT), 'no pull after the drain\'s UNSUB of the inbox');
        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(2, $processed);
        self::assertSame(ConnectionState::Draining, $stateWhenTheRunEnded, 'the run ended while the drain still flushed');
        self::assertSame(['handler m-1 (Open)', 'ack of m-1 sent', 'handler m-2 (Draining)', 'ack of m-2 sent'], $log->getArrayCopy());
        self::assertSame(2, self::acksOnTheWire($transport));
        self::assertSame([0], $server->epochs['C'] ?? []);
        self::assertSame([], $recorder->errors);
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * The drain waits for every run on the connection: two runs, of consumers C and D, each with a pull (batch 3) that
     * holds one message, c-1 and d-1, and a handler that holds its message on a gate of its own. The drain asks both,
     * and both hand over while the connection is Draining. The test opens C's gate: C's run ends with 1 while drain()
     * is still pending, since D's handler holds d-1. Once D's gate opens, D's run ends with 1 and drain() resolves,
     * both acks on the wire, and neither run wrote a pull after the drain's UNSUBs. On 2.23.0 the drain asked neither:
     * it closed the connection, and both runs threw "Connection is not open", their handlers never called.
     */
    public function testTheDrainWaitsForEveryRunOnTheConnection(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport);
        $flush = new DrainFlushWatcher($transport, $watched, $client);
        $gates = ['C' => new DeferredFuture(), 'D' => new DeferredFuture()];
        $logs = ['C' => self::log(), 'D' => self::log()];
        $runs = [];
        foreach (['C', 'D'] as $consumer) {
            $runs[$consumer] = $client->jetStream()->pullConsumer('S', $consumer)->setBatching(3)->setDepth(1)->setExpiresMs(30_000)
                ->handle(self::ackingHandler($logs[$consumer], $client, $gates[$consumer]));
        }
        $this->waitUntil(static fn(): bool => count($server->sids) === 2 && $watched->readsUnderWay === 1);
        // c-1 into C's pull and d-1 into D's, in one chunk, read by whichever run is on the socket.
        $reads = $watched->reads;
        $transport->pushFrame(self::messages($server->sids['C'], 'C', 'c-1') . self::messages($server->sids['D'], 'D', 'd-1'));
        $this->waitUntil(static fn(): bool => $watched->reads > $reads && $watched->readsUnderWay === 1);

        $drain = $client->drain();
        $this->waitUntil(static fn(): bool => (count($logs['C']) === 1 && count($logs['D']) === 1) || $drain->isComplete());
        $gates['C']->complete();
        [$processedByC, $errorOfC] = self::settle($runs['C']);
        $drainPendingWhenCEnded = !$drain->isComplete();
        $gates['D']->complete();
        [$processedByD, $errorOfD] = self::settle($runs['D']);
        $drain->await(new TimeoutCancellation(5));

        self::assertSame(['handler c-1 (Draining)', 'ack of c-1 sent'], $logs['C']->getArrayCopy());
        self::assertSame(['handler d-1 (Draining)', 'ack of d-1 sent'], $logs['D']->getArrayCopy());
        self::assertTrue($drainPendingWhenCEnded, 'the drain waited for D once C had ended');
        self::assertNull($errorOfC, sprintf('C\'s handle() threw %s', self::describe($errorOfC)));
        self::assertNull($errorOfD, sprintf('D\'s handle() threw %s', self::describe($errorOfD)));
        self::assertSame([1, 1], [$processedByC, $processedByD]);
        self::assertSame(2, self::acksOnTheWire($transport));
        foreach (['C', 'D'] as $consumer) {
            self::assertTrue($flush->unsubscribed($server->sids[$consumer]));
            self::assertSame([], $flush->pullsAfterTheUnsubscribeOf($server->sids[$consumer], self::PULL_PREFIX . $consumer));
            self::assertSame([0], $server->epochs[$consumer] ?? []);
        }
        self::assertSame([], $recorder->errors);
    }

    /**
     * Guard, passes on both: the drain does not wait for a run that ended before it. A finite run of one iteration gets
     * m-1 and returns 1; the application then drains the client, which resolves at once, well within its two-second
     * budget, and reports nothing. A run left registered as a drain participant after it ended would be asked and never
     * answer, and hold the drain to its budget.
     */
    public function testARunThatEndedBeforeTheDrainIsNotWaitedFor(): void
    {
        [$transport, , $client, $recorder] = $this->client();
        $this->pullServer($transport, static fn(string $consumer, string $replyTo, int $sid, int $pull): array => [self::messages($sid, 'C', 'm-1')]);
        $log = self::log();
        $processed = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000)->setIterations(1)
            ->handle(self::ackingHandler($log, $client))->await(new TimeoutCancellation(5));

        $startedAt = hrtime(true);
        $client->drain()->await(new TimeoutCancellation(5));
        $drainedIn = $this->secondsSince($startedAt);

        self::assertSame(1, $processed);
        self::assertSame(['handler m-1 (Open)', 'ack of m-1 sent'], $log->getArrayCopy());
        self::assertLessThan(1.0, $drainedIn, sprintf('the drain took %.3f s: it waited for a run that had ended', $drainedIn));
        self::assertSame([], $recorder->errors);
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * A run waiting in its idle backoff is woken when the drain asks, and ends at once with nothing in flight: seven
     * no_wait pulls answered empty at once back the engine off for 10, 20, 40, 80, 160, 320 and then 500 ms, and a
     * timer 20 ms into the last backoff, with no read on the socket, drains the client. handle() returns 0 well before
     * the backoff could have ended, no pull follows, and drain() resolves. On 2.23.0 the drain closed the connection
     * without asking the run, which waited out its backoff and then failed with "Connection is not open" at its next
     * pull's write.
     */
    public function testARunInItsIdleBackoffIsWokenByTheDrainAndEndsAtOnce(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $signal = new class {
            /** When the timer called drain(), on the monotonic clock. */
            public ?int $at = null;
            /** Reads of the socket under way then: 0 while the engine waits in its backoff. */
            public ?int $readsUnderWay = null;
            /** @var Future<void>|null */
            public ?Future $drain = null;
        };
        $this->pullServer($transport, static function (string $consumer, string $replyTo, int $sid, int $pull) use ($watched, $client, $signal): array {
            if ($pull === 7) {
                EventLoop::delay(0.02, static function () use ($watched, $client, $signal): void {
                    $signal->readsUnderWay = $watched->readsUnderWay;
                    $signal->at = hrtime(true);
                    $signal->drain = $client->drain();
                });
            }

            return [self::statusFrame($replyTo, $sid, 404, 'No Messages')];
        });

        [$processed, $error] = self::settle($client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setNoWait()->setExpiresMs(3_000)
            ->handle(static function (): void {
                self::fail('every pull is answered empty');
            }));
        $calledAt = $signal->at;
        if ($calledAt === null || $signal->drain === null) {
            self::fail('the timer never fired');
        }
        $returnedAfter = $this->secondsSince($calledAt);
        $signal->drain->await(new TimeoutCancellation(5));

        self::assertSame(0, $signal->readsUnderWay, 'the engine was in its backoff when the drain was called, not in a read');
        self::assertLessThan(0.25, $returnedAfter, sprintf('handle() returned %.3f s after the drain was called; it would have waited out the 500 ms backoff', $returnedAfter));
        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(0, $processed);
        self::assertCount(7, $transport->controlLinesStartingWith('PUB ' . self::PULL_SUBJECT . ' '), 'no pull after the drain');
        self::assertSame([], $recorder->errors);
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * The same where the drain asks while onError awaits in the retire phase, its wake-up fired before the engine's
     * next wait: six no_wait pulls answered empty at once, then a seventh answered with 503, whose report onError
     * awaits on a gate, the engine then on its way to the 500 ms backoff. The application drains the client, which
     * reads its own PONG, the engine waiting in onError, and asks the run; then the test opens the gate. The engine
     * skips the backoff and the run returns 0 at once, with no pull after the drain. Without that check the backoff,
     * which leaves a fired wake-up out, waited its 500 ms. On 2.23.0 the drain did not ask the run, which waited out
     * the backoff and then failed with "Connection is not open".
     */
    public function testADrainThatAsksWhileOnErrorAwaitsSkipsTheIdleBackoff(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $flush = new DrainFlushWatcher($transport, $watched, $client);
        $this->pullServer($transport, static fn(string $consumer, string $replyTo, int $sid, int $pull): array => [$pull === 7
            ? self::statusFrame($replyTo, $sid, 503, 'No Responders')
            : self::statusFrame($replyTo, $sid, 404, 'No Messages')]);
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        $reports = new class {
            /** @var list<int> The code of each status onError got. */
            public array $codes = [];
        };
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setNoWait()->setExpiresMs(3_000)
            ->setOnError(static function (\Throwable $status) use ($reports, $gate): void {
                $reports->codes[] = (int) $status->getCode();
                $gate->getFuture()->await();
            })
            ->handle(static function (): void {
                self::fail('every pull is answered empty');
            });
        $this->waitUntil(static fn(): bool => $reports->codes !== [], 3.0);

        $drain = $client->drain();
        $this->waitUntil(static fn(): bool => $flush->flushDone() || $drain->isComplete());
        $openedAt = hrtime(true);
        $gate->complete();
        [$processed, $error] = self::settle($run);
        $returnedAfter = $this->secondsSince($openedAt);
        $drain->await(new TimeoutCancellation(5));

        self::assertSame([503], $reports->codes);
        self::assertLessThan(0.25, $returnedAfter, sprintf('handle() returned %.3f s after onError did; it would have waited out the 500 ms backoff', $returnedAfter));
        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(0, $processed);
        self::assertCount(7, $transport->controlLinesStartingWith('PUB ' . self::PULL_SUBJECT . ' '), 'no pull after the drain');
        self::assertSame([], $recorder->errors);
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * A handler that throws during the hand-over the drain asks for ends the run with its exception, as anywhere else,
     * and the drain does not wait out its budget for it: the pull (batch 3) holds m-1 and m-2, and the handler throws
     * on m-1. handle() throws the handler's exception, m-2 is not handed over, and drain() resolves at once, with no
     * "drain deadline exceeded" report, which would have named m-2 had the drain waited for the run to its deadline. On
     * 2.23.0 the handler was never called, and handle() threw "Connection is not open".
     */
    public function testAHandlerThatThrowsDuringTheHandOverEndsTheRunAndTheDrainDoesNotWaitForIt(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport);
        $failure = new \RuntimeException('the handler failed on m-1');
        $log = self::log();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($log, $client, $failure): void {
                $log[] = 'handler ' . $message->payload . ' (' . $client->state()->name . ')';

                throw $failure;
            });
        $this->waitUntil(static fn(): bool => isset($server->sids['C']) && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server->sids['C'], 'C', ['m-1', 'm-2']);

        $startedAt = hrtime(true);
        $client->drain()->await(new TimeoutCancellation(5));
        $drainedIn = $this->secondsSince($startedAt);
        [, $error] = self::settle($run);

        self::assertSame(['handler m-1 (Draining)'], $log->getArrayCopy());
        self::assertSame($failure, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame([], $recorder->errorsContaining('drain deadline exceeded'), 'the drain did not wait for the run to its deadline');
        self::assertLessThan(1.0, $drainedIn, sprintf('the drain took %.3f s of its two-second budget', $drainedIn));
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * A stop() during the hand-over the drain asks for leaves the rest undelivered, as anywhere else: the pull (batch
     * 3) holds m-1 and m-2, and the handler acks m-1 and stops the run. m-2 is not handed over, handle() returns 1, and
     * drain() resolves without waiting out its budget, reporting nothing. On 2.23.0 the handler was never called, and
     * handle() threw "Connection is not open".
     */
    public function testAStopDuringTheHandOverLeavesTheRestUndelivered(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport);
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000);
        $log = self::log();
        $acking = self::ackingHandler($log, $client);
        $run = $iterator->handle(static function (NatsMessage $message, JetStreamContext $js) use ($acking, $iterator): void {
            $acking($message, $js);
            $iterator->stop();
        });
        $this->waitUntil(static fn(): bool => isset($server->sids['C']) && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server->sids['C'], 'C', ['m-1', 'm-2']);

        $startedAt = hrtime(true);
        $client->drain()->await(new TimeoutCancellation(5));
        $drainedIn = $this->secondsSince($startedAt);
        [$processed, $error] = self::settle($run);

        self::assertSame(['handler m-1 (Draining)', 'ack of m-1 sent'], $log->getArrayCopy(), 'the stop leaves m-2 undelivered');
        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(1, $processed);
        self::assertSame(1, self::acksOnTheWire($transport));
        self::assertSame([], $recorder->errors);
        self::assertLessThan(1.0, $drainedIn, sprintf('the drain took %.3f s of its two-second budget', $drainedIn));
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * A drain that waits for a reconnect still gets what the pulls held before the connection dropped: the pull (batch
     * 3) holds m-1 and m-2 when the connection drops, the reconnect's dial held, so that the engine's read and the
     * application's drain both wait for the reconnect. Once the dial is let through, the drain takes the reopened
     * connection over, and the server answers the PING of its flush 200 ms later, as over a slow round trip, so that
     * the engine, its read woken by the reconnect, comes round the top of its loop well before the drain asks it. The
     * handler gets m-1 and m-2 while the connection is Draining, both acks go out (an ack subject is valid on any
     * connection), handle() returns 2, and no pull follows the drain's UNSUB of the inbox. The run leaves its pulls
     * alone while the connection is Draining, so that the handler gets m-1 only once the drain's flush is done, in
     * the hand-over the drain asks for, and what the server sent the pull before the UNSUB would still reach it. A run
     * that ended its pulls at the reconnect while Draining, as it does outside a drain (#187), handed them over before
     * that flush was done, and then ended with nothing in flight; the reset at the top of the loop used to drop them
     * there, and without the check of the Draining state the hand-over found nothing, handle() returned 0, and the
     * drain reported nothing. On 2.23.0 the drain asked no run, and the handler got nothing.
     */
    public function testADrainThatWaitedForAReconnectStillGetsWhatThePullsHeld(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $flush = new DrainFlushWatcher($transport, $watched, $client);
        $server = $this->pullServer($transport);
        $log = self::log();
        $acking = self::ackingHandler($log, $client);
        $flushDoneAtTheHandOver = null;
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message, JetStreamContext $js) use ($acking, $flush, &$flushDoneAtTheHandOver): void {
                $flushDoneAtTheHandOver ??= $flush->flushDone();
                $acking($message, $js);
            });
        $this->waitUntil(static fn(): bool => isset($server->sids['C']) && $watched->readsUnderWay === 1);
        $sid = $server->sids['C'];
        $this->sendEachInAReadOfItsOwn($transport, $watched, $sid, 'C', ['m-1', 'm-2']);

        $transport->holdNextDial();
        $transport->dropConnection();
        // The reconnect is dialling, held, and the engine's read waits for it.
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
        $transport->pongDelay = 0.2;
        $drain = $client->drain();
        $transport->releaseDial();
        $drain->await(new TimeoutCancellation(5));
        [$processed, $error] = self::settle($run);

        self::assertSame(['handler m-1 (Draining)', 'ack of m-1 sent', 'handler m-2 (Draining)', 'ack of m-2 sent'], $log->getArrayCopy(), 'what the pulls held before the reconnect is handed over');
        self::assertTrue($flushDoneAtTheHandOver, 'the run kept its pull until the drain asked for it, once its flush was done');
        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(2, $processed);
        self::assertSame(2, self::acksOnTheWire($transport));
        self::assertSame([], $flush->pullsAfterTheUnsubscribeOf($sid, self::PULL_SUBJECT), 'no pull after the drain\'s UNSUB of the inbox');
        self::assertSame([0], $server->epochs['C'] ?? [], 'no pull on the new connection');
        self::assertSame([], $recorder->errorsContaining('drain deadline exceeded'));
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * Guard, passes on both: a drain without a connection to drain asks no run. The pull holds m-1 and m-2 when the
     * connection drops, its reconnect's dials refused, and the engine's read waits for that reconnect; the application
     * drains the client, whose budget (requestTimeoutMs 300) runs out while it waits for the reconnect too. Nothing was
     * flushed, so the drain asks nothing: it stops the reconnect and closes the connection, the run's read then fails,
     * and what the pull held is discarded, as a disconnect() discards it. The handler gets nothing, and handle() throws
     * "Connection is not open". A run asked there would have returned its count, having handed nothing over.
     */
    public function testADrainWithoutAConnectionAsksNoRun(): void
    {
        [$transport, $watched, $client] = $this->client(requestTimeoutMs: 300);
        $server = $this->pullServer($transport);
        $log = self::log();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000)
            ->handle(self::ackingHandler($log, $client));
        $this->waitUntil(static fn(): bool => isset($server->sids['C']) && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server->sids['C'], 'C', ['m-1', 'm-2']);

        $transport->refuseDials();
        $transport->dropConnection();
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Connecting);
        $client->drain()->await(new TimeoutCancellation(5));
        [$processed, $error] = self::settle($run);

        self::assertNull($processed, 'the run fails');
        self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame('Connection is not open', $error->getMessage());
        self::assertSame([], $log->getArrayCopy(), 'the close discards what the pull held');
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * A connected client over a watched ReconnectingTransport, closed in tearDown, with no heartbeat, so that only the
     * runs, the drain and the test read: reconnect on with waiting for it enabled, and the drain's budget
     * $requestTimeoutMs.
     *
     * @param (\Closure(\Throwable): void)|null $onError Also called with each error reported, after the recorder.
     * @return array{ReconnectingTransport, WatchedTransport, NatsClient, LifecycleRecorder}
     */
    private function client(int $requestTimeoutMs = 2_000, ?\Closure $onError = null): array
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $recorderListener = $recorder->errorListener();
        $client = new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: $requestTimeoutMs,
            reconnectEnabled: true,
            maxReconnectAttempts: 1_000,
            reconnectDelayMs: 5,
            reconnectMaxDelayMs: 20,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            connectionListener: $recorder->connectionListener(),
            errorListener: static function (\Throwable $error) use ($recorderListener, $onError): void {
                $recorderListener($error);
                if ($onError !== null) {
                    $onError($error);
                }
            },
        ), $watched);
        $this->opened[] = $client;
        $client->connect()->await();

        return [$transport, $watched, $client, $recorder];
    }

    /**
     * The scripted server's side of the pull consumers of stream S: records, by consumer, the sid of the run's inbox
     * and the epoch (session) of every pull, and answers each pull with what $onPull returns, given the consumer, the
     * pull's reply subject, the inbox's sid and the pull's number for that consumer (1 for the first); with no $onPull,
     * or no frames, the server holds the pull, as one with no message does until it expires.
     *
     * @param (\Closure(string, string, int, int): list<string>)|null $onPull
     * @return object{sids: array<string, int>, epochs: array<string, list<int>>}
     */
    private function pullServer(ReconnectingTransport $transport, ?\Closure $onPull = null): object
    {
        $server = new class {
            /** @var array<string, int> The sid each consumer's run subscribed its inbox with. */
            public array $sids = [];
            /** @var array<string, list<int>> The epoch of each pull, by consumer, in order. */
            public array $epochs = [];
        };
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport, $server, $onPull): array {
            if ($replyTo === null || !str_starts_with($subject, self::PULL_PREFIX)) {
                return [];
            }

            $consumer = substr($subject, strlen(self::PULL_PREFIX));
            $epochs = $server->epochs[$consumer] ?? [];
            $epochs[] = $transport->epoch();
            $server->epochs[$consumer] = $epochs;
            $sid = $transport->sidFor($replyTo);
            if ($sid === null) {
                return [];
            }

            $server->sids[$consumer] ??= $sid;

            return $onPull === null ? [] : $onPull($consumer, $replyTo, $sid, count($epochs));
        };

        return $server;
    }

    /**
     * Sends each payload on a run's inbox in a read of its own: the next goes out once the engine has taken the
     * previous and its next read is on the socket.
     *
     * @param list<string> $payloads
     */
    private function sendEachInAReadOfItsOwn(ReconnectingTransport $transport, WatchedTransport $watched, int $sid, string $consumer, array $payloads): void
    {
        foreach ($payloads as $payload) {
            $reads = $watched->reads;
            $transport->pushFrame(self::messages($sid, $consumer, $payload));
            $this->waitUntil(static fn(): bool => $watched->reads > $reads && $watched->readsUnderWay === 1);
        }
    }

    /**
     * A handler that logs each message with the state of the connection when it got it, holds the first message on
     * $gate when there is one, and acks each message, logging whether the ack was written.
     *
     * @param \ArrayObject<int, string> $log
     * @param DeferredFuture<null>|null $gate
     * @return \Closure(NatsMessage, JetStreamContext): void
     */
    private static function ackingHandler(\ArrayObject $log, NatsClient $client, ?DeferredFuture $gate = null): \Closure
    {
        $first = true;

        return static function (NatsMessage $message, JetStreamContext $js) use ($log, $client, $gate, &$first): void {
            $log[] = 'handler ' . $message->payload . ' (' . $client->state()->name . ')';
            if ($first) {
                $first = false;
                $gate?->getFuture()->await();
            }
            self::ack($js, $message, $log);
        };
    }

    /**
     * Acks $message, logging whether the ack was written: on a Draining connection it goes out, on a Closed one it
     * fails.
     *
     * @param \ArrayObject<int, string> $log
     */
    private static function ack(JetStreamContext $js, NatsMessage $message, \ArrayObject $log): void
    {
        try {
            $js->ack($message)->await();
            $log[] = 'ack of ' . $message->payload . ' sent';
        } catch (\Throwable $ackFailure) {
            $log[] = 'ack of ' . $message->payload . ' failed: ' . $ackFailure->getMessage();
        }
    }

    /** How many acks the client wrote to the server. */
    private static function acksOnTheWire(ReconnectingTransport $transport): int
    {
        return count($transport->controlLinesStartingWith('PUB $JS.ACK.S.'));
    }

    /** One chunk of messages on a run's inbox, each as a real server delivers a pull's message, with an ack subject. */
    private static function messages(int $sid, string $consumer, string ...$payloads): string
    {
        $chunk = '';
        foreach ($payloads as $payload) {
            $chunk .= ReconnectingTransport::msgFrame('evt.s', $sid, $payload, '$JS.ACK.S.' . $consumer . '.1.1.1.0.0');
        }

        return $chunk;
    }

    /** A pull status frame, on the pull's reply subject as the server sends it. */
    private static function statusFrame(string $replyTo, int $sid, int $code, string $description): string
    {
        return ReconnectingTransport::hmsgFrame($replyTo, $sid, sprintf("NATS/1.0 %d %s\r\n\r\n", $code, $description), '');
    }

    /** @return \ArrayObject<int, string> What a handler, and the test, logged, in order. */
    private static function log(): \ArrayObject
    {
        return new \ArrayObject();
    }

    /**
     * Awaits a run: its count, or what it threw.
     *
     * @param Future<int> $run
     * @return array{?int, ?\Throwable}
     */
    private static function settle(Future $run): array
    {
        try {
            return [$run->await(new TimeoutCancellation(8)), null];
        } catch (\Throwable $e) {
            return [null, $e];
        }
    }

    private static function describe(?\Throwable $error): string
    {
        return $error === null ? 'nothing' : $error::class . ': ' . $error->getMessage();
    }
}
