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
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * What an infinite pull consumer run receives that no pull in flight has room for goes to its handler (#187). The
 * pipelined engine behind PullConsumerIterator::handle() counts its pulls to decide how many requests to issue, while
 * the server can hold more of the run's requests than that, after a reconnect (see PullConsumerReconnectTest): a message
 * none of the run's pulls in flight has room for used to be dropped as a straggler, gone on a consumer without acks or
 * with max_deliver 1, delivered again after the ack wait otherwise. The run now holds it in its overflow, which follows
 * the rules of everything else the run holds:
 *
 *  - it goes to the handler in arrival order, right behind the pulls the run retires, and counts in the run's total;
 *    what arrives while the handler runs joins it, behind what it holds, and the run issues no pull meanwhile;
 *  - the iterator's drain() and the client's drain() let it reach the handler; stop(), a disconnect() and a drain()
 *    whose budget has run out leave the rest undelivered, and the drain's deadline report counts it, once;
 *  - a handler that throws ends the run with its exception; a run that fails, on a read or on a rejected inbox, hands
 *    it over after its pulls before it throws its own failure, a handler failing there being reported instead;
 *  - a terminal status hands over the pulls behind the one it ended, then the overflow, and the run then ends normally;
 *  - a frame that ended the connection leaves it to the next pass of an infinite run going on past that frame (#210);
 *  - a group pin is captured from its first message, as from a pull's.
 *
 * A finite run (setIterations()) has no overflow: it keeps its exact count and drops what its last pull has no room for.
 * In these tests the server answers a pull with more messages than the run asked for, standing in for the requests a
 * reconnect leaves on the server; the scripted server in tests/Support/ReconnectingTransport.php delivers them, seen
 * through tests/Support/WatchedTransport.php. A handler holds a message on a gate where the test acts while it runs;
 * the time bounds only keep a broken run from hanging the suite. Each test fails on 2.24.2, except the declared guard;
 * the three #189 restart data sets also fail on 2.24.6, where the handle() after the stop cleared it.
 */
final class PullConsumerOverflowTest extends TestCase
{
    use ReconnectScenarios;

    private const PULL_PREFIX = '$JS.API.CONSUMER.MSG.NEXT.S.';

    /** @var list<DeferredFuture<null>> Gates a handler waits on, opened in tearDown for a test that failed early. */
    private array $gates = [];

    protected function tearDown(): void
    {
        foreach ($this->gates as $gate) {
            if (!$gate->isComplete()) {
                $gate->complete();
            }
        }
        $this->gates = [];
        $this->closeOpenedConnections();
    }

    /**
     * Every message reaches the handler in the order it arrived, however many the pulls in flight have room for: depth
     * 2, batch 1, and the server answers the second pull with five messages. The two pulls take m-1 and m-2; m-3 to m-5
     * go to the overflow and reach the handler right behind them, and the drained run returns 5. The router used to drop
     * m-3 to m-5 as stragglers, and the run returned 2.
     */
    public function testMoreMessagesThanThePullsInFlightHaveRoomForAllReachTheHandlerInOrder(): void
    {
        [$transport, , $client] = $this->client();
        $server = $this->pullServer($transport, static fn(string $consumer, int $pull, int $sid): array => $pull === 2
            ? [self::messages($sid, 'm', 1, 5)]
            : []);

        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(30_000);
        $processed = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            $iterator->drain();
        })->await(new TimeoutCancellation(5));

        self::assertSame(['m-1', 'm-2', 'm-3', 'm-4', 'm-5'], $handled->getArrayCopy());
        self::assertSame(5, $processed);
        self::assertSame(['C' => 2], $server->pulls);
    }

    /**
     * The run issues no pull while its overflow holds a message: depth 2, batch 1, and the first pull's write held up,
     * so that the application's read takes the server's answer to it, m-1 and m-2, while the issue phase waits for
     * that write. The pull takes m-1 and the overflow m-2. Once the write is let go, the run issues no second pull: it
     * hands both messages over first, and the drained run then ends with one pull written. Issuing on, it wrote the
     * second pull before the handler had either message. The router used to drop m-2.
     */
    public function testNoPullIsIssuedWhileTheOverflowHoldsMessages(): void
    {
        [$transport, , $client] = $this->client();
        $transport->stallNextWriteContaining(self::PULL_PREFIX, 5.0);
        $server = $this->pullServer($transport, static fn(): array => []);

        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(30_000);
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator, $server): void {
            $handled[] = $message->payload . ' after ' . array_sum($server->pulls) . ' pull(s)';
            $iterator->drain();
        });
        $this->waitUntil(static fn(): bool => $transport->writesStalled() === 1);
        $received = $client->statistics()->inMsgs;
        $transport->pushFrame(self::messages(self::inboxSid($transport), 'm', 1, 2));
        // The application's reads: the router gives m-1 to the pull, whose write is still held up, and m-2 to the
        // overflow. (The first read can bring the PONG to the PING behind the inbox's SUB.)
        while ($client->statistics()->inMsgs < $received + 2) {
            $client->processIncoming(new TimeoutCancellation(2))->await();
        }
        $transport->releaseStalledWrites();
        $processed = $run->await(new TimeoutCancellation(5));

        self::assertSame(['m-1 after 1 pull(s)', 'm-2 after 1 pull(s)'], $handled->getArrayCopy());
        self::assertSame(2, $processed);
        self::assertSame(['C' => 1], $server->pulls, 'no pull while the overflow held m-2');
    }

    /** @return iterable<string, array{string, int}> What happens while the handler holds the overflow's first message. */
    public static function whileTheHandlerHoldsTheOverflow(): iterable
    {
        yield "the iterator's drain()" => ['iterator drain', 1];
        yield 'stop()' => ['stop', 1];
        yield 'a disconnect()' => ['disconnect', 1];
        yield "the client's drain()" => ['client drain', 1];
        yield "the client's drain() running out of budget" => ['drain deadline', 1];
        yield 'the handler throwing' => ['handler failure', 1];
        yield "the client's drain() running out of budget over two runs" => ['drain deadline', 2];
    }

    /**
     * What arrives while the handler holds the overflow's first message joins the overflow behind what it holds, and
     * follows the rule of $action: batch 1, depth 1, and the server answers the first pull of each run with four
     * messages, so that the pull takes the first and the overflow the other three. The handler holds the second, the
     * overflow's first, on a gate, and the server sends two more meanwhile, which the application's read gives to the
     * overflow. Then $action, and the gate opens:
     *
     *  - the iterator's drain(), and the client's drain() within its budget (which waits for the handler): the handler
     *    gets all six in order, and the run returns 6;
     *  - stop(), and a disconnect(): the handler gets nothing more, and the run returns 2 or fails with the close;
     *  - the client's drain() whose budget (200 ms) runs out while the handler holds the gate: its report counts the
     *    four messages each run's overflow holds and the handler has not got (the two after the one it holds, and the
     *    two that came meanwhile), once each, eight over two runs; none of them reaches the handler after the close;
     *  - the handler throwing on the message it held: the run fails with its exception.
     *
     * Each run writes one pull: none while its overflow holds messages, nor once the run is ending. The router used to
     * drop the messages past the pull's batch: the handler got two, and the report counted none of them.
     */
    #[DataProvider('whileTheHandlerHoldsTheOverflow')]
    public function testWhatArrivesWhileTheHandlerRunsFollowsTheOverflowAndItsRules(string $action, int $runs): void
    {
        // Only the drain that runs out of budget gets a short one, which runs out while the handler holds the gate.
        [$transport, , $client, $recorder] = $this->client(requestTimeoutMs: $action === 'drain deadline' ? 200 : 2_000);
        $server = $this->pullServer($transport, static fn(string $consumer, int $pull, int $sid): array => $pull === 1
            ? [self::messages($sid, $consumer, 1, 4)]
            : []);

        $handled = [];
        $runsByConsumer = [];
        $iterators = [];
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        $this->gates[] = $gate;
        for ($index = 0; $index < $runs; ++$index) {
            $consumer = 'C' . $index;
            $handled[$consumer] = self::log();
            /** @var DeferredFuture<null> $holding */
            $holding = new DeferredFuture();
            $iterator = $client->jetStream()->pullConsumer('S', $consumer)->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
            $log = $handled[$consumer];
            $runsByConsumer[$consumer] = $iterator->handle(static function (NatsMessage $message) use ($log, $consumer, $gate, $holding, $action): void {
                $log[] = $message->payload;
                if ($message->payload === $consumer . '-2') {
                    $holding->complete();
                    $gate->getFuture()->await();
                    if ($action === 'handler failure') {
                        throw new \RuntimeException('the handler failed');
                    }
                }
            });
            $iterators[] = $iterator;
            $holding->getFuture()->await(new TimeoutCancellation(3));
        }

        // Two more for each run while its handler holds the overflow's first message: the application's read gives them
        // to the overflow, behind the two it still holds.
        $chunk = '';
        foreach (array_keys($runsByConsumer) as $consumer) {
            $chunk .= self::messages($server->sids[$consumer], $consumer, 5, 6);
        }
        $received = $client->statistics()->inMsgs;
        $transport->pushFrame($chunk);
        while ($client->statistics()->inMsgs < $received + 2 * $runs) {
            $client->processIncoming(new TimeoutCancellation(2))->await();
        }

        $drain = null;
        if ($action === 'iterator drain') {
            $iterators[0]->drain();
        } elseif ($action === 'stop') {
            $iterators[0]->stop();
        } elseif ($action === 'disconnect') {
            $client->disconnect()->await();
        } elseif ($action === 'client drain') {
            $drain = $client->drain();
            $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Draining);
        } elseif ($action === 'drain deadline') {
            $client->drain()->await(new TimeoutCancellation(3));
            self::assertSame(ConnectionState::Closed, $client->state());
            self::assertSame(
                ['drain deadline exceeded: ' . (4 * $runs) . ' buffered message(s) were not delivered before close'],
                $recorder->errorsContaining('drain deadline exceeded'),
            );
        }
        self::assertFalse($drain?->isComplete() ?? false, 'the client\'s drain() waits for the handler');
        $gate->complete();

        $delivered = in_array($action, ['iterator drain', 'client drain'], true) ? 6 : 2;
        foreach ($runsByConsumer as $consumer => $run) {
            [$processed, $error] = self::settle($run);
            self::assertSame(self::payloads($consumer, 1, $delivered), $handled[$consumer]->getArrayCopy(), $consumer);
            if ($action === 'handler failure') {
                self::assertSame('the handler failed', $error?->getMessage());
            } elseif ($action === 'disconnect') {
                self::assertNotNull($error, 'a run whose connection a disconnect() closed fails');
            } else {
                self::assertNull($error, sprintf('handle() threw %s', $error?->getMessage() ?? ''));
                self::assertSame($delivered, $processed);
            }
        }
        $drain?->await(new TimeoutCancellation(3));
        self::assertSame(array_fill_keys(array_keys($runsByConsumer), 1), $server->pulls, 'one pull a run');
    }

    /**
     * A terminal status hands over what the pulls behind the one it ended hold, and then the overflow, before the run
     * ends: depth 2, batch 1, and the server answers the second pull with a 409 Consumer Deleted for the first one,
     * then C-1, which the second pull takes, and C-2, which goes to the overflow. The run retires the first pull,
     * onError gets the 409 once, and the handler then gets C-1 and C-2, in that order; the run returns 2, normally. A
     * run that handed over only the overflow would have left C-1 behind, older than C-2. The run used to abandon C-1,
     * and the router to drop C-2: the handler got nothing, and the run returned 0.
     */
    public function testATerminalStatusHandsThePullsBehindItAndThenTheOverflowOverBeforeTheRunEnds(): void
    {
        [$transport, , $client] = $this->client();
        $firstReplyTo = null;
        $this->pullServer($transport, static function (string $consumer, int $pull, int $sid, string $replyTo) use (&$firstReplyTo): array {
            $firstReplyTo ??= $replyTo;

            return $pull === 2
                ? [self::statusFrame($firstReplyTo, $sid, 409, 'Consumer Deleted') . self::messages($sid, $consumer, 1, 2)]
                : [];
        });

        $handled = self::log();
        $errors = self::log();
        $processed = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(30_000)
            ->setOnError(static function (\Throwable $error) use ($errors): void {
                $errors[] = $error->getMessage();
            })
            ->handle(static function (NatsMessage $message) use ($handled): void {
                $handled[] = $message->payload;
            })->await(new TimeoutCancellation(5));

        self::assertSame(['C-1', 'C-2'], $handled->getArrayCopy());
        self::assertSame(2, $processed);
        self::assertSame(['JetStream pull request ended with status 409: Consumer Deleted'], $errors->getArrayCopy());
    }

    /** @return iterable<string, array{string, string, list<string>}> How the run hands over, where it restarts, what it delivers. */
    public static function restartsWhileTheRunHandsOver(): iterable
    {
        yield 'the overflow behind a retired pull' => ['retire', 'C-2', ['C-1', 'C-2']];
        yield 'the overflow of a run whose inbox is rejected' => ['rejected', 'C-2', ['C-1', 'C-2']];
        yield 'the pull and the overflow behind a terminal status' => ['terminal', 'C-1', ['C-1']];
    }

    /**
     * The restart idiom from the handler while the run hands over what it holds leaves the rest undelivered (#189): the
     * handler stops the run and starts another on $restartOn, and the first handler gets $expected only, the rest left
     * undelivered as after any stop():
     *
     *  - behind a retired pull: batch 1, depth 1, the first pull answered with C-1 to C-3, so that the overflow holds C-2
     *    and C-3, and the restart on C-2, the overflow's first: the run returns 2;
     *  - the same, the -ERR with which the server rejects the run's inbox in the same chunk: handle() still throws the
     *    rejection;
     *  - behind a terminal status: batch 1, depth 2, the second pull answered with a 409 Consumer Deleted for the first
     *    one, then C-1, which the second pull takes, and C-2 and C-3, which go to the overflow, and the restart on C-1:
     *    the run returns 1, onError having heard the 409 once.
     *
     * In each the second run pulls on an inbox of its own, until a stop() ends it. The handle() after the stop used to
     * clear it before the delivery checked it again: the first handler got everything the run held, and behind a
     * retired pull the first run went on pulling next to the second.
     *
     * @param list<string> $expected
     */
    #[DataProvider('restartsWhileTheRunHandsOver')]
    public function testARestartFromTheHandlerLeavesTheRestOfWhatTheRunHoldsUndelivered(string $handOver, string $restartOn, array $expected): void
    {
        [$transport, , $client] = $this->client();
        $firstReplyTo = null;
        $server = $this->pullServer($transport, static function (string $consumer, int $pull, int $sid, string $replyTo) use ($handOver, &$firstReplyTo): array {
            $firstReplyTo ??= $replyTo;
            if ($handOver === 'terminal') {
                return $pull === 2
                    ? [self::statusFrame($firstReplyTo, $sid, 409, 'Consumer Deleted') . self::messages($sid, $consumer, 1, 3)]
                    : [];
            }
            if ($pull !== 1) {
                return [];
            }

            $rejection = sprintf("-ERR 'Permissions Violation for Subscription to \"%s.*\" (sid \"%d\")'\r\n", self::inboxOf($replyTo), $sid);

            return [self::messages($sid, $consumer, 1, 3) . ($handOver === 'rejected' ? $rejection : '')];
        });

        $handled = self::log();
        $errors = self::log();
        $restarted = new class {
            /** @var Future<int>|null The run the first run's handler started. */
            public ?Future $run = null;
        };
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth($handOver === 'terminal' ? 2 : 1)->setExpiresMs(30_000)
            ->setOnError(static function (\Throwable $error) use ($errors): void {
                $errors[] = $error->getMessage();
            });
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator, $restarted, $restartOn): void {
            $handled[] = $message->payload;
            if ($message->payload === $restartOn) {
                $iterator->stop();
                $restarted->run = $iterator->handle(static function (NatsMessage $message) use ($handled): void {
                    $handled[] = 'second run: ' . $message->payload;
                });
                // Settled below; should the test fail first, nothing is to report the run's failure.
                $restarted->run->ignore();
            }
        });
        [$processed, $error] = self::settle($run);

        self::assertSame($expected, $handled->getArrayCopy(), 'the stop leaves the rest undelivered');
        if ($handOver === 'rejected') {
            self::assertInstanceOf(JetStreamException::class, $error);
            self::assertStringContainsString('was rejected by server permissions', $error->getMessage(), 'the run still ends with its failure');
        } else {
            self::assertNull($error, sprintf('handle() threw %s', $error?->getMessage() ?? ''));
            self::assertSame(count($expected), $processed);
        }
        self::assertSame($handOver === 'terminal' ? ['JetStream pull request ended with status 409: Consumer Deleted'] : [], $errors->getArrayCopy());
        $second = $restarted->run;
        if ($second === null) {
            self::fail('the handler never restarted the consumer');
        }
        $firstRunsPulls = $handOver === 'terminal' ? 2 : 1;
        $this->waitUntil(static fn(): bool => ($server->pulls['C'] ?? 0) > $firstRunsPulls);
        self::assertSame(self::inboxSid($transport), $server->sids['C'], 'the latest pull is on the second run\'s inbox');

        $iterator->stop();
        [$secondProcessed, $secondError] = self::settle($second);
        self::assertNull($secondError, sprintf('the second run threw %s', $secondError?->getMessage() ?? ''));
        self::assertSame(0, $secondProcessed);
        self::assertSame($expected, $handled->getArrayCopy(), 'the second run got nothing the first run held');
    }

    /** @return iterable<string, array{bool}> */
    public static function handlerDuringTheHandOver(): iterable
    {
        yield 'the handler gets everything' => [false];
        yield 'the handler throws on the overflow\'s first message' => [true];
    }

    /**
     * A run that fails hands the overflow over after its pulls, then throws its own failure: batch 1, depth 1, and the
     * server answers the pull with C-1 to C-3 and, in the same chunk, a message for another subscription whose handler
     * throws, which with handlerErrorsFailOperations fails the engine's read. The pull holds C-1 and the overflow C-2
     * and C-3: the handler gets all three, in order, and handle() throws the read's failure. A handler that throws on
     * C-2 ends that delivery, its exception reported to the error listener, and handle() still throws the read's
     * failure, which came first. The router used to drop C-2 and C-3.
     */
    #[DataProvider('handlerDuringTheHandOver')]
    public function testARunThatFailsHandsTheOverflowOverAfterItsPulls(bool $handlerThrows): void
    {
        [$transport, , $client, $recorder] = $this->client(handlerErrorsFailOperations: true);
        $faultSid = $client->subscribe('fault', static function (): void {
            throw new \RuntimeException('the other subscription failed');
        })->await();
        $this->pullServer($transport, static fn(string $consumer, int $pull, int $sid): array => [
            self::messages($sid, $consumer, 1, 3) . ReconnectingTransport::msgFrame('fault', $faultSid, 'x'),
        ]);

        $handled = self::log();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled, $handlerThrows): void {
                $handled[] = $message->payload;
                if ($handlerThrows && $message->payload === 'C-2') {
                    throw new \RuntimeException('the handler failed on C-2');
                }
            });
        [$processed, $error] = self::settle($run);

        self::assertNull($processed);
        self::assertNotNull($error);
        self::assertStringContainsString('the other subscription failed', $error->getMessage());
        self::assertSame($handlerThrows ? ['C-1', 'C-2'] : ['C-1', 'C-2', 'C-3'], $handled->getArrayCopy());
        self::assertSame($handlerThrows ? ['the handler failed on C-2'] : [], $recorder->errorsContaining('the handler failed'));
    }

    /**
     * A run whose inbox the server rejects hands the overflow over after its pull, then fails: batch 1, depth 1, and the
     * chunk that answers the pull brings C-1 to C-3, then the -ERR with which nats-server withdraws a subscribe
     * permission on a configuration reload. The pull holds C-1 and the overflow C-2 and C-3: the handler gets all
     * three, in order, and handle() throws the JetStreamException saying the server's permissions rejected the inbox.
     * The router used to drop C-2 and C-3.
     */
    public function testARunWhoseInboxIsRejectedHandsTheOverflowOverAfterItsPull(): void
    {
        [$transport, , $client] = $this->client();
        $this->pullServer($transport, static fn(string $consumer, int $pull, int $sid, string $replyTo): array => [
            self::messages($sid, $consumer, 1, 3)
            . sprintf("-ERR 'Permissions Violation for Subscription to \"%s.*\" (sid \"%d\")'\r\n", self::inboxOf($replyTo), $sid),
        ]);

        $handled = self::log();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled): void {
                $handled[] = $message->payload;
            });
        [, $error] = self::settle($run);

        self::assertInstanceOf(JetStreamException::class, $error);
        self::assertStringContainsString('was rejected by server permissions', $error->getMessage());
        self::assertSame(['C-1', 'C-2', 'C-3'], $handled->getArrayCopy());
        self::assertSame(ConnectionState::Open, $client->state());
    }

    /**
     * Reconnects while the handler goes through the overflow change neither its order nor its count: batch 1, depth 1,
     * and the first pull is answered with C-1 to C-3, so that the overflow holds C-2 and C-3. The handler of C-2 and
     * the handler of C-3 each drop the connection and publish, so that the publish's failed write runs the reconnect
     * inline, and the server, which outlived the connection, sends two more messages for the run's requests as soon as
     * each reconnect has subscribed the run's inbox again: C-4 and C-5, then C-6 and C-7. They join the overflow behind
     * what it holds, and all seven reach the handler in order; the drained run returns 7 after two reconnects, with the
     * one pull it wrote. The router used to drop C-2 to C-7.
     */
    public function testReconnectsWhileTheHandlerGoesThroughTheOverflowKeepItsOrder(): void
    {
        [$transport, , $client] = $this->client();
        $server = $this->pullServer($transport, static fn(string $consumer, int $pull, int $sid): array => $pull === 1
            ? [self::messages($sid, $consumer, 1, 3)]
            : []);
        $transport->afterWrite = static function (string $bytes) use ($transport, $server): void {
            if (str_starts_with($bytes, 'SUB _INBOX.JS.PULL.') && $transport->epoch() > 0 && isset($server->sids['C'])) {
                $first = $transport->epoch() === 1 ? 4 : 6;
                $transport->pushFrame(self::messages(self::inboxSid($transport), 'C', $first, $first + 1));
            }
        };

        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        $processed = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator, $transport, $client): void {
            $handled[] = $message->payload;
            $iterator->drain();
            if ($message->payload === 'C-2' || $message->payload === 'C-3') {
                $transport->dropConnection();
                $client->publish('trigger.recovery', '')->await();
                $client->flush()->await();
            }
        })->await(new TimeoutCancellation(5));

        self::assertSame(self::payloads('C', 1, 7), $handled->getArrayCopy());
        self::assertSame(7, $processed);
        self::assertSame(2, $client->statistics()->reconnects);
        self::assertSame(['C' => 1], $server->pulls);
    }

    /**
     * An infinite run going on past a frame that ended the connection (#210) hands the overflow over on its next pass,
     * behind what the pulls held: batch 1, depth 1, and the server answers the pull with C-1 to C-3 and a -ERR 'Stale
     * Connection' in one chunk. The pull holds C-1 and the overflow C-2 and C-3 when the frame's error reaches the
     * engine, whose read waited for the reconnect: the frame's error goes to the error listener, the handler gets C-1
     * from the hand-over and then C-2 and C-3, and the drained run returns 3 after one reconnect. The router used to
     * drop C-2 and C-3.
     */
    public function testAFrameThatEndedTheConnectionLeavesTheOverflowToTheNextPass(): void
    {
        [$transport, , $client, $recorder] = $this->client();
        $this->pullServer($transport, static fn(string $consumer, int $pull, int $sid): array => $pull === 1
            ? [self::messages($sid, $consumer, 1, 3) . "-ERR 'Stale Connection'\r\n"]
            : []);

        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        $processed = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            $iterator->drain();
        })->await(new TimeoutCancellation(5));

        self::assertSame(['C-1', 'C-2', 'C-3'], $handled->getArrayCopy());
        self::assertSame(3, $processed);
        self::assertSame(1, $client->statistics()->reconnects);
        self::assertNotSame([], $recorder->errorsContaining('Stale Connection'), 'the frame\'s error goes to the error listener');
    }

    /** @return iterable<string, array{?string, list<?string>}> The pin the first message carries, and the pulls' pins. */
    public static function groupsFirstDelivery(): iterable
    {
        yield 'a pinned_client group: the pin is captured' => ['overflow-pin', [null, 'overflow-pin', 'overflow-pin']];
        yield 'an overflow group, which never pins: the bootstrap ends' => [null, [null, null, null]];
    }

    /**
     * A group's first delivery can come from the overflow alone, and it captures the pin and ends the group's
     * bootstrap as a pull's does: depth 2, a group, and the server answers the first pull, pulled alone while the
     * group has no pin, with a 408 for it and C-1, which goes to the overflow. The handler gets C-1, and the next
     * generation fans out to two pulls: a pinned_client group's carry the pin C-1 brought, and an overflow group's,
     * whose messages carry none, go out without one. Both end with a terminal 409, which ends the run with 1. Without
     * the capture the pinned group's next pull went out without the pin, and without the end of the bootstrap either
     * group went on pulling alone. The router used to drop C-1, and the run pulled again alone, without a pin.
     *
     * @param list<?string> $pins
     */
    #[DataProvider('groupsFirstDelivery')]
    public function testAGroupsFirstDeliveryFromTheOverflowCapturesItsPinAndEndsTheBootstrap(?string $pin, array $pins): void
    {
        [$transport, , $client] = $this->client();
        $requests = [];
        $this->pullServer($transport, static function (string $consumer, int $pull, int $sid, string $replyTo, string $payload) use ($pin, &$requests): array {
            /** @var array<string, mixed> $request */
            $request = json_decode($payload, true, flags: JSON_THROW_ON_ERROR);
            $requests[] = $request['id'] ?? null;
            if ($pull === 1) {
                $headers = $pin === null ? "NATS/1.0\r\n\r\n" : "NATS/1.0\r\nNats-Pin-Id: " . $pin . "\r\n\r\n";

                return [
                    self::statusFrame($replyTo, $sid, 408, 'Request Timeout')
                    . ReconnectingTransport::hmsgFrame('evt.s', $sid, $headers, 'C-1', '$JS.ACK.S.C.1.1.1.0.0'),
                ];
            }

            return [self::statusFrame($replyTo, $sid, 409, 'Consumer Deleted')];
        });

        $handled = self::log();
        $processed = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(30_000)
            ->setGroup('workers')
            ->setOnError(static function (): void {})
            ->handle(static function (NatsMessage $message) use ($handled): void {
                $handled[] = $message->payload;
            })->await(new TimeoutCancellation(5));

        self::assertSame(['C-1'], $handled->getArrayCopy());
        self::assertSame(1, $processed);
        self::assertSame($pins, $requests, 'one bootstrap pull, then a generation of two');
    }

    /**
     * Guard, passes on both: a finite run has no overflow. setIterations(1), batch 1, and the server answers the one
     * pull with two messages: the handler gets the first, the second is dropped as a straggler past the run's last
     * pull, as fetchBatch() drops one past its batch, and the run returns 1 after one pull.
     */
    public function testAFiniteRunStillDropsWhatItsLastPullHasNoRoomFor(): void
    {
        [$transport, , $client] = $this->client();
        $server = $this->pullServer($transport, static fn(string $consumer, int $pull, int $sid): array => [self::messages($sid, $consumer, 1, 2)]);

        $handled = self::log();
        $processed = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setIterations(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled): void {
                $handled[] = $message->payload;
            })->await(new TimeoutCancellation(5));

        self::assertSame(['C-1'], $handled->getArrayCopy());
        self::assertSame(1, $processed);
        self::assertSame(['C' => 1], $server->pulls);
    }

    /**
     * A connected client over a watched ReconnectingTransport, closed in tearDown, with no heartbeat, so that only the
     * runs and the test read: reconnect on with waiting for it enabled, a 5 to 20 ms backoff, and a drain budget of
     * $requestTimeoutMs.
     *
     * @return array{ReconnectingTransport, WatchedTransport, NatsClient, LifecycleRecorder}
     */
    private function client(int $requestTimeoutMs = 2_000, bool $handlerErrorsFailOperations = false): array
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->own(new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: $requestTimeoutMs,
            reconnectEnabled: true,
            maxReconnectAttempts: 1_000,
            reconnectDelayMs: 5,
            reconnectMaxDelayMs: 20,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            connectionListener: $recorder->connectionListener(),
            errorListener: $recorder->errorListener(),
            handlerErrorsFailOperations: $handlerErrorsFailOperations,
        ), $watched));
        $this->opened[] = $client;
        $client->connect()->await();

        return [$transport, $watched, $client, $recorder];
    }

    /**
     * The scripted server's side of the pull consumers of stream S: records, by consumer, how many pulls reached a live
     * session and the sid of the run's inbox, and answers each pull with what $answer returns, given the consumer, the
     * pull's number for it (1 for the first), the inbox's sid, the pull's reply subject and its payload; with no frames,
     * the server holds the pull, as one with no message does until it expires.
     *
     * @param \Closure(string, int, int, string, string): list<string> $answer
     * @return object{pulls: array<string, int>, sids: array<string, int>}
     */
    private function pullServer(ReconnectingTransport $transport, \Closure $answer): object
    {
        $server = new class {
            /** @var array<string, int> How many pulls each consumer's run wrote. */
            public array $pulls = [];
            /** @var array<string, int> The sid each consumer's run subscribed its inbox with, on the latest session. */
            public array $sids = [];
        };
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport, $server, $answer): array {
            if ($replyTo === null || !str_starts_with($subject, self::PULL_PREFIX)) {
                return [];
            }

            $consumer = substr($subject, strlen(self::PULL_PREFIX));
            $server->pulls[$consumer] = ($server->pulls[$consumer] ?? 0) + 1;
            $sid = $transport->sidFor($replyTo);
            self::assertNotNull($sid, 'the run\'s inbox is subscribed on the session its pull went out on');
            $server->sids[$consumer] = $sid;

            return $answer($consumer, $server->pulls[$consumer], $sid, $replyTo, $payload);
        };

        return $server;
    }

    /** The sid of the pull inbox the run subscribed on the live session (one run). */
    private static function inboxSid(ReconnectingTransport $transport): int
    {
        $subscribed = $transport->controlLinesStartingWith('SUB _INBOX.JS.PULL.', $transport->epoch());
        self::assertNotSame([], $subscribed, 'the run subscribed its inbox');
        $parts = explode(' ', $subscribed[count($subscribed) - 1]);

        return (int) $parts[count($parts) - 1];
    }

    /** One chunk of messages on a run's inbox, "<prefix>-<n>" for n from $first to $last, each with an ack subject. */
    private static function messages(int $sid, string $prefix, int $first, int $last): string
    {
        $chunk = '';
        foreach (self::payloads($prefix, $first, $last) as $index => $payload) {
            $chunk .= ReconnectingTransport::msgFrame('evt.s', $sid, $payload, sprintf('$JS.ACK.S.C.1.%d.%d.0.0', $first + $index, $first + $index));
        }

        return $chunk;
    }

    /** @return list<string> "<prefix>-<n>" for n from $first to $last. */
    private static function payloads(string $prefix, int $first, int $last): array
    {
        return array_map(static fn(int $sequence): string => $prefix . '-' . $sequence, range($first, $last));
    }

    /** A pull status frame, on the pull's reply subject as the server sends it. */
    private static function statusFrame(string $replyTo, int $sid, int $code, string $description): string
    {
        return ReconnectingTransport::hmsgFrame($replyTo, $sid, sprintf("NATS/1.0 %d %s\r\n\r\n", $code, $description), '');
    }

    /** The run's inbox a pull's reply subject belongs to: the subject without its token. */
    private static function inboxOf(string $replyTo): string
    {
        return substr($replyTo, 0, (int) strrpos($replyTo, '.'));
    }

    /** @return \ArrayObject<int, string> What a handler, or onError, logged, in order. */
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
            return [$run->await(new TimeoutCancellation(5)), null];
        } catch (\Throwable $e) {
            return [null, $e];
        }
    }
}
