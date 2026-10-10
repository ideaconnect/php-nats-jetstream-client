<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\IncomingChunkResult;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;

/**
 * The application's own read - processIncoming() or readIncoming() - finding the socket read held by
 * another fiber: a request waiting for its reply, or the heartbeat reading what its PING brings. The
 * transport allows one read at a time, so the call waits for that read to finish, bounded by its
 * cancellation, and returns without reading. A SubscriptionQueue polled without a timeout waits the same
 * way, up to its non-blocking bound.
 *
 * It used to return 0 at once. A loop of such calls then ran on futures that finish straight away, which
 * never hand the event loop control: no timer or socket read ran again - that other read's included - and
 * the process spun at 100% CPU for good. The loops below are capped, so the old behaviour fails them
 * instead of hanging the suite.
 *
 * The other way round, the application's read can deliver what an operation waits for while that operation
 * is between its reads: a SubscriptionQueue polled with a timeout, pausing, or the pipelined pull engine,
 * writing its next pull request. The operation takes it before it reads again (#174).
 */
final class ConcurrentReadTest extends TestCase
{
    use ReconnectScenarios;

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{string}> */
    public static function otherReaders(): iterable
    {
        yield 'a request waiting for its reply' => ['request'];
        yield "the heartbeat's read" => ['heartbeat'];
    }

    /** A loop of processIncoming() calls lets the read that another fiber holds finish. */
    #[DataProvider('otherReaders')]
    public function testReadLoopLetsTheReadAnotherFiberHoldsFinish(string $reader): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $other = $this->startReadingOnAnotherFiber($reader, $connection, $transport);

        $calls = 0;
        while (!$other->isComplete() && $calls < 1_000) {
            $calls++;
            try {
                $connection->processIncoming(new TimeoutCancellation(0.1))->await();
            } catch (CancelledException) {
                // This loop's own read of an idle socket, once the other read is done.
            }
        }

        self::assertTrue($other->isComplete(), 'the loop never let the other read finish');
        self::assertLessThan(5, $calls);
    }

    /** @return iterable<string, array{string}> */
    public static function ownReads(): iterable
    {
        yield 'processIncoming()' => ['processIncoming'];
        yield 'readIncoming()' => ['readIncoming'];
    }

    /**
     * The call returns as soon as the other read is done, having read nothing itself: that read delivered
     * what it brought. It does not go on to wait for more.
     */
    #[DataProvider('ownReads')]
    public function testReadReturnsOnceTheReadAnotherFiberHoldsIsDone(string $read): void
    {
        $transport = new ReconnectingTransport();
        // A ten-second ping interval gives the heartbeat's read a two-second window, so that it is still there to
        // deliver the message when a slow runner brings it late; its timer never ticks within the test.
        $connection = $this->connect($transport, pingIntervalSeconds: 10);
        $updates = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $sid = $connection->subscribe('updates', static function (NatsMessage $message) use ($updates): void {
            $updates->payloads[] = $message->payload;
        })->await();
        $heartbeat = $this->startReadingOnAnotherFiber('heartbeat', $connection, $transport, ReconnectingTransport::msgFrame('updates', $sid, 'u1'));

        $start = hrtime(true);
        // Bounded, so a call that went on to wait for another read fails the test instead of hanging it.
        $result = $read === 'processIncoming'
            ? $connection->processIncoming()->await(new TimeoutCancellation(2))
            : $connection->readIncoming()->await(new TimeoutCancellation(2));

        self::assertTrue($heartbeat->isComplete(), 'it waited for the heartbeat\'s read');
        self::assertSame(['u1'], $updates->payloads, 'delivered by the heartbeat\'s read');
        self::assertEquals($read === 'processIncoming' ? 0 : new IncomingChunkResult(0, false), $result);
        // Well before the call's own two-second bound, with room for a pause of 0.8 s on a slow runner.
        self::assertLessThan(1.5, $this->secondsSince($start));
    }

    /**
     * The wait for the other read ends when the call's own cancellation fires; that read goes on
     * undisturbed.
     */
    public function testReadWaitingForAnotherFibersReadGivesUpAtItsCancellation(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        // The server answers only once the test has checked the wait: the request holds the read until then.
        $holder = new class {
            public ?string $replyTo = null;
        };
        $transport->responder = static function (string $subject, ?string $replyTo) use ($holder): array {
            $holder->replyTo = $replyTo;

            return [];
        };
        $request = async(static fn(): string => $connection->request('svc.echo', 'hi', 5_000)->await()->payload);
        delay(0.01);
        self::assertFalse($request->isComplete(), 'the request is waiting for its reply');

        $start = hrtime(true);
        try {
            $connection->processIncoming(new TimeoutCancellation(0.05))->await();
            self::fail('expected the cancellation');
        } catch (CancelledException) {
            // Expected.
        }

        // A wait that ignored its cancellation would last as long as the request's read: five seconds, until the
        // request gives up, since the reply only comes below.
        self::assertLessThan(2.0, $this->secondsSince($start));
        self::assertFalse($request->isComplete(), 'the request is still waiting for its reply');
        self::assertNotNull($holder->replyTo);
        $transport->pushFrame(implode('', $transport->replyFrame($holder->replyTo, 'echo:hi')));
        self::assertSame('echo:hi', $request->await());
    }

    /** @return iterable<string, array{string}> */
    public static function nonBlockingPolls(): iterable
    {
        yield 'fetch()' => ['fetch'];
        yield 'next() without a timeout' => ['next'];
    }

    /**
     * A loop polling a SubscriptionQueue without a timeout lets the read that another fiber holds finish,
     * and gets the message that read brings: each poll waits up to its non-blocking bound. Each poll used to
     * return at once, and the loop spun the same way.
     */
    #[DataProvider('nonBlockingPolls')]
    public function testQueuePollingLoopLetsTheReadAnotherFiberHoldsFinish(string $poll): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->connectClient($transport);
        $queue = $client->subscribeQueue('jobs')->await();
        // The server answers a request 30 ms later, with a job for the queue in the same read.
        $transport->responseDelay = 0.03;
        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $subject === 'svc.echo' && $replyTo !== null
            ? [ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1') . implode('', $transport->replyFrame($replyTo, 'echo:' . $payload))]
            : [];
        $request = async(static fn(): string => $client->request('svc.echo', 'hi', 2_000)->await()->payload);
        delay(0.01);
        self::assertFalse($request->isComplete(), 'the request is waiting for its reply');

        $job = null;
        $calls = 0;
        while ($job === null && $calls < 1_000) {
            $calls++;
            $job = $poll === 'fetch' ? $queue->fetch() : $queue->next();
        }

        self::assertSame('job-1', $job?->payload, 'the loop never let the request\'s read finish');
        self::assertSame('echo:hi', $request->await());
    }

    /** @return iterable<string, array{string}> */
    public static function pollsWithATimeout(): iterable
    {
        yield 'next() with a timeout' => ['next'];
        yield 'fetchAll(1) with a timeout' => ['fetchAll'];
    }

    /**
     * A queue polled with a timeout returns a message that another fiber's read delivers while the poll
     * pauses between reads (#174). The application's processIncoming() holds the socket read when the poll
     * starts, so the poll's first read waits for it and finds nothing. The message that ends that read is
     * for another subscription, whose handler defers a second read of the application's together with the
     * queue's message: the deferred callback runs in the next event-loop tick, before any timer, and so
     * while the poll pauses for 1 ms. The poll used to start another read before it looked at the queue, and
     * that read waited on a socket with nothing more to come until the whole timeout ran out.
     */
    #[DataProvider('pollsWithATimeout')]
    public function testPollReturnsAMessageAnotherReadDeliversDuringItsPause(string $poll): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->connectClient($transport);
        $queue = $client->subscribeQueue('jobs')->await();
        $other = $client->subscribe('other', static function () use ($transport, $client, $queue): void {
            EventLoop::defer(static function () use ($transport, $client, $queue): void {
                $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
                async(static fn(): int => $client->processIncoming()->await())->ignore();
            });
        })->await();
        // The application's read holds the socket when the poll starts; the message that ends it is for "other".
        $reader = async(static fn(): int => $client->processIncoming()->await());
        delay(0.01);
        EventLoop::delay(0.05, static fn() => $transport->pushFrame(ReconnectingTransport::msgFrame('other', $other, 'o1')));
        self::assertFalse($reader->isComplete(), 'the application\'s read holds the socket when the poll starts');

        $start = hrtime(true);
        $result = $poll === 'next'
            ? $queue->setTimeout(5.0)->next()?->payload
            : array_map(static fn(NatsMessage $message): string => $message->payload, $queue->setTimeout(5.0)->fetchAll(1));
        $elapsed = (hrtime(true) - $start) / 1e9;

        self::assertSame($poll === 'next' ? 'job-1' : ['job-1'], $result);
        self::assertLessThan(2.5, $elapsed, 'the poll returned the message once it was there, not at the end of its 5 s timeout');
        self::assertSame(1, $reader->await(), 'the application\'s first read brought the other subscription\'s message');
    }

    /** @return iterable<string, array{SlowConsumerPolicy}> */
    public static function slowConsumerPolicies(): iterable
    {
        yield 'DropOldest' => [SlowConsumerPolicy::DropOldest];
        yield 'DropNewest' => [SlowConsumerPolicy::DropNewest];
        yield 'Error' => [SlowConsumerPolicy::Error];
    }

    /**
     * fetchAll() takes what another fiber's read delivered during its pause before it reads again, also
     * when that does not complete the call, so its own next read has the queue's whole buffer. The queue
     * holds 2 messages. The application's second read fills it during the poll's pause, as in the test
     * above, and the poll's own read then brings 2 more. The poll used to read with its buffer still full,
     * and the 2 that read brought overflowed it: DropOldest dropped the first 2, DropNewest the last 2, and
     * Error failed the call with a SlowConsumerException.
     */
    #[DataProvider('slowConsumerPolicies')]
    public function testFetchAllTakesWhatAnotherReadDeliveredDuringItsPauseBeforeReadingOn(SlowConsumerPolicy $policy): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->own(new NatsClient(new NatsOptions(pingIntervalSeconds: 0, maxPendingMessagesPerSubscription: 2, slowConsumerPolicy: $policy), $transport));
        $this->opened[] = $client;
        $client->connect()->await();
        $queue = $client->subscribeQueue('jobs')->await();
        $other = $client->subscribe('other', static function () use ($transport, $client, $queue): void {
            EventLoop::defer(static function () use ($transport, $client, $queue): void {
                $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j1') . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j2'));
                async(static fn(): int => $client->processIncoming()->await())->ignore();
                // The next 2 come a little later, for the poll's own read.
                EventLoop::delay(0.1, static fn() => $transport->pushFrame(
                    ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j3') . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j4'),
                ));
            });
        })->await();
        $reader = async(static fn(): int => $client->processIncoming()->await());
        delay(0.01);
        EventLoop::delay(0.05, static fn() => $transport->pushFrame(ReconnectingTransport::msgFrame('other', $other, 'o1')));
        self::assertFalse($reader->isComplete(), 'the application\'s read holds the socket when the poll starts');

        $messages = $queue->setTimeout(5.0)->fetchAll(4);

        self::assertSame(['j1', 'j2', 'j3', 'j4'], array_map(static fn(NatsMessage $message): string => $message->payload, $messages));
        self::assertSame(0, $queue->droppedCount());
        self::assertSame(1, $reader->await(), 'the application\'s first read brought the other subscription\'s message');
    }

    /** @return iterable<string, array{float}> */
    public static function secondPullRequestWrites(): iterable
    {
        yield 'written at once' => [0.0];
        yield 'held up 100 ms' => [0.1];
    }

    /**
     * The pipelined pull engine behind PullConsumerIterator::handle() hands over a batch that another
     * fiber's read delivered while the engine issued its next pull, the same shape as #174. The
     * application's processIncoming() loop runs beside the engine. The server answers the first pull at once
     * and holds the second, the stream being empty, and the application's read delivers the first pull's
     * message while the engine waits for the write of the second pull's request. The test transport's
     * writes take a few event-loop hops; one held up 100 ms is like a socket under backpressure. The engine
     * looked at its head pull only before it issued pulls, so it went on to read with the batch already
     * there, and the handler got the message only at the first pull's deadline: its 5 s expiry plus 1 s.
     */
    #[DataProvider('secondPullRequestWrites')]
    public function testPipelinedPullEngineHandsOverABatchAnotherReadDeliveredWhileItIssued(float $stall): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->connectClient($transport);
        $pulls = new class {
            public int $count = 0;
            public bool $stallArmed = false;
        };
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport, $pulls): array {
            if (!str_starts_with($subject, '$JS.API.CONSUMER.MSG.NEXT.S.C') || $replyTo === null || ++$pulls->count > 1) {
                return [];
            }

            $sid = $transport->sidFor($replyTo);
            self::assertNotNull($sid);

            return [ReconnectingTransport::msgFrame('orders.new', $sid, 'a', '$JS.ACK.S.C.1.1.1.0.0')];
        };
        $transport->afterWrite = static function (string $bytes) use ($transport, $pulls, $stall): void {
            // Written after the first pull request: hold up the second.
            if ($stall > 0.0 && !$pulls->stallArmed && str_contains($bytes, 'CONSUMER.MSG.NEXT')) {
                $pulls->stallArmed = true;
                $transport->stallNextWriteContaining('CONSUMER.MSG.NEXT', $stall);
            }
        };
        $run = new class {
            public bool $over = false;
            public ?float $handledAfter = null;
        };
        $reader = async(static function () use ($client, $run): void {
            while (!$run->over) {
                $client->processIncoming()->await();
            }
        });
        $reader->ignore();
        delay(0.01);

        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(5_000);
        $start = hrtime(true);
        $processed = $iterator->handle(static function () use ($iterator, $run, $start): void {
            $run->handledAfter = (hrtime(true) - $start) / 1e9;
            $iterator->stop();
        })->await(new TimeoutCancellation(10));
        $run->over = true;

        self::assertSame(1, $processed);
        self::assertNotNull($run->handledAfter);
        self::assertLessThan(3.0, $run->handledAfter, 'the handler got the message once it was there, not at the first pull\'s 6 s deadline');
        self::assertSame(2, $pulls->count, 'both pulls were issued before the first one was handed over');
    }

    /**
     * The reported freeze, with no fiber of the application's own: a single processIncoming() loop, a
     * handler that awaits something (during which the heartbeat's timer sends a PING and starts reading),
     * and a server that answers the PING a little later. The loop's next call used to find the heartbeat
     * reading, return 0 at once, and spin from then on: the timer that answers the PING never ran, and the
     * second job never came.
     */
    public function testSingleProcessIncomingLoopKeepsRunningWhileTheHeartbeatReads(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, pingIntervalSeconds: 0.05);
        $pingSent = new DeferredFuture();
        $transport->answerPings = false;
        $transport->afterWrite = static function (string $bytes) use ($transport, $pingSent): void {
            if ($bytes !== "PING\r\n") {
                return;
            }

            if (!$pingSent->isComplete()) {
                $pingSent->complete();
            }

            // The server answers 20 ms later.
            EventLoop::delay(0.02, static fn() => $transport->pushFrame("PONG\r\n"));
        };
        $jobs = new class {
            public int $handled = 0;
        };
        $sid = $connection->subscribe('work', static function () use ($jobs, $pingSent): void {
            // An async call inside the handler, during which the heartbeat sends its PING and starts
            // reading its answer.
            $pingSent->getFuture()->await();
            delay(0.005);
            $jobs->handled++;
        })->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('work', $sid, 'job-1'));
        EventLoop::delay(0.2, static fn() => $transport->pushFrame(ReconnectingTransport::msgFrame('work', $sid, 'job-2')));

        $calls = 0;
        while ($jobs->handled < 2 && $calls < 100_000) {
            $calls++;
            $connection->processIncoming(new TimeoutCancellation(2))->await();
        }

        self::assertSame(2, $jobs->handled, 'the loop froze after ' . $calls . ' calls');
    }

    /** @return iterable<string, array{string}> */
    public static function readsFromInsideAReconnect(): iterable
    {
        yield 'a Reconnected listener' => ['reconnected'];
        yield 'a handler the reconnect delivers to' => ['handler'];
        yield 'the error listener told of the failure' => ['error'];
    }

    /**
     * A read issued while a failed read recovers the connection - by the Reconnected listener, which runs once
     * the reconnect is over, by a handler of a message that arrived during the reconnect, or by the error
     * listener told of the failure - does not wait for that read, which cannot end before the recovery does.
     * The failed read used to hold the socket read through the whole reconnect, and such a read waited for it
     * forever.
     */
    #[DataProvider('readsFromInsideAReconnect')]
    public function testReadFromInsideAReconnectDoesNotWaitForTheReadThatStartedIt(string $from): void
    {
        $transport = new ReconnectingTransport();
        $nested = new class {
            public bool $armed = false;
            public ?NatsConnection $connection = null;
            public int $sid = 0;
            public ?int $frames = null;
        };
        $readOnce = static function () use ($nested, $transport, $from): void {
            if (!$nested->armed) {
                return;
            }

            $nested->armed = false;
            // With the connection back, the server has a message for the read. (The error listener's read
            // finds the old socket dead instead, and runs the reconnect itself.)
            if ($from !== 'error') {
                $transport->pushFrame(ReconnectingTransport::msgFrame('work', $nested->sid, 'for-the-nested-read'));
            }
            $nested->frames = $nested->connection?->processIncoming()->await();
        };
        $connection = $this->connect(
            $transport,
            connectionListener: static function (ConnectionEvent $event) use ($from, $readOnce): void {
                if ($from === 'reconnected' && $event === ConnectionEvent::Reconnected) {
                    $readOnce();
                }
            },
            errorListener: static function () use ($from, $readOnce): void {
                if ($from === 'error') {
                    $readOnce();
                }
            },
        );
        $nested->connection = $connection;
        $nested->sid = $connection->subscribe('work', static function () use ($from, $readOnce): void {
            if ($from === 'handler') {
                $readOnce();
            }
        })->await();
        // A message that arrives while the reconnect re-subscribes: the reconnect delivers it once it is done.
        $transport->afterWrite = static function (string $bytes) use ($transport, $nested): void {
            if (str_contains($bytes, 'SUB work ')) {
                $transport->afterWrite = null;
                $transport->pushFrame(ReconnectingTransport::msgFrame('work', $nested->sid, 'during-the-reconnect'));
            }
        };
        // The application's read holds the socket when the connection drops.
        $reader = async(static fn(): int => $connection->processIncoming()->await());
        delay(0.01);
        $nested->armed = true;

        $transport->dropConnection();

        $reader->await(new TimeoutCancellation(2));
        self::assertNotNull($nested->frames, 'the read made during the recovery returned');
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $connection->statistics()->reconnects, 'one reconnect, however many reads noticed the drop');
    }

    /**
     * A request made by a handler of a message that arrived while the connection reconnected - an ordered
     * consumer recreating itself after a gap, say - gets its reply. The read whose failure started the
     * reconnect still held the socket read while the reconnect delivered that message, so the reply could
     * not be read and the request timed out.
     */
    public function testRequestFromAHandlerTheReconnectDeliversToGetsItsReply(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $replyTo !== null
            ? $transport->replyFrame($replyTo, 'reply:' . $payload)
            : [];
        $connection = $this->connect($transport);
        // Sets up the reply inbox before the drop.
        $connection->request('svc', 'warm-up', 500)->await();
        $handler = new class {
            public bool $armed = true;
            public ?string $reply = null;
        };
        $sid = $connection->subscribe('work', static function () use ($connection, $handler): void {
            if ($handler->armed) {
                $handler->armed = false;
                $handler->reply = $connection->request('svc', 'from-the-handler', 500)->await()->payload;
            }
        })->await();
        $transport->afterWrite = static function (string $bytes) use ($transport, $sid): void {
            if (str_contains($bytes, 'SUB work ')) {
                $transport->afterWrite = null;
                $transport->pushFrame(ReconnectingTransport::msgFrame('work', $sid, 'during-the-reconnect'));
            }
        };
        $reader = async(static fn(): int => $connection->processIncoming()->await());
        delay(0.01);

        $transport->dropConnection();

        $reader->await(new TimeoutCancellation(2));
        self::assertSame('reply:from-the-handler', $handler->reply);
    }

    /**
     * drain() called from a Reconnected listener reads its flush at once instead of waiting out its budget:
     * the read whose failure started the reconnect no longer holds the socket read meanwhile.
     */
    public function testDrainFromAReconnectedListenerDoesNotWaitOutItsBudget(): void
    {
        $transport = new ReconnectingTransport();
        $listener = new class {
            public ?NatsConnection $connection = null;
            public ?float $drainSeconds = null;
        };
        $connection = $this->connect(
            $transport,
            requestTimeoutMs: 10_000,
            connectionListener: static function (ConnectionEvent $event) use ($listener): void {
                if ($event === ConnectionEvent::Reconnected && $listener->drainSeconds === null && $listener->connection !== null) {
                    $start = hrtime(true);
                    $listener->connection->drain()->await();
                    $listener->drainSeconds = (hrtime(true) - $start) / 1e9;
                }
            },
        );
        $listener->connection = $connection;
        $connection->subscribe('work', static function (): void {})->await();
        $reader = async(static fn(): int => $connection->processIncoming()->await());
        delay(0.01);

        $transport->dropConnection();

        $reader->await(new TimeoutCancellation(15));
        self::assertNotNull($listener->drainSeconds);
        self::assertLessThan(2.0, $listener->drainSeconds, 'well within the 10 s budget');
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * With waitForReconnect off, a read parked behind a read that then fails returns once that read is over,
     * rather than when the reconnect it runs is: a read issued during a reconnect fails at once there. It used
     * to wait for the whole reconnect.
     */
    public function testReadParkedBehindAFailingReadDoesNotWaitOutTheReconnect(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, waitForReconnect: false, reconnectDelayMs: 50, reconnectMaxDelayMs: 50);
        // Fiber A holds the socket read; fiber B's read is parked behind it.
        $a = async(static fn(): int => $connection->processIncoming()->await());
        delay(0.01);
        $b = async(static function () use ($connection): float {
            $start = hrtime(true);
            $connection->processIncoming(new TimeoutCancellation(10))->await();

            return (hrtime(true) - $start) / 1e9;
        });
        delay(0.01);

        // The server goes away and refuses dials until the test lets them through below: A's read fails and A
        // runs the reconnect.
        $transport->refuseDials();
        $transport->dropConnection();

        // A read that waited for the whole reconnect would still be waiting when the five seconds below ran out.
        self::assertLessThan(2.0, $b->await(new TimeoutCancellation(5)));
        self::assertSame(ConnectionState::Connecting, $connection->state(), 'the reconnect is still backing off');
        $transport->acceptDials();
        $a->await(new TimeoutCancellation(3));
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * Starts a read on another fiber and returns once that read holds the socket. The server answers it
     * $replyDelay seconds after that: a request gets its reply ("echo:hi"); the heartbeat's read gets $frames
     * (by default a PONG). The delay runs from the moment the read is known to wait, so that a slow runner
     * cannot bring the answer in before it. The heartbeat's read waits only as long as the ping interval allows
     * (50 ms when the heartbeat is off, two seconds at most), so a test that needs that read to deliver the answer
     * gives the connection a longer interval.
     *
     * @return Future<mixed>
     */
    private function startReadingOnAnotherFiber(
        string $reader,
        NatsConnection $connection,
        ReconnectingTransport $transport,
        string $frames = "PONG\r\n",
        float $replyDelay = 0.03,
    ): Future {
        if ($reader === 'request') {
            $request = new class {
                public ?string $replyTo = null;
            };
            $transport->responder = static function (string $subject, ?string $replyTo) use ($request): array {
                if ($subject === 'svc.echo') {
                    $request->replyTo = $replyTo;
                }

                return [];
            };
            $other = async(static fn(): string => $connection->request('svc.echo', 'hi', 2_000)->await()->payload);
            $answer = static function () use ($transport, $request): void {
                if ($request->replyTo !== null) {
                    $transport->pushFrame(implode('', $transport->replyFrame($request->replyTo, 'echo:hi')));
                }
            };
        } else {
            // What the heartbeat's timer runs once its PING is written.
            $other = async(static fn(): mixed => (new \ReflectionMethod(NatsConnection::class, 'consumeHeartbeatResponse'))->invoke($connection));
            $answer = static fn() => $transport->pushFrame($frames);
        }

        delay(0.01);
        self::assertFalse($other->isComplete(), 'the other read is waiting for the server');
        EventLoop::delay($replyDelay, $answer);

        return $other;
    }
}
