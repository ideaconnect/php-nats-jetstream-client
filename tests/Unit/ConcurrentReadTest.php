<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\IncomingChunkResult;
use IDCT\NATS\Connection\NatsConnection;
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
        $connection = $this->connect($transport);
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
        self::assertLessThan(1.0, $this->secondsSince($start));
    }

    /**
     * The wait for the other read ends when the call's own cancellation fires; that read goes on
     * undisturbed.
     */
    public function testReadWaitingForAnotherFibersReadGivesUpAtItsCancellation(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        // The reply takes 0.3 s: the request holds the read that long.
        $request = $this->startReadingOnAnotherFiber('request', $connection, $transport, replyDelay: 0.3);

        $start = hrtime(true);
        try {
            $connection->processIncoming(new TimeoutCancellation(0.05))->await();
            self::fail('expected the cancellation');
        } catch (CancelledException) {
            // Expected.
        }

        self::assertLessThan(0.25, $this->secondsSince($start));
        self::assertFalse($request->isComplete(), 'the request is still waiting for its reply');
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
     * A read issued from inside the reconnect that a failed read started - by a Reconnected listener, by a
     * handler of a message that arrived during the reconnect, or by the error listener told of the failure -
     * does not wait for that read, which cannot end before the reconnect does. The failed read used to hold
     * the socket read through the whole reconnect, and such a read waited for it forever.
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
        self::assertNotNull($nested->frames, 'the read from inside the reconnect returned');
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
            requestTimeoutMs: 800,
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

        $reader->await(new TimeoutCancellation(3));
        self::assertNotNull($listener->drainSeconds);
        self::assertLessThan(0.4, $listener->drainSeconds, 'well within the 0.8 s budget');
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
            $connection->processIncoming(new TimeoutCancellation(3))->await();

            return (hrtime(true) - $start) / 1e9;
        });
        delay(0.01);

        // The server goes away and refuses dials for 1 s: A's read fails and A runs the reconnect.
        $transport->refuseDials();
        $transport->dropConnection();
        EventLoop::delay(1.0, static fn() => $transport->acceptDials());

        self::assertLessThan(0.5, $b->await(new TimeoutCancellation(2)));
        self::assertSame(ConnectionState::Connecting, $connection->state(), 'the reconnect is still backing off');
        $a->await(new TimeoutCancellation(3));
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * Starts a read on another fiber and returns once that read holds the socket. The server answers it
     * $replyDelay seconds later: a request gets its reply ("echo:hi"); the heartbeat's read gets $frames
     * (by default a PONG).
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
            $transport->responseDelay = $replyDelay;
            $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $subject === 'svc.echo' && $replyTo !== null
                ? $transport->replyFrame($replyTo, 'echo:' . $payload)
                : [];
            $other = async(static fn(): string => $connection->request('svc.echo', 'hi', 2_000)->await()->payload);
        } else {
            // What the heartbeat's timer runs once its PING is written.
            EventLoop::delay($replyDelay, static fn() => $transport->pushFrame($frames));
            $other = async(static fn(): mixed => (new \ReflectionMethod(NatsConnection::class, 'consumeHeartbeatResponse'))->invoke($connection));
        }

        delay(0.01);
        self::assertFalse($other->isComplete(), 'the other read is waiting for the server');

        return $other;
    }
}
