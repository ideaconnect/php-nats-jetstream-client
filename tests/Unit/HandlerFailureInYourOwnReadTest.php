<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Core\SubscriptionQueue;
use IDCT\NATS\Exception\SlowConsumerException;
use IDCT\NATS\Tests\Support\HeldUpDelivery;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use IDCT\NATS\Transport\TransportInterface;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;
use Psr\Log\LoggerInterface;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\Future\awaitFirst;

/**
 * A handler that throws in your own read, processIncoming() or readIncoming(), and what the read does with the
 * messages behind it (#177). The read used to stop at the handler that threw and leave every message behind it
 * queued, for every subscription, until a later read received bytes: an operation in another fiber whose result
 * was among them, a SubscriptionQueue poll, a request, a fetch, waited out its whole deadline with its message
 * queued the whole time, and on a real server, which pings only every two minutes, every time. The read now
 * delivers the other subscriptions' messages first, in sid order, and throws once the pass is over. Only the
 * failing subscription's own later messages stay queued, in order, and your next read continues them before it
 * reads (#186), so that every failure of that subscription still reaches the application one read at a time; a
 * second subscription that fails in the same read has its exception reported, since one read throws one exception.
 * Under NatsOptions::$handlerErrorsFailOperations an operation's read does the same before it fails the operation.
 *
 * The later tests pin the edges of that rule: a handler's CancelledException, your own and under the option, where a
 * request whose reply arrived behind it now completes with the reply; the failure of an operation's own subscription,
 * held and thrown after the pass; a second failing subscription without an error listener; held overflows next to a
 * held failure, under either option; a handler that reads again itself, catching the nested read's failure or
 * letting another queue's overflow escape; an auto-unsubscribe across a held failure; and the parse-error and
 * dispatch-error paths of readChunk(), which deliver and hold the same way. The #186 tests pin what your next read
 * continues before it reads: the subscriptions a held failure stopped, in sid order, whichever read stopped them,
 * and any that a nested read stops meanwhile, but not the later subscriptions of a delivery held up in another
 * fiber's handler unless a failure had stopped one of them, nor a remainder whose delivery is under way; that it does
 * so before it waits for another fiber's read, never reads beside that read when a handler it runs there awaits, and
 * does so with its cancellation already fired, or with a dropped connection it has not noticed yet; that a read
 * already waiting on the socket is not woken by a stop meanwhile, a documented boundary; and that the stopped mark
 * goes with the remainder.
 */
final class HandlerFailureInYourOwnReadTest extends TestCase
{
    use ReconnectScenarios;

    private const POISON = 'poison handler';

    /** The deadline of each operation, in seconds. */
    private const TIMEOUT = 3.0;

    /** An operation that gets its result in time returns within this many seconds; a broken one at its deadline. */
    private const PROMPT = 1.5;

    /** The payload of the message each operation waits for. */
    private const RESULTS = [
        'next' => 'job-1',
        'fetchAll' => 'job-1',
        'request' => 'reply-1',
        'requestMany' => 'reply-1',
        'fetchBatch' => 'm1',
        'pullConsumer' => 'm1',
    ];

    private const ACK_SUBJECT = '$JS.ACK.S.C.1.1.1.0.0';

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{bool}> */
    public static function remaindersOfTheFailingSubscription(): iterable
    {
        yield "the failing subscription's next message is delivered by the next read" => [false];
        yield "the failing subscription's next handler throws too, in the next read" => [true];
    }

    /**
     * One chunk holds two messages for "a", whose handler throws on the first, and one for "b". The read delivers
     * b's message, then throws a's exception; a's second message stays queued, and the next read continues with it
     * before it reads (here a server PING, which it then reads and counts), and throws again, without reading, when
     * that handler throws too. The read used to throw with b's message still queued.
     */
    #[DataProvider('remaindersOfTheFailingSubscription')]
    public function testYourOwnReadDeliversTheOtherSubscriptionsBeforeItThrowsAndLeavesTheRestOfTheFailingOne(bool $yThrowsToo): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connection($transport, $recorder);
        $seen = [];
        $sidA = $connection->subscribe('a', static function (NatsMessage $message) use (&$seen, $yThrowsToo): void {
            $seen[] = 'a:' . $message->payload;
            if ($message->payload === 'x' || $yThrowsToo) {
                throw new \RuntimeException('handler a on ' . $message->payload);
            }
        })->await();
        $sidB = $connection->subscribe('b', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'b:' . $message->payload;
        })->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('a', $sidA, 'x')
            . ReconnectingTransport::msgFrame('a', $sidA, 'y')
            . ReconnectingTransport::msgFrame('b', $sidB, 'z'),
        );

        $this->assertReadThrows($connection, 'handler a on x');

        self::assertSame(['a:x', 'b:z'], $seen, "b's message is delivered before the exception is thrown; a's next message stays queued");
        self::assertSame([], $recorder->errors, 'thrown, not reported');

        // The next read continues with a's remainder, and nothing else, before it reads: then the PING, which stays
        // unread when y's handler throws too.
        $transport->pushFrame("PING\r\n");
        if ($yThrowsToo) {
            $this->assertReadThrows($connection, 'handler a on y');
        } else {
            self::assertSame(1, $connection->processIncoming()->await());
        }

        self::assertSame(['a:x', 'b:z', 'a:y'], $seen);
        self::assertSame([], $recorder->errors);

        // Nothing is left of a's remainder: a third read brings no more.
        $transport->pushFrame("PING\r\n");
        self::assertSame(1, $connection->processIncoming()->await());
        self::assertSame(['a:x', 'b:z', 'a:y'], $seen);
    }

    /** @return iterable<string, array{bool, list<string>}> */
    public static function remaindersTheNextReadDelivers(): iterable
    {
        yield "the rest of the failing subscription is delivered by the loop's next read" => [false, ['handler slow on s1']];
        yield "the next handler throws too, in the loop's next read, which then does not read" => [true, ['handler slow on s1', 'handler slow on s2']];
    }

    /**
     * The application's catch-and-continue loop of #186, nothing else arriving: one chunk holds three messages for
     * "slow", whose handler throws on the first (and on the second, in the second data set), and one for "jobs". The
     * loop's read delivers jobs' message and throws slow's failure; the loop catches it and reads again, and that read
     * delivers the rest of slow's messages before it takes the socket, throwing again when the next handler throws.
     * Decided by the order of events: what had been delivered when the loop's next read took the socket. The next read
     * used to take the socket at once and leave slow's messages queued until the server sent anything, on an idle
     * connection its PING, two minutes away by default.
     *
     * @param list<string> $expectedCaught
     */
    #[DataProvider('remaindersTheNextReadDelivers')]
    public function testTheApplicationsNextReadDeliversTheRestOfTheFailingSubscriptionBeforeItReadsTheSocket(bool $s2Throws, array $expectedCaught): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->client($watched, $recorder);
        $seen = [];
        $slow = $client->subscribe('slow', static function (NatsMessage $message) use (&$seen, $s2Throws): void {
            $seen[] = 'slow:' . $message->payload;
            if ($message->payload === 's1' || ($s2Throws && $message->payload === 's2')) {
                throw new \RuntimeException('handler slow on ' . $message->payload);
            }
        })->await();
        $jobs = $client->subscribe('jobs', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'jobs:' . $message->payload;
        })->await();
        // What had been delivered each time a read took the socket.
        $deliveredWhenReading = [];
        /** @var DeferredFuture<null> $secondSocketRead */
        $secondSocketRead = new DeferredFuture();
        $watched->onRead = static function () use (&$deliveredWhenReading, &$seen, $secondSocketRead): void {
            $deliveredWhenReading[] = $seen;
            if (count($deliveredWhenReading) === 2) {
                $secondSocketRead->complete();
            }
        };
        $caught = [];
        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop, $caught);

        $transport->pushFrame(
            ReconnectingTransport::msgFrame('slow', $slow, 's1')
            . ReconnectingTransport::msgFrame('slow', $slow, 's2')
            . ReconnectingTransport::msgFrame('slow', $slow, 's3')
            . ReconnectingTransport::msgFrame('jobs', $jobs, 'j1'),
        );
        // A guard against a hang only: the loop's next read takes the socket at once on either code.
        $secondSocketRead->getFuture()->await(new TimeoutCancellation(2));
        $stop->cancel();

        self::assertSame([], $deliveredWhenReading[0], "the loop's first read took the socket before anything was delivered");
        self::assertSame(
            ['slow:s1', 'jobs:j1', 'slow:s2', 'slow:s3'],
            $deliveredWhenReading[1],
            "the rest of slow was delivered before the loop's next read of the socket",
        );
        self::assertSame($expectedCaught, $caught, 'each failure of slow reached the loop, one read at a time');
        self::assertSame([], $recorder->errors, 'thrown, not reported');
    }

    /**
     * The order of that delivery is the sid order, whatever order the subscriptions stopped in (#186): "b" stops first,
     * in your read, its handler throwing on y1 with y2 and y3 queued; then an operation's read, which under
     * handlerErrorsFailOperations holds what a handler throws, stops "a", the lower sid, at x1 with x2 queued, and its
     * pass meets b's remainder too, whose handler throws again on y2, so that b stays stopped ahead of a. Your next
     * read continues a before b, as a read that receives a chunk delivers them, and before it takes the socket. It
     * used to deliver both only with what it read.
     */
    public function testYourNextReadContinuesTheStoppedSubscriptionsInSidOrderWhateverOrderTheyStoppedIn(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->client($watched, $recorder, failOperations: true);
        $seen = [];
        $failingOn = static function (string $subject, string ...$payloads) use (&$seen): \Closure {
            return static function (NatsMessage $message) use (&$seen, $subject, $payloads): void {
                $seen[] = $subject . ':' . $message->payload;
                if (in_array($message->payload, $payloads, true)) {
                    throw new \RuntimeException('handler ' . $subject . ' on ' . $message->payload);
                }
            };
        };
        $a = $client->subscribe('a', $failingOn('a', 'x1'))->await();
        $b = $client->subscribe('b', $failingOn('b', 'y1', 'y2'))->await();

        $transport->pushFrame(
            ReconnectingTransport::msgFrame('b', $b, 'y1')
            . ReconnectingTransport::msgFrame('b', $b, 'y2')
            . ReconnectingTransport::msgFrame('b', $b, 'y3'),
        );
        $this->assertReadThrows($client, 'handler b on y1');

        $transport->pushFrame(ReconnectingTransport::msgFrame('a', $a, 'x1') . ReconnectingTransport::msgFrame('a', $a, 'x2'));
        $thrown = null;
        try {
            $client->readIncomingForOperation()->await();
        } catch (\RuntimeException $e) {
            $thrown = $e;
        }

        self::assertNotNull($thrown, "expected the operation's read to fail with a's failure");
        self::assertSame('handler a on x1', $thrown->getMessage());
        self::assertSame(['b:y1', 'a:x1', 'b:y2'], $seen, 'a stopped at x1, b again at y2');
        self::assertSame(['handler b on y2'], $recorder->errors, "the second failure of the operation's read is reported");

        // What had been delivered when your next read took the socket.
        $whenReading = new class {
            /** @var list<string>|null */
            public ?array $seen = null;
        };
        $watched->onRead = static function () use ($whenReading, &$seen): void {
            $whenReading->seen ??= $seen;
        };
        $transport->pushFrame("PING\r\n");

        self::assertSame(1, $client->processIncoming()->await());
        self::assertSame(['b:y1', 'a:x1', 'b:y2', 'a:x2', 'b:y3'], $whenReading->seen, 'a before b, both before the read took the socket');
        self::assertSame(['handler b on y2'], $recorder->errors);
    }

    /**
     * What your read continues is only what a handler failure stopped (#186): the later subscriptions of a delivery
     * held up in another fiber's handler that awaits stay queued for that delivery. The first read is held up in a's
     * handler with b's message queued behind it; a second processIncoming(), in another fiber, takes the socket with
     * b's message still queued, and the first read delivers it once a's handler returns. A guard: it passes on the
     * code before #186 as well, and fails when your read delivers every leftover, as a serving loop's read does.
     */
    public function testYourOwnReadLeavesTheLaterSubscriptionsOfADeliveryHeldUpInAnotherFiberToIt(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->client($watched, $recorder);
        $seen = [];
        $hold = new HeldUpDelivery(fallbackSeconds: 3.0);
        $a = $client->subscribe('a', static function (NatsMessage $message) use (&$seen, $hold): void {
            $seen[] = 'a:' . $message->payload;
            $hold->holdUp();
        })->await();
        $b = $client->subscribe('b', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'b:' . $message->payload;
        })->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('a', $a, 'x') . ReconnectingTransport::msgFrame('b', $b, 'y'));
        $first = $client->processIncoming(new TimeoutCancellation(3));
        $hold->began->getFuture()->await(new TimeoutCancellation(2));

        // What had been delivered when the second read took the socket.
        $whenReading = new class {
            /** @var list<string>|null */
            public ?array $seen = null;
        };
        /** @var DeferredFuture<null> $socketTaken */
        $socketTaken = new DeferredFuture();
        $watched->onRead = static function () use ($whenReading, &$seen, $socketTaken): void {
            $whenReading->seen = $seen;
            $socketTaken->complete();
        };
        $stopSecond = new DeferredCancellation();
        $second = $client->processIncoming($stopSecond->getCancellation());
        $socketTaken->getFuture()->await(new TimeoutCancellation(2));
        $watched->onRead = null;
        $stopSecond->cancel();
        try {
            $second->await();
            self::fail('expected the second read to end with its cancellation');
        } catch (CancelledException) {
            // It read nothing.
        }

        self::assertSame(['a:x'], $whenReading->seen, "the second read took the socket with b's message queued");
        self::assertTrue($hold->held, "a's handler still holds the first read up");
        $hold->end('the test');
        self::assertSame(2, $first->await(), 'the first read delivers the rest of its chunk once the handler returns');
        self::assertSame(['a:x', 'b:y'], $seen);
        self::assertSame([], $recorder->errors);
    }

    /**
     * A later subscription of a delivery held up in another fiber is continued by your read when a handler failure had
     * already stopped it, and then whole (#186), the messages the held-up delivery brought for it included: "c" stops
     * at c1 in your read, with c2 queued; an operation's read, which continues nothing, then brings a1, b1 and c3 and
     * is held up in a's handler, the lowest sid. Your read, in another fiber, delivers c2 and c3, in order, before it
     * takes the socket, and leaves b1, which nothing stopped, to the held-up delivery, which delivers it once a's
     * handler returns. Decided by the order of events: what had been delivered when your read took the socket. On
     * 2.19.0 your read took the socket at once, with c2 and c3 queued.
     */
    public function testYourReadContinuesAStoppedSubscriptionWholeAheadOfADeliveryHeldUpInAnotherFiber(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->client($watched, $recorder);
        $seen = [];
        $hold = new HeldUpDelivery(fallbackSeconds: 3.0);
        $a = $client->subscribe('a', static function (NatsMessage $message) use (&$seen, $hold): void {
            $seen[] = 'a:' . $message->payload;
            $hold->holdUp();
        })->await();
        $b = $client->subscribe('b', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'b:' . $message->payload;
        })->await();
        $c = $client->subscribe('c', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'c:' . $message->payload;
            if ($message->payload === 'c1') {
                throw new \RuntimeException('handler c on c1');
            }
        })->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('c', $c, 'c1') . ReconnectingTransport::msgFrame('c', $c, 'c2'));
        $this->assertReadThrows($client, 'handler c on c1');

        // The operation's read brings a1, b1 and c3, and is held up in a's handler with b1 and c3 queued behind it.
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('a', $a, 'a1')
            . ReconnectingTransport::msgFrame('b', $b, 'b1')
            . ReconnectingTransport::msgFrame('c', $c, 'c3'),
        );
        $operation = $client->readIncomingForOperation(new TimeoutCancellation(3));
        // Should an assertion below fail first, these reads end with their cancellation: no error of the next test.
        $operation->ignore();
        $hold->began->getFuture()->await(new TimeoutCancellation(2));

        // What had been delivered when your read took the socket.
        $whenReading = new class {
            /** @var list<string>|null */
            public ?array $seen = null;
        };
        /** @var DeferredFuture<null> $socketTaken */
        $socketTaken = new DeferredFuture();
        $watched->onRead = static function () use ($whenReading, &$seen, $socketTaken): void {
            $whenReading->seen = $seen;
            $socketTaken->complete();
        };
        $mine = $client->processIncoming(new TimeoutCancellation(3));
        $mine->ignore();
        $socketTaken->getFuture()->await(new TimeoutCancellation(2));
        $watched->onRead = null;

        self::assertSame(['c:c1', 'a:a1', 'c:c2', 'c:c3'], $whenReading->seen, 'your read continued c whole, and left b1 to the held-up delivery');
        self::assertTrue($hold->held, "a's handler still holds the operation's read up");

        // Once a's handler returns, the held-up delivery goes on with b1, and finds nothing left of c.
        $hold->end('the test');
        self::assertSame(3, $operation->await()->frames, "the operation's read counts the three frames it read");
        self::assertSame(['c:c1', 'a:a1', 'c:c2', 'c:c3', 'b:b1'], $seen);

        // Your read then reads the server's next bytes.
        $transport->pushFrame("PING\r\n");
        self::assertSame(1, $mine->await());
        self::assertSame([], $recorder->errors);
    }

    /**
     * A remainder whose delivery is under way is left to that delivery (#186): "s" stops at s1, your next read in
     * fiber A continues it with s2, whose handler awaits, and a read in fiber B then takes the socket without
     * delivering s3, which would run s's handler on top of the suspended one and out of order. Once the hold-up ends,
     * A delivers s3 and then reads. Decided by the order of events: what had been delivered when each read took the
     * socket. A's read used to take the socket at once, with s2 and s3 queued.
     */
    public function testYourNextReadLeavesARemainderWhoseDeliveryIsUnderWayToThatDelivery(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->client($watched, $recorder);
        $seen = [];
        $hold = new HeldUpDelivery(fallbackSeconds: 3.0);
        $s = $client->subscribe('s', static function (NatsMessage $message) use (&$seen, $hold): void {
            $seen[] = 's:' . $message->payload;
            if ($message->payload === 's1') {
                throw new \RuntimeException('handler s on s1');
            }

            if ($message->payload === 's2') {
                $hold->holdUp();
            }
        })->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('s', $s, 's1')
            . ReconnectingTransport::msgFrame('s', $s, 's2')
            . ReconnectingTransport::msgFrame('s', $s, 's3'),
        );
        $this->assertReadThrows($client, 'handler s on s1');

        // What had been delivered each time a read took the socket from here on.
        $deliveredWhenReading = [];
        /** @var DeferredFuture<null> $socketTaken */
        $socketTaken = new DeferredFuture();
        $watched->onRead = static function () use (&$deliveredWhenReading, &$seen, $socketTaken): void {
            $deliveredWhenReading[] = $seen;
            if (!$socketTaken->isComplete()) {
                $socketTaken->complete();
            }
        };

        // Fiber A: your next read continues s with s2, whose handler awaits, before it takes the socket.
        $readA = $client->processIncoming(new TimeoutCancellation(3));
        $readA->ignore();
        awaitFirst([$hold->began->getFuture(), $socketTaken->getFuture()], new TimeoutCancellation(2));
        self::assertTrue($hold->held, 'the next read continued s with s2 before it took the socket');
        self::assertSame([], $deliveredWhenReading, 'no read took the socket meanwhile');

        // Fiber B: a read while s2's delivery is under way takes the socket without delivering s3.
        $stopB = new DeferredCancellation();
        $readB = $client->processIncoming($stopB->getCancellation());
        $socketTaken->getFuture()->await(new TimeoutCancellation(2));
        $stopB->cancel();
        try {
            $readB->await();
            self::fail("expected B's read to end with its cancellation");
        } catch (CancelledException) {
            // It read nothing.
        }

        self::assertSame([['s:s1', 's:s2']], $deliveredWhenReading, "B's read took the socket with s3 left to the delivery under way");

        // Once the hold-up ends, A delivers s3, then reads the PING.
        $transport->pushFrame("PING\r\n");
        $hold->end('the test');
        self::assertSame(1, $readA->await(), "A's read counts only what it read");
        self::assertSame([['s:s1', 's:s2'], ['s:s1', 's:s2', 's:s3']], $deliveredWhenReading, 'A delivered s3 before it took the socket');
        self::assertSame([], $recorder->errors);
    }

    /**
     * Your read continues a remainder before it waits for another fiber's read, not once that read is over (#186): an
     * operation's read holds the socket, waiting on it, when your read starts, and a:y is delivered at once, while that
     * read still waits; your read then waits for it, and returns without reading once that read has brought b:z.
     * Decided by the order of events: a:y is delivered while the operation's read is still on the socket. A read that
     * continued the remainder only once it could take the socket itself would leave a:y queued until the server's next
     * bytes, and deliver it behind b:z.
     */
    public function testYourReadContinuesTheRemainderBeforeItWaitsForAnotherFibersRead(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->client($watched, $recorder);
        $seen = [];
        /** @var DeferredFuture<null> $yDelivered */
        $yDelivered = new DeferredFuture();
        $a = $client->subscribe('a', static function (NatsMessage $message) use (&$seen, $yDelivered): void {
            $seen[] = 'a:' . $message->payload;
            if ($message->payload === 'x') {
                throw new \RuntimeException('handler a on x');
            }

            $yDelivered->complete();
        })->await();
        $b = $client->subscribe('b', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'b:' . $message->payload;
        })->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('a', $a, 'x') . ReconnectingTransport::msgFrame('a', $a, 'y'));
        $this->assertReadThrows($client, 'handler a on x');

        // An operation's read takes the socket and waits on it: the server has nothing more yet.
        /** @var DeferredFuture<null> $socketTaken */
        $socketTaken = new DeferredFuture();
        $watched->onRead = static function () use ($socketTaken): void {
            $socketTaken->complete();
        };
        $operation = $client->readIncomingForOperation(new TimeoutCancellation(3));
        // Should an assertion below fail first, these reads end with their cancellation: no error of the next test.
        $operation->ignore();
        $socketTaken->getFuture()->await(new TimeoutCancellation(2));
        $watched->onRead = null;

        // Your read finds the socket taken. A guard against a hang only: a:y is delivered as your read starts.
        $mine = $client->processIncoming(new TimeoutCancellation(3));
        $mine->ignore();
        $yDelivered->getFuture()->await(new TimeoutCancellation(2));
        self::assertFalse($operation->isComplete(), "a:y was delivered while the operation's read still waited on the socket");
        self::assertFalse($mine->isComplete(), 'your read then waits for that read');

        $transport->pushFrame(ReconnectingTransport::msgFrame('b', $b, 'z'));
        self::assertSame(1, $operation->await()->frames);
        self::assertSame(0, $mine->await(), 'your read returns without reading');
        self::assertSame(['a:x', 'a:y', 'b:z'], $seen);
        self::assertSame([], $recorder->errors);
    }

    /**
     * A handler your read continues that awaits does not leave two reads on the socket (#186): your read continues a
     * with a:y, whose handler holds up, and meanwhile an operation's read takes the socket and waits on it. Once the
     * handler returns, your read finds the socket taken and waits for that read instead of reading beside it, and
     * returns without reading once that read has brought b:z. Decided by the order of events: how many reads of the
     * socket were under way each time one started. A read that continued the remainder after it had decided to take
     * the socket would read beside the operation's read once the handler returned.
     */
    public function testAHandlerYourReadContinuesThatAwaitsLeavesTheSocketToTheReadThatTookItMeanwhile(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->client($watched, $recorder);
        $seen = [];
        $hold = new HeldUpDelivery(fallbackSeconds: 3.0);
        $a = $client->subscribe('a', static function (NatsMessage $message) use (&$seen, $hold): void {
            $seen[] = 'a:' . $message->payload;
            if ($message->payload === 'x') {
                throw new \RuntimeException('handler a on x');
            }

            $hold->holdUp();
        })->await();
        $b = $client->subscribe('b', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'b:' . $message->payload;
        })->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('a', $a, 'x') . ReconnectingTransport::msgFrame('a', $a, 'y'));
        $this->assertReadThrows($client, 'handler a on x');

        // How many reads of the socket were under way each time one started.
        $underWayWhenReading = [];
        /** @var DeferredFuture<null> $socketTaken */
        $socketTaken = new DeferredFuture();
        $watched->onRead = static function () use (&$underWayWhenReading, $watched, $socketTaken): void {
            $underWayWhenReading[] = $watched->readsUnderWay;
            if (!$socketTaken->isComplete()) {
                $socketTaken->complete();
            }
        };

        // Your read continues a with a:y, whose handler holds up, before it takes the socket.
        $mine = $client->processIncoming(new TimeoutCancellation(3));
        $mine->ignore();
        awaitFirst([$hold->began->getFuture(), $socketTaken->getFuture()], new TimeoutCancellation(2));
        self::assertTrue($hold->held, 'your read continued a with a:y before it took the socket');

        // Meanwhile an operation's read takes the socket and waits on it.
        $operation = $client->readIncomingForOperation(new TimeoutCancellation(3));
        $operation->ignore();
        $socketTaken->getFuture()->await(new TimeoutCancellation(2));

        // a:y's handler returns: your read waits for the operation's read instead of reading beside it.
        $hold->end('the test');
        $hold->returned->getFuture()->await(new TimeoutCancellation(2));
        self::assertSame([1], $underWayWhenReading, 'one read of the socket at a time');
        self::assertFalse($mine->isComplete(), "your read waits for the operation's read");

        $transport->pushFrame(ReconnectingTransport::msgFrame('b', $b, 'z'));
        self::assertSame(1, $operation->await()->frames, "the operation's read brought b:z");
        self::assertSame(0, $mine->await(), 'your read returned without reading');
        self::assertSame([1], $underWayWhenReading, 'no read of the socket followed');
        self::assertSame(['a:x', 'a:y', 'b:z'], $seen);
        self::assertSame([], $recorder->errors);
    }

    /**
     * A boundary that readIncoming() documents, pinned so that a change to it is made on purpose (#186): your read
     * already waiting on the socket is not woken when another fiber's read stops a subscription meanwhile, and that
     * remainder waits for the server's next bytes. Read A brings r1, s1 and s2 and is held up in r's handler; your
     * read B, made meanwhile, finds nothing stopped yet and takes the socket; once r's handler returns, A stops "s" at
     * s1, with s2 queued, and throws. B still waits, s2 with it, until the server sends a PING, which B reads and
     * delivers s2 with. Decided by the order of events: s2 is still queued once A has thrown, and B returns the PING's
     * frame. The code before #186 did the same. A read woken by the stop would return without reading, s2 left to
     * the next read.
     */
    public function testAMarkSetWhileYourOtherReadWaitsOnTheSocketWaitsForTheServersNextBytes(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->client($watched, $recorder);
        $seen = [];
        $hold = new HeldUpDelivery(fallbackSeconds: 3.0);
        $r = $client->subscribe('r', static function (NatsMessage $message) use (&$seen, $hold): void {
            $seen[] = 'r:' . $message->payload;
            $hold->holdUp();
        })->await();
        $s = $client->subscribe('s', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 's:' . $message->payload;
            if ($message->payload === 's1') {
                throw new \RuntimeException('handler s on s1');
            }
        })->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('r', $r, 'r1')
            . ReconnectingTransport::msgFrame('s', $s, 's1')
            . ReconnectingTransport::msgFrame('s', $s, 's2'),
        );

        // A: reads the chunk; its delivery is held up in r's handler.
        $readA = $client->processIncoming(new TimeoutCancellation(3));
        // Should an assertion below fail first, these reads end with their cancellation: no error of the next test.
        $readA->ignore();
        $hold->began->getFuture()->await(new TimeoutCancellation(2));

        // B: nothing is stopped yet, so it takes the socket and waits on it.
        /** @var DeferredFuture<null> $socketTaken */
        $socketTaken = new DeferredFuture();
        $watched->onRead = static function () use ($socketTaken): void {
            $socketTaken->complete();
        };
        $readB = $client->processIncoming(new TimeoutCancellation(3));
        $readB->ignore();
        $socketTaken->getFuture()->await(new TimeoutCancellation(2));
        $watched->onRead = null;

        // r's handler returns: A stops s at s1, with s2 queued, and throws.
        $hold->end('the test');
        try {
            $readA->await();
            self::fail('expected the handler failure');
        } catch (\RuntimeException $e) {
            self::assertSame('handler s on s1', $e->getMessage());
        }

        self::assertSame(['r:r1', 's:s1'], $seen, 's2 is still queued');
        self::assertFalse($readB->isComplete(), 'B still waits on the socket');

        // The server's next bytes: B reads them and delivers s2 with them.
        $transport->pushFrame("PING\r\n");
        self::assertSame(1, $readB->await(), "B read the server's PING");
        self::assertSame(['r:r1', 's:s1', 's:s2'], $seen);
        self::assertSame([], $recorder->errors);
    }

    /**
     * The cancellation of your read bounds its waits, not the continuation it makes first (#186): a read whose
     * cancellation has already fired still delivers a:y, which a's failure left queued, and then throws its
     * CancelledException, as its read of the socket does. Nothing is lost and nothing is reported. On 2.19.0 the read
     * threw at once, with a:y still queued.
     */
    public function testAReadWhoseCancellationHasAlreadyFiredStillContinuesTheRemainderThenThrowsCancelled(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connection($transport, $recorder);
        $seen = [];
        $a = $connection->subscribe('a', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'a:' . $message->payload;
            if ($message->payload === 'x') {
                throw new \RuntimeException('handler a on x');
            }
        })->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('a', $a, 'x') . ReconnectingTransport::msgFrame('a', $a, 'y'));
        $this->assertReadThrows($connection, 'handler a on x');

        $cancelled = new DeferredCancellation();
        $cancelled->cancel();
        $thrown = null;
        try {
            $connection->processIncoming($cancelled->getCancellation())->await();
        } catch (CancelledException $e) {
            $thrown = $e;
        }

        self::assertNotNull($thrown, 'expected the read to throw its cancellation');
        self::assertSame(['a:x', 'a:y'], $seen, 'the cancelled read delivered a:y before it threw');
        self::assertSame([], $recorder->errors);
    }

    /**
     * Your catch-and-continue loop on a connection that drops, reconnect off (#186): one chunk holds a:x1 (its handler
     * throws), a:y1, a:x2 (its handler throws too) and a:y2, and the connection drops once the first read has thrown.
     * The loop's next read delivers a:y1 and throws a:x2's failure without reading, the drop not noticed yet; the read
     * after it delivers a:y2, then reads, meets the drop and closes the connection ("Reconnect is disabled"). Nothing
     * is discarded. On 2.19.0 the next read met the drop at once, and the close discarded a:y1, a:x2 and a:y2
     * ("Connection closed: 3 parsed inbound message(s) were discarded undelivered").
     */
    public function testWithReconnectOffYourNextReadsDeliverTheRemainderAndItsFailureBeforeTheyMeetADrop(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = new NatsConnection(new NatsOptions(
            connectTimeoutMs: 500,
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
        ), $transport);
        $this->opened[] = $connection;
        $connection->connect()->await();
        $seen = [];
        $a = $connection->subscribe('a', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'a:' . $message->payload;
            if (str_starts_with($message->payload, 'x')) {
                throw new \RuntimeException('handler a on ' . $message->payload);
            }
        })->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('a', $a, 'x1')
            . ReconnectingTransport::msgFrame('a', $a, 'y1')
            . ReconnectingTransport::msgFrame('a', $a, 'x2')
            . ReconnectingTransport::msgFrame('a', $a, 'y2'),
        );
        $this->assertReadThrows($connection, 'handler a on x1');

        $transport->dropConnection();

        $this->assertReadThrows($connection, 'handler a on x2');
        self::assertSame(['a:x1', 'a:y1', 'a:x2'], $seen, 'the next read delivered a:y1 and threw on a:x2');
        self::assertSame(ConnectionState::Open, $connection->state(), 'it did not read: the drop is not noticed yet');

        $this->assertReadThrows($connection, 'Reconnect is disabled');
        self::assertSame(['a:x1', 'a:y1', 'a:x2', 'a:y2'], $seen, 'the read after it delivered a:y2 before it met the drop');
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(['Socket closed by peer (EOF)'], $recorder->errors, 'nothing was discarded');
    }

    /**
     * Two subscriptions fail in one read: "a" on its first message and "b" on its first, with "c" behind them. The
     * read throws a's exception, reports b's through the error listener and the logger at error level, and delivers
     * c's message; a's and b's second messages stay queued, and the next read continues with both, in sid order,
     * before it reads. The read used to throw a's exception with everything behind it queued.
     */
    public function testTheSecondSubscriptionThatFailsInTheSameReadIsReportedAndTheRestIsStillDelivered(): void
    {
        $logger = new class extends AbstractLogger {
            /** @var list<string> */
            public array $entries = [];

            /** @param array<mixed> $context */
            public function log($level, string|\Stringable $message, array $context = []): void
            {
                if (str_contains((string) $message, 'handler ')) {
                    $this->entries[] = (string) $level . ': ' . (string) $message;
                }
            }
        };
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connection($transport, $recorder, $logger);
        $seen = [];
        $failingOn = static function (string $subject, string $payload) use (&$seen): \Closure {
            return static function (NatsMessage $message) use (&$seen, $subject, $payload): void {
                $seen[] = $subject . ':' . $message->payload;
                if ($message->payload === $payload) {
                    throw new \RuntimeException('handler ' . $subject . ' on ' . $payload);
                }
            };
        };
        $sidA = $connection->subscribe('a', $failingOn('a', 'x'))->await();
        $sidB = $connection->subscribe('b', $failingOn('b', 'y'))->await();
        $sidC = $connection->subscribe('c', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'c:' . $message->payload;
        })->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('a', $sidA, 'x')
            . ReconnectingTransport::msgFrame('a', $sidA, 'x2')
            . ReconnectingTransport::msgFrame('b', $sidB, 'y')
            . ReconnectingTransport::msgFrame('b', $sidB, 'y2')
            . ReconnectingTransport::msgFrame('c', $sidC, 'z'),
        );

        $this->assertReadThrows($connection, 'handler a on x');

        self::assertSame(['a:x', 'b:y', 'c:z'], $seen, "each failing subscription stops at its failing message; c's message is delivered");
        self::assertSame(['handler b on y'], $recorder->errors, "the second subscription's failure is reported: one read throws one exception");
        self::assertSame(['error: NATS connection error: handler b on y'], $logger->entries, 'and logged at error level');

        $transport->pushFrame("PING\r\n");
        self::assertSame(1, $connection->processIncoming()->await());

        self::assertSame(['a:x', 'b:y', 'c:z', 'a:x2', 'b:y2'], $seen, 'the next read continues both remainders, in sid order');
        self::assertSame(['handler b on y'], $recorder->errors);
    }

    /** @return iterable<string, array{string}> */
    public static function operationsBehindAHandlerThatThrows(): iterable
    {
        yield 'SubscriptionQueue::next()' => ['next'];
        yield 'SubscriptionQueue::fetchAll(1)' => ['fetchAll'];
        yield 'request()' => ['request'];
        yield 'requestMany(max 1)' => ['requestMany'];
        yield 'JetStreamContext::fetchBatch(1)' => ['fetchBatch'];
        yield 'PullConsumerIterator::handle()' => ['pullConsumer'];
    }

    /**
     * The scenario of #177, end to end. The application's processIncoming() loop, which catches a handler's
     * exception and reads on, holds the socket when the operation starts, so the operation's read waits behind it.
     * The chunk the loop reads holds a message for "low", the lowest sid, whose handler throws, and then the
     * operation's message. The loop's read delivers that message before it throws, the operation's wake-up fires,
     * and the operation returns its result within 1.5 s of its 3 s deadline. It used to return at the deadline, or
     * with nothing: the loop's read stopped at the throwing handler, and the message stayed queued until a read
     * received bytes, which none did.
     */
    #[DataProvider('operationsBehindAHandlerThatThrows')]
    public function testAnOperationGetsItsResultTheApplicationsReadQueuedBehindAHandlerThatThrew(string $operation): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->client($watched, $recorder);
        // Subscribed first: the lowest sid, so its message is delivered ahead of the operation's.
        $lowSid = $client->subscribe('low', static function (): void {
            throw new \RuntimeException(self::POISON);
        })->await();
        $lead = ReconnectingTransport::msgFrame('low', $lowSid, 'l1');
        $server = new class {
            public int $pulls = 0;
        };
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport, $lead, $server): array {
            if ($replyTo === null) {
                return [];
            }

            $sid = (int) $transport->sidFor($replyTo);
            if ($subject === 'svc.echo') {
                return [$lead . ReconnectingTransport::msgFrame($replyTo, $sid, 'reply-1')];
            }

            if (str_starts_with($subject, '$JS.API.CONSUMER.MSG.NEXT.')) {
                // Only the first pull finds a message; the server holds later ones until they expire.
                return ++$server->pulls === 1
                    ? [$lead . ReconnectingTransport::msgFrame('evt.s', $sid, 'm1', self::ACK_SUBJECT)]
                    : [];
            }

            return $transport->replyFrame($replyTo, 'ok');
        };
        // Sets the reply inbox up before the application's read starts.
        $client->request('svc.warm', 'x', 1_000)->await();
        $queue = self::isPoll($operation) ? $client->subscribeQueue('jobs')->await() : null;
        $caught = [];
        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop, $caught);
        $this->awaitRead($watched);
        if ($queue !== null) {
            // The chunk arrives once the poll has started, and waits behind the loop's read.
            EventLoop::delay(0.02, static fn() => $transport->pushFrame($lead . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1')));
        }

        [$payloads, $elapsed, $error] = $this->runOperation($operation, $client, $queue);
        $stop->cancel();

        $summary = sprintf('%s returned %s after %.3f s%s', $operation, json_encode($payloads), $elapsed, $error === null ? '' : ', throwing ' . $error);
        self::assertSame([self::RESULTS[$operation]], $payloads, $summary);
        self::assertLessThan(self::PROMPT, $elapsed, $summary);
        self::assertSame([self::POISON], $caught, "the loop's read threw the handler's exception");
        self::assertSame([], $recorder->errorsContaining(self::POISON), 'thrown to the application, not reported');
    }

    /**
     * With handlerErrorsFailOperations, an operation's read that meets a poisoned handler still fails the operation
     * with its exception, as before that option existed, but delivers the messages behind it first: a service's
     * request read behind the poisoned message is answered by that read, and the request() fails with the handler's
     * exception, which is not reported on top. The read used to fail at once, leaving the service's request queued
     * for a later read.
     */
    public function testAnOperationsReadThatFailsOnAHandlerStillDeliversTheMessagesBehindIt(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->client($transport, $recorder, failOperations: true);
        $poison = $client->subscribe('poison', static function (): void {
            throw new \RuntimeException(self::POISON);
        })->await();
        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => 'run:' . $message->payload);
        $service->start()->await();
        $echoSid = $transport->sidFor('svc.echo');
        self::assertNotNull($echoSid);
        // The server answers the request with the poisoned message, then the service's request, then the reply.
        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $subject === 'backend.op' && $replyTo !== null
            ? [
                ReconnectingTransport::msgFrame('poison', $poison, 'p1')
                . ReconnectingTransport::msgFrame('svc.echo', $echoSid, 'hello', '_INBOX.req')
                . implode('', $transport->replyFrame($replyTo, 'pong')),
            ]
            : [];

        $thrown = null;
        try {
            $client->request('backend.op', 'q', 2_000)->await();
        } catch (\RuntimeException $e) {
            $thrown = $e;
        }

        self::assertNotNull($thrown, 'expected the request to fail with the handler failure');
        self::assertSame(self::POISON, $thrown->getMessage());
        self::assertCount(1, $transport->controlLinesStartingWith('PUB _INBOX.req '), "the service's request was answered by the read that failed");
        self::assertStringContainsString("run:hello\r\n", implode('', array_column($transport->writes, 'bytes')));
        self::assertSame([], $recorder->errorsContaining(self::POISON), 'thrown once, not reported on top');
    }

    /**
     * With handlerErrorsFailOperations a handler's CancelledException ends an operation's wait early, as it did
     * before that option existed, unless the operation's own result arrived in the same read behind it: the read
     * delivers the result first, and the operation completes with it, the CancelledException neither thrown nor
     * reported. The request used to time out with its reply queued behind the cancelling handler (#177).
     */
    public function testUnderFailOperationsARequestWhoseReplyArrivedBehindACancellingHandlerCompletesWithIt(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->client($transport, $recorder, failOperations: true);
        $cancelling = $client->subscribe('cancelling', static function (): void {
            throw new CancelledException();
        })->await();
        $transport->responder = static fn(string $subject, ?string $replyTo): array => $subject === 'svc.echo' && $replyTo !== null
            ? [ReconnectingTransport::msgFrame('cancelling', $cancelling, 'c1') . implode('', $transport->replyFrame($replyTo, 'reply-1'))]
            : [];

        $reply = $client->request('svc.echo', 'hi', 2_000)->await();

        self::assertSame('reply-1', $reply->payload, 'the reply, delivered behind the cancelling handler, completes the request');
        self::assertSame([], $recorder->errors, 'the CancelledException is neither thrown nor reported');
    }

    /**
     * With handlerErrorsFailOperations a SubscriptionQueue poll whose read meets a poisoned handler fails with its
     * exception, as before that option existed, although the poll's own message arrived behind it: that message is
     * delivered before the failure is thrown and waits in the queue's buffer for the next poll. It used to stay
     * queued at the connection, undelivered, until a read received bytes.
     */
    public function testUnderFailOperationsAPollFailsWithTheHandlersExceptionAndKeepsItsMessageForTheNextPoll(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->client($transport, $recorder, failOperations: true);
        $low = $client->subscribe('low', static function (): void {
            throw new \RuntimeException(self::POISON);
        })->await();
        $jobs = $client->subscribeQueue('jobs')->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('low', $low, 'l1') . ReconnectingTransport::msgFrame('jobs', $jobs->sid, 'j1'));

        $thrown = null;
        try {
            $jobs->setTimeout(1.0)->next();
        } catch (\RuntimeException $e) {
            $thrown = $e;
        }

        self::assertNotNull($thrown, 'expected the poll to fail with the handler failure');
        self::assertSame(self::POISON, $thrown->getMessage());
        $next = $jobs->setTimeout(0.2)->next();
        self::assertNotNull($next, 'the message delivered behind the poisoned one waits in the buffer for the next poll');
        self::assertSame('j1', $next->payload);
        self::assertSame([], $recorder->errors);
    }

    /**
     * With handlerErrorsFailOperations an operation's read stops a subscription at the handler that threw, as your own
     * read does, and your next read continues it (#186): a poll's read meets low:l1, whose handler throws, with low:l2
     * behind it, and fails with that exception; your next processIncoming() delivers low:l2 before it takes the
     * socket. It used to wait for a read that received anything.
     */
    public function testUnderFailOperationsYourNextReadContinuesWhatAnOperationsReadStopped(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->client($watched, $recorder, failOperations: true);
        $seen = [];
        $low = $client->subscribe('low', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'low:' . $message->payload;
            if ($message->payload === 'l1') {
                throw new \RuntimeException(self::POISON);
            }
        })->await();
        $jobs = $client->subscribeQueue('jobs')->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('low', $low, 'l1')
            . ReconnectingTransport::msgFrame('low', $low, 'l2')
            . ReconnectingTransport::msgFrame('jobs', $jobs->sid, 'j1'),
        );

        $thrown = null;
        try {
            $jobs->setTimeout(1.0)->next();
        } catch (\RuntimeException $e) {
            $thrown = $e;
        }

        self::assertNotNull($thrown, 'expected the poll to fail with the handler failure');
        self::assertSame(self::POISON, $thrown->getMessage());
        self::assertSame(['low:l1'], $seen, "the poll's read stopped low at its failing message");

        // What had been delivered when your next read took the socket.
        $whenReading = new class {
            /** @var list<string>|null */
            public ?array $seen = null;
        };
        $watched->onRead = static function () use ($whenReading, &$seen): void {
            $whenReading->seen ??= $seen;
        };
        $transport->pushFrame("PING\r\n");

        self::assertSame(1, $client->processIncoming()->await());
        self::assertSame(['low:l1', 'low:l2'], $whenReading->seen, 'your next read continued low before it took the socket');
        $next = $jobs->setTimeout(0.2)->next();
        self::assertNotNull($next, "the poll's own message waits in the buffer for the next poll");
        self::assertSame('j1', $next->payload);
        self::assertSame([], $recorder->errors);
    }

    /**
     * An operation's read in its default reporting mode whose own subscription's handler throws: that failure still
     * fails the operation, but it is held and thrown once the pass is over, so the later subscription's message
     * behind it is delivered first, and the rest of the operation's own subscription stays queued for the next
     * read. It used to be thrown at once, with the later message left queued.
     */
    public function testAHandlerFailureOfTheOperationsOwnSubscriptionIsThrownAfterTheOtherSubscriptionsAreDelivered(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connection($transport, $recorder);
        $seen = [];
        $own = $connection->subscribe('own', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'own:' . $message->payload;
            if ($message->payload === 'x') {
                throw new \RuntimeException('handler own on x');
            }
        })->await();
        $later = $connection->subscribe('later', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'later:' . $message->payload;
        })->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('own', $own, 'x')
            . ReconnectingTransport::msgFrame('own', $own, 'y')
            . ReconnectingTransport::msgFrame('later', $later, 'z'),
        );

        $thrown = null;
        try {
            $connection->readIncomingForOperation(new TimeoutCancellation(2), $own)->await();
        } catch (\RuntimeException $e) {
            $thrown = $e;
        }

        self::assertNotNull($thrown, "expected the own subscription's failure to fail the read");
        self::assertSame('handler own on x', $thrown->getMessage());
        self::assertSame(['own:x', 'later:z'], $seen, "the later subscription's message is delivered before the failure is thrown");
        self::assertSame([], $recorder->errors, 'thrown, not reported');

        $transport->pushFrame("PING\r\n");
        self::assertSame(1, $connection->processIncoming()->await());
        self::assertSame(['own:x', 'later:z', 'own:y'], $seen, 'the rest of the own subscription is continued by the next read');
    }

    /**
     * A CancelledException a handler throws is a failure like any other to your own read: thrown once the other
     * subscriptions' messages are delivered, and not reported.
     */
    public function testAHandlersCancelledExceptionIsThrownAfterTheOtherSubscriptionsAreDelivered(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connection($transport, $recorder);
        $seen = [];
        $a = $connection->subscribe('a', static function (): void {
            throw new CancelledException();
        })->await();
        $b = $connection->subscribe('b', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'b:' . $message->payload;
        })->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('a', $a, 'x') . ReconnectingTransport::msgFrame('b', $b, 'z'));

        $thrown = null;
        try {
            $connection->processIncoming()->await();
        } catch (\Throwable $e) {
            $thrown = $e;
        }

        self::assertInstanceOf(CancelledException::class, $thrown);
        self::assertSame(['b:z'], $seen, "b's message is delivered before the CancelledException is thrown");
        self::assertSame([], $recorder->errors);
    }

    /**
     * Two subscriptions with remainders after a read that threw: the next read continues both in sid order before it
     * reads, throws a's second failure, without reading, once it has delivered b's remainder behind it, and reports
     * nothing more; a third read has nothing left of them.
     */
    public function testTheNextReadContinuesBothRemaindersInSidOrderAndThrowsAgain(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connection($transport, $recorder);
        $seen = [];
        $a = $connection->subscribe('a', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'a:' . $message->payload;
            throw new \RuntimeException('handler a on ' . $message->payload);
        })->await();
        $b = $connection->subscribe('b', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'b:' . $message->payload;
            if ($message->payload === 'y') {
                throw new \RuntimeException('handler b on y');
            }
        })->await();
        $c = $connection->subscribe('c', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'c:' . $message->payload;
        })->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('a', $a, 'x')
            . ReconnectingTransport::msgFrame('a', $a, 'x2')
            . ReconnectingTransport::msgFrame('b', $b, 'y')
            . ReconnectingTransport::msgFrame('b', $b, 'y2')
            . ReconnectingTransport::msgFrame('c', $c, 'z'),
        );

        $this->assertReadThrows($connection, 'handler a on x');
        self::assertSame(['a:x', 'b:y', 'c:z'], $seen);
        self::assertSame(['handler b on y'], $recorder->errors);

        $transport->pushFrame("PING\r\n");
        $this->assertReadThrows($connection, 'handler a on x2');
        self::assertSame(['a:x', 'b:y', 'c:z', 'a:x2', 'b:y2'], $seen, "a before b, and b's remainder delivered although a's second message threw");
        self::assertSame(['handler b on y'], $recorder->errors, 'nothing more is reported');

        $transport->pushFrame("PING\r\n");
        self::assertSame(1, $connection->processIncoming()->await());
        self::assertSame(['a:x', 'b:y', 'c:z', 'a:x2', 'b:y2'], $seen, 'nothing is left of either remainder');
    }

    /**
     * Without an errorListener or a logger, the second subscription's failure in the same read is seen nowhere: its
     * message is consumed by the handler that threw, the first failure is the one thrown, and the next read has
     * nothing left of it to throw. Before #177 that read stopped at the first failure, and the next read with bytes
     * threw the second. Register an errorListener to see such failures.
     */
    public function testTheSecondFailingSubscriptionsExceptionIsSeenNowhereWithoutAnErrorListener(): void
    {
        $transport = new ReconnectingTransport();
        $connection = new NatsConnection(new NatsOptions(connectTimeoutMs: 500, pingIntervalSeconds: 0), $transport);
        $this->opened[] = $connection;
        $connection->connect()->await();
        $seen = [];
        $a = $connection->subscribe('a', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'a:' . $message->payload;
            throw new \RuntimeException('handler a');
        })->await();
        $b = $connection->subscribe('b', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'b:' . $message->payload;
            throw new \RuntimeException('handler b');
        })->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('a', $a, 'x') . ReconnectingTransport::msgFrame('b', $b, 'y'));

        $this->assertReadThrows($connection, 'handler a');
        self::assertSame(['a:x', 'b:y'], $seen, "b's handler ran and threw in the same read, consuming its message");

        // Nothing is left of b's failure to throw: the next read brings only the PING.
        $transport->pushFrame("PING\r\n");
        self::assertSame(1, $connection->processIncoming()->await());
        self::assertSame(['a:x', 'b:y'], $seen);
    }

    /**
     * Your own read under SlowConsumerPolicy::Error meets, in one pass, a SubscriptionQueue whose buffer is full, an
     * overflow it holds to throw, and a handler that throws: the handler's failure is thrown, the held overflow is
     * reported instead, and the message behind both is delivered. One read throws one exception and reports the
     * rest, the rule dispatchFrames() (#128) and the held overflows (#158) follow.
     */
    public function testAHeldOverflowIsReportedWhenYourOwnReadThrowsAHandlerFailure(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder);
        $backlog = $this->fullQueue($client, $transport);
        $seen = [];
        $a = $client->subscribe('a', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'a:' . $message->payload;
            throw new \RuntimeException('handler a');
        })->await();
        $b = $client->subscribe('b', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'b:' . $message->payload;
        })->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('backlog', $backlog->sid, 'b3')
            . ReconnectingTransport::msgFrame('a', $a, 'x')
            . ReconnectingTransport::msgFrame('b', $b, 'z'),
        );

        $thrown = null;
        try {
            $client->processIncoming()->await();
        } catch (\Throwable $e) {
            $thrown = $e;
        }

        self::assertInstanceOf(\RuntimeException::class, $thrown);
        self::assertSame('handler a', $thrown->getMessage(), 'the handler failure is thrown ahead of the held overflow');
        self::assertSame(['a:x', 'b:z'], $seen, "b's message is delivered");
        self::assertSame(['Subscription queue overflow for sid ' . $backlog->sid], $recorder->errorsContaining('overflow'), 'the held overflow is reported instead');
        self::assertSame(1, $backlog->droppedCount());
    }

    /**
     * Both options on, slowConsumerErrorsFailOperations and handlerErrorsFailOperations: a poll's read holds another
     * queue's overflow, to throw, and a poisoned handler's failure, with the poll's own message behind them. The
     * handler's failure is thrown, the overflow is reported, and the message is kept for the next poll.
     */
    public function testUnderBothFailOptionsTheHandlerFailureIsThrownAheadOfTheHeldOverflow(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder, overflowsFailOperations: true, failOperations: true);
        $backlog = $this->fullQueue($client, $transport);
        $low = $client->subscribe('low', static function (): void {
            throw new \RuntimeException(self::POISON);
        })->await();
        $jobs = $client->subscribeQueue('jobs')->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('backlog', $backlog->sid, 'b3')
            . ReconnectingTransport::msgFrame('low', $low, 'l1')
            . ReconnectingTransport::msgFrame('jobs', $jobs->sid, 'j1'),
        );

        $thrown = null;
        try {
            $jobs->setTimeout(1.0)->next();
        } catch (\Throwable $e) {
            $thrown = $e;
        }

        self::assertInstanceOf(\RuntimeException::class, $thrown);
        self::assertSame(self::POISON, $thrown->getMessage(), 'the handler failure is thrown ahead of the held overflow');
        self::assertSame(['Subscription queue overflow for sid ' . $backlog->sid], $recorder->errorsContaining('overflow'), 'the overflow is reported');
        self::assertSame([], $recorder->errorsContaining(self::POISON), 'the handler failure is thrown, not reported');
        $next = $jobs->setTimeout(0.2)->next();
        self::assertNotNull($next, "the poll's own message waits in the buffer for the next poll");
        self::assertSame('j1', $next->payload);
    }

    /**
     * A handler that reads again itself, a nested processIncoming(), and catches what that read throws. The nested
     * read leaves the subscription being dispatched alone, stops b at its failing message, delivers c and throws b's
     * failure into a's handler; the outer pass then goes on with a's next message, which the nested read queued, and
     * the outer read returns its own frame count. b's remainder waits for the next read, which continues it before it
     * reads.
     */
    public function testAHandlerThatReadsAgainAndCatchesTheNestedFailureLetsTheOuterPassGoOn(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connection($transport, $recorder);
        $seen = [];
        $caught = [];
        $sids = new class {
            public int $a = 0;
            public int $b = 0;
            public int $c = 0;
        };
        $sids->a = $connection->subscribe('a', static function (NatsMessage $message) use (&$seen, &$caught, $connection, $transport, $sids): void {
            $seen[] = 'a:' . $message->payload;
            if ($message->payload !== 'm1') {
                return;
            }

            $transport->pushFrame(
                ReconnectingTransport::msgFrame('a', $sids->a, 'm2')
                . ReconnectingTransport::msgFrame('b', $sids->b, 'x')
                . ReconnectingTransport::msgFrame('b', $sids->b, 'y')
                . ReconnectingTransport::msgFrame('c', $sids->c, 'z'),
            );
            try {
                $connection->processIncoming()->await();
            } catch (\RuntimeException $e) {
                $caught[] = $e->getMessage();
            }
        })->await();
        $sids->b = $connection->subscribe('b', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'b:' . $message->payload;
            if ($message->payload === 'x') {
                throw new \RuntimeException('handler b on x');
            }
        })->await();
        $sids->c = $connection->subscribe('c', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'c:' . $message->payload;
        })->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('a', $sids->a, 'm1'));

        self::assertSame(1, $connection->processIncoming()->await(), 'the outer read returns its own frame count');
        self::assertSame(['handler b on x'], $caught, "the nested read threw b's failure into a's handler");
        self::assertSame(['a:m1', 'b:x', 'c:z', 'a:m2'], $seen, 'the nested read stopped b at x and delivered c; the outer pass went on with m2');
        self::assertSame([], $recorder->errors);

        $transport->pushFrame("PING\r\n");
        self::assertSame(1, $connection->processIncoming()->await());
        self::assertSame(['a:m1', 'b:x', 'c:z', 'a:m2', 'b:y'], $seen, "b's remainder is continued by the next read");
    }

    /**
     * A subscription that a nested read stops while your read continues another is continued by the same pass, before
     * your read takes the socket (#186): your next read continues "a" with a2, whose handler reads again itself and
     * catches what that read throws. The nested read takes the socket, brings c1 and c2, stops "c" at c1 and throws
     * its failure into a2's handler. Once that handler returns, the outer pass goes on to c and delivers c2, and only
     * then does your read take the socket. Decided by the order of events: what had been delivered each time a read
     * took the socket. A pass over only the subscriptions stopped when it began left c2 queued, with your read
     * waiting on the socket for the server's next bytes; on 2.19.0 your read took the socket at once, with a2 queued.
     */
    public function testASubscriptionStoppedByANestedReadDuringThePassIsContinuedBeforeTheOuterReadTakesTheSocket(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->client($watched, $recorder);
        $seen = [];
        $caughtInHandler = [];
        $a = $client->subscribe('a', static function (NatsMessage $message) use (&$seen, &$caughtInHandler, $client): void {
            $seen[] = 'a:' . $message->payload;
            if ($message->payload === 'a1') {
                throw new \RuntimeException('handler a on a1');
            }

            // A guard against a hang only: c's messages are already on the socket.
            try {
                $client->processIncoming(new TimeoutCancellation(2))->await();
            } catch (\RuntimeException $e) {
                $caughtInHandler[] = $e->getMessage();
            }
        })->await();
        $c = $client->subscribe('c', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'c:' . $message->payload;
            if ($message->payload === 'c1') {
                throw new \RuntimeException('handler c on c1');
            }
        })->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('a', $a, 'a1') . ReconnectingTransport::msgFrame('a', $a, 'a2'));
        $this->assertReadThrows($client, 'handler a on a1');

        // c's messages wait on the socket for the nested read.
        $transport->pushFrame(ReconnectingTransport::msgFrame('c', $c, 'c1') . ReconnectingTransport::msgFrame('c', $c, 'c2'));
        // What had been delivered each time a read took the socket.
        $deliveredWhenReading = [];
        /** @var DeferredFuture<null> $secondSocketRead */
        $secondSocketRead = new DeferredFuture();
        $watched->onRead = static function () use (&$deliveredWhenReading, &$seen, $secondSocketRead): void {
            $deliveredWhenReading[] = $seen;
            if (count($deliveredWhenReading) === 2) {
                $secondSocketRead->complete();
            }
        };

        $outer = $client->processIncoming(new TimeoutCancellation(3));
        // Should an assertion below fail first, the read ends with its cancellation: no error of the next test.
        $outer->ignore();
        // A guard against a hang only: the second read of the socket follows the first at once on either code.
        $secondSocketRead->getFuture()->await(new TimeoutCancellation(2));
        $watched->onRead = null;

        self::assertSame(['a:a1', 'a:a2'], $deliveredWhenReading[0], "the nested read in a2's handler took the socket first");
        self::assertSame(['handler c on c1'], $caughtInHandler, "the nested read threw c1's failure into a2's handler");
        self::assertSame(['a:a1', 'a:a2', 'c:c1', 'c:c2'], $deliveredWhenReading[1], 'the outer pass continued c before your read took the socket');

        $transport->pushFrame("PING\r\n");
        self::assertSame(1, $outer->await(), 'your read then read the PING');
        self::assertSame([], $recorder->errors);
    }

    /**
     * The holding rule of the overflow branch: a handler reads again itself, a nested processIncoming() whose chunk
     * overflows another subscription's SubscriptionQueue (SlowConsumerPolicy::Error), and lets the overflow that read
     * throws escape. In the outer pass that overflow is the handler's own failure: held like any other, so the pass
     * goes on to the message behind it, the first held failure (a's) is thrown, and the overflow is reported once.
     */
    public function testAnOverflowOfAnotherSubscriptionEscapingAHandlersNestedReadIsHeldLikeAFailure(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder);
        $seen = [];
        $a = $client->subscribe('a', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'a:' . $message->payload;
            throw new \RuntimeException('handler a on ' . $message->payload);
        })->await();
        $c = $client->subscribe('c', static function (NatsMessage $message) use (&$seen, $client): void {
            $seen[] = 'c:' . $message->payload;
            // The nested read brings the next chunk, whose message overflows the full queue, and throws that overflow.
            $client->processIncoming()->await();
        })->await();
        $backlog = $this->fullQueue($client, $transport);
        $d = $client->subscribe('d', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'd:' . $message->payload;
        })->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('a', $a, 'x')
            . ReconnectingTransport::msgFrame('c', $c, 'z')
            . ReconnectingTransport::msgFrame('d', $d, 'w'),
        );
        $transport->pushFrame(ReconnectingTransport::msgFrame('backlog', $backlog->sid, 'b3'));

        $thrown = null;
        try {
            $client->processIncoming()->await();
        } catch (\Throwable $e) {
            $thrown = $e;
        }

        self::assertInstanceOf(\RuntimeException::class, $thrown);
        self::assertSame('handler a on x', $thrown->getMessage(), "a's failure, the first held, is thrown");
        self::assertSame(['a:x', 'c:z', 'd:w'], $seen, "d's message is delivered: the overflow that escaped c's handler did not end the pass");
        self::assertSame(['Subscription queue overflow for sid ' . $backlog->sid], $recorder->errors, "c's failure, the overflow, is reported once");
        self::assertSame(1, $backlog->droppedCount());
    }

    /**
     * The overflow held as a handler's failure stops that subscription like any other held failure, and your next read
     * continues it (#186): the staging above with a second message for "c", whose handler reads again only for its
     * first. The outer read throws a's failure with c:z2 queued, and your next read delivers c:z2 before it takes the
     * socket. It used to wait for a read that received anything.
     */
    public function testAnOverflowHeldAsAHandlersFailureLeavesARemainderYourNextReadContinues(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($watched, $recorder);
        $seen = [];
        $a = $client->subscribe('a', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'a:' . $message->payload;
            throw new \RuntimeException('handler a on ' . $message->payload);
        })->await();
        $c = $client->subscribe('c', static function (NatsMessage $message) use (&$seen, $client): void {
            $seen[] = 'c:' . $message->payload;
            if ($message->payload === 'z') {
                // The nested read brings the next chunk, whose message overflows the full queue, and throws that
                // overflow.
                $client->processIncoming()->await();
            }
        })->await();
        $backlog = $this->fullQueue($client, $transport);
        $d = $client->subscribe('d', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'd:' . $message->payload;
        })->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('a', $a, 'x')
            . ReconnectingTransport::msgFrame('c', $c, 'z')
            . ReconnectingTransport::msgFrame('c', $c, 'z2')
            . ReconnectingTransport::msgFrame('d', $d, 'w'),
        );
        $transport->pushFrame(ReconnectingTransport::msgFrame('backlog', $backlog->sid, 'b3'));

        $this->assertReadThrows($client, 'handler a on x');
        self::assertSame(['a:x', 'c:z', 'd:w'], $seen, "c's delivery stopped at the overflow that escaped its handler, with c:z2 queued");
        self::assertSame(['Subscription queue overflow for sid ' . $backlog->sid], $recorder->errors);

        // What had been delivered when your next read took the socket.
        $whenReading = new class {
            /** @var list<string>|null */
            public ?array $seen = null;
        };
        $watched->onRead = static function () use ($whenReading, &$seen): void {
            $whenReading->seen ??= $seen;
        };
        $transport->pushFrame("PING\r\n");

        self::assertSame(1, $client->processIncoming()->await());
        self::assertSame(['a:x', 'c:z', 'd:w', 'c:z2'], $whenReading->seen, 'your next read continued c before it took the socket');
        self::assertSame(['Subscription queue overflow for sid ' . $backlog->sid], $recorder->errors, 'nothing more is reported');
    }

    /**
     * An auto-unsubscribe (max 2) whose first message's handler throws: the held failure stops a's delivery for this
     * read with its second message queued, the next read delivers it and completes the auto-unsubscribe, and a third
     * message is not delivered.
     */
    public function testAnAutoUnsubscribeCompletesAcrossAHeldFailure(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connection($transport, $recorder);
        $seen = [];
        $a = $connection->subscribe('a', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'a:' . $message->payload;
            if ($message->payload === 'x') {
                throw new \RuntimeException('handler a');
            }
        })->await();
        $connection->unsubscribe($a, 2)->await();
        $b = $connection->subscribe('b', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'b:' . $message->payload;
        })->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('a', $a, 'x')
            . ReconnectingTransport::msgFrame('a', $a, 'y')
            . ReconnectingTransport::msgFrame('b', $b, 'z'),
        );

        $this->assertReadThrows($connection, 'handler a');
        self::assertSame(['a:x', 'b:z'], $seen);

        $transport->pushFrame("PING\r\n");
        self::assertSame(1, $connection->processIncoming()->await());
        self::assertSame(['a:x', 'b:z', 'a:y'], $seen, 'the second message completes the auto-unsubscribe');

        // A third message past the max, a replayed frame say, is not delivered.
        $transport->pushFrame(ReconnectingTransport::msgFrame('a', $a, 'w') . ReconnectingTransport::msgFrame('b', $b, 'w'));
        $connection->processIncoming()->await();
        self::assertSame(['a:x', 'b:z', 'a:y', 'b:w'], $seen, 'a is gone, b still delivers');
        self::assertSame([], $recorder->errorsContaining('handler'));
    }

    /**
     * The dispatch-error path of readChunk(): an -ERR the server keeps the connection open for in the same chunk as
     * a throwing handler and another subscription's message. The -ERR is thrown (#158), the held handler failure is
     * reported behind it, b's message is delivered, and the connection stays Open.
     */
    public function testANonClosingErrInTheSameChunkIsThrownAndTheHeldHandlerFailureReported(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connection($transport, $recorder);
        $seen = [];
        $a = $connection->subscribe('a', static function (): void {
            throw new \RuntimeException('handler a');
        })->await();
        $b = $connection->subscribe('b', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'b:' . $message->payload;
        })->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('a', $a, 'x')
            . ReconnectingTransport::msgFrame('b', $b, 'z')
            . "-ERR 'maximum subscriptions exceeded'\r\n",
        );

        $thrown = null;
        try {
            $connection->processIncoming()->await();
        } catch (\Throwable $e) {
            $thrown = $e;
        }

        self::assertNotNull($thrown, 'expected the -ERR');
        self::assertStringContainsString('maximum subscriptions exceeded', $thrown->getMessage(), 'the -ERR is thrown');
        self::assertSame(['b:z'], $seen, "b's message is delivered");
        self::assertSame(['handler a'], $recorder->errorsContaining('handler a'), 'the held handler failure is reported behind the -ERR');
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * The parse-error path of readChunk(): a line that does not parse behind a throwing handler and another
     * subscription's message. The frames ahead of it are delivered, b's message included, before the corrupt stream
     * is reported and recovered with a reconnect; the held failure is then thrown, not reported. Before #177 the
     * delivery stopped at a's handler, and b's message waited for the delivery after the reconnect (#173).
     */
    public function testACorruptLineBehindAThrowingHandlerIsRecoveredAndTheHeldFailureThrown(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, errorListener: $recorder->errorListener());
        $seen = [];
        $a = $connection->subscribe('a', static function (): void {
            throw new \RuntimeException('handler a');
        })->await();
        $b = $connection->subscribe('b', static function (NatsMessage $message) use (&$seen, $transport): void {
            // Which dial the connection is on when the message arrives: the first, or the recovery's.
            $seen[] = 'b:' . $message->payload . ' on dial ' . count($transport->connectCalls);
        })->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('a', $a, 'x')
            . ReconnectingTransport::msgFrame('b', $b, 'z')
            . "BOGUS LINE\r\n",
        );

        $this->assertReadThrows($connection, 'handler a');

        self::assertSame(['b:z on dial 1'], $seen, "b's message, read ahead of the corrupt line, is delivered before the stream is recovered");
        self::assertCount(2, $transport->connectCalls, 'the corrupt stream was recovered with a reconnect');
        self::assertCount(1, $recorder->errors, 'the corrupt stream was reported, once');
        self::assertSame([], $recorder->errorsContaining('handler a'), 'the held failure is thrown, not reported');
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * The mark that has your next read continue a stopped subscription (#186), read by reflection: set by either
     * branch that holds a handler's failure, the one for a failure and the one for another subscription's overflow
     * that escaped the handler, as a nested read's does; gone once the remainder is delivered, once the subscription
     * is unsubscribed, and once the connection is closed. A stale mark would cost your next read only an empty pass,
     * which nothing else shows.
     */
    public function testTheStoppedMarkGoesWithTheRemainder(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connection($transport, $recorder);
        $failingOnX = static function (NatsMessage $message): void {
            if ($message->payload === 'x') {
                throw new \RuntimeException('handler on x');
            }
        };
        $a = $connection->subscribe('a', $failingOnX)->await();
        $b = $connection->subscribe('b', static function (NatsMessage $message): void {
            if ($message->payload === 'x') {
                throw new SlowConsumerException(999);
            }
        })->await();
        $c = $connection->subscribe('c', $failingOnX)->await();
        self::assertSame([], $this->stoppedMarks($connection));

        // a stops at x, and the next read delivers its remainder.
        $transport->pushFrame(ReconnectingTransport::msgFrame('a', $a, 'x') . ReconnectingTransport::msgFrame('a', $a, 'y'));
        $this->assertReadThrows($connection, 'handler on x');
        self::assertSame([$a => true], $this->stoppedMarks($connection), 'marked while its remainder is queued');
        $transport->pushFrame("PING\r\n");
        self::assertSame(1, $connection->processIncoming()->await());
        self::assertSame([], $this->stoppedMarks($connection), 'gone with the delivered remainder');

        // b stops at the overflow its handler let escape, and is unsubscribed with its remainder queued.
        $transport->pushFrame(ReconnectingTransport::msgFrame('b', $b, 'x') . ReconnectingTransport::msgFrame('b', $b, 'y'));
        $this->assertReadThrows($connection, 'Subscription queue overflow for sid 999');
        self::assertSame([$b => true], $this->stoppedMarks($connection), 'marked by the overflow branch as well');
        $connection->unsubscribe($b)->await();
        self::assertSame([], $this->stoppedMarks($connection), 'gone with the subscription');

        // c stops at x, and the connection is closed with its remainder queued.
        $transport->pushFrame(ReconnectingTransport::msgFrame('c', $c, 'x') . ReconnectingTransport::msgFrame('c', $c, 'y'));
        $this->assertReadThrows($connection, 'handler on x');
        self::assertSame([$c => true], $this->stoppedMarks($connection));
        $connection->disconnect()->await();
        self::assertSame([], $this->stoppedMarks($connection), 'gone with the closed connection');
        self::assertSame([], $recorder->errors);
    }

    private function assertReadThrows(NatsConnection|NatsClient $connection, string $message): void
    {
        $thrown = null;
        try {
            $connection->processIncoming()->await();
        } catch (\RuntimeException $e) {
            $thrown = $e;
        }

        self::assertNotNull($thrown, 'expected the handler failure ' . $message);
        self::assertSame($message, $thrown->getMessage());
    }

    /**
     * The subscriptions marked as stopped by a handler failure, read by reflection
     * ({@see NatsConnection::$stoppedByAFailure}).
     */
    private function stoppedMarks(NatsConnection $connection): mixed
    {
        return (new \ReflectionProperty(NatsConnection::class, 'stoppedByAFailure'))->getValue($connection);
    }

    private function connection(ReconnectingTransport $transport, LifecycleRecorder $recorder, ?LoggerInterface $logger = null): NatsConnection
    {
        $connection = new NatsConnection(new NatsOptions(
            connectTimeoutMs: 500,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
            logger: $logger,
        ), $transport);
        $this->opened[] = $connection;
        $connection->connect()->await();

        return $connection;
    }

    private function client(TransportInterface $transport, LifecycleRecorder $recorder, bool $failOperations = false): NatsClient
    {
        $client = new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: 2_000,
            reconnectEnabled: true,
            maxReconnectAttempts: 1_000,
            reconnectDelayMs: 5,
            reconnectMaxDelayMs: 20,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
            handlerErrorsFailOperations: $failOperations,
        ), $transport);
        $this->opened[] = $client;
        $client->connect()->await();

        return $client;
    }

    /** A client whose subscription queues hold two messages and fail on the third (SlowConsumerPolicy::Error). */
    private function errorPolicyClient(
        TransportInterface $transport,
        LifecycleRecorder $recorder,
        bool $overflowsFailOperations = false,
        bool $failOperations = false,
    ): NatsClient {
        $client = new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: 2_000,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
            maxPendingMessagesPerSubscription: 2,
            slowConsumerPolicy: SlowConsumerPolicy::Error,
            slowConsumerErrorsFailOperations: $overflowsFailOperations,
            handlerErrorsFailOperations: $failOperations,
        ), $transport);
        $this->opened[] = $client;
        $client->connect()->await();

        return $client;
    }

    /** A SubscriptionQueue for 'backlog' whose buffer of two is full: its next message overflows it. */
    private function fullQueue(NatsClient $client, ReconnectingTransport $transport): SubscriptionQueue
    {
        $queue = $client->subscribeQueue('backlog')->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('backlog', $queue->sid, 'b1')
            . ReconnectingTransport::msgFrame('backlog', $queue->sid, 'b2'),
        );
        $client->processIncoming()->await();

        return $queue;
    }

    /**
     * Starts the application's own read loop, processIncoming() over and over on a fiber of its own, until $stop is
     * cancelled: the loop of #177, which catches a handler's exception, records its message in $caught, and reads on.
     *
     * @param list<string> $caught
     */
    private function startApplicationReadLoop(NatsClient $client, DeferredCancellation $stop, array &$caught): void
    {
        async(static function () use ($client, $stop, &$caught): void {
            while (!$stop->isCancelled()) {
                try {
                    $client->processIncoming($stop->getCancellation())->await();
                } catch (CancelledException) {
                    return;
                } catch (\Throwable $e) {
                    $caught[] = $e->getMessage();
                }
            }
        })->ignore();
    }

    /** Waits until a read takes the socket of $watched. */
    private function awaitRead(WatchedTransport $watched): void
    {
        /** @var DeferredFuture<null> $reading */
        $reading = new DeferredFuture();
        $watched->onRead = static function () use ($reading): void {
            if (!$reading->isComplete()) {
                $reading->complete();
            }
        };
        $reading->getFuture()->await(new TimeoutCancellation(2));
        $watched->onRead = null;
    }

    /**
     * Runs $operation to its end, waiting a second past its deadline for it to give up.
     *
     * @return array{list<string>, float, ?string} The payloads it got, the seconds it took, and what it threw.
     */
    private function runOperation(string $operation, NatsClient $client, ?SubscriptionQueue $queue): array
    {
        $start = hrtime(true);
        $future = $this->startOperation($operation, $client, $queue);
        $error = null;
        try {
            $payloads = $future->await(new TimeoutCancellation(self::TIMEOUT + 1.0));
        } catch (\Throwable $e) {
            // An operation that never gives up on its own (the pull consumer re-pulls at each expiry) is left to the
            // close in tearDown().
            $future->ignore();
            $payloads = [];
            $error = $e::class . ': ' . $e->getMessage();
        }

        return [$payloads, $this->secondsSince($start), $error];
    }

    /**
     * Starts $operation with a deadline of {@see TIMEOUT} seconds: the JetStream operations wait for their expiry
     * plus one second, so they expire a second earlier. The pull consumer pulls one message at a time, one pull in
     * flight, and stops at the first message.
     *
     * @return Future<list<string>> The payloads the operation got.
     */
    private function startOperation(string $operation, NatsClient $client, ?SubscriptionQueue $queue): Future
    {
        $ms = (int) round(self::TIMEOUT * 1000);

        return match ($operation) {
            'next' => async(static function () use ($queue): array {
                $message = self::queueOf($queue)->setTimeout(self::TIMEOUT)->next();

                return $message === null ? [] : [$message->payload];
            }),
            'fetchAll' => async(static fn(): array => self::payloads(self::queueOf($queue)->setTimeout(self::TIMEOUT)->fetchAll(1))),
            'request' => $client->request('svc.echo', 'hi', $ms)->map(static fn(NatsMessage $message): array => [$message->payload]),
            'requestMany' => $client->requestMany('svc.echo', 'hi', null, 1, $ms)->map(self::payloads(...)),
            'fetchBatch' => $client->jetStream()->fetchBatch('S', 'C', 1, $ms - 1_000)->map(self::payloads(...)),
            'pullConsumer' => async(static function () use ($client, $ms): array {
                $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs($ms - 1_000);
                $handled = new class {
                    /** @var list<string> */
                    public array $payloads = [];
                };
                $iterator->handle(static function (NatsMessage $message) use ($iterator, $handled): void {
                    $handled->payloads[] = $message->payload;
                    $iterator->stop();
                })->await();

                return $handled->payloads;
            }),
            default => throw new \LogicException('Unknown operation ' . $operation),
        };
    }

    private static function isPoll(string $operation): bool
    {
        return $operation === 'next' || $operation === 'fetchAll';
    }

    private static function queueOf(?SubscriptionQueue $queue): SubscriptionQueue
    {
        self::assertNotNull($queue);

        return $queue;
    }

    /**
     * @param list<NatsMessage> $messages
     * @return list<string>
     */
    private static function payloads(array $messages): array
    {
        return array_map(static fn(NatsMessage $message): string => $message->payload, $messages);
    }
}
