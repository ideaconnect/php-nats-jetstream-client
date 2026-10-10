<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\IncomingChunkResult;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Protocol\ServerInfo;
use IDCT\NATS\Tests\Support\CountingCancellation;
use IDCT\NATS\Tests\Support\HeldUpDelivery;
use IDCT\NATS\Tests\Support\KeepsEventLoopAlive;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ListenerAction;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;

/**
 * A lame-duck failover whose INFO the read of one of the library's operations brings (#191): a request() or
 * requestMany() waiting for its reply, a SubscriptionQueue poll, a JetStream fetch or pull consumer, Key/Value keys(),
 * a flush() or rtt() waiting for its PONG. The failover (#47) ran inline in the read that dispatched the INFO, so such
 * an operation waited for the whole failover, its dials, handshake, subscription replay and any backoff, before it
 * could meet its deadline. The read now starts the failover in a fiber of its own and waits for it only within the
 * operation's own wait, as it does for a reconnect its failed read starts (#178): the operation times out at its
 * deadline, or returns what a delivery brings meanwhile, and with waiting for a reconnect disabled fails at once, while
 * the failover carries on. The rest of the chunk is the leaving server's from the moment the failover is started
 * (#182), though its PONG still answers the leaving connection's PING. Your own processIncoming() still runs the
 * failover inline.
 *
 * Over the scripted server in tests/Support/ReconnectingTransport.php, whose every dial opens a new session (epoch 0,
 * then 1 after the failover), with the lame-duck INFO naming the second server in connect_urls, so that the pool grows
 * to two and the failover dials it; a delivery is held up by tests/Support/HeldUpDelivery.php. The failover's dial is
 * held until the operation has returned, so that the outcome is decided by the order of events: the session the
 * connection was on (0, the first server's) and its state (Connecting, the failover under way) when the operation
 * returned. A time-out only guards against a hang: the code before the fix did not return while the dial was held.
 * Each regression test fails on that code (how, in its docblock); the guards pass on both.
 */
final class OperationReadLameDuckFailoverTest extends TestCase
{
    use KeepsEventLoopAlive;
    use ReconnectScenarios;

    /** The first server's lame-duck INFO, naming the second server: the pool grows to two and the connection fails over. */
    private const LAME_DUCK = 'INFO {"server_id":"S1","version":"2.12.0","max_payload":1048576,"ldm":true,"connect_urls":["10.0.0.2:4222"]}' . "\r\n";

    /** The second server's own lame-duck INFO, naming a third. */
    private const SECOND_LAME_DUCK = 'INFO {"server_id":"S2","version":"2.12.0","max_payload":1048576,"ldm":true,"connect_urls":["10.0.0.3:4222"]}' . "\r\n";

    /** An INFO of the first server behind its lame-duck INFO: no lame-duck flag, and a server id of its own. */
    private const STALE_INFO = 'INFO {"server_id":"S1-leaving","version":"2.12.0","max_payload":1048576}' . "\r\n";

    private const PING = "PING\r\n";

    private const PONG = "PONG\r\n";

    private const LIMIT_ERR = "-ERR 'maximum subscriptions exceeded'\r\n";

    /** The second server, as the client dials the one the lame-duck INFO names. */
    private const SECOND_SERVER = 'tcp://10.0.0.2:4222';

    private const PULL_SUBJECT = '$JS.API.CONSUMER.MSG.NEXT.S.C';

    private const ACK_SUBJECT = '$JS.ACK.S.C.1.1.1.0.0';

    protected function setUp(): void
    {
        $this->keepEventLoopAlive();
    }

    protected function tearDown(): void
    {
        $this->letEventLoopEnd();
        $this->closeOpenedConnections();
    }

    /**
     * A request() with a 300 ms timeout whose own read brings [INFO ldm | PING]: the request times out at its own
     * deadline while the failover's dial is held, on the first session with the connection Connecting, and the
     * failover completes on the second server once the dial goes through. The leaving server's PING is answered on
     * neither connection. The request used to wait for the whole failover inside its read: it did not return while
     * the dial was held.
     */
    public function testARequestWhoseOwnReadBringsTheLameDuckInfoTimesOutAtItsDeadlineWhileTheFailoverDials(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = self::answering($transport, static fn(): string => self::LAME_DUCK . self::PING);
        $connection = $this->connect($transport);
        // Sets the reply inbox up: the request below writes no SUB of its own, and its read is the one on the socket.
        self::assertSame('ok', $connection->request('svc.warm', 'x', 1_000)->await()->payload);
        $transport->holdNextDial();

        $outcome = $this->settle($connection->request('svc.echo', 'y', 300), $transport, $connection);

        self::assertReturnedWhileTheFailoverDialled($outcome);
        self::assertInstanceOf(TimeoutException::class, $outcome['thrown'], 'the request timed out on its own');
        self::assertSame('Request timed out for subject svc.echo', $outcome['thrown']->getMessage());
        self::assertFailedOverToTheSecondServer($connection, $transport);
        self::assertSame([], $transport->controlLinesStartingWith('PONG'), 'the leaving server\'s PING was answered on neither connection');
    }

    /** @return iterable<string, array{string}> */
    public static function otherOperations(): iterable
    {
        yield 'requestMany(max 1), 0.3 s' => ['requestMany'];
        yield 'SubscriptionQueue::next(), timeout 0.3 s' => ['next'];
        yield 'JetStreamContext::fetchBatch(1), expiry 0.3 s' => ['fetchBatch'];
        yield 'PullConsumerIterator::handle(), one iteration, expiry 0.3 s' => ['pullConsumer'];
        yield 'KeyValueBucket::keys(), 0.3 s' => ['keys'];
    }

    /**
     * The other operations that read for a result of their own, each with a deadline or an expiry of 0.3 s, whose own
     * read brings [INFO ldm | PING] where the result would have come: each ends at its deadline as it does when nothing
     * comes, requestMany() with nothing collected, the poll with nothing, the fetch with its 408, the pull consumer's
     * one iteration with nothing handled, keys() with its stall, on the first session with the connection Connecting,
     * and the failover then completes on the second server. Each used to wait for the whole failover inside its read,
     * and did not return while the dial was held. The pull consumer that runs on re-issues its pull on the second
     * server: {@see testAPullConsumerWhoseReadBringsTheLameDuckInfoReissuesItsPullOnTheSecondServer()}.
     */
    #[DataProvider('otherOperations')]
    public function testAnOperationWhoseOwnReadBringsTheLameDuckInfoEndsAtItsDeadlineWhileTheFailoverDials(string $operation): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->clientOver($transport);
        $start = self::scriptOperation($operation, $client, $transport);
        $transport->holdNextDial();

        $outcome = $this->settle($start(), $transport, $client);

        self::assertReturnedWhileTheFailoverDialled($outcome);
        $thrown = $outcome['thrown'];
        switch ($operation) {
            case 'fetchBatch':
                self::assertInstanceOf(JetStreamException::class, $thrown);
                self::assertSame(408, $thrown->getCode(), 'an empty pull: ' . $thrown->getMessage());
                break;
            case 'keys':
                self::assertInstanceOf(JetStreamException::class, $thrown);
                self::assertStringStartsWith('Key enumeration stalled', $thrown->getMessage());
                break;
            default:
                self::assertNull($thrown, sprintf('%s threw %s', $operation, $thrown?->getMessage() ?? ''));
                self::assertSame(match ($operation) {
                    'requestMany' => [],
                    'next' => null,
                    default => 0,
                }, $outcome['result']);
        }
        self::assertFailedOverToTheSecondServer($client, $transport);
        self::assertSame([], $transport->controlLinesStartingWith('PONG'), 'the leaving server\'s PING was answered on neither connection');
    }

    /**
     * The request's reply comes in the same chunk, behind the lame-duck INFO: [INFO ldm | reply]. The request returns it
     * while the failover's dial is held, on the first session with the connection Connecting. It used to return it only
     * once the failover was over, the reply waiting behind it in the read.
     */
    public function testAReplyBehindTheLameDuckInfoIsReturnedWhileTheFailoverDials(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = self::answering($transport, static fn(string $replyTo): string => self::LAME_DUCK . self::reply($transport, $replyTo));
        $connection = $this->connect($transport);
        $connection->request('svc.warm', 'x', 1_000)->await();
        $transport->holdNextDial();

        $outcome = $this->settle($connection->request('svc.echo', 'y', 2_000), $transport, $connection);

        self::assertReturnedWhileTheFailoverDialled($outcome);
        self::assertNull($outcome['thrown'], 'the request got its reply');
        self::assertSame('reply-1', $outcome['result']?->payload);
        self::assertFailedOverToTheSecondServer($connection, $transport);
    }

    /**
     * A poll's message comes in a delivery still under way in another fiber while the poll's read waits for the
     * failover its chunk started. The poll's own SubscriptionQueue holds one message, so that the application's read
     * overflows it: the drop report calls an error listener that awaits, which holds that delivery up after the oldest
     * message was taken out and before the new one is put in, so the poll finds nothing of its own to take (#179) and
     * reads the socket. Its read brings the lame-duck INFO, and once the failover's dial is held the test lets the
     * delivery go on: the poll returns the message, its wake-up having ended the wait, on the first session with the
     * connection Connecting. It used to wait for the whole failover inside its read, which the wake-up could not end.
     */
    public function testADeliveryDuringTheFailoverBringsTheOperationItsResult(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $hold = new HeldUpDelivery(fallbackSeconds: 5.0);
        $client = $this->clientOver($watched, [
            'maxPendingMessagesPerSubscription' => 1,
            'errorListener' => static function (\Throwable $error) use ($hold): void {
                if (str_contains($error->getMessage(), 'dropped oldest')) {
                    $hold->holdUp();
                }
            },
        ]);
        $queue = $client->subscribeQueue('jobs')->await();
        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop);
        $this->awaitRead($watched);
        // job-0 fills the queue's buffer, which nobody polls yet; the application's read then waits on the socket again.
        $readsBefore = $watched->reads;
        $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-0'));
        $this->waitUntil(static fn(): bool => $watched->reads > $readsBefore);
        // job-1 overflows it: the drop report's listener holds the delivery up, with the buffer empty.
        $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
        $hold->began->getFuture()->await(new TimeoutCancellation(2));
        $transport->holdNextDial();

        $poll = async(static fn(): ?NatsMessage => $queue->setTimeout(3.0)->next());
        // The application's read is held up in the listener, so the read that takes the socket is the poll's.
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->pushFrame(self::LAME_DUCK);
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
        $hold->end('the test');
        $outcome = $this->settle($poll, $transport, $client);
        $stop->cancel();

        self::assertSame('the test', $hold->endedBy);
        self::assertReturnedWhileTheFailoverDialled($outcome);
        self::assertNull($outcome['thrown']);
        self::assertSame('job-1', $outcome['result']?->payload, 'the delivery brought the poll its message');
        self::assertFailedOverToTheSecondServer($client, $transport);
    }

    /** @return iterable<string, array{bool}> */
    public static function chunksWithWaitingDisabled(): iterable
    {
        yield 'the lame-duck INFO alone: the request fails at once' => [false];
        yield 'its reply behind the INFO: the request returns it' => [true];
    }

    /**
     * With waiting for a reconnect disabled, a request whose own read brings the lame-duck INFO fails at once with
     * "Connection is not open", as every operation does while a reconnect is in flight, without a cause (a lame duck
     * is not a failure), before the failover has even dialled the second server; a reply that came in the same chunk
     * is returned instead: the wake-up still wins. Either way on the first session with the connection Connecting.
     * Both used to wait for the whole failover inside the read.
     */
    #[DataProvider('chunksWithWaitingDisabled')]
    public function testWithWaitingDisabledAnOperationWhoseReadBringsTheLameDuckInfoFailsAtOnce(bool $replyBehind): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = self::answering(
            $transport,
            static fn(string $replyTo): string => self::LAME_DUCK . ($replyBehind ? self::reply($transport, $replyTo) : ''),
        );
        $connection = $this->connect($transport, waitForReconnect: false);
        $connection->request('svc.warm', 'x', 1_000)->await();
        $transport->holdNextDial();

        $outcome = $this->settle($connection->request('svc.echo', 'y', 2_000), $transport, $connection);

        self::assertReturnedWhileTheFailoverDialled($outcome);
        if ($replyBehind) {
            self::assertNull($outcome['thrown'], 'the request got its reply');
            self::assertSame('reply-1', $outcome['result']?->payload);
        } else {
            self::assertInstanceOf(ConnectionException::class, $outcome['thrown']);
            self::assertSame('Connection is not open', $outcome['thrown']->getMessage());
            self::assertNull($outcome['thrown']->getPrevious(), 'a lame duck is not a failure: no cause');
        }
        self::assertFailedOverToTheSecondServer($connection, $transport);
    }

    /** @return iterable<string, array{list<string>}> */
    public static function partialCollections(): iterable
    {
        yield 'two replies, then the INFO: the two are returned' => [['r-1', 'r-2']];
        yield 'the INFO alone: the collection fails at once' => [[]];
    }

    /**
     * With waiting for a reconnect disabled, a requestMany() for three replies whose read brings the lame-duck INFO,
     * alone as a server sends it, after two replies each in a read of its own, returns the two while the failover's
     * dial is held, on the first session with the connection Connecting, as it returns what it collected when the
     * connection closes; with nothing collected it fails at once with "Connection is not open". Thrown out of the
     * collection, the read's failure would lose the two replies; the code before #191 ran the failover inline and did
     * not return while the dial was held. The caller's cancellation counts the collection's looks: it returns at once,
     * rather than look again until its total timeout, 30 s away.
     *
     * @param list<string> $replies
     */
    #[DataProvider('partialCollections')]
    public function testWithWaitingDisabledARequestManyWhoseReadBringsTheLameDuckInfoReturnsWhatItCollected(array $replies): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport, $replies): array {
            if ($replyTo === null) {
                return [];
            }

            if ($subject === 'svc.warm') {
                return $transport->replyFrame($replyTo, 'ok');
            }

            // A read for each reply, then one for the lame-duck INFO.
            $frames = [];
            foreach ($replies as $reply) {
                $frames = [...$frames, ...$transport->replyFrame($replyTo, $reply)];
            }
            $frames[] = self::LAME_DUCK;

            return $frames;
        };
        $connection = $this->connect($transport, waitForReconnect: false);
        $connection->request('svc.warm', 'x', 1_000)->await();
        $transport->holdNextDial();

        $looks = new CountingCancellation();
        $outcome = $this->settle($connection->requestMany('svc.many', 'y', maxResponses: 3, totalTimeoutMs: 30_000, cancellation: $looks), $transport, $connection, static fn(): int => $looks->isRequestedCalls);

        self::assertReturnedWhileTheFailoverDialled($outcome);
        self::assertLessThan(20, $outcome['noted'], 'a look per read');
        if ($replies === []) {
            self::assertInstanceOf(ConnectionException::class, $outcome['thrown']);
            self::assertSame('Connection is not open', $outcome['thrown']->getMessage());
        } else {
            self::assertNull($outcome['thrown'], $outcome['thrown'] === null ? '' : $outcome['thrown']::class . ': ' . $outcome['thrown']->getMessage());
            self::assertSame($replies, array_map(static fn(NatsMessage $message): string => $message->payload, $outcome['result'] ?? []));
        }
        self::assertFailedOverToTheSecondServer($connection, $transport);
    }

    /** @return iterable<string, array{list<string>}> */
    public static function partialBatches(): iterable
    {
        yield 'two messages, then the INFO: the two are returned' => [['m-1', 'm-2']];
        yield 'the INFO alone: the fetch fails at once' => [[]];
    }

    /**
     * With waiting for a reconnect disabled, a fetchBatch() of three whose read brings the lame-duck INFO, alone as a
     * server sends it, after two messages each in a read of its own, returns the two while the failover's dial is held,
     * on the first session with the connection Connecting, as it returns a partial batch when the heartbeats stop; with
     * nothing received it fails at once with "Connection is not open". Thrown out of the fetch, the read's failure
     * would lose the two messages: redelivered after the ack wait on an explicit-ack consumer, gone for good on one
     * without acks. The code before #191 ran the failover inline and did not return while the dial was held.
     *
     * @param list<string> $received
     */
    #[DataProvider('partialBatches')]
    public function testWithWaitingDisabledAFetchWhoseReadBringsTheLameDuckInfoReturnsThePartialBatch(array $received): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->clientOver($transport, ['waitForReconnect' => false]);
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport, $received): array {
            $sid = $replyTo === null ? null : $transport->sidFor($replyTo);
            if ($subject !== self::PULL_SUBJECT || $sid === null) {
                return [];
            }

            // A read for each message, then one for the lame-duck INFO.
            $frames = array_map(static fn(string $payload): string => ReconnectingTransport::msgFrame('evt.s', $sid, $payload, self::ACK_SUBJECT), $received);
            $frames[] = self::LAME_DUCK;

            return $frames;
        };
        $transport->holdNextDial();

        // Counts the event loop's microtask rounds until the fetch returns, up to a cap: the fetch takes a few, and one
        // that looked again at once after each failed read, without a pause, would take them all, its pause a timer
        // that never ran while it looked. The count follows from the order in which the loop runs callbacks.
        $fetch = $client->jetStream()->fetchBatch('S', 'C', 3, 30_000);
        $rounds = new class {
            public int $count = 0;
        };
        $count = static function () use (&$count, $rounds, $fetch): void {
            if (!$fetch->isComplete() && ++$rounds->count < 20_000) {
                EventLoop::queue($count);
            }
        };
        EventLoop::queue($count);
        $outcome = $this->settle($fetch, $transport, $client, static fn(): int => $rounds->count);

        self::assertReturnedWhileTheFailoverDialled($outcome);
        self::assertLessThan(1_000, $outcome['noted'], 'microtask rounds until the fetch returned');
        if ($received === []) {
            self::assertInstanceOf(ConnectionException::class, $outcome['thrown']);
            self::assertSame('Connection is not open', $outcome['thrown']->getMessage());
        } else {
            self::assertNull($outcome['thrown'], $outcome['thrown'] === null ? '' : $outcome['thrown']::class . ': ' . $outcome['thrown']->getMessage());
            self::assertSame($received, array_map(static fn(NatsMessage $message): string => $message->payload, $outcome['result'] ?? []));
        }
        self::assertFailedOverToTheSecondServer($client, $transport);
    }

    /** @return iterable<string, array{string}> */
    public static function flushes(): iterable
    {
        yield 'flush()' => ['flush'];
        yield 'rtt()' => ['rtt'];
    }

    /**
     * A flush whose PING is out, and whose read brings the lame-duck INFO, its PONG never to come: the failover's first
     * attempt ends the leaving connection's PONGs, which is the flush's wake-up, and the flush fails with "Connection
     * lost before the server answered the PING" while the failover's dial is held, on the first session with the
     * connection Connecting. It used to fail with the same error only once the failover was over.
     */
    #[DataProvider('flushes')]
    public function testAFlushWhoseReadBringsTheLameDuckInfoFailsAsSoonAsItsPongIsLost(string $flush): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        self::answerTheFlushPingWith($transport, self::LAME_DUCK);
        $transport->holdNextDial();

        $outcome = $this->settle(self::startFlush($connection, $flush), $transport, $connection);

        self::assertReturnedWhileTheFailoverDialled($outcome);
        self::assertInstanceOf(ConnectionException::class, $outcome['thrown']);
        self::assertSame('Connection lost before the server answered the PING', $outcome['thrown']->getMessage());
        self::assertFailedOverToTheSecondServer($connection, $transport);
    }

    /**
     * The flush's PONG comes behind the lame-duck INFO: [INFO ldm | PONG]. The PONG answers the leaving connection's
     * PING, whose slot is still that connection's until the failover's first attempt, so the flush succeeds, while the
     * failover's dial is held, on the first session with the connection Connecting. It used to succeed only once the
     * failover was over. The PONG of a connection already replaced completes nothing on the new one:
     * LameDuckChunkRemainderTest (testAStalePongBehindTheLameDuckInfoDoesNotConfirmTheReplayedReplyInbox).
     */
    #[DataProvider('flushes')]
    public function testAFlushWhosePongCameBehindTheLameDuckInfoSucceeds(string $flush): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        self::answerTheFlushPingWith($transport, self::LAME_DUCK . self::PONG);
        $transport->holdNextDial();

        $outcome = $this->settle(self::startFlush($connection, $flush), $transport, $connection);

        self::assertReturnedWhileTheFailoverDialled($outcome);
        self::assertNull($outcome['thrown'], $flush . ' got its PONG');
        if ($flush === 'rtt') {
            self::assertIsFloat($outcome['result']);
        }
        self::assertFailedOverToTheSecondServer($connection, $transport);
    }

    /** @return iterable<string, array{string, string}> */
    public static function flushesWithThePongInTheChunk(): iterable
    {
        yield 'flush(), [PONG | INFO ldm]' => ['flush', self::PONG . self::LAME_DUCK];
        yield 'flush(), [INFO ldm | PONG]' => ['flush', self::LAME_DUCK . self::PONG];
        yield 'rtt(), [PONG | INFO ldm]' => ['rtt', self::PONG . self::LAME_DUCK];
        yield 'rtt(), [INFO ldm | PONG]' => ['rtt', self::LAME_DUCK . self::PONG];
    }

    /**
     * With waiting for a reconnect disabled, a flush whose PONG comes in the same chunk as the lame-duck INFO, ahead of
     * it or behind it, has what it waits for and succeeds while the failover's dial is held, on the first session with
     * the connection Connecting. Its wake-up fires only from a queued callback ({@see NatsConnection::wakeOnPong()}),
     * so the read looks at the flush's slot itself: going by the wake-up, it would fail the flush with "Connection is
     * not open". The code before #191 ran the failover inline and did not return while the dial was held.
     */
    #[DataProvider('flushesWithThePongInTheChunk')]
    public function testWithWaitingDisabledAFlushWhosePongCameWithTheLameDuckInfoSucceeds(string $flush, string $chunk): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, waitForReconnect: false);
        self::answerTheFlushPingWith($transport, $chunk);
        $transport->holdNextDial();

        $outcome = $this->settle(self::startFlush($connection, $flush), $transport, $connection);

        self::assertReturnedWhileTheFailoverDialled($outcome);
        self::assertNull($outcome['thrown'], sprintf('%s got its PONG, yet threw: %s', $flush, $outcome['thrown']?->getMessage() ?? ''));
        self::assertFailedOverToTheSecondServer($connection, $transport);
    }

    /**
     * With waiting for a reconnect disabled, a request's read brings [INFO ldm | MSG of a subscription whose handler
     * awaits]: the read starts the failover, and its delivery is held up in that handler while the failover reopens
     * the connection on the second server, whose Reconnected listener awaits in turn. The request's reply is then on
     * the second server's socket. Let go, the read meets the failover still under way, only announcing the new
     * connection, and returns, so that the request looks again on the new connection, as one issued then would run on
     * it, and gets its reply there. Failing at once there, it would fail with "Connection is not open" on an open
     * connection. The code before
     * #191 delivered the message only after the inline failover, so the handler never held the read up there.
     */
    public function testWithWaitingDisabledAReadThatMeetsTheFailoverOnlyAnnouncingTheNewConnectionLooksAgain(): void
    {
        $transport = new ReconnectingTransport();
        $handlerHold = new HeldUpDelivery(fallbackSeconds: 5.0);
        $listenerHold = new HeldUpDelivery(fallbackSeconds: 5.0);
        $connection = $this->connect($transport, waitForReconnect: false, connectionListener: static function (ConnectionEvent $event) use ($listenerHold): void {
            if ($event === ConnectionEvent::Reconnected) {
                $listenerHold->holdUp();
            }
        });
        $sid = $connection->subscribe('slow', $handlerHold->handler())->await();
        $sent = new class {
            public ?string $replyTo = null;
        };
        $transport->responder = self::answering($transport, static function (string $replyTo) use ($sent, $sid): string {
            $sent->replyTo = $replyTo;

            return self::LAME_DUCK . ReconnectingTransport::msgFrame('slow', $sid, 's-1');
        });
        $connection->request('svc.warm', 'x', 1_000)->await();

        $request = $connection->request('svc.echo', 'y', 2_000);
        $request->ignore();
        $handlerHold->began->getFuture()->await(new TimeoutCancellation(2));
        $listenerHold->began->getFuture()->await(new TimeoutCancellation(2));
        $stateWhenLetGo = $connection->state();
        // The reply, on the second server, where the replay subscribed the reply inbox again.
        $transport->pushFrame(self::reply($transport, (string) $sent->replyTo));
        $handlerHold->end('the test');
        $thrown = null;
        $result = null;
        try {
            $result = $request->await(new TimeoutCancellation(5));
        } catch (\Throwable $e) {
            $thrown = $e;
        }
        $listenerHold->end('the test');

        self::assertSame(ConnectionState::Open, $stateWhenLetGo, 'the failover had reopened the connection');
        self::assertSame(1, $transport->epoch());
        self::assertNull($thrown, $thrown === null ? '' : $thrown::class . ': ' . $thrown->getMessage());
        self::assertSame('reply-1', $result?->payload);
    }

    /**
     * The caller's cancellation and the read's wake-up have both fired by the time the read's wait for the failover
     * ends: the read's own delivery of [INFO ldm | MSG of its subscription] fires the wake-up before the wait, and the
     * Disconnected listener, which runs in the failover's fiber before the wait resumes, cancels the caller's
     * cancellation with a reason. The wake-up won inside the composite the read waits with, and the read throws the
     * caller's own CancelledException, with the reason, as the read's other waits do: the caller's deadline wins.
     * Thrown as it came, the wake-up's exception would reach the caller without the reason. The code before #191 ran
     * the failover inline and did not return while the dial was held.
     */
    public function testAReadWhoseWakeUpAndCallerBothFiredDuringTheWaitThrowsTheCallersOwnCancellation(): void
    {
        $transport = new ReconnectingTransport();
        $callerCancel = new DeferredCancellation();
        $connection = $this->connect($transport, connectionListener: static function (ConnectionEvent $event) use ($callerCancel): void {
            if ($event === ConnectionEvent::Disconnected) {
                $callerCancel->cancel(new \RuntimeException('the caller gave up'));
            }
        });
        $sid = $connection->subscribe('updates', static function (): void {})->await();
        $transport->holdNextDial();
        $transport->pushFrame(self::LAME_DUCK . ReconnectingTransport::msgFrame('updates', $sid, 'u-1'));

        $outcome = $this->settle($connection->readIncomingForOperation($callerCancel->getCancellation(), $sid), $transport, $connection);

        self::assertReturnedWhileTheFailoverDialled($outcome);
        self::assertInstanceOf(CancelledException::class, $outcome['thrown']);
        self::assertSame('the caller gave up', $outcome['thrown']->getPrevious()?->getMessage(), 'the caller\'s own cancellation, with its reason');
        self::assertFailedOverToTheSecondServer($connection, $transport);
    }

    /**
     * A request's read brings [INFO ldm | -ERR 'maximum subscriptions exceeded' | INFO of the first server], the
     * failover's dial held: the rest of the chunk is the leaving server's. The -ERR is reported as that server's ("The
     * server the connection left sent an error frame: ...") and does not fail the request, which times out on its own;
     * the stale INFO does not replace the lame-duck INFO, which serverInfo() still returns when the request returns, on
     * the first session with the connection Connecting; once the failover is over serverInfo() is the second server's.
     * The request used to wait for the whole failover inside its read.
     */
    public function testTheRestOfTheChunkIsTheLeavingServersWhileTheFailoverIsUnderWay(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = self::answering($transport, static fn(): string => self::LAME_DUCK . self::LIMIT_ERR . self::STALE_INFO);
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, errorListener: $recorder->errorListener());
        $connection->request('svc.warm', 'x', 1_000)->await();
        $transport->holdNextDial();

        $outcome = $this->settle($connection->request('svc.echo', 'y', 300), $transport, $connection, static fn(): ?ServerInfo => $connection->serverInfo());

        self::assertReturnedWhileTheFailoverDialled($outcome);
        self::assertInstanceOf(TimeoutException::class, $outcome['thrown'], 'the request timed out on its own, the -ERR reported instead');
        self::assertSame(
            ["The server the connection left sent an error frame: 'maximum subscriptions exceeded'"],
            $recorder->errorsContaining('maximum subscriptions exceeded'),
        );
        $infoAtReturn = $outcome['noted'];
        self::assertInstanceOf(ServerInfo::class, $infoAtReturn);
        self::assertSame('S1', $infoAtReturn->serverId, 'the stale INFO did not replace the lame-duck INFO');
        self::assertTrue($infoAtReturn->lameDuckMode);
        self::assertFailedOverToTheSecondServer($connection, $transport);
        self::assertSame('n1', $connection->serverInfo()?->serverName, 'the second server\'s INFO, from its handshake');
        self::assertFalse($connection->serverInfo()->lameDuckMode);
    }

    /**
     * Guard: a drain()'s flush read brings [INFO ldm | PING | PONG], the drain's close-intent set: no failover starts,
     * in a fiber of its own or inline, and the PING is answered on the draining connection, which the flush's PONG then
     * lets close. Started anyway, a failover in a fiber of its own would not run under close-intent, and the PING behind
     * the INFO, marked as the leaving server's, would get no PONG.
     */
    public function testADrainUnderWayLeavesThePingBehindTheLameDuckInfoAnswered(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, connectionListener: $recorder->connectionListener());
        self::answerTheFlushPingWith($transport, self::LAME_DUCK . self::PING . self::PONG);

        $connection->drain()->await(new TimeoutCancellation(5));

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertCount(1, $transport->connectCalls, 'no failover');
        self::assertSame(['PONG'], $transport->controlLinesStartingWith('PONG', 0), 'the PING was answered on the draining connection');
        self::assertSame([
            ConnectionEvent::Connected,
            ConnectionEvent::DiscoveredServers,
            ConnectionEvent::LameDuck,
            ConnectionEvent::Closed,
        ], $recorder->events);
    }

    /**
     * Guard: a failover that gives up, its one attempt refused, is reported once through the error listener ("Reconnect
     * attempts exhausted") and ends the connection with one Closed event; the request whose read brought the INFO ends
     * with "Connection is not open" when it next looks, its read having waited for the failover to end within its 2 s.
     */
    public function testAFailoverThatGivesUpIsReportedOnceAndTheOperationEndsAtItsNextLook(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = self::answering($transport, static fn(): string => self::LAME_DUCK);
        $recorder = new LifecycleRecorder();
        $connection = $this->connect(
            $transport,
            maxReconnectAttempts: 1,
            connectionListener: $recorder->connectionListener(),
            errorListener: $recorder->errorListener(),
        );
        $connection->request('svc.warm', 'x', 1_000)->await();
        $transport->refuseDials();

        $thrown = null;
        try {
            $connection->request('svc.echo', 'y', 2_000)->await(new TimeoutCancellation(5));
        } catch (\Throwable $e) {
            $thrown = $e;
        }

        self::assertInstanceOf(ConnectionException::class, $thrown);
        self::assertSame('Connection is not open', $thrown->getMessage());
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertCount(1, $recorder->errorsContaining('Reconnect attempts exhausted'), 'reported once');
        self::assertSame(1, $recorder->closedEvents());
        self::assertCount(2, $transport->connectCalls, 'one dial, of the second server, refused');
        self::assertStringStartsWith(self::SECOND_SERVER, $transport->connectCalls[1]);
    }

    /** @return iterable<string, array{string, list<string>}> */
    public static function collectionsWhoseFailoverGivesUp(): iterable
    {
        yield 'fetchBatch(3), two messages received' => ['fetchBatch', ['m-1', 'm-2']];
        yield 'fetchBatch(3), nothing received' => ['fetchBatch', []];
        yield 'requestMany(max 3), two replies collected' => ['requestMany', ['r-1', 'r-2']];
        yield 'requestMany(max 3), nothing collected' => ['requestMany', []];
    }

    /**
     * A fetchBatch() of three, or a requestMany() for three replies, its deadline 30 s away, receives two, each in a
     * read of its own, and then the lame-duck INFO alone, with dials refused and one attempt: its read waits for the
     * failover, which gives up, reports it through the error listener and closes the connection. At its next look the
     * collection finds the connection closed and returns the two; with nothing received it fails with "Connection is
     * not open". The fetch used to fail with that with the two received as well, which were lost. Guards: the data sets
     * with nothing received, and requestMany(), whose look at a terminal close returned what it had collected before as
     * well.
     *
     * @param list<string> $received
     */
    #[DataProvider('collectionsWhoseFailoverGivesUp')]
    public function testACollectionWhoseFailoverGivesUpReturnsWhatItReceivedAtItsNextLook(string $operation, array $received): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->clientOver($transport, ['maxReconnectAttempts' => 1, 'errorListener' => $recorder->errorListener()]);
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport, $operation, $received): array {
            if ($replyTo === null) {
                return [];
            }

            if ($subject === 'svc.warm') {
                return $transport->replyFrame($replyTo, 'ok');
            }

            $sid = (int) $transport->sidFor($replyTo);
            // A read for each, then one for the lame-duck INFO.
            $frames = array_map(
                static fn(string $payload): string => $operation === 'requestMany'
                    ? ReconnectingTransport::msgFrame($replyTo, $sid, $payload)
                    : ReconnectingTransport::msgFrame('evt.s', $sid, $payload, self::ACK_SUBJECT),
                $received,
            );
            $frames[] = self::LAME_DUCK;

            return $frames;
        };
        $client->request('svc.warm', 'x', 1_000)->await();
        $transport->refuseDials();

        $thrown = null;
        $result = null;
        try {
            $collection = $operation === 'requestMany'
                ? $client->requestMany('svc.many', 'y', null, 3, 30_000)
                : $client->jetStream()->fetchBatch('S', 'C', 3, 30_000);
            // A guard against a hang only, far from the collection's own 30 s.
            $result = $collection->await(new TimeoutCancellation(5));
        } catch (\Throwable $e) {
            $thrown = $e;
        }

        self::assertSame(ConnectionState::Closed, $client->state());
        self::assertCount(1, $recorder->errorsContaining('Reconnect attempts exhausted'), 'the failover gave up');
        self::assertCount(2, $transport->connectCalls, 'one dial, of the second server, refused');
        if ($received === []) {
            self::assertInstanceOf(ConnectionException::class, $thrown, sprintf('%s threw %s', $operation, $thrown?->getMessage() ?? 'nothing'));
            self::assertSame('Connection is not open', $thrown->getMessage());

            return;
        }

        self::assertNull($thrown, sprintf('%s threw %s', $operation, $thrown?->getMessage() ?? ''));
        self::assertIsArray($result);
        self::assertSame($received, array_map(static fn(NatsMessage $message): string => $message->payload, $result));
    }

    /**
     * The connection events around a request whose read brings the lame-duck INFO: LameDuck, and the failover's
     * Disconnected, come before the request returns at its deadline, since the failover's fiber starts while the read
     * waits; Reconnected comes only once the dial is let go. The request used to wait for the whole failover inside its
     * read.
     */
    public function testTheEventsComeInOrderAroundTheOperation(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = self::answering($transport, static fn(): string => self::LAME_DUCK);
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, connectionListener: $recorder->connectionListener());
        $connection->request('svc.warm', 'x', 1_000)->await();
        $transport->holdNextDial();

        $outcome = $this->settle($connection->request('svc.echo', 'y', 300), $transport, $connection, static fn(): array => $recorder->events);
        $this->waitUntil(static fn(): bool => count($recorder->events) === 5);

        self::assertReturnedWhileTheFailoverDialled($outcome);
        self::assertInstanceOf(TimeoutException::class, $outcome['thrown']);
        self::assertSame([
            ConnectionEvent::Connected,
            ConnectionEvent::DiscoveredServers,
            ConnectionEvent::LameDuck,
            ConnectionEvent::Disconnected,
        ], $outcome['noted'], 'when the request returned');
        self::assertSame([
            ConnectionEvent::Connected,
            ConnectionEvent::DiscoveredServers,
            ConnectionEvent::LameDuck,
            ConnectionEvent::Disconnected,
            ConnectionEvent::Reconnected,
        ], $recorder->events, 'once the failover was over');
    }

    /**
     * Guard: your own processIncoming() that brings [INFO ldm | PING] still runs the failover inline: it has not
     * returned while the failover's dial is held, and returns the two frames once it is let go, with the connection open
     * on the second server. The PING is answered on neither connection.
     */
    public function testYourOwnReadStillRunsTheFailoverInline(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $transport->holdNextDial();

        $transport->pushFrame(self::LAME_DUCK . self::PING);
        $read = $connection->processIncoming(new TimeoutCancellation(5));
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
        $returnedWhileTheDialWasHeld = $read->isComplete();
        $transport->releaseDial();
        $frames = $read->await(new TimeoutCancellation(5));

        self::assertFalse($returnedWhileTheDialWasHeld, 'the read was still inside the failover, at its held dial');
        self::assertSame(2, $frames);
        self::assertFailedOverToTheSecondServer($connection, $transport);
        self::assertSame([], $transport->controlLinesStartingWith('PONG'));
    }

    /**
     * Guard: the dispatches of a reconnect keep the inline failover, a no-op inside the reconnect (#157). The server's
     * handshake PONG carries a lame-duck INFO and a PING behind it while the reconnect your own read runs is under way:
     * LameDuck is announced, no failover of its own starts, and the PING, the new connection's, is answered there.
     * Started in a fiber of its own, the failover would have marked the new connection as being left, and the PING
     * would have gone unanswered.
     */
    public function testALameDuckInfoBehindTheReconnectsHandshakeLeavesThePingBehindItAnswered(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, connectionListener: $recorder->connectionListener());
        $transport->handshakeTrailer = self::LAME_DUCK . self::PING;

        $transport->dropConnection();
        $frames = $connection->processIncoming(new TimeoutCancellation(5))->await();

        self::assertSame(0, $frames);
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $transport->epoch(), 'one reconnect');
        self::assertCount(2, $transport->connectCalls, 'no failover of its own');
        self::assertSame(['PONG'], $transport->controlLinesStartingWith('PONG', 1), 'the PING was answered on the new connection');
        self::assertSame([
            ConnectionEvent::Connected,
            ConnectionEvent::Disconnected,
            ConnectionEvent::DiscoveredServers,
            ConnectionEvent::LameDuck,
            ConnectionEvent::Reconnected,
        ], $recorder->events);
    }

    /** @return iterable<string, array{bool}> */
    public static function failoversThatReturn(): iterable
    {
        yield 'a failover that reached the second server' => [false];
        yield 'a failover that did not run, the LameDuck listener having started a disconnect()' => [true];
    }

    /**
     * The mark of a failover started in a fiber of its own, which the rest of the chunk and the reads of the leaving
     * connection go by, is there while the failover is under way and gone once it returns: once it reached the second
     * server, and once it did not run at all because the LameDuck listener started a disconnect() (not awaited), whose
     * close-intent the failover's fiber finds when it starts. The rest of that chunk was the leaving server's all the
     * same: its PING was not answered. A mark left behind would treat the connection's later chunks as a leaving
     * server's. Read by reflection: there was no such mark before ("Property ... lameDuckFailover does not exist").
     */
    #[DataProvider('failoversThatReturn')]
    public function testTheFailoverMarkIsClearedWhenTheFailoverReturns(bool $closedByTheLameDuckListener): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = self::answering($transport, static fn(): string => self::LAME_DUCK . self::PING);
        $holder = new class {
            public ?NatsConnection $connection = null;
        };
        $connection = $this->connect($transport, connectionListener: static function (ConnectionEvent $event) use ($holder, $closedByTheLameDuckListener): void {
            if ($closedByTheLameDuckListener && $event === ConnectionEvent::LameDuck) {
                // Not awaited: it runs once the read suspends, ahead of the failover's fiber.
                $holder->connection?->disconnect()->ignore();
            }
        });
        $holder->connection = $connection;
        $mark = new \ReflectionProperty(NatsConnection::class, 'lameDuckFailover');
        $connection->request('svc.warm', 'x', 1_000)->await();

        if ($closedByTheLameDuckListener) {
            $thrown = null;
            try {
                $connection->request('svc.echo', 'y', 2_000)->await(new TimeoutCancellation(5));
            } catch (\Throwable $e) {
                $thrown = $e;
            }

            self::assertInstanceOf(ConnectionException::class, $thrown, 'the request ended with the close');
            self::assertSame('Connection is not open', $thrown->getMessage());
            self::assertSame(ConnectionState::Closed, $connection->state());
            self::assertCount(1, $transport->connectCalls, 'the failover did not run');
            self::assertSame([], $transport->controlLinesStartingWith('PONG'), 'the rest of the chunk was the leaving server\'s');
            self::assertNull($mark->getValue($connection), 'the mark went with the failover');

            return;
        }

        $transport->holdNextDial();
        $outcome = $this->settle($connection->request('svc.echo', 'y', 300), $transport, $connection, static fn(): mixed => $mark->getValue($connection));

        self::assertReturnedWhileTheFailoverDialled($outcome);
        $markWhileUnderWay = $outcome['noted'];
        self::assertIsArray($markWhileUnderWay, 'the mark was there while the failover was under way');
        self::assertInstanceOf(Future::class, $markWhileUnderWay['failover']);
        $markWhileUnderWay['failover']->await(new TimeoutCancellation(5));
        self::assertNull($mark->getValue($connection), 'the mark went with the failover');
    }

    /**
     * A failover leaves the mark of a later one alone when it returns. The Reconnected listener of the failover a
     * request's read started makes a request of its own, with a 300 ms timeout, whose read brings the second server's
     * lame-duck INFO: a second failover starts in a fiber of its own, its dial held, and the listener's request times
     * out while it dials. The first failover then returns, and the mark is still the second one's, under way, until
     * that one returns in turn, on the third connection. Read by reflection: there was no such mark before ("Property
     * ... lameDuckFailover does not exist"), and the listener's request then waited inside the second failover, which
     * never returned while its dial was held.
     */
    public function testAFailoverThatReturnsLeavesTheMarkOfALaterOneAlone(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport): array {
            if ($replyTo === null) {
                return [];
            }

            return match ($subject) {
                'svc.warm' => $transport->replyFrame($replyTo, 'ok'),
                // The second server goes into lame duck mode in its turn, naming a third.
                'svc.listener' => [self::SECOND_LAME_DUCK],
                default => [self::LAME_DUCK],
            };
        };
        $action = new ListenerAction(new LifecycleRecorder(), static function (NatsConnection $connection) use ($transport): string {
            $transport->holdNextDial();
            try {
                $connection->request('svc.listener', 'z', 300)->await();

                return 'answered';
            } catch (TimeoutException) {
                return 'timed out';
            }
        });
        $connection = $this->connect($transport, connectionListener: $action->listener());
        $action->connection = $connection;
        $mark = new \ReflectionProperty(NatsConnection::class, 'lameDuckFailover');
        $connection->request('svc.warm', 'x', 1_000)->await();

        $request = $connection->request('svc.echo', 'y', 300);
        // The listener returns, and with it the first failover, which does not suspend again before its own end.
        $this->waitUntil(static fn(): bool => $action->seconds !== null, 3.0);
        $markOnceTheFirstReturned = $mark->getValue($connection);
        $underWayThen = is_array($markOnceTheFirstReturned) && !$markOnceTheFirstReturned['failover']->isComplete();
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 3);
        $transport->releaseDial();
        $this->waitUntil(static fn(): bool => $transport->epoch() === 2 && $connection->state() === ConnectionState::Open);
        try {
            $request->await(new TimeoutCancellation(5));
        } catch (TimeoutException) {
            // No reply comes for it on any server.
        }

        self::assertSame('timed out', $action->result, 'the listener\'s request timed out while the second failover dialled');
        self::assertIsArray($markOnceTheFirstReturned, 'the second failover\'s mark outlived the first failover');
        self::assertTrue($underWayThen, 'it was the second failover\'s, still under way');
        self::assertInstanceOf(Future::class, $markOnceTheFirstReturned['failover']);
        $markOnceTheFirstReturned['failover']->await(new TimeoutCancellation(5));
        self::assertNull($mark->getValue($connection), 'gone once the second failover returned');
        self::assertCount(3, $transport->connectCalls, 'two failovers, one dial each');
    }

    /** @return iterable<string, array{bool}> */
    public static function waitEnders(): iterable
    {
        yield 'its deadline' => [false];
        yield 'its wake-up, the subscription it waits for removed' => [true];
    }

    /**
     * An operation's read, readIncomingForOperation() for a subscription of its own, brings the lame-duck INFO with the
     * failover's dial held: it waits for the failover only within the operation's wait. Its deadline ends the wait with
     * the deadline's own CancelledException, and its wake-up, here the subscription removed meanwhile, with the read
     * returning what it read; either way on the first session with the connection Connecting. The read used to run the
     * failover inline and not return while the dial was held.
     */
    #[DataProvider('waitEnders')]
    public function testAnOperationsReadWaitsForTheFailoverOnlyWithinItsWait(bool $woken): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $sid = $connection->subscribe('updates', static function (): void {})->await();
        $transport->holdNextDial();
        $transport->pushFrame(self::LAME_DUCK);

        $deadline = new TimeoutCancellation($woken ? 4 : 0.3);
        $read = $connection->readIncomingForOperation($deadline, $sid);
        if ($woken) {
            $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
            $connection->unsubscribe($sid)->await();
        }
        $outcome = $this->settle($read, $transport, $connection);

        self::assertReturnedWhileTheFailoverDialled($outcome);
        if ($woken) {
            self::assertNull($outcome['thrown']);
            self::assertInstanceOf(IncomingChunkResult::class, $outcome['result']);
            self::assertSame(1, $outcome['result']->frames, 'the read returned what it read');
            self::assertTrue($outcome['result']->consumedBytes);
        } else {
            self::assertInstanceOf(CancelledException::class, $outcome['thrown']);
            try {
                $deadline->throwIfRequested();
                self::fail('the deadline fired');
            } catch (CancelledException $own) {
                self::assertSame($own, $outcome['thrown'], 'the deadline\'s own exception');
            }
        }
        self::assertFailedOverToTheSecondServer($connection, $transport);
    }

    /**
     * With waiting for a reconnect disabled, the same read, its deadline 4 s away, fails at once with "Connection is
     * not open", without a cause, on the first session with the connection Connecting: the read itself fails, as one
     * that meets a lost connection does (#178), rather than return and leave its operation to find, at its next read,
     * either the closed connection or its PONG failed by the failover's first attempt, whichever the failover has got
     * to. The read used to run the failover inline and not return while the dial was held.
     */
    public function testWithWaitingDisabledAnOperationsReadFailsAtOnce(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, waitForReconnect: false);
        $sid = $connection->subscribe('updates', static function (): void {})->await();
        $transport->holdNextDial();
        $transport->pushFrame(self::LAME_DUCK);

        $outcome = $this->settle($connection->readIncomingForOperation(new TimeoutCancellation(4), $sid), $transport, $connection);

        self::assertReturnedWhileTheFailoverDialled($outcome);
        self::assertInstanceOf(ConnectionException::class, $outcome['thrown'], 'the read itself failed');
        self::assertSame('Connection is not open', $outcome['thrown']->getMessage());
        self::assertNull($outcome['thrown']->getPrevious(), 'a lame duck is not a failure: no cause');
        self::assertFailedOverToTheSecondServer($connection, $transport);
    }

    /**
     * Guard: the same read, its deadline 4 s away, with the failover's dial let go once it is held: the failover that
     * ends ends the read's wait, and the read returns what it read with the connection open on the second server, so
     * that the operation looks again on the new connection.
     */
    public function testAnOperationsReadReturnsOnceTheFailoverIsOver(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $sid = $connection->subscribe('updates', static function (): void {})->await();
        $transport->holdNextDial();
        $transport->pushFrame(self::LAME_DUCK);

        $read = async(static function () use ($connection, $sid, $transport): array {
            $result = $connection->readIncomingForOperation(new TimeoutCancellation(4), $sid)->await();

            return [$result, $transport->epoch(), $connection->state()];
        });
        $this->releaseHeldDial($transport);
        [$result, $epoch, $state] = $read->await(new TimeoutCancellation(5));

        self::assertSame(1, $result->frames);
        self::assertSame(1, $epoch, 'the read returned once the failover had reached the second server');
        self::assertSame(ConnectionState::Open, $state);
        self::assertFailedOverToTheSecondServer($connection, $transport);
    }

    /**
     * Guard: the Reconnected listener of a failover that a request's read started makes a request of its own on the new
     * connection, and gets its reply. The listener runs in the failover's fiber, before the failover returns and its
     * mark goes: the reads of the new connection do not wait for that failover, which is waiting for the listener. The
     * server sends a +OK ahead of the reply, in a read of its own, so that one read of the listener's request brings
     * nothing for it: a read that waited for the failover there would wait out the request's timeout.
     */
    public function testAnOperationInTheReconnectedListenerOfTheFailoverDoesNotWaitForIt(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport): array {
            if ($replyTo === null) {
                return [];
            }

            return match (true) {
                // The request on the first server gets the lame-duck INFO.
                $subject === 'svc.echo' && $transport->epoch() === 0 => [self::LAME_DUCK],
                $subject === 'svc.listener' => ["+OK\r\n", ...$transport->replyFrame($replyTo, 'ok')],
                default => $transport->replyFrame($replyTo, 'ok'),
            };
        };
        $action = new ListenerAction(
            new LifecycleRecorder(),
            static fn(NatsConnection $connection): string => $connection->request('svc.listener', 'z', 1_000)->await()->payload,
        );
        $connection = $this->connect($transport, connectionListener: $action->listener());
        $action->connection = $connection;
        $connection->request('svc.warm', 'x', 1_000)->await();

        try {
            $connection->request('svc.echo', 'y', 300)->await(new TimeoutCancellation(5));
        } catch (TimeoutException) {
            // No reply comes for it on either server.
        }
        $this->waitUntil(static fn(): bool => $action->seconds !== null, 3.0);

        self::assertNull($action->failure, $action->failure === null ? '' : $action->failure::class . ': ' . $action->failure->getMessage());
        self::assertSame('ok', $action->result);
        self::assertFailedOverToTheSecondServer($connection, $transport);
    }

    /**
     * Guard: the second server sends a PING right behind its handshake PONG, while the failover a request's read
     * started is still under way and the first connection still marked as being left: that PING is the new
     * connection's, and is answered there. The request, whose reply comes on neither server, times out on its own.
     */
    public function testAPingTheSecondServerSendsDuringTheFailoverIsAnsweredThere(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = self::answering($transport, static fn(): string => self::LAME_DUCK);
        $connection = $this->connect($transport);
        $connection->request('svc.warm', 'x', 1_000)->await();
        $transport->handshakeTrailer = self::PING;

        try {
            $connection->request('svc.echo', 'y', 300)->await(new TimeoutCancellation(5));
            self::fail('expected the request to time out: its reply comes on neither server');
        } catch (TimeoutException) {
            // Its own timeout, as expected.
        }
        $this->waitUntil(static fn(): bool => $transport->epoch() === 1 && $connection->state() === ConnectionState::Open);

        self::assertFailedOverToTheSecondServer($connection, $transport);
        self::assertSame(['PONG'], $transport->controlLinesStartingWith('PONG', 1), 'answered on the second connection');
        self::assertSame([], $transport->controlLinesStartingWith('PONG', 0));
    }

    /**
     * Guard: a pull consumer run whose read brings the lame-duck INFO, its pull's expiry 30 s away and the failover's
     * dial let go once it is held: the read waits for the failover, which ends the wait, and the engine then re-issues
     * its pull on the second server, the first one lost with the first connection, and gets the message there. One pull
     * on each session; the run would otherwise have waited out the lost pull's expiry, past the 5 s guard.
     */
    public function testAPullConsumerWhoseReadBringsTheLameDuckInfoReissuesItsPullOnTheSecondServer(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->clientOver($transport);
        $pulls = new class {
            /** @var list<int> The session of each pull. */
            public array $epochs = [];
        };
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport, $pulls): array {
            $sid = $replyTo === null ? null : $transport->sidFor($replyTo);
            if ($subject !== self::PULL_SUBJECT || $sid === null) {
                return [];
            }

            $pulls->epochs[] = $transport->epoch();

            return [$transport->epoch() === 0 ? self::LAME_DUCK : ReconnectingTransport::msgFrame('evt.s', $sid, 'm-after', self::ACK_SUBJECT)];
        };
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        $handled = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $transport->holdNextDial();

        $run = $iterator->handle(static function (NatsMessage $message) use ($iterator, $handled): void {
            $handled->payloads[] = $message->payload;
            $iterator->stop();
        });
        $this->releaseHeldDial($transport);
        $run->await(new TimeoutCancellation(5));

        self::assertSame(['m-after'], $handled->payloads);
        self::assertSame([0, 1], $pulls->epochs, 'one pull on each session');
        self::assertFailedOverToTheSecondServer($client, $transport);
    }

    /** @return iterable<string, array{bool}> */
    public static function readsThatBringTheInfo(): iterable
    {
        yield 'the read of a request, the failover in a fiber of its own' => [true];
        yield 'your own processIncoming(), the failover inline' => [false];
    }

    /**
     * The LameDuck listener awaits a flush() on a connection that has just died: the flush's failed write starts a
     * reconnect, which replaces the connection before the listener returns, and the flush fails. The connection the
     * INFO came on is then gone, and the failover leaves the one the reconnect opened alone: one reconnect in all,
     * the connection open on session 1. The failover used to take the connection that was current once the listener
     * had returned, and failed the new one over as well: two reconnects, session 2. The inline failover of your own
     * read did so up to 2.20.0; the reflection the other data set waits with fails there, the mark not existing.
     */
    #[DataProvider('readsThatBringTheInfo')]
    public function testAFailoverWhoseConnectionAReconnectReplacedDuringTheLameDuckListenerLeavesTheNewOneAlone(bool $operationRead): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $holder = new class {
            public ?NatsConnection $connection = null;
            public ?string $flushOutcome = null;
        };
        $record = $recorder->connectionListener();
        $connection = $this->connect($transport, connectionListener: static function (ConnectionEvent $event, ?\Throwable $error = null) use ($holder, $transport, $record): void {
            $record($event, $error);
            if ($event !== ConnectionEvent::LameDuck || $holder->connection === null || $holder->flushOutcome !== null) {
                return;
            }

            $transport->dropConnection();
            try {
                $holder->connection->flush()->await();
                $holder->flushOutcome = 'flushed';
            } catch (\Throwable $e) {
                $holder->flushOutcome = $e->getMessage();
            }
        });
        $holder->connection = $connection;
        $transport->responder = self::answering($transport, static fn(): string => self::LAME_DUCK);
        $connection->request('svc.warm', 'x', 1_000)->await();

        if ($operationRead) {
            try {
                $connection->request('svc.echo', 'y', 300)->await(new TimeoutCancellation(5));
            } catch (TimeoutException) {
                // Its reply comes on no server.
            }
            // Once the failover in a fiber of its own has returned, its mark gone, the session tells whether it failed
            // the new connection over as well.
            $mark = new \ReflectionProperty(NatsConnection::class, 'lameDuckFailover');
            $this->waitUntil(static fn(): bool => $mark->getValue($connection) === null && $connection->state() === ConnectionState::Open);
        } else {
            // The inline failover is over when the read returns.
            $transport->pushFrame(self::LAME_DUCK);
            $connection->processIncoming(new TimeoutCancellation(5))->await();
            $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Open);
        }
        $settled = $transport->epoch();

        self::assertSame('Connection lost before the server answered the PING', $holder->flushOutcome);
        self::assertSame(1, $settled, 'one reconnect, the flush\'s');
        self::assertCount(2, $transport->connectCalls);
        self::assertSame(
            [
                ConnectionEvent::Connected,
                ConnectionEvent::DiscoveredServers,
                ConnectionEvent::LameDuck,
                ConnectionEvent::Disconnected,
                ConnectionEvent::Reconnected,
            ],
            $recorder->events,
        );
    }

    /**
     * Awaits $operation while the failover's dial is held, with a time-out that only guards against a hang: the
     * guard's own CancelledException means that the operation had not returned. Returns whether it returned, what it
     * returned or threw, and the session and the state the connection was in when it did, with what $atReturn noted
     * then. The held dial is then let go, an operation still inside the failover let finish, and the connection waited
     * for on the second server, so that nothing is left running whatever the outcome.
     *
     * @template T
     * @param Future<T> $operation
     * @param (\Closure(): mixed)|null $atReturn
     * @return array{returned: bool, result: T|null, thrown: \Throwable|null, epoch: int, state: ConnectionState, noted: mixed}
     */
    private function settle(Future $operation, ReconnectingTransport $transport, NatsConnection|NatsClient $connection, ?\Closure $atReturn = null): array
    {
        $guard = new TimeoutCancellation(5);
        $result = null;
        $thrown = null;
        try {
            $result = $operation->await($guard);
        } catch (\Throwable $e) {
            $thrown = $e;
        }
        $returned = !($thrown instanceof CancelledException && $guard->isRequested());
        $outcome = [
            'returned' => $returned,
            'result' => $result,
            'thrown' => $returned ? $thrown : null,
            'epoch' => $transport->epoch(),
            'state' => $connection->state(),
            'noted' => $atReturn === null ? null : $atReturn(),
        ];

        $this->releaseHeldDial($transport);
        if (!$returned) {
            try {
                $operation->await(new TimeoutCancellation(5));
            } catch (\Throwable) {
                // Its own outcome, once the failover let it go: the assertions report that it had not returned.
            }
        }
        $this->waitUntil(static fn(): bool => $transport->epoch() === 1 && $connection->state() === ConnectionState::Open);

        return $outcome;
    }

    /** Lets the failover's dial go once it is held: a release before the dial starts would be lost. */
    private function releaseHeldDial(ReconnectingTransport $transport): void
    {
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) >= 2);
        $transport->releaseDial();
    }

    /**
     * The operation returned on its own while the failover's dial was held: before the failover had reached the second
     * server (session 0), and with the failover under way (Connecting).
     *
     * @param array{returned: bool, result: mixed, thrown: \Throwable|null, epoch: int, state: ConnectionState, noted: mixed} $outcome
     */
    private static function assertReturnedWhileTheFailoverDialled(array $outcome): void
    {
        self::assertTrue($outcome['returned'], 'the operation returned while the failover\'s dial was held');
        self::assertSame(0, $outcome['epoch'], 'the operation returned before the failover had dialled the second server');
        self::assertSame(ConnectionState::Connecting, $outcome['state'], 'the failover was under way');
    }

    /** The failover dialled the second server, once, and the connection is open there. */
    private static function assertFailedOverToTheSecondServer(NatsConnection|NatsClient $connection, ReconnectingTransport $transport): void
    {
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $transport->epoch(), 'one failover');
        self::assertCount(2, $transport->connectCalls, 'one dial, of the second server');
        self::assertStringStartsWith(self::SECOND_SERVER, $transport->connectCalls[1]);
    }

    /**
     * Answers svc.warm with "ok" on the reply inbox, so that a test sets the inbox up first, and every other request
     * with the chunk that $chunk builds for its reply subject, in one read, where the server would have sent the reply.
     *
     * @param \Closure(string): string $chunk
     * @return \Closure(string, ?string, string): list<string>
     */
    private static function answering(ReconnectingTransport $transport, \Closure $chunk): \Closure
    {
        return static function (string $subject, ?string $replyTo, string $payload) use ($transport, $chunk): array {
            if ($replyTo === null) {
                return [];
            }

            return $subject === 'svc.warm' ? $transport->replyFrame($replyTo, 'ok') : [$chunk($replyTo)];
        };
    }

    /** The reply "reply-1" to $replyTo, on the sid the client subscribed for it. */
    private static function reply(ReconnectingTransport $transport, string $replyTo): string
    {
        return implode('', $transport->replyFrame($replyTo, 'reply-1'));
    }

    /**
     * The server answers the PING of a flush, and only that one, with $chunk, in one read: PINGs after the handshake go
     * unanswered until it is out.
     */
    private static function answerTheFlushPingWith(ReconnectingTransport $transport, string $chunk): void
    {
        $transport->answerPings = false;
        $transport->afterWrite = static function (string $bytes) use ($transport, $chunk): void {
            if ($bytes === self::PING) {
                $transport->afterWrite = null;
                $transport->answerPings = true;
                $transport->pushFrame($chunk);
            }
        };
    }

    /** @return Future<mixed> */
    private static function startFlush(NatsConnection $connection, string $flush): Future
    {
        return $flush === 'flush' ? $connection->flush() : $connection->rtt();
    }

    /**
     * Scripts the server so that $operation's own read brings [INFO ldm | PING] in one chunk, where its result would
     * have come, and returns what starts the operation, with a deadline, an expiry or a progress bound of 0.3 s.
     *
     * @return \Closure(): Future<mixed>
     */
    private static function scriptOperation(string $operation, NatsClient $client, ReconnectingTransport $transport): \Closure
    {
        $chunk = self::LAME_DUCK . self::PING;
        $replay = new class {
            /** The deliver subject of the Key/Value consumer, until its SUB is written. */
            public ?string $deliver = null;
        };
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport, $chunk, $replay): array {
            if ($replyTo === null) {
                return [];
            }

            if ($subject === 'svc.warm') {
                return $transport->replyFrame($replyTo, 'ok');
            }

            if (str_starts_with($subject, '$JS.API.CONSUMER.CREATE.KV_b')) {
                /** @var array{config: array{deliver_subject: string}} $request */
                $request = json_decode($payload, true, flags: JSON_THROW_ON_ERROR);
                $replay->deliver = $request['config']['deliver_subject'];

                return $transport->replyFrame($replyTo, (string) json_encode(['stream_name' => 'KV_b', 'name' => 'c1', 'config' => $request['config'], 'num_pending' => 1]));
            }

            // requestMany()'s request, the fetch's pull, the pull consumer's: the chunk comes where the answer would have.
            return [$chunk];
        };
        // The Key/Value consumer's replay would begin once the client has subscribed to its deliver subject.
        $transport->afterWrite = static function (string $bytes) use ($transport, $chunk, $replay): void {
            $deliver = $replay->deliver;
            if ($deliver !== null && str_contains($bytes, 'SUB ' . $deliver . ' ')) {
                $replay->deliver = null;
                $transport->pushFrame($chunk);
            }
        };
        // The reply inbox, for requestMany() and for the Key/Value consumer's create request.
        $client->request('svc.warm', 'x', 1_000)->await();

        if ($operation === 'next') {
            $queue = $client->subscribeQueue('jobs')->await();

            // A poll writes nothing: the chunk is on the socket when its read takes it.
            return static function () use ($queue, $transport, $chunk): Future {
                $transport->pushFrame($chunk);

                return async(static fn(): ?NatsMessage => $queue->setTimeout(0.3)->next());
            };
        }

        return match ($operation) {
            'requestMany' => static fn(): Future => $client->requestMany('svc.echo', 'y', null, 1, 300),
            'fetchBatch' => static fn(): Future => $client->jetStream()->fetchBatch('S', 'C', 1, 300),
            'pullConsumer' => static fn(): Future => $client->jetStream()->pullConsumer('S', 'C')
                ->setBatching(1)
                ->setIterations(1)
                ->setExpiresMs(300)
                ->handle(static function (): void {}),
            'keys' => static fn(): Future => $client->jetStream()->keyValue('b')->keys(0.3),
            default => throw new \LogicException('Unknown operation ' . $operation),
        };
    }

    /**
     * A client over $transport, reconnecting at once to any server, with $overrides of the NatsOptions arguments.
     *
     * @param array<string, mixed> $overrides
     */
    private function clientOver(ReconnectingTransport|WatchedTransport $transport, array $overrides = []): NatsClient
    {
        $client = $this->own(new NatsClient(new NatsOptions(...array_merge([
            'connectTimeoutMs' => 500,
            'requestTimeoutMs' => 2_000,
            'reconnectEnabled' => true,
            'maxReconnectAttempts' => 1_000,
            'reconnectDelayMs' => 5,
            'reconnectMaxDelayMs' => 20,
            'reconnectJitterMs' => 0,
            'pingIntervalSeconds' => 0,
            'waitForReconnect' => true,
        ], $overrides)), $transport));
        $this->opened[] = $client;
        $client->connect()->await();

        return $client;
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
     * Starts the application's own read loop, processIncoming() over and over on a fiber of its own, until $stop is
     * cancelled or a read fails.
     */
    private function startApplicationReadLoop(NatsClient $client, DeferredCancellation $stop): void
    {
        async(static function () use ($client, $stop): void {
            while (!$stop->isCancelled()) {
                try {
                    $client->processIncoming($stop->getCancellation())->await();
                } catch (\Throwable) {
                    return;
                }
            }
        })->ignore();
    }
}
