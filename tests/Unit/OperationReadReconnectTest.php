<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Core\SubscriptionQueue;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\JetStream\KeyValue\KeyValueEntry;
use IDCT\NATS\Tests\Support\HeldUpDelivery;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use IDCT\NATS\Transport\TransportClosedException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;

/**
 * An operation whose own read is the first to notice a lost connection (#178): a SubscriptionQueue poll, request() or
 * requestMany(), fetchBatch() or directGetBatch(), the pull consumer, Key/Value keys() or history(), or a flush
 * waiting for its PONG, whose read is the one on the socket as the connection drops. That read used to run the
 * reconnect itself, inline, and nothing could end the wait before the reconnect was over: not the operation's
 * deadline, and not its wake-up (#174). A request with a 1 s timeout returned after a 2 s outage, and so did a poll
 * whose message a delivery still under way brought 50 ms into it. The read now starts the reconnect in a fiber of its
 * own and waits for it only within the operation's own wait, as it waits for a reconnect another fiber runs, and the
 * reconnect carries on without it; with waiting disabled the operation fails at once, as one whose write noticed the
 * loss does. Your own processIncoming() still runs the reconnect itself and waits for all of it, and the heartbeat's
 * read still does the same.
 *
 * Over the scripted server in tests/Support/ReconnectingTransport.php, seen through tests/Support/WatchedTransport.php,
 * which tells the test when the operation's read is on the socket, with deliveries held up by
 * tests/Support/HeldUpDelivery.php. The outcomes are decided by the order of events where possible: the connection is
 * still Connecting when an operation returns before the reconnect is over, and Open once a read waited for all of it.
 * The time bounds sit far from both the expected and the old behaviour.
 */
final class OperationReadReconnectTest extends TestCase
{
    use ReconnectScenarios;

    /** The payload each operation waits for. */
    private const RESULTS = [
        'next' => 'job-1',
        'fetchAll' => 'job-1',
        'request' => 'reply-1',
        'requestMany' => 'reply-1',
        'fetchBatch' => 'm1',
        'directGet' => 'v1',
        'pullConsumer' => 'm1',
        'kvKeys' => 'k',
        'kvHistory' => 'v1',
    ];

    private const ACK_SUBJECT = '$JS.ACK.S.C.1.1.1.0.0';
    private const KV_ACK_SUBJECT = '$JS.ACK.KV_b.c1.1.1.1.1700000000000000000.0';
    private const DIRECT_GET_HEADERS = "NATS/1.0\r\nNats-Subject: k\r\nNats-Num-Pending: 0\r\n\r\n";

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{string}> */
    public static function operations(): iterable
    {
        yield 'SubscriptionQueue::next()' => ['next'];
        yield 'SubscriptionQueue::fetchAll(1)' => ['fetchAll'];
        yield 'request()' => ['request'];
        yield 'requestMany(max 1)' => ['requestMany'];
        yield 'JetStreamContext::fetchBatch(1)' => ['fetchBatch'];
        yield 'JetStreamContext::directGetBatch()' => ['directGet'];
        yield 'PullConsumerIterator::handle()' => ['pullConsumer'];
        yield 'KeyValueBucket::keys()' => ['kvKeys'];
        yield 'KeyValueBucket::history()' => ['kvHistory'];
    }

    /** @return iterable<string, array{string}> The operations whose reply shares the mux inbox, not an own sid. */
    public static function requestOperations(): iterable
    {
        yield 'request()' => ['request'];
        yield 'requestMany(max 1)' => ['requestMany'];
    }

    /**
     * @return iterable<string, array{string}> The own-sid operations whose result the new connection can bring on its
     *         own: a poll's subscription is replayed, the pull engine re-issues its pull, a Key/Value replay re-creates
     *         its consumer. fetchBatch() and directGetBatch() are one-shot requests that do not re-issue after a
     *         reconnect, so their result cannot arrive on the new connection; their own-sid take is covered by
     *         {@see OperationTakesItsQueuedMessageTest} and their reconnect deadline by
     *         {@see testAnOperationWhoseOwnReadRunsTheReconnectEndsAtItsOwnDeadline()} (fetchBatch).
     */
    public static function ownSidOperations(): iterable
    {
        yield 'SubscriptionQueue::next()' => ['next'];
        yield 'SubscriptionQueue::fetchAll(1)' => ['fetchAll'];
        yield 'PullConsumerIterator::handle()' => ['pullConsumer'];
        yield 'KeyValueBucket::keys()' => ['kvKeys'];
        yield 'KeyValueBucket::history()' => ['kvHistory'];
    }

    /**
     * A request() or requestMany() whose reply was already in flight on the mux inbox when the connection dropped: the
     * application's read took a chunk holding, ahead of the reply, a message for a lower-sid subscription whose handler
     * awaits. The operation's read takes the socket during the hold-up, and the connection drops with dials refused for
     * 2 s: that read is the first to notice, and starts the reconnect. The handler returns 50 ms later, and the delivery
     * brings the reply during the outage. The operation, with a 1 s deadline, returns within 1 s, and the connection is
     * Open again once the server lets dials through. It used to return after the whole outage: its read ran the reconnect
     * inline and only then looked again. A reply shares the mux inbox, not an own sid, so #179's own-sid take does not
     * reach it; the own-sid operations are {@see testAnOwnSidOperationWhoseOwnReadRunsTheReconnectGetsItsResultOnTheNewConnection()}.
     */
    #[DataProvider('requestOperations')]
    public function testARequestWhoseOwnReadRunsTheReconnectGetsTheReplyStillUnderWay(string $operation): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->connectWatched($watched);
        $hold = new HeldUpDelivery(fallbackSeconds: 5.0);
        // Subscribed first: the lowest sid, so its message is delivered ahead of the reply.
        $slowSid = $client->subscribe('slow', $hold->handler())->await();
        $lead = ReconnectingTransport::msgFrame('slow', $slowSid, 's');
        self::scriptAnswers($transport, $lead);
        // Sets the reply inbox up before the application's read starts.
        $client->request('svc.warm', 'x', 1_000)->await();
        $stop = new DeferredCancellation();
        $this->startApplicationReadLoop($client, $stop);
        $this->awaitRead($watched);

        $start = hrtime(true);
        $result = self::startOperation($operation, $client, null, 1.0);
        $hold->began->getFuture()->await(new TimeoutCancellation(2));
        // The application's read is held up in the handler, so the read that takes the socket is the operation's.
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->refuseDials();
        $transport->dropConnection();
        EventLoop::delay(0.05, static fn() => $hold->end('the test'));
        $this->acceptDialsAfter($transport, 2.0);
        [$payloads, $elapsed, $error] = $this->settle($result, $start);
        $stop->cancel();

        self::assertSame('the test', $hold->endedBy, 'the delivery went on during the outage');
        self::assertNull($error, sprintf('%s threw %s', $operation, $error?->getMessage() ?? ''));
        self::assertSame([self::RESULTS[$operation]], $payloads);
        self::assertLessThan(1.0, $elapsed, sprintf('%s returned after %.3f s, with a 1 s deadline and a 2 s outage', $operation, $elapsed));
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open, 4.0);
        self::assertSame(1, $transport->epoch(), 'the reconnect the read started reopened the connection');
    }

    /**
     * An own-sid operation whose own read is the first to notice a lost connection: its result arrives on the new
     * connection, once the reconnect has replayed the operation's subscription (a poll's) or the pull engine has
     * re-issued its pull (a fetch's, a Key/Value replay's). Nothing is queued for the operation when it looks - with
     * #179 a queued own-sid message would be taken before the drop, so this staging delivers the result only on the new
     * connection (the server answers the result on epoch 1) - so the operation's own read, the sole reader, goes to the
     * socket, is the one that notices the drop, and starts the reconnect. With dials refused for 0.2 s and a 2 s
     * deadline, the operation returns its result once the connection is back, on the second session. It used to return
     * after the whole outage: its read ran the reconnect inline and only then looked again.
     *
     * The during-outage form of this (a delivery still under way bringing the result before the reconnect) no longer
     * applies to an own-sid operation: #179 takes a message queued before the drop at once, so a result that reaches an
     * own subscription during an outage can only be one the new connection brings. The deadline and waiting-disabled
     * paths (nothing delivered) are {@see testAnOperationWhoseOwnReadRunsTheReconnectEndsAtItsOwnDeadline()} and
     * {@see testWithWaitingDisabledAnOperationWhoseOwnReadRunsTheReconnectFailsAtOnce()}.
     */
    #[DataProvider('ownSidOperations')]
    public function testAnOwnSidOperationWhoseOwnReadRunsTheReconnectGetsItsResultOnTheNewConnection(string $operation): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->connectWatched($watched);
        $server = new class {
            /** The Key/Value replay consumer's deliver subject, until its SUB is replayed on the new connection. */
            public ?string $deliver = null;
        };
        // Setup (svc.warm, a Key/Value consumer) is answered on either connection; the operation's RESULT is answered
        // only on the new connection (epoch 1), so nothing is queued for the operation on the first one.
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport, $server): array {
            if ($replyTo === null) {
                return [];
            }

            if ($subject === 'svc.warm') {
                return $transport->replyFrame($replyTo, 'ok');
            }

            $sid = $transport->sidFor($replyTo);
            if ($sid === null) {
                return [];
            }

            if (str_starts_with($subject, '$JS.API.CONSUMER.CREATE.KV_b')) {
                /** @var array{config: array{deliver_subject: string}} $request */
                $request = json_decode($payload, true, flags: JSON_THROW_ON_ERROR);
                $server->deliver = $request['config']['deliver_subject'];

                return $transport->replyFrame($replyTo, (string) json_encode(['stream_name' => 'KV_b', 'name' => 'c1', 'config' => $request['config'], 'num_pending' => 1]));
            }

            if ($transport->epoch() < 1) {
                // The result waits for the new connection: the operation's own read stays on the socket until the drop.
                return [];
            }

            return match (true) {
                str_starts_with($subject, '$JS.API.CONSUMER.MSG.NEXT.') => [ReconnectingTransport::msgFrame('evt.s', $sid, 'm1', self::ACK_SUBJECT)],
                str_starts_with($subject, '$JS.API.DIRECT.GET.') => [ReconnectingTransport::hmsgFrame($replyTo, $sid, self::DIRECT_GET_HEADERS, 'v1')],
                default => [],
            };
        };
        $transport->afterWrite = static function (string $bytes) use ($transport, $server): void {
            $deliver = $server->deliver;
            if ($deliver !== null && $transport->epoch() >= 1 && str_contains($bytes, 'SUB ' . $deliver . ' ')) {
                $server->deliver = null;
                $transport->pushFrame(ReconnectingTransport::msgFrame('$KV.b.k', (int) $transport->sidFor($deliver), 'v1', self::KV_ACK_SUBJECT));
            }
        };
        $client->request('svc.warm', 'x', 1_000)->await();
        $queue = self::isPoll($operation) ? $client->subscribeQueue('jobs')->await() : null;
        if ($queue !== null) {
            // The poll's message arrives once its SUB is replayed on the new connection.
            $jobsSent = false;
            $previous = $transport->afterWrite;
            $transport->afterWrite = static function (string $bytes) use ($transport, $queue, &$jobsSent, $previous): void {
                $previous($bytes);
                if (!$jobsSent && $transport->epoch() === 1 && str_contains($bytes, 'SUB jobs ')) {
                    $jobsSent = true;
                    $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
                }
            };
        }

        $start = hrtime(true);
        $result = self::startOperation($operation, $client, $queue, 2.0);
        // The operation's own read is the sole reader, so it is the one on the socket when the connection drops.
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->refuseDials();
        $transport->dropConnection();
        $this->acceptDialsAfter($transport, 0.2);
        [$payloads, $elapsed, $error] = $this->settle($result, $start);

        self::assertNull($error, sprintf('%s threw %s', $operation, $error?->getMessage() ?? ''));
        self::assertSame([self::RESULTS[$operation]], $payloads);
        self::assertLessThan(2.0, $elapsed, sprintf('%s returned after %.3f s, with a 2 s deadline', $operation, $elapsed));
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open, 4.0);
        self::assertSame(1, $transport->epoch(), 'the operation\'s own read ran the reconnect that reopened the connection');
    }

    /** @return iterable<string, array{string, float}> */
    public static function operationsWithADeadline(): iterable
    {
        yield 'SubscriptionQueue::next(), 0.5 s' => ['next', 0.5];
        yield 'request(), 0.5 s' => ['request', 0.5];
        yield 'requestMany(max 1), 0.5 s' => ['requestMany', 0.5];
        yield 'JetStreamContext::fetchBatch(1), expiry 0.5 s' => ['fetchBatch', 1.5];
    }

    /**
     * No delivery brings anything: the operation's read, the only one on the socket, meets the dropped connection and
     * starts the reconnect, with dials refused for 3.5 s. The operation ends with its own timeout at its deadline (a
     * poll returns nothing, a request throws its timeout, requestMany() returns what it collected, a fetch its 408),
     * while the connection is still Connecting: the reconnect goes on without it, and once the server lets dials
     * through the connection is Open and a request succeeds. The operation used to end only after the whole outage.
     */
    #[DataProvider('operationsWithADeadline')]
    public function testAnOperationWhoseOwnReadRunsTheReconnectEndsAtItsOwnDeadline(string $operation, float $deadline): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->connectWatched($watched);
        $answer = self::scriptAnswers($transport, '', answering: false);
        $client->request('svc.warm', 'x', 1_000)->await();
        $queue = self::isPoll($operation) ? $client->subscribeQueue('jobs')->await() : null;

        $start = hrtime(true);
        $result = self::startOperation($operation, $client, $queue, 0.5);
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->refuseDials();
        $transport->dropConnection();
        $this->acceptDialsAfter($transport, 3.5);
        [$payloads, $elapsed, $error] = $this->settle($result, $start);
        $stateOnReturn = $client->state();

        self::assertSame([], $payloads);
        switch ($operation) {
            case 'request':
                self::assertInstanceOf(TimeoutException::class, $error);
                self::assertSame('Request timed out for subject svc.echo', $error->getMessage());
                break;
            case 'fetchBatch':
                self::assertInstanceOf(JetStreamException::class, $error);
                self::assertSame(408, $error->getCode());
                break;
            default:
                self::assertNull($error, sprintf('%s threw %s', $operation, $error?->getMessage() ?? ''));
        }
        self::assertSame(ConnectionState::Connecting, $stateOnReturn, 'the operation ended before the reconnect was over');
        self::assertLessThan($deadline + 1.0, $elapsed, sprintf('%s ended after %.3f s, with a %.1f s deadline and a 3.5 s outage', $operation, $elapsed, $deadline));
        self::assertGreaterThan($deadline - 0.3, $elapsed, sprintf('%s ended after %.3f s, before its %.1f s deadline', $operation, $elapsed, $deadline));

        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open, 5.0);
        $answer();
        self::assertSame('reply-1', $client->request('svc.echo', 'again', 2_000)->await()->payload, 'the reconnected connection serves requests');
        self::assertSame(1, $transport->epoch());
    }

    /** @return iterable<string, array{string}> */
    public static function operationsThatCannotWait(): iterable
    {
        yield 'SubscriptionQueue::next()' => ['next'];
        yield 'request()' => ['request'];
        yield 'requestMany(max 1)' => ['requestMany'];
        yield 'JetStreamContext::fetchBatch(1)' => ['fetchBatch'];
    }

    /**
     * The same with waiting for a reconnect disabled: the operation whose read noticed the lost connection fails at
     * once with "Connection is not open", the read's error its cause, as the option promises for every operation issued
     * while a reconnect is in flight and as one whose control write noticed the loss does, and the reconnect its read
     * started goes on without it. It used to wait for the whole reconnect.
     */
    #[DataProvider('operationsThatCannotWait')]
    public function testWithWaitingDisabledAnOperationWhoseOwnReadRunsTheReconnectFailsAtOnce(string $operation): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = new NatsClient($this->options(false, 2_000, 1_000, 0, 2, null, 5, 20, null), $watched);
        $this->opened[] = $client;
        $client->connect()->await();
        self::scriptAnswers($transport, '', answering: false);
        $client->request('svc.warm', 'x', 1_000)->await();
        $queue = self::isPoll($operation) ? $client->subscribeQueue('jobs')->await() : null;

        $start = hrtime(true);
        $result = self::startOperation($operation, $client, $queue, 2.0);
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->refuseDials();
        $transport->dropConnection();
        $this->acceptDialsAfter($transport, 1.5);
        [$payloads, $elapsed, $error] = $this->settle($result, $start);
        $stateOnReturn = $client->state();

        self::assertSame([], $payloads);
        self::assertInstanceOf(ConnectionException::class, $error, sprintf('%s threw %s', $operation, $error?->getMessage() ?? 'nothing'));
        self::assertSame('Connection is not open', $error->getMessage());
        self::assertInstanceOf(TransportClosedException::class, $error->getPrevious(), 'the read\'s error is its cause');
        self::assertSame('Socket closed by peer (EOF)', $error->getPrevious()->getMessage());
        self::assertSame(ConnectionState::Connecting, $stateOnReturn, 'the reconnect the read started goes on');
        self::assertLessThan(0.5, $elapsed, sprintf('%s failed after %.3f s, with a 1.5 s outage', $operation, $elapsed));
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open, 3.0);
        self::assertSame(1, $transport->epoch());
    }

    /** @return iterable<string, array{string}> */
    public static function flushes(): iterable
    {
        yield 'flush()' => ['flush'];
        yield 'rtt()' => ['rtt'];
    }

    /** @return iterable<string, array{string, float}> */
    public static function flushesThatCannotWait(): iterable
    {
        foreach (['flush', 'rtt'] as $flush) {
            yield $flush . '(), the transport closes at once' => [$flush, 0.0];
            yield $flush . '(), the transport\'s close takes 50 ms' => [$flush, 0.05];
        }
    }

    /**
     * With waiting for a reconnect disabled, a flush whose own read is the first to notice the lost connection fails
     * at once with "Connection is not open", the read's error its cause, as every operation does while a reconnect is
     * in flight, and the reconnect its read started goes on without it. Whether the reconnect's first attempt has
     * ended the lost connection's PONGs by then depends on how long the transport's close takes - a TLS or WebSocket
     * close handshake, say - so the error does not: a flush that read on instead met the lost PONG with a close that
     * ends at once, and the closed connection with one that takes 50 ms.
     */
    #[DataProvider('flushesThatCannotWait')]
    public function testWithWaitingDisabledAFlushWhoseOwnReadRunsTheReconnectFailsAtOnce(string $flush, float $closeDelay): void
    {
        $transport = new ReconnectingTransport();
        $transport->closeDelay = $closeDelay;
        $client = new NatsClient($this->options(false, 2_000, 1_000, 0, 2, null, 5, 20, null), $transport);
        $this->opened[] = $client;
        $client->connect()->await();
        $transport->answerPings = false;
        // The connection dies once the flush's PING is out: the flush's read is the one on the socket.
        $transport->afterWrite = static function (string $bytes) use ($transport): void {
            if ($bytes === "PING\r\n") {
                $transport->afterWrite = null;
                $transport->refuseDials();
                $transport->dropConnection();
            }
        };
        $this->acceptDialsAfter($transport, 1.0);

        $start = hrtime(true);
        try {
            if ($flush === 'flush') {
                $client->flush()->await();
            } else {
                $client->rtt()->await();
            }
            self::fail('expected the flush to fail');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
            self::assertInstanceOf(TransportClosedException::class, $e->getPrevious(), 'the read\'s error is its cause');
            self::assertSame('Socket closed by peer (EOF)', $e->getPrevious()->getMessage());
        }
        $elapsed = $this->secondsSince($start);
        $stateOnFailure = $client->state();

        self::assertSame(ConnectionState::Connecting, $stateOnFailure, 'the reconnect the read started goes on');
        self::assertLessThan(0.5, $elapsed, sprintf('%s failed after %.3f s, with a 1 s outage', $flush, $elapsed));
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open, 3.0);
        self::assertSame(1, $transport->epoch());
    }

    /**
     * A flush whose own read is the first to notice the lost connection, its PING out and its PONG never to come: the
     * reconnect's first attempt ends the lost connection's PONGs, which is the flush's wake-up, and the flush fails
     * with "Connection lost before the server answered the PING" while the reconnect, with dials refused for 1.5 s,
     * is still under way. It used to fail with the same error once the reconnect was over.
     */
    #[DataProvider('flushes')]
    public function testAFlushWhoseOwnReadRunsTheReconnectFailsAsSoonAsItsPongIsLost(string $flush): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->connectClient($transport);
        $transport->answerPings = false;
        // The connection dies once the flush's PING is out: the flush's read is the one on the socket.
        $transport->afterWrite = static function (string $bytes) use ($transport): void {
            if ($bytes === "PING\r\n") {
                $transport->afterWrite = null;
                $transport->refuseDials();
                $transport->dropConnection();
            }
        };
        $this->acceptDialsAfter($transport, 1.5);

        $start = hrtime(true);
        try {
            if ($flush === 'flush') {
                $client->flush()->await();
            } else {
                $client->rtt()->await();
            }
            self::fail('expected the flush to fail');
        } catch (ConnectionException $e) {
            self::assertSame('Connection lost before the server answered the PING', $e->getMessage());
        }
        $elapsed = $this->secondsSince($start);
        $stateOnFailure = $client->state();

        self::assertSame(ConnectionState::Connecting, $stateOnFailure, 'the flush failed before the reconnect was over');
        self::assertLessThan(1.0, $elapsed, sprintf('%s failed after %.3f s, with a 1.5 s outage', $flush, $elapsed));
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open, 3.0);
        self::assertSame(1, $transport->epoch());
    }

    /**
     * Guard: your own read that is the first to notice the lost connection still runs the reconnect itself and returns
     * only once it is over, with dials refused for 0.3 s meanwhile: the connection is Open when processIncoming()
     * returns, on the second session, and the refused dials happened while it waited.
     */
    public function testYourOwnReadThatIsTheFirstToNoticeStillRunsTheReconnectAndWaitsForAllOfIt(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $transport->refuseDials();
        $transport->dropConnection();
        $this->acceptDialsAfter($transport, 0.3);

        $start = hrtime(true);
        $frames = $connection->processIncoming(new TimeoutCancellation(5))->await();
        $elapsed = $this->secondsSince($start);

        self::assertSame(0, $frames);
        self::assertSame(ConnectionState::Open, $connection->state(), 'the read returned once the reconnect was over');
        self::assertSame(1, $transport->epoch());
        self::assertGreaterThan(2, count($transport->connectCalls), 'dials were refused while the read waited');
        self::assertLessThan(2.0, $elapsed, sprintf('the read returned after %.3f s, with a 0.3 s outage', $elapsed));
    }

    /**
     * Guard: the reconnect a poll's read starts runs in a fiber of its own, and that fiber is the recovery fiber. A
     * connection listener called during the reconnect runs inside it, so an operation it awaits is refused at once,
     * as before: waiting there would wait on the very reconnect that is suspended in the listener (#145). The
     * reconnect itself is unaffected, and the poll, which waited for it, returns nothing at its deadline.
     */
    public function testOperationsFromAListenerInsideAReconnectAPollsReadStartedAreRefusedAtOnce(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        /** @var array<string, \Closure(NatsClient): mixed> $operations */
        $operations = [
            'request' => static fn(NatsClient $c): mixed => $c->request('svc.echo', 'x', 2_000)->await(),
            'subscribe' => static fn(NatsClient $c): mixed => $c->subscribe('updates', static function (): void {})->await(),
            'flush' => static function (NatsClient $c): mixed {
                $c->flush()->await();

                return null;
            },
            'processIncoming' => static fn(NatsClient $c): mixed => $c->processIncoming(new TimeoutCancellation(2))->await(),
            'connect' => static function (NatsClient $c): mixed {
                $c->connect()->await();

                return null;
            },
        ];
        /** @var array<string, array{\Throwable|null, float}> $outcomes */
        $outcomes = [];
        $holder = new class {
            public ?NatsClient $client = null;
        };
        $listener = static function (ConnectionEvent $event) use ($holder, &$outcomes, $operations): void {
            $client = $holder->client;
            if ($event !== ConnectionEvent::Disconnected || $client === null) {
                return;
            }

            foreach ($operations as $name => $operation) {
                $start = hrtime(true);
                $error = null;
                try {
                    $operation($client);
                } catch (\Throwable $e) {
                    $error = $e;
                }
                $outcomes[$name] = [$error, (hrtime(true) - $start) / 1e9];
            }
        };
        $client = new NatsClient($this->options(true, 2_000, 1_000, 0, 2, $listener, 5, 20, null), $watched);
        $this->opened[] = $client;
        $client->connect()->await();
        $holder->client = $client;
        $queue = $client->subscribeQueue('jobs')->await();

        // The poll's read is the one on the socket when the connection drops: it starts the reconnect.
        $poll = async(static fn(): ?NatsMessage => $queue->setTimeout(1.0)->next());
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->dropConnection();
        $message = $poll->await(new TimeoutCancellation(5));

        self::assertNull($message);
        self::assertSame(ConnectionState::Open, $client->state(), 'the refused operations must not affect the reconnect itself');
        self::assertSame(1, $client->statistics()->reconnects);
        self::assertSame(array_keys($operations), array_keys($outcomes), 'the listener ran every operation during the reconnect');
        foreach ($outcomes as $name => [$error, $elapsed]) {
            if (!$error instanceof ConnectionException) {
                self::fail($name . ' must be refused with ConnectionException, got ' . ($error === null ? 'success' : $error::class));
            }
            if ($name === 'connect') {
                self::assertStringContainsString('cannot join the in-flight recovery from a connection/error listener', $error->getMessage());
            } else {
                self::assertSame('Connection is not open', $error->getMessage(), $name);
            }
            self::assertLessThan(1.0, $elapsed, $name . ' must be refused at once, not wait on the reconnect it runs inside');
        }
    }

    /**
     * Guard: the heartbeat's read that meets the lost connection still runs the reconnect itself, with dials refused
     * for 0.3 s; a request issued meanwhile waits for that reconnect within its timeout and gets its reply.
     */
    public function testTheHeartbeatsReadThatNoticesTheLostConnectionStillRunsTheReconnectItself(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->connectClient($transport, pingIntervalSeconds: 0.05);
        self::scriptAnswers($transport, '');
        // The connection dies once the heartbeat's PING is out: the heartbeat's read meets it.
        $transport->afterWrite = static function (string $bytes) use ($transport): void {
            if ($bytes === "PING\r\n") {
                $transport->afterWrite = null;
                $transport->refuseDials();
                $transport->dropConnection();
            }
        };
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Connecting);
        $this->acceptDialsAfter($transport, 0.3);

        $reply = $client->request('svc.echo', 'hi', 2_000)->await()->payload;

        self::assertSame('reply-1', $reply);
        self::assertSame(ConnectionState::Open, $client->state());
        self::assertSame(1, $client->statistics()->reconnects);
        self::assertSame(1, $transport->epoch());
    }

    /** @return iterable<string, array{string}> */
    public static function operationsThatFailWithTheConnection(): iterable
    {
        yield 'SubscriptionQueue::next()' => ['next'];
        yield 'request()' => ['request'];
    }

    /**
     * Guard: with reconnect off, the operation whose read noticed the lost connection still fails with "Reconnect is
     * disabled" chained to the read's error (#172), the recovery it started in a fiber of its own having closed the
     * connection for good.
     */
    #[DataProvider('operationsThatFailWithTheConnection')]
    public function testAnOperationWhoseOwnReadNoticesTheLossLearnsWhyWhenReconnectIsOff(string $operation): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = new NatsClient(new NatsOptions(connectTimeoutMs: 500, requestTimeoutMs: 2_000, reconnectEnabled: false, pingIntervalSeconds: 0), $watched);
        $this->opened[] = $client;
        $client->connect()->await();
        self::scriptAnswers($transport, '', answering: false);
        $client->request('svc.warm', 'x', 1_000)->await();
        $queue = self::isPoll($operation) ? $client->subscribeQueue('jobs')->await() : null;

        $result = self::startOperation($operation, $client, $queue, 2.0);
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->dropConnection();
        [$payloads, , $error] = $this->settle($result, hrtime(true));

        self::assertSame([], $payloads);
        self::assertInstanceOf(ConnectionException::class, $error, sprintf('%s threw %s', $operation, $error?->getMessage() ?? 'nothing'));
        self::assertSame('Reconnect is disabled', $error->getMessage());
        self::assertInstanceOf(TransportClosedException::class, $error->getPrevious());
        self::assertSame('Socket closed by peer (EOF)', $error->getPrevious()->getMessage());
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * Guard: a reconnect that gives up, three attempts with dials refused, still fails the operation whose read
     * started it with its own error and cause (#172), the operation's deadline not yet reached.
     */
    #[DataProvider('operationsThatFailWithTheConnection')]
    public function testAnOperationWhoseOwnReadRunsAReconnectThatGivesUpGetsItsError(string $operation): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = new NatsClient($this->options(true, 2_000, 3, 0, 2, null, 5, 20, null), $watched);
        $this->opened[] = $client;
        $client->connect()->await();
        self::scriptAnswers($transport, '', answering: false);
        $client->request('svc.warm', 'x', 1_000)->await();
        $queue = self::isPoll($operation) ? $client->subscribeQueue('jobs')->await() : null;

        $result = self::startOperation($operation, $client, $queue, 3.0);
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->refuseDials();
        $transport->dropConnection();
        [$payloads, $elapsed, $error] = $this->settle($result, hrtime(true));

        self::assertSame([], $payloads);
        self::assertInstanceOf(ConnectionException::class, $error, sprintf('%s threw %s', $operation, $error?->getMessage() ?? 'nothing'));
        self::assertSame('Reconnect attempts exhausted', $error->getMessage());
        self::assertNotNull($error->getPrevious(), 'with its cause');
        self::assertSame(ConnectionState::Closed, $client->state());
        self::assertCount(4, $transport->connectCalls, 'the first dial and three reconnect attempts');
        self::assertLessThan(2.0, $elapsed, sprintf('%s failed after %.3f s, as soon as the reconnect gave up', $operation, $elapsed));
    }

    /**
     * Scripts the server's answers on $transport: svc.warm gets "ok" at once; svc.echo gets reply-1, a pull gets m1, a
     * Direct Get batch gets v1 and a Key/Value replay gets one record, each behind $lead in the same chunk, while the
     * server answers. Returns a closure that makes it answer when it did not before.
     *
     * @return \Closure(): void
     */
    private static function scriptAnswers(ReconnectingTransport $transport, string $lead, bool $answering = true): \Closure
    {
        $replay = new class {
            /** The deliver subject of the Key/Value replay's consumer, until its SUB is written. */
            public ?string $deliver = null;
        };
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport, $lead, $replay, &$answering): array {
            if ($replyTo === null) {
                return [];
            }

            if ($subject === 'svc.warm') {
                return $transport->replyFrame($replyTo, 'ok');
            }

            $sid = $transport->sidFor($replyTo);
            if (!$answering || $sid === null) {
                return [];
            }

            if (str_starts_with($subject, '$JS.API.CONSUMER.CREATE.KV_b')) {
                /** @var array{config: array{deliver_subject: string}} $request */
                $request = json_decode($payload, true, flags: JSON_THROW_ON_ERROR);
                $replay->deliver = $request['config']['deliver_subject'];

                return $transport->replyFrame($replyTo, (string) json_encode(['stream_name' => 'KV_b', 'name' => 'c1', 'config' => $request['config'], 'num_pending' => 1]));
            }

            return match (true) {
                $subject === 'svc.echo' => [$lead . ReconnectingTransport::msgFrame($replyTo, $sid, 'reply-1')],
                str_starts_with($subject, '$JS.API.CONSUMER.MSG.NEXT.') => [$lead . ReconnectingTransport::msgFrame('evt.s', $sid, 'm1', self::ACK_SUBJECT)],
                str_starts_with($subject, '$JS.API.DIRECT.GET.') => [$lead . ReconnectingTransport::hmsgFrame($replyTo, $sid, self::DIRECT_GET_HEADERS, 'v1')],
                default => [],
            };
        };
        // The Key/Value replay's record goes out once the client subscribed to the consumer's deliver subject.
        $transport->afterWrite = static function (string $bytes) use ($transport, $lead, $replay): void {
            $deliver = $replay->deliver;
            if ($deliver !== null && str_contains($bytes, 'SUB ' . $deliver . ' ')) {
                $replay->deliver = null;
                $transport->pushFrame($lead . ReconnectingTransport::msgFrame('$KV.b.k', (int) $transport->sidFor($deliver), 'v1', self::KV_ACK_SUBJECT));
            }
        };

        return static function () use (&$answering): void {
            $answering = true;
        };
    }

    /**
     * Starts $operation with a deadline of $wait seconds. The JetStream pulls and the Direct Get batch expire after
     * $wait seconds, and their deadline is a second later, the slack they allow the server; the Key/Value replays take
     * $wait seconds as their progress timeout.
     *
     * @return Future<list<string>> The payloads the operation got.
     */
    private static function startOperation(string $operation, NatsClient $client, ?SubscriptionQueue $queue, float $wait): Future
    {
        $ms = (int) round($wait * 1000);

        return match ($operation) {
            'next' => async(static function () use ($queue, $wait): array {
                $message = self::queueOf($queue)->setTimeout($wait)->next();

                return $message === null ? [] : [$message->payload];
            }),
            'fetchAll' => async(static fn(): array => self::payloads(self::queueOf($queue)->setTimeout($wait)->fetchAll(1))),
            'request' => $client->request('svc.echo', 'hi', $ms)->map(static fn(NatsMessage $message): array => [$message->payload]),
            'requestMany' => $client->requestMany('svc.echo', 'hi', null, 1, $ms)->map(self::payloads(...)),
            'fetchBatch' => $client->jetStream()->fetchBatch('S', 'C', 1, $ms)->map(self::payloads(...)),
            'directGet' => $client->jetStream()->directGetBatch('S', ['batch' => 1, 'seq' => 1], $ms)->map(self::payloads(...)),
            'kvKeys' => $client->jetStream()->keyValue('b')->keys($wait),
            'kvHistory' => $client->jetStream()->keyValue('b')->history('k', $wait)->map(
                static fn(array $entries): array => array_map(static fn(KeyValueEntry $entry): string => (string) $entry->value, $entries),
            ),
            'pullConsumer' => async(static function () use ($client, $ms): array {
                $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs($ms);
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

    /**
     * Awaits $operation: its payloads, the seconds since $start, and what it threw.
     *
     * @param Future<list<string>> $operation
     * @return array{list<string>, float, ?\Throwable}
     */
    private function settle(Future $operation, int $start): array
    {
        $error = null;
        try {
            $payloads = $operation->await(new TimeoutCancellation(8));
        } catch (\Throwable $e) {
            $payloads = [];
            $error = $e;
        }

        return [$payloads, $this->secondsSince($start), $error];
    }

    private function connectWatched(WatchedTransport $watched): NatsClient
    {
        $client = new NatsClient($this->options(true, 2_000, 1_000, 0, 2, null, 5, 20, null), $watched);
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
