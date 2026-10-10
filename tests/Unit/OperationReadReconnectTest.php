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
 * read still does the same. A requestMany() or fetchBatch() whose read fails that way, or because the reconnect gave
 * up or reconnect is off, returns what it has received rather than lose it to the read's error, and so does one whose
 * read meets a fatal -ERR, also once the reconnect has reopened the connection (#196).
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
        $client = $this->own(new NatsClient($this->options(false, 2_000, 1_000, 0, 2, null, 5, 20, null), $watched));
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
        $client = $this->own(new NatsClient($this->options(false, 2_000, 1_000, 0, 2, null, 5, 20, null), $transport));
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
        $client = $this->own(new NatsClient($this->options(true, 2_000, 1_000, 0, 2, $listener, 5, 20, null), $watched));
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
        $client = $this->own(new NatsClient(new NatsOptions(connectTimeoutMs: 500, requestTimeoutMs: 2_000, reconnectEnabled: false, pingIntervalSeconds: 0), $watched));
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
        $client = $this->own(new NatsClient($this->options(true, 2_000, 3, 0, 2, null, 5, 20, null), $watched));
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

    /** @return iterable<string, array{string, string, list<string>}> */
    public static function collectionsWhoseReadFailsWithTheConnectionGoing(): iterable
    {
        foreach (['waiting disabled', 'the reconnect gives up', 'reconnect off'] as $failure) {
            yield 'requestMany(max 3), ' . $failure . ', two replies collected' => ['requestMany', $failure, ['r-1', 'r-2']];
            yield 'requestMany(max 3), ' . $failure . ', nothing collected' => ['requestMany', $failure, []];
            yield 'JetStreamContext::fetchBatch(3), ' . $failure . ', two messages received' => ['fetchBatch', $failure, ['m-1', 'm-2']];
            yield 'JetStreamContext::fetchBatch(3), ' . $failure . ', nothing received' => ['fetchBatch', $failure, []];
        }
    }

    /**
     * A requestMany() for three replies, or a fetchBatch() of three, its deadline 30 s away, has received two, each in
     * a read of its own, when the connection drops under its next read: with waiting for a reconnect disabled that read
     * fails at once with "Connection is not open"; with dials refused and three attempts it waits for the reconnect,
     * which gives up ("Reconnect attempts exhausted"); with reconnect off the connection closes ("Reconnect is
     * disabled": the configuration of symfony-nats-messenger, whose get() fetches). The collection returns the two
     * then, well before its deadline, as requestMany() does when the connection closes and fetchBatch() when the
     * heartbeats stop, with the connection still Connecting, or Closed; with nothing received it fails with the read's
     * error. It used to throw that error with the two received as well, which were lost: a fetched message the server
     * counted as delivered came again only after the ack wait. The nothing-received data sets are guards.
     *
     * @param list<string> $received
     */
    #[DataProvider('collectionsWhoseReadFailsWithTheConnectionGoing')]
    public function testACollectionWhoseReadFailsWithTheConnectionGoingReturnsWhatItReceived(string $operation, string $failure, array $received): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->own(new NatsClient(match ($failure) {
            'waiting disabled' => $this->options(false, 2_000, 1_000, 0, 2, null, 5, 20, null),
            'the reconnect gives up' => $this->options(true, 2_000, 3, 0, 2, null, 5, 20, null),
            default => new NatsOptions(connectTimeoutMs: 500, requestTimeoutMs: 2_000, reconnectEnabled: false, pingIntervalSeconds: 0),
        }, $watched));
        $this->opened[] = $client;
        $client->connect()->await();
        $inbox = new class {
            /** Where the operation receives what it waits for: its reply subject, or its pull's inbox, and that sid. */
            public ?string $subject = null;
            public int $sid = 0;
        };
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport, $inbox): array {
            if ($replyTo === null) {
                return [];
            }

            if ($subject === 'svc.warm') {
                return $transport->replyFrame($replyTo, 'ok');
            }

            // What the operation receives the test sends below, a read at a time.
            $inbox->subject = $replyTo;
            $inbox->sid = (int) $transport->sidFor($replyTo);

            return [];
        };
        // Sets the reply inbox up first: the collection's reads are then the only ones on the socket.
        $client->request('svc.warm', 'x', 1_000)->await();

        $result = $operation === 'requestMany'
            ? $client->requestMany('svc.many', 'y', null, 3, 30_000)->map(self::payloads(...))
            : $client->jetStream()->fetchBatch('S', 'C', 3, 30_000)->map(self::payloads(...));
        $this->waitUntil(static fn(): bool => $inbox->subject !== null && $watched->readsUnderWay === 1);
        foreach ($received as $payload) {
            $reads = $watched->reads;
            $transport->pushFrame($operation === 'requestMany'
                ? ReconnectingTransport::msgFrame((string) $inbox->subject, $inbox->sid, $payload)
                : ReconnectingTransport::msgFrame('evt.s', $inbox->sid, $payload, self::ACK_SUBJECT));
            // The collection took it, and its next read is on the socket.
            $this->waitUntil(static fn(): bool => $watched->reads > $reads && $watched->readsUnderWay === 1);
        }
        $transport->refuseDials();
        $transport->dropConnection();
        [$payloads, $elapsed, $error] = $this->settle($result, hrtime(true));
        $stateOnReturn = $client->state();

        [$readsError, $state] = match ($failure) {
            'waiting disabled' => ['Connection is not open', ConnectionState::Connecting],
            'the reconnect gives up' => ['Reconnect attempts exhausted', ConnectionState::Closed],
            default => ['Reconnect is disabled', ConnectionState::Closed],
        };
        self::assertSame($state, $stateOnReturn, $failure === 'waiting disabled' ? 'the reconnect the read started goes on' : 'the connection is gone');
        // Far from both: a collection that went on reading instead looked until its deadline, 30 s away.
        self::assertLessThan(5.0, $elapsed, sprintf('%s returned after %.3f s, its deadline 30 s away', $operation, $elapsed));
        if ($received !== []) {
            self::assertNull($error, sprintf('%s threw %s', $operation, $error?->getMessage() ?? ''));
            self::assertSame($received, $payloads);

            return;
        }

        self::assertInstanceOf(ConnectionException::class, $error, sprintf('%s threw %s', $operation, $error?->getMessage() ?? 'nothing'));
        self::assertSame($readsError, $error->getMessage());
        if ($failure === 'the reconnect gives up') {
            self::assertNotNull($error->getPrevious(), 'with its cause');
        } else {
            self::assertInstanceOf(TransportClosedException::class, $error->getPrevious(), 'the read\'s error is its cause');
            self::assertSame('Socket closed by peer (EOF)', $error->getPrevious()->getMessage());
        }
    }

    /** @return iterable<string, array{string, string, string, bool, list<string>}> */
    public static function collectionsWhoseReadMeetsAFatalError(): iterable
    {
        foreach (['requestMany' => 'requestMany(max 3)', 'fetchBatch' => 'JetStreamContext::fetchBatch(3)'] as $operation => $name) {
            foreach (['Stale Connection', 'User Authentication Expired'] as $serverError) {
                yield sprintf("%s, '%s', the reconnect reopens it, two received", $name, $serverError) => [$operation, $serverError, 'the reconnect reopens it', false, ['m-1', 'm-2']];
            }

            yield $name . ", 'Stale Connection' in the chunk of the two received, the reconnect reopens it" => [$operation, 'Stale Connection', 'the reconnect reopens it', true, ['m-1', 'm-2']];
            foreach (['the reconnect reopens it', 'waiting disabled', 'the reconnect gives up', 'reconnect off', 'its deadline comes first'] as $recovery) {
                if ($recovery !== 'the reconnect reopens it') {
                    yield sprintf("%s, 'Stale Connection', %s, two received", $name, $recovery) => [$operation, 'Stale Connection', $recovery, false, ['m-1', 'm-2']];
                }

                yield sprintf("%s, 'Stale Connection', %s, nothing received", $name, $recovery) => [$operation, 'Stale Connection', $recovery, false, []];
            }
        }
    }

    /**
     * A requestMany() for three replies, or a fetchBatch() of three, has received two, each in a read of its own, when a
     * fatal -ERR, 'Stale Connection' or 'User Authentication Expired', comes in its next read, or in the chunk that
     * brings the two. The server closes the connection after such an -ERR, and the read recovers it within the
     * collection's wait before it throws the -ERR (#171): with a quick reconnect, the connection is Open again on a new
     * socket by the time the collection looks at the failure. The collection returns the two then, as it does when its
     * read fails with the connection going any other way, sends nothing on the new connection, and the connection takes
     * the next request. It used to throw the -ERR because the connection was Open, and the two were lost (#196):
     * replies a scatter-gather never saw, and fetched messages the server counted as delivered, which came again only
     * after the ack wait, or never with no acks or max_deliver 1. The other ends of the recovery (waiting disabled, a
     * reconnect that gives up, reconnect off, the collection's deadline before the held dial) already returned the
     * two, and with nothing received every one of them still fails with the -ERR: those data sets are guards.
     *
     * @param list<string> $received
     */
    #[DataProvider('collectionsWhoseReadMeetsAFatalError')]
    public function testACollectionWhoseReadMeetsAFatalErrorReturnsWhatItReceivedAlsoOnceTheReconnectReopenedTheConnection(
        string $operation,
        string $serverError,
        string $recovery,
        bool $inOneChunk,
        array $received,
    ): void {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->own(new NatsClient(match ($recovery) {
            'waiting disabled' => $this->options(false, 2_000, 1_000, 0, 2, null, 5, 20, null),
            'the reconnect gives up' => $this->options(true, 2_000, 3, 0, 2, null, 5, 20, null),
            'reconnect off' => new NatsOptions(connectTimeoutMs: 500, requestTimeoutMs: 2_000, reconnectEnabled: false, pingIntervalSeconds: 0),
            default => $this->options(true, 2_000, 1_000, 0, 2, null, 5, 20, null),
        }, $watched));
        $this->opened[] = $client;
        $client->connect()->await();
        self::scriptAnswers($transport, '', answering: false);
        // Sets the reply inbox up first: the collection's reads are then the only ones on the socket.
        $client->request('svc.warm', 'x', 1_000)->await();

        // Its deadline is 30 s away, or comes first: after 300 ms for requestMany(), and after 1.2 s for fetchBatch(),
        // whose pull expires after 200 ms and which allows the server a second more.
        $deadlineFirst = $recovery === 'its deadline comes first';
        $result = self::startCollection($operation, $client, 3, $deadlineFirst ? ($operation === 'requestMany' ? 300 : 200) : 30_000);
        $inbox = $this->awaitCollectionRead($transport, $watched, $operation);
        $frames = array_map(static fn(string $payload): string => self::collectionFrame($operation, $inbox, $payload), $received);
        if (!$inOneChunk) {
            foreach ($frames as $frame) {
                $this->pushToCollection($transport, $watched, $frame);
            }

            $frames = [];
        }

        if ($recovery === 'the reconnect gives up') {
            $transport->refuseDials();
        } elseif ($recovery === 'waiting disabled' || $deadlineFirst) {
            $transport->holdNextDial();
        }

        $transport->pushFrame(implode('', $frames) . "-ERR '" . $serverError . "'\r\n");
        [$payloads, $elapsed, $error] = $this->settle($result, hrtime(true));
        $stateOnReturn = $client->state();

        self::assertSame(match ($recovery) {
            'the reconnect reopens it' => ConnectionState::Open,
            'waiting disabled', 'its deadline comes first' => ConnectionState::Connecting,
            default => ConnectionState::Closed,
        }, $stateOnReturn);
        self::assertCount(1, $transport->controlLinesStartingWith(self::collectionPublishPrefix($operation)), 'sent once, and not again on the new connection');
        if (!$deadlineFirst) {
            // Far from both: a collection that went on reading instead looked until its deadline, 30 s away.
            self::assertLessThan(5.0, $elapsed, sprintf('%s returned after %.3f s, its deadline 30 s away', $operation, $elapsed));
        }

        if ($received !== []) {
            self::assertNull($error, sprintf('%s threw %s', $operation, $error?->getMessage() ?? ''));
            self::assertSame($received, $payloads);
        } else {
            self::assertInstanceOf(ConnectionException::class, $error, sprintf('%s threw %s', $operation, $error?->getMessage() ?? 'nothing'));
            self::assertSame("Server sent error frame: '" . $serverError . "'", $error->getMessage());
            self::assertTrue($client->endedTheConnection($error), 'the -ERR that ended the connection');
        }

        if ($stateOnReturn === ConnectionState::Open) {
            self::assertSame(1, $client->statistics()->reconnects);
            if ($operation === 'fetchBatch') {
                self::assertNull($transport->sidFor($inbox[0]), 'the fetch released its inbox');
            }

            self::assertSame('ok', $client->request('svc.warm', 'x', 1_000)->await()->payload, 'the reopened connection takes the next request');
        }
    }

    /** @return iterable<string, array{string}> */
    public static function collectionsOfOne(): iterable
    {
        yield 'requestMany(max 1)' => ['requestMany'];
        yield 'JetStreamContext::fetchBatch(1)' => ['fetchBatch'];
        yield 'JetStreamContext::fetchNext()' => ['fetchNext'];
    }

    /**
     * A requestMany() for one reply, a fetchBatch() of one or a fetchNext() gets its message in the chunk that brings a
     * fatal 'Stale Connection' right behind it: its read hands it the message, meets the -ERR, and has the connection
     * reopened by the reconnect before the collection can see that it is complete. It returns the message, the
     * connection Open. It used to throw the -ERR, and the complete result was lost as a partial one was (#196).
     */
    #[DataProvider('collectionsOfOne')]
    public function testACompleteCollectionWhoseChunkAlsoBringsAFatalErrorReturnsItsResult(string $operation): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->connectWatched($watched);
        self::scriptAnswers($transport, '', answering: false);
        $client->request('svc.warm', 'x', 1_000)->await();

        $result = self::startCollection($operation, $client, 1, 30_000);
        $inbox = $this->awaitCollectionRead($transport, $watched, $operation);
        $transport->pushFrame(self::collectionFrame($operation, $inbox, 'm-1') . "-ERR 'Stale Connection'\r\n");
        [$payloads, , $error] = $this->settle($result, hrtime(true));

        self::assertNull($error, sprintf('%s threw %s', $operation, $error?->getMessage() ?? ''));
        self::assertSame(['m-1'], $payloads);
        self::assertSame(ConnectionState::Open, $client->state());
        self::assertSame(1, $client->statistics()->reconnects);
        self::assertCount(1, $transport->controlLinesStartingWith(self::collectionPublishPrefix($operation)), 'sent once');
    }

    /** @return iterable<string, array{string, string}> */
    public static function collectionsWhoseReadFailsWithTheConnectionStayingOpen(): iterable
    {
        foreach (['requestMany' => 'requestMany(max 3)', 'fetchBatch' => 'JetStreamContext::fetchBatch(3)'] as $operation => $name) {
            yield $name . ', an -ERR refusing a reply subject' => [$operation, 'reply subject refused'];
            yield $name . ", 'Invalid Publish Subject'" => [$operation, 'invalid publish subject'];
            yield $name . ', a handler throwing in the words of a fatal -ERR' => [$operation, 'handler in the words'];
            yield $name . ', a handler throwing after a reconnect of its own' => [$operation, 'handler after a reconnect'];
        }
    }

    /**
     * Guard: a requestMany() for three replies, or a fetchBatch() of three, that has received two still fails when its
     * next read fails with the connection staying open: on an -ERR the server keeps the connection open for, or, with
     * NatsOptions::$handlerErrorsFailOperations, on another subscription's handler that throws, be it a
     * ConnectionException in the words of a fatal -ERR, or one thrown once the handler's own publish lost the
     * connection and the reconnect reopened it. The collection asks which failure ended the connection, not how the
     * connection looks (#196): a count of reconnects would have taken the handler's for its own.
     */
    #[DataProvider('collectionsWhoseReadFailsWithTheConnectionStayingOpen')]
    public function testACollectionWhoseReadFailsWithTheConnectionStayingOpenStillFails(string $operation, string $failure): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->own(new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: 2_000,
            maxReconnectAttempts: 1_000,
            reconnectDelayMs: 5,
            reconnectMaxDelayMs: 20,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            handlerErrorsFailOperations: true,
        ), $watched));
        $this->opened[] = $client;
        $client->connect()->await();
        self::scriptAnswers($transport, '', answering: false);
        $client->request('svc.warm', 'x', 1_000)->await();
        $side = $client->subscribe('side', static function () use ($client, $transport, $failure): void {
            if ($failure === 'handler after a reconnect') {
                $transport->failNextWriteContaining('PUB side.reply ');
                $client->publish('side.reply', 'x')->await();

                throw new ConnectionException('The handler failed after its own reconnect');
            }

            throw new ConnectionException("Server sent error frame: 'Stale Connection'");
        })->await();

        $result = self::startCollection($operation, $client, 3, 30_000);
        $inbox = $this->awaitCollectionRead($transport, $watched, $operation);
        foreach (['m-1', 'm-2'] as $payload) {
            $this->pushToCollection($transport, $watched, self::collectionFrame($operation, $inbox, $payload));
        }

        $transport->pushFrame(match ($failure) {
            'reply subject refused' => "-ERR 'Permissions Violation for Publish with Reply of \"_INBOX.reserved\"'\r\n",
            'invalid publish subject' => "-ERR 'Invalid Publish Subject'\r\n",
            default => ReconnectingTransport::msgFrame('side', $side, 'side-1'),
        });
        [, , $error] = $this->settle($result, hrtime(true));

        self::assertInstanceOf(ConnectionException::class, $error, sprintf('%s threw %s', $operation, $error?->getMessage() ?? 'nothing'));
        self::assertSame(match ($failure) {
            'reply subject refused' => "Server sent error frame: 'Permissions Violation for Publish with Reply of \"_INBOX.reserved\"'",
            'invalid publish subject' => "Server sent error frame: 'Invalid Publish Subject'",
            'handler in the words' => "Server sent error frame: 'Stale Connection'",
            default => 'The handler failed after its own reconnect',
        }, $error->getMessage());
        self::assertFalse($client->endedTheConnection($error), 'not a failure that ended the connection');
        self::assertSame(ConnectionState::Open, $client->state());
        self::assertSame($failure === 'handler after a reconnect' ? 1 : 0, $client->statistics()->reconnects);
        self::assertCount(1, $transport->controlLinesStartingWith(self::collectionPublishPrefix($operation)), 'sent once');
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
     * Starts the collection $operation for up to $max messages within $ms: a requestMany() on svc.many, or a pull of S/C
     * by fetchBatch() or fetchNext(), whose pull expires after $ms and whose deadline is a second later.
     *
     * @return Future<list<string>> The payloads it returns.
     */
    private static function startCollection(string $operation, NatsClient $client, int $max, int $ms): Future
    {
        return match ($operation) {
            'requestMany' => $client->requestMany('svc.many', 'y', null, $max, $ms)->map(self::payloads(...)),
            'fetchBatch' => $client->jetStream()->fetchBatch('S', 'C', $max, $ms)->map(self::payloads(...)),
            'fetchNext' => $client->jetStream()->fetchNext('S', 'C', $ms)->map(static fn(NatsMessage $message): array => [$message->payload]),
            default => throw new \LogicException('Unknown collection ' . $operation),
        };
    }

    /** How the control line of the request or the pull that the collection $operation sends starts. */
    private static function collectionPublishPrefix(string $operation): string
    {
        return $operation === 'requestMany' ? 'PUB svc.many ' : 'PUB $JS.API.CONSUMER.MSG.NEXT.S.C ';
    }

    /**
     * Waits until the collection $operation has sent its request or its pull and its read is on the socket, and returns
     * where it receives what it waits for: the request's reply subject or the pull's inbox, with that subject's sid.
     *
     * @return array{string, int}
     */
    private function awaitCollectionRead(ReconnectingTransport $transport, WatchedTransport $watched, string $operation): array
    {
        $this->waitUntil(static fn(): bool => self::collectionInbox($transport, $operation) !== null && $watched->readsUnderWay === 1);
        $inbox = self::collectionInbox($transport, $operation);
        self::assertNotNull($inbox);

        return $inbox;
    }

    /** @return array{string, int}|null The reply subject of the collection's request or pull, and its sid. */
    private static function collectionInbox(ReconnectingTransport $transport, string $operation): ?array
    {
        foreach ($transport->controlLinesStartingWith(self::collectionPublishPrefix($operation)) as $line) {
            $replyTo = explode(' ', $line)[2] ?? '';
            $sid = $transport->sidFor($replyTo);
            if ($sid !== null) {
                return [$replyTo, $sid];
            }
        }

        return null;
    }

    /**
     * A message with $payload for the collection $operation, on its $inbox: a reply, or a pulled message with its ack
     * subject.
     *
     * @param array{string, int} $inbox
     */
    private static function collectionFrame(string $operation, array $inbox, string $payload): string
    {
        return $operation === 'requestMany'
            ? ReconnectingTransport::msgFrame($inbox[0], $inbox[1], $payload)
            : ReconnectingTransport::msgFrame('evt.s', $inbox[1], $payload, self::ACK_SUBJECT);
    }

    /** Delivers $frame to the collection reading the socket, and waits until it took it and its next read is on. */
    private function pushToCollection(ReconnectingTransport $transport, WatchedTransport $watched, string $frame): void
    {
        $reads = $watched->reads;
        $transport->pushFrame($frame);
        $this->waitUntil(static fn(): bool => $watched->reads > $reads && $watched->readsUnderWay === 1);
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
        $client = $this->own(new NatsClient($this->options(true, 2_000, 1_000, 0, 2, null, 5, 20, null), $watched));
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
