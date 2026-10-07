<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Core\SubscriptionQueue;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Tests\Support\HeldUpDelivery;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;

/**
 * The reconnect that an operation's own read starts ({@see OperationReadReconnectTest}, #178) through its life and
 * beside other fibers. The read waits for it only within the operation's own wait, so:
 *
 *  - the delivery that brings the operation's result ends the wait at whatever moment of the reconnect it comes: any
 *    number of event-loop hops after the drop, as the first dial goes through, or during the subscription replay;
 *  - the operation's deadline ends it wherever it lands around the reconnect's end, and a request afterwards succeeds;
 *  - two reads that fail together, the heartbeat failing with them or not, start one reconnect, and each ends at its
 *    own deadline; a read that fails while the heartbeat's reconnect is under way joins it within its wait;
 *  - a disconnect() or a drain() during the reconnect wins, and the operation ends with the close;
 *  - a stall of the event loop around the drop, or in a listener, changes none of it;
 *  - a reconnect nobody waits for any more gives up on its own terms, its Closed event saying why; an operation that
 *    still waits gets the exhaustion error with its cause, after a Closed listener that suspends;
 *  - a poll that waits for all of the reconnect returns after the Reconnected listener (README reconnect item 8);
 *  - the application's read parked behind the operation's still waits for the whole reconnect, a listener's poll whose
 *    read finds the new connection gone reconnects it again, a poller rides out three outages in a row, the pull
 *    consumer re-issues its pull right after the reconnect, and an operation parked on the reconnect costs nothing.
 *
 * Over the scripted server in tests/Support/ReconnectingTransport.php seen through tests/Support/WatchedTransport.php,
 * which tells the test when the operation's read is on the socket, with deliveries held up by
 * tests/Support/HeldUpDelivery.php. Where the order of events can decide, it does: the second session is not up yet
 * (epoch 0), or the connection is still Connecting, when an operation returns before the reconnect is over.
 */
final class OperationReadReconnectLifecycleTest extends TestCase
{
    use ReconnectScenarios;

    /** The payload each operation waits for. */
    private const RESULTS = [
        'next' => 'job-1',
        'request' => 'reply-1',
        'fetchBatch' => 'm1',
        'pullConsumer' => 'm1',
    ];

    private const ACK_SUBJECT = '$JS.ACK.S.C.1.1.1.0.0';

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /**
     * @return iterable<string, array{string, int}> Only request() here: its reply shares the mux inbox, so a reply
     *         still in flight when the connection dropped is delivered during the outage whatever the hops. An own-sid
     *         operation's queued message is taken before the drop (#179), so it cannot be the subject of a during-outage
     *         delivery at various hops; its reconnect-gets-result path is
     *         {@see OperationReadReconnectTest::testAnOwnSidOperationWhoseOwnReadRunsTheReconnectGetsItsResultOnTheNewConnection()}.
     */
    public static function hopsAfterTheDrop(): iterable
    {
        foreach (['request' => [0, 8]] as $operation => $hops) {
            foreach ($hops as $count) {
                yield sprintf('%s, the delivery goes on %d hop(s) after the drop', $operation, $count) => [$operation, $count];
            }
        }
    }

    /**
     * The reply, held up in a lower-sid handler with the operation's reply queued behind it on the mux inbox, goes on
     * $hops event-loop hops after the connection drops: before the operation's read has met the EOF, while it reports
     * it, or once it waits for the reconnect it started. The operation gets its reply while the second session is still
     * refused (dials refused for 0.5 s), and the connection comes back on its own.
     */
    #[DataProvider('hopsAfterTheDrop')]
    public function testTheDeliveryThatBringsTheResultEndsTheWaitWhateverTheHopsAfterTheDrop(string $operation, int $hops): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->lifecycleClient($watched, $recorder);
        $hold = new HeldUpDelivery(fallbackSeconds: 5.0);
        $slowSid = $client->subscribe('slow', $hold->handler())->await();
        $lead = ReconnectingTransport::msgFrame('slow', $slowSid, 's');
        self::scriptServer($transport, $lead);
        $client->request('svc.warm', 'x', 1_000)->await();
        $queue = $operation === 'next' ? $client->subscribeQueue('jobs')->await() : null;
        $stop = new DeferredCancellation();
        self::startApplicationReadLoop($client, $stop);
        self::awaitRead($watched);
        if ($queue !== null) {
            $transport->pushFrame($lead . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
        }

        $start = hrtime(true);
        $result = self::startOperation($operation, $client, $queue, 2.0);
        $hold->began->getFuture()->await(new TimeoutCancellation(3));
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->refuseDials();
        $transport->dropConnection();
        self::afterHops($hops, static fn() => $hold->end('the test'));
        $this->acceptDialsAfter($transport, 0.5);
        [$payloads, $elapsed, $error] = $this->settle($result, $start);
        $epochOnReturn = $transport->epoch();
        $stop->cancel();

        self::assertNull($error, sprintf('%s threw %s', $operation, $error?->getMessage() ?? ''));
        self::assertSame([self::RESULTS[$operation]], $payloads);
        self::assertSame(0, $epochOnReturn, sprintf('%s returned after %.3f s, once the reconnect was over', $operation, $elapsed));
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open && $client->statistics()->reconnects === 1, 4.0);
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected], $recorder->events);
    }

    /**
     * @return iterable<string, array{string, string}> Only request() here: its reply shares the mux inbox, so a reply
     *         still in flight is delivered during the outage as the reconnect writes its CONNECT or its SUB. An own-sid
     *         operation's queued message is taken before the drop (#179), so its reconnect-gets-result path is the
     *         new-connection one, {@see OperationReadReconnectTest::testAnOwnSidOperationWhoseOwnReadRunsTheReconnectGetsItsResultOnTheNewConnection()}.
     */
    public static function momentsOfTheReconnect(): iterable
    {
        foreach (['request'] as $operation) {
            yield $operation . ', as the first dial goes through' => [$operation, 'CONNECT'];
            yield $operation . ', during the subscription replay' => [$operation, 'SUB '];
        }
    }

    /**
     * The delivery goes on as the reconnect's first successful dial writes its CONNECT, or as the reconnect replays its
     * first SUB, with the operation's read waiting for the reconnect: the operation gets its message, within the
     * outage's 0.3 s and a little more, and the reconnect completes. Guard: nothing here distinguishes the fixed read
     * from the old one, which also looked again once the reconnect was over.
     */
    #[DataProvider('momentsOfTheReconnect')]
    public function testTheDeliveryThatBringsTheResultEndsTheWaitDuringTheDialOrTheReplay(string $operation, string $written): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->lifecycleClient($watched, $recorder);
        $hold = new HeldUpDelivery(fallbackSeconds: 5.0);
        $slowSid = $client->subscribe('slow', $hold->handler())->await();
        $lead = ReconnectingTransport::msgFrame('slow', $slowSid, 's');
        self::scriptServer($transport, $lead);
        $client->request('svc.warm', 'x', 1_000)->await();
        $queue = $operation === 'next' ? $client->subscribeQueue('jobs')->await() : null;
        $stop = new DeferredCancellation();
        self::startApplicationReadLoop($client, $stop);
        self::awaitRead($watched);
        if ($queue !== null) {
            $transport->pushFrame($lead . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
        }

        $start = hrtime(true);
        $result = self::startOperation($operation, $client, $queue, 2.0);
        $hold->began->getFuture()->await(new TimeoutCancellation(3));
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        self::alsoAfterWrite($transport, static function (string $bytes) use ($transport, $written, $hold): void {
            if ($transport->epoch() === 1 && str_starts_with($bytes, $written)) {
                $hold->end('the test');
            }
        });
        $transport->refuseDials();
        $transport->dropConnection();
        $this->acceptDialsAfter($transport, 0.3);
        [$payloads, $elapsed, $error] = $this->settle($result, $start);
        $stop->cancel();

        self::assertNull($error, sprintf('%s threw %s', $operation, $error?->getMessage() ?? ''));
        self::assertSame([self::RESULTS[$operation]], $payloads);
        self::assertSame('the test', $hold->endedBy, 'the delivery went on as the reconnect wrote ' . trim($written));
        self::assertLessThan(1.5, $elapsed, sprintf('%s returned after %.3f s, with a 2 s deadline and a 0.3 s outage', $operation, $elapsed));
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open && $client->statistics()->reconnects === 1, 4.0);
        self::assertSame(1, $transport->epoch());
    }

    /** @return iterable<string, array{string, float, bool}> */
    public static function deadlinesAroundTheReconnectsEnd(): iterable
    {
        yield 'next(), its deadline 0.1 s before the outage ends' => ['next', -0.1, false];
        yield 'next(), its deadline as the outage ends' => ['next', 0.0, false];
        yield 'request(), its deadline as the outage ends' => ['request', 0.0, false];
        yield 'next(), its deadline 0.1 s after the outage ends, a message on the new connection' => ['next', 0.1, true];
        yield 'request(), its deadline 0.1 s after the outage ends' => ['request', 0.1, false];
    }

    /**
     * Nothing is delivered during the outage (0.8 s, dials refused), and the operation's deadline lands just before,
     * at, or just after its end. The operation ends within a few hundred milliseconds of its deadline either way: with
     * its timeout, or, once the connection is back, with the message the new connection brings. A request afterwards
     * succeeds on the second session.
     */
    #[DataProvider('deadlinesAroundTheReconnectsEnd')]
    public function testTheDeadlineLandingAroundTheReconnectsEnd(string $operation, float $offset, bool $messageOnTheNewConnection): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->lifecycleClient($watched, $recorder);
        $answer = self::scriptServer($transport, '', answering: false);
        $client->request('svc.warm', 'x', 1_000)->await();
        $queue = $operation === 'next' ? $client->subscribeQueue('jobs')->await() : null;
        $outage = 0.8;
        $deadline = $outage + $offset;
        if ($messageOnTheNewConnection && $queue !== null) {
            // Right behind the first SUB the reconnect replays, read by the reconnect itself.
            $pushed = false;
            self::alsoAfterWrite($transport, static function (string $bytes) use ($transport, $queue, &$pushed): void {
                if (!$pushed && $transport->epoch() === 1 && str_starts_with($bytes, 'SUB ')) {
                    $pushed = true;
                    $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-late'));
                }
            });
        }

        $start = hrtime(true);
        $result = self::startOperation($operation, $client, $queue, $deadline);
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->refuseDials();
        $transport->dropConnection();
        $this->acceptDialsAfter($transport, $outage);
        [$payloads, $elapsed, $error] = $this->settle($result, $start);

        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open, 5.0);
        $answer();
        $reply = $client->request('svc.echo', 'again', 2_000)->await()->payload;
        $summary = sprintf('%s with its deadline %+.1f s from the outage end: %s after %.3f s%s', $operation, $offset, json_encode($payloads), $elapsed, $error === null ? '' : ' (' . $error::class . ': ' . $error->getMessage() . ')');

        self::assertLessThan($deadline + 0.6, $elapsed, $summary);
        self::assertGreaterThan(min($deadline, $outage) - 0.3, $elapsed, $summary);
        if ($error !== null) {
            self::assertInstanceOf(TimeoutException::class, $error, $summary);
        }
        self::assertSame($messageOnTheNewConnection ? ['job-late'] : [], $payloads, $summary);
        self::assertSame('reply-1', $reply, $summary);
        self::assertSame(1, $client->statistics()->reconnects, $summary);
        self::assertSame(1, $transport->epoch(), $summary);
    }

    /** @return iterable<string, array{string}> */
    public static function heartbeatFailures(): iterable
    {
        yield 'the heartbeat PING is written, its read waits behind the poll' => ['pingWritten'];
        yield 'the heartbeat PING write fails with the drop' => ['pingWriteFails'];
        yield 'no heartbeat' => ['none'];
    }

    /**
     * A poll's read is on the socket and a request's read is parked behind it when the connection drops, with the
     * heartbeat's PING just out and unanswered, failing its write with the drop, or off. One reconnect is started, by
     * whichever noticed first, and both operations end at their own 0.5 s deadlines while it runs on (dials refused
     * for 1 s), the poll with nothing and the request with its timeout; a request afterwards succeeds.
     */
    #[DataProvider('heartbeatFailures')]
    public function testTwoReadsThatFailTogetherStartOneReconnect(string $heartbeat): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->lifecycleClient($watched, $recorder, pingIntervalSeconds: $heartbeat === 'none' ? 0 : 0.05);
        $answer = self::scriptServer($transport, '', answering: false);
        $client->request('svc.warm', 'x', 1_000)->await();
        $queue = $client->subscribeQueue('jobs')->await();
        $transport->answerPings = $heartbeat !== 'pingWritten';

        $start = hrtime(true);
        $poll = self::startOperation('next', $client, $queue, 0.5);
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        // The request's read is parked behind the poll's.
        $request = self::startOperation('request', $client, null, 0.5);
        delay(0.02);
        self::assertSame(1, $watched->readsUnderWay);

        $transport->refuseDials();
        if ($heartbeat === 'pingWriteFails') {
            $transport->failNextWriteContaining('PING');
            $this->waitUntil(static fn(): bool => !$transport->sessionLive(), 1.0);
        } else {
            $transport->dropConnection();
        }
        $this->acceptDialsAfter($transport, 1.0);
        [$pollPayloads, $pollElapsed, $pollError] = $this->settle($poll, $start);
        [, $requestElapsed, $requestError] = $this->settle($request, $start);
        $stateOnReturn = $client->state();

        self::assertSame([], $pollPayloads);
        self::assertNull($pollError, sprintf('the poll threw %s', $pollError?->getMessage() ?? ''));
        self::assertInstanceOf(TimeoutException::class, $requestError);
        self::assertSame(ConnectionState::Connecting, $stateOnReturn, 'both ended while the reconnect ran on');
        self::assertLessThan(1.5, $pollElapsed, sprintf('the poll ended after %.3f s, with a 0.5 s deadline', $pollElapsed));
        self::assertLessThan(1.5, $requestElapsed, sprintf('the request ended after %.3f s, with a 0.5 s deadline', $requestElapsed));

        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open, 4.0);
        $answer();
        self::assertSame('reply-1', $client->request('svc.echo', 'again', 2_000)->await()->payload);
        self::assertSame(1, $client->statistics()->reconnects, 'one reconnect for the two failed reads and the heartbeat');
        self::assertSame(1, $transport->epoch());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected], $recorder->events);
    }

    /** @return iterable<string, array{string, string}> */
    public static function closesDuringTheReconnect(): iterable
    {
        foreach (['next', 'request'] as $operation) {
            yield $operation . ', disconnect()' => [$operation, 'disconnect'];
            yield $operation . ', drain()' => [$operation, 'drain'];
        }
    }

    /**
     * The reconnect an operation's read started is backing off between refused dials when the application closes the
     * connection. disconnect() stops the reconnect, which does not dial again, and the operation, with a 3 s deadline,
     * ends with the close, not at its deadline. drain() waits for the reconnect within its budget (dials go through
     * 0.5 s later), drains the new connection and closes it, and the operation ends with that close. Either way the
     * connection is Closed, announced once.
     */
    #[DataProvider('closesDuringTheReconnect')]
    public function testACloseDuringTheReconnectAnOperationsReadStartedWins(string $operation, string $close): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->lifecycleClient($watched, $recorder);
        self::scriptServer($transport, '', answering: false);
        $client->request('svc.warm', 'x', 1_000)->await();
        $queue = $operation === 'next' ? $client->subscribeQueue('jobs')->await() : null;

        $start = hrtime(true);
        $result = self::startOperation($operation, $client, $queue, 3.0);
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->refuseDials();
        $transport->dropConnection();
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Connecting);
        delay(0.1);
        $dialsBeforeClose = count($transport->connectCalls);

        if ($close === 'drain') {
            $this->acceptDialsAfter($transport, 0.5);
            $client->drain()->await(new TimeoutCancellation(5));
        } else {
            $client->disconnect()->await(new TimeoutCancellation(3));
        }
        [$payloads, $elapsed, $error] = $this->settle($result, $start);
        $dialsAtClose = count($transport->connectCalls);
        delay(0.3);

        self::assertSame(ConnectionState::Closed, $client->state());
        self::assertSame([], $payloads);
        self::assertNotNull($error, sprintf('%s must end with the close', $operation));
        self::assertLessThan(2.0, $elapsed, sprintf('%s ended after %.3f s: with the close, not at its 3 s deadline', $operation, $elapsed));
        self::assertSame(1, $recorder->closedEvents(), 'one Closed event');
        self::assertSame($dialsAtClose, count($transport->connectCalls), 'nothing dials on after the close');
        if ($close === 'disconnect') {
            self::assertSame($dialsBeforeClose, $dialsAtClose, 'the stopped reconnect did not dial again');
        } else {
            self::assertSame(1, $transport->epoch(), 'the drain waited for the reconnect and drained the new connection');
        }
    }

    /**
     * A flush's read fails while its error listener awaits, as one logging to a remote service does; meanwhile the
     * heartbeat's PING write fails and the heartbeat starts the reconnect. Back from the listener, the flush's read
     * finds a reconnect in flight and joins it within its wait: the reconnect's first attempt ends the flush's PONG,
     * and the flush fails with "Connection lost before the server answered the PING" while the reconnect, with dials
     * refused for 1 s, runs on. One reconnect in all.
     */
    public function testAFlushWhoseFailedReadJoinsAReconnectTheHeartbeatStartedMeanwhile(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $reports = 0;
        $errorListener = static function (\Throwable $error) use (&$reports, $recorder): void {
            ($recorder->errorListener())($error);
            if (++$reports === 1) {
                delay(0.15);
            }
        };
        $client = $this->lifecycleClient(new WatchedTransport($transport), $recorder, pingIntervalSeconds: 0.05, errorListener: $errorListener);
        $transport->answerPings = false;
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
            $client->flush()->await(new TimeoutCancellation(5));
            self::fail('expected the flush to fail');
        } catch (ConnectionException $e) {
            self::assertSame('Connection lost before the server answered the PING', $e->getMessage());
        }
        $elapsed = $this->secondsSince($start);
        $stateOnFailure = $client->state();

        self::assertSame(ConnectionState::Connecting, $stateOnFailure, 'the flush failed while the reconnect ran on');
        self::assertLessThan(0.7, $elapsed, sprintf('the flush failed after %.3f s, with a 1 s outage', $elapsed));
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open, 4.0);
        self::assertSame(1, $client->statistics()->reconnects);
        self::assertSame(1, $transport->epoch());
        self::assertSame(['Socket closed by peer (EOF)'], $recorder->errors, 'the lost connection is reported once; the flush\'s own failure is thrown, not reported');
    }

    /**
     * The heartbeat's PING write fails while a poll's read is on the socket: the heartbeat starts the reconnect, and
     * closing the socket fails the poll's read, which joins that reconnect within the poll's 0.5 s deadline (dials
     * refused for 1 s): the poll returns nothing at its deadline while the reconnect runs on.
     */
    public function testAPollWhoseReadFailsWhileTheHeartbeatsReconnectIsUnderWayJoinsItWithinItsDeadline(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->lifecycleClient($watched, $recorder, pingIntervalSeconds: 0.1);
        $queue = $client->subscribeQueue('jobs')->await();
        $transport->refuseDials();
        $transport->failNextWriteContaining('PING');
        $this->acceptDialsAfter($transport, 1.0);

        $start = hrtime(true);
        $poll = async(static fn(): ?NatsMessage => $queue->setTimeout(0.5)->next());
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $message = $poll->await(new TimeoutCancellation(5));
        $elapsed = $this->secondsSince($start);
        $stateOnReturn = $client->state();

        self::assertNull($message);
        self::assertSame(ConnectionState::Connecting, $stateOnReturn, 'the poll returned while the reconnect ran on');
        self::assertLessThan(1.5, $elapsed, sprintf('the poll returned after %.3f s, with a 0.5 s deadline', $elapsed));
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open, 3.0);
        self::assertSame(1, $client->statistics()->reconnects);
        self::assertSame(1, $transport->epoch());
    }

    /** @return iterable<string, array{string}> */
    public static function stalls(): iterable
    {
        yield 'right after the drop' => ['afterDrop'];
        yield 'in the error listener reporting the EOF' => ['errorListener'];
        yield 'in the Disconnected listener' => ['disconnectedListener'];
    }

    /**
     * The event loop stalls for 0.2 s - the test blocks it right after the drop, or a listener does - while the poll's
     * own read runs the reconnect it started (0.6 s, dials refused). Nothing is queued for the poll before the drop
     * (with #179 a queued own-sid message would be taken before the drop, so it cannot be the subject of a during-outage
     * delivery), so the poll's own read is the one that notices the drop and its message arrives on the new connection
     * once the SUB is replayed: the poll gets it once the loop runs again and the connection is back, and the reconnect
     * completes, whatever the stall.
     */
    #[DataProvider('stalls')]
    public function testAStallOfTheEventLoopAroundTheDropChangesNothing(string $point): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $stalled = false;
        $stall = static function () use (&$stalled): void {
            if (!$stalled) {
                $stalled = true;
                usleep(200_000);
            }
        };
        $errorListener = static function (\Throwable $error) use ($recorder, $point, $stall): void {
            ($recorder->errorListener())($error);
            if ($point === 'errorListener') {
                $stall();
            }
        };
        $connectionListener = static function (ConnectionEvent $event, ?\Throwable $error = null) use ($recorder, $point, $stall): void {
            ($recorder->connectionListener())($event, $error);
            if ($point === 'disconnectedListener' && $event === ConnectionEvent::Disconnected) {
                $stall();
            }
        };
        $client = $this->lifecycleClient($watched, $recorder, errorListener: $errorListener, connectionListener: $connectionListener);
        $queue = $client->subscribeQueue('jobs')->await();
        // Nothing is queued for the poll before the drop; its message arrives on the new connection once its SUB is
        // replayed. The poll's own read is the sole reader, so it is the one that notices the drop and runs the reconnect.
        $jobsSent = false;
        $transport->afterWrite = static function (string $bytes) use ($transport, $queue, &$jobsSent): void {
            if (!$jobsSent && $transport->epoch() === 1 && str_contains($bytes, 'SUB jobs ')) {
                $jobsSent = true;
                $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
            }
        };

        $start = hrtime(true);
        $result = self::startOperation('next', $client, $queue, 2.5);
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->refuseDials();
        $transport->dropConnection();
        if ($point === 'afterDrop') {
            $stall();
        }
        $this->acceptDialsAfter($transport, 0.6);
        [$payloads, $elapsed, $error] = $this->settle($result, $start);
        $epochOnReturn = $transport->epoch();

        self::assertTrue($stalled, 'the loop stalled ' . $point);
        self::assertNull($error, sprintf('the poll threw %s', $error?->getMessage() ?? ''));
        self::assertSame(['job-1'], $payloads);
        self::assertSame(1, $epochOnReturn, sprintf('the poll returned after %.3f s, once the reconnect reopened the connection', $elapsed));
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open && $client->statistics()->reconnects === 1, 4.0);
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected], $recorder->events);
    }

    /**
     * A poll's read starts a reconnect of three attempts, 300 ms apart, with dials refused, and the poll times out at
     * 0.3 s, long before the reconnect gives up: the reconnect fails on its own terms, the Closed event says why, and
     * a request afterwards is refused at once.
     */
    public function testAReconnectNobodyWaitsForAnyMoreGivesUpOnItsOwnTerms(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->lifecycleClient($watched, $recorder, reconnectDelayMs: 300, reconnectMaxDelayMs: 300, maxReconnectAttempts: 3);
        $queue = $client->subscribeQueue('jobs')->await();

        $start = hrtime(true);
        $poll = async(static fn(): ?NatsMessage => $queue->setTimeout(0.3)->next());
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->refuseDials();
        $transport->dropConnection();
        $message = $poll->await(new TimeoutCancellation(5));
        $elapsed = $this->secondsSince($start);
        $stateOnReturn = $client->state();

        self::assertNull($message);
        self::assertSame(ConnectionState::Connecting, $stateOnReturn, 'the poll ended while the reconnect ran on');
        self::assertLessThan(0.8, $elapsed, sprintf('the poll returned after %.3f s, with a 0.3 s deadline', $elapsed));

        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Closed, 5.0);
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Closed], $recorder->events, 'the reconnect gave up on its own');
        self::assertSame(['Connection refused'], $recorder->closedErrors, 'the Closed event says why');
        self::assertCount(4, $transport->connectCalls, 'the first dial and three reconnect attempts');
        try {
            $client->request('svc.echo', 'x', 500)->await();
            self::fail('expected the request to be refused');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }
    }

    /** @return iterable<string, array{float, float, string}> */
    public static function closedListenerDelays(): iterable
    {
        yield 'the listener suspends 0.3 s, the deadline is 3 s: the exhaustion error' => [0.3, 3.0, 'exhausted'];
        yield 'the listener suspends 0.8 s, the deadline is 0.3 s: the poll times out' => [0.8, 0.3, 'timeout'];
    }

    /**
     * The reconnect a poll's read started gives up (three attempts, dials refused) and its Closed listener suspends.
     * A poll whose deadline outlasts the listener gets "Reconnect attempts exhausted" with its cause (#172), after
     * the listener returned; one whose deadline comes first times out with nothing instead, the connection Closed by
     * then: the exhaustion error reaches only an operation that still waits for the reconnect.
     */
    #[DataProvider('closedListenerDelays')]
    public function testAClosedListenerThatSuspendsWhileTheReconnectGivesUp(float $listenerDelay, float $deadline, string $expected): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $record = $recorder->connectionListener();
        $listenerReturned = false;
        $listener = static function (ConnectionEvent $event, ?\Throwable $error = null) use ($record, $listenerDelay, &$listenerReturned): void {
            $record($event, $error);
            if ($event === ConnectionEvent::Closed) {
                delay($listenerDelay);
                $listenerReturned = true;
            }
        };
        $client = $this->lifecycleClient($watched, $recorder, connectionListener: $listener, maxReconnectAttempts: 3);
        $queue = $client->subscribeQueue('jobs')->await();

        $poll = async(static fn(): ?NatsMessage => $queue->setTimeout($deadline)->next());
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->refuseDials();
        $transport->dropConnection();
        $error = null;
        $message = null;
        try {
            $message = $poll->await(new TimeoutCancellation(8));
        } catch (\Throwable $e) {
            $error = $e;
        }
        $returnedAfterListener = $listenerReturned;

        self::assertSame(ConnectionState::Closed, $client->state());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Closed], $recorder->events);
        if ($expected === 'exhausted') {
            self::assertInstanceOf(ConnectionException::class, $error);
            self::assertSame('Reconnect attempts exhausted', $error->getMessage());
            self::assertNotNull($error->getPrevious(), 'with its cause');
            self::assertTrue($returnedAfterListener, 'the poll waited for the Closed listener');
        } else {
            self::assertNull($error, sprintf('the poll threw %s instead of timing out', $error?->getMessage() ?? ''));
            self::assertNull($message);
            self::assertFalse($returnedAfterListener, 'the poll timed out before the Closed listener returned');
        }
        $this->waitUntil(static function () use (&$listenerReturned): bool {
            return $listenerReturned;
        }, 3.0);
        self::assertCount(4, $transport->connectCalls, 'the first dial and three reconnect attempts');
    }

    /**
     * README reconnect item 8: an operation whose own read started the reconnect, and which waits for it to its end,
     * returns after the Reconnected listener. The listener pushes the poll's message and then suspends; the poll
     * returns the message once the listener returned.
     */
    public function testAPollThatWaitsForAllOfTheReconnectReturnsAfterTheReconnectedListener(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $record = $recorder->connectionListener();
        $listenerReturned = false;
        $sidHolder = new class {
            public int $sid = 0;
        };
        $listener = static function (ConnectionEvent $event, ?\Throwable $error = null) use ($record, $transport, $sidHolder, &$listenerReturned): void {
            $record($event, $error);
            if ($event !== ConnectionEvent::Reconnected) {
                return;
            }

            $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $sidHolder->sid, 'job-1'));
            delay(0.2);
            $listenerReturned = true;
        };
        $client = $this->lifecycleClient($watched, $recorder, connectionListener: $listener);
        $queue = $client->subscribeQueue('jobs')->await();
        $sidHolder->sid = $queue->sid;

        $poll = async(static fn(): ?NatsMessage => $queue->setTimeout(3.0)->next());
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->refuseDials();
        $transport->dropConnection();
        $this->acceptDialsAfter($transport, 0.2);
        $message = $poll->await(new TimeoutCancellation(5));
        $returnedAfterListener = $listenerReturned;

        self::assertSame('job-1', $message?->payload);
        self::assertTrue($returnedAfterListener, 'the poll returned after the Reconnected listener');
        self::assertSame(ConnectionState::Open, $client->state());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected], $recorder->events);
    }

    /**
     * A request issued during the reconnect a poll's read started waits for it within its own 0.3 s timeout and times
     * out while the reconnect runs on; the poll, whose 3 s deadline outlasts the outage, gets the message the new
     * connection brings.
     */
    public function testAnOperationIssuedDuringTheReconnectAPollsReadStartedWaitsWithinItsOwnTimeout(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->lifecycleClient($watched, $recorder);
        $queue = $client->subscribeQueue('jobs')->await();

        $poll = async(static fn(): ?NatsMessage => $queue->setTimeout(3.0)->next());
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->refuseDials();
        $transport->dropConnection();
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Connecting);

        $start = hrtime(true);
        try {
            $client->request('svc.echo', 'x', 300)->await();
            self::fail('expected the request to time out');
        } catch (TimeoutException $e) {
            self::assertStringContainsString('Request timed out for subject svc.echo', $e->getMessage());
        }
        $elapsed = $this->secondsSince($start);
        $stateOnTimeout = $client->state();

        self::assertSame(ConnectionState::Connecting, $stateOnTimeout, 'the request timed out while the reconnect ran on');
        self::assertLessThan(1.0, $elapsed, sprintf('the request timed out after %.3f s, with a 0.3 s timeout', $elapsed));

        $transport->acceptDials();
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open, 3.0);
        $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-1'));
        self::assertSame('job-1', $poll->await(new TimeoutCancellation(5))?->payload);
    }

    /**
     * The application's processIncoming(), which has no wake-up, is parked behind the poll's read when the connection
     * drops: the poll ends at its 0.5 s deadline while the reconnect runs on (dials refused for 1 s), and the
     * application's read returns when the poll's read ends, as a read parked behind another fiber's read does. The
     * application's next read joins the reconnect and returns only once it is over, with the connection Open, then
     * reads the new connection.
     */
    public function testTheApplicationsReadParkedBehindTheOperationsReadWaitsForTheWholeReconnect(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->lifecycleClient($watched, $recorder);
        $queue = $client->subscribeQueue('jobs')->await();

        $start = hrtime(true);
        $poll = self::startOperation('next', $client, $queue, 0.5);
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $parkedRead = $client->processIncoming(new TimeoutCancellation(5));
        delay(0.02);
        $transport->refuseDials();
        $transport->dropConnection();
        $this->acceptDialsAfter($transport, 1.0);
        [$pollPayloads, $pollElapsed, $pollError] = $this->settle($poll, $start);
        $stateAtThePollsEnd = $client->state();
        $parkedFrames = $parkedRead->await(new TimeoutCancellation(6));
        $stateAtTheParkedReadsEnd = $client->state();
        async(function () use ($client, $transport, $queue): void {
            $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open && $client->statistics()->reconnects === 1, 4.0);
            $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-after'));
        })->ignore();
        $nextFrames = $client->processIncoming(new TimeoutCancellation(6))->await();
        $stateAtTheNextReadsEnd = $client->state();

        self::assertNull($pollError, sprintf('the poll threw %s', $pollError?->getMessage() ?? ''));
        self::assertSame([], $pollPayloads);
        self::assertSame(ConnectionState::Connecting, $stateAtThePollsEnd, 'the poll ended while the reconnect ran on');
        self::assertLessThan(1.5, $pollElapsed, sprintf('the poll ended after %.3f s, with a 0.5 s deadline', $pollElapsed));
        self::assertSame(0, $parkedFrames, 'the parked read returned when the read it waited for ended');
        self::assertSame(ConnectionState::Connecting, $stateAtTheParkedReadsEnd);
        self::assertSame(ConnectionState::Open, $stateAtTheNextReadsEnd, 'the next read waited for the whole reconnect');
        self::assertSame(1, $nextFrames, 'and read the new connection');
        self::assertSame(1, $client->statistics()->reconnects);
        self::assertSame('job-after', $queue->setTimeout(0.2)->next()?->payload);
    }

    /**
     * A Reconnected listener polls, and the new connection drops as soon as the poll's read is on the socket: that
     * read is the first to notice, starts a second reconnect in a fiber of its own and waits within the poll's
     * timeout. The second open is announced from the event loop, since the listener call is still under way, and
     * the poll gets the message the third connection brings; the listener hears Disconnected and Reconnected again
     * while its call is still running.
     */
    public function testAPollFromAReconnectedListenerWhoseOwnReadFindsTheNewConnectionGoneReconnectsItAgain(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $record = $recorder->connectionListener();
        $outcome = new class {
            public ?string $payload = null;
            public ?\Throwable $error = null;
            public int $calls = 0;
        };
        $holder = new class {
            public ?NatsClient $client = null;
            public ?SubscriptionQueue $queue = null;
        };
        $listener = function (ConnectionEvent $event, ?\Throwable $error = null) use ($record, $outcome, $holder, $transport, $watched): void {
            $record($event, $error);
            $queue = $holder->queue;
            $client = $holder->client;
            if ($event !== ConnectionEvent::Reconnected || $queue === null || $client === null || ++$outcome->calls > 1) {
                return;
            }

            // The new connection dies as soon as the listener's poll has its read on the socket; once the reconnect
            // that read starts is over, a message on the third connection reaches the poll.
            $watched->onRead = static function () use ($transport, $watched): void {
                $watched->onRead = null;
                $transport->dropConnection();
            };
            async(function () use ($client, $transport, $queue): void {
                $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open && $client->statistics()->reconnects === 2, 4.0);
                $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-from-listener'));
            })->ignore();
            try {
                $message = $queue->setTimeout(3.0)->next();
                $outcome->payload = $message === null ? 'nothing' : $message->payload;
            } catch (\Throwable $e) {
                $outcome->error = $e;
            }
        };
        $client = $this->lifecycleClient($watched, $recorder, connectionListener: $listener);
        $holder->client = $client;
        $queue = $client->subscribeQueue('jobs')->await();
        $holder->queue = $queue;

        $poll = self::startOperation('next', $client, $queue, 1.0);
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->dropConnection();
        [$pollPayloads, , $pollError] = $this->settle($poll, hrtime(true));
        $this->waitUntil(static fn(): bool => $outcome->calls >= 1 && ($outcome->payload !== null || $outcome->error !== null), 6.0);
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open && $client->statistics()->reconnects === 2, 4.0);
        delay(0.05);

        self::assertNull($outcome->error, sprintf('the listener\'s poll threw %s', $outcome->error?->getMessage() ?? ''));
        self::assertSame('job-from-listener', $outcome->payload);
        self::assertNull($pollError, sprintf('the poll threw %s', $pollError?->getMessage() ?? ''));
        self::assertSame([], $pollPayloads, 'the poll that met the first drop timed out, with nothing delivered to it');
        self::assertSame(2, $client->statistics()->reconnects);
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected], $recorder->events);
    }

    /**
     * One poller through three outages in a row, its read the only one on the socket each time: every poll, with a
     * 0.3 s deadline, ends at its deadline while the reconnect (dials refused for 0.6 s) runs on, the connection comes
     * back each time, and a message on each new connection reaches the next poll.
     */
    public function testAPollThroughThreeOutagesInARow(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->lifecycleClient($watched, $recorder);
        $queue = $client->subscribeQueue('jobs')->await();

        for ($outage = 1; $outage <= 3; $outage++) {
            $readsBefore = $watched->reads;
            $start = hrtime(true);
            $poll = async(static fn(): ?NatsMessage => $queue->setTimeout(0.3)->next());
            $this->waitUntil(static fn(): bool => $watched->reads > $readsBefore && $watched->readsUnderWay === 1);
            $transport->refuseDials();
            $transport->dropConnection();
            $this->acceptDialsAfter($transport, 0.6);
            $message = $poll->await(new TimeoutCancellation(5));
            $elapsed = $this->secondsSince($start);
            $stateOnReturn = $client->state();

            self::assertNull($message, sprintf('outage %d: the poll got %s', $outage, $message === null ? 'nothing' : $message->payload));
            self::assertSame(ConnectionState::Connecting, $stateOnReturn, sprintf('outage %d: the poll ended while the reconnect ran on', $outage));
            self::assertLessThan(1.0, $elapsed, sprintf('outage %d: the poll ended after %.3f s, with a 0.3 s deadline', $outage, $elapsed));
            $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open && $client->statistics()->reconnects === $outage, 4.0);
            $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job-' . $outage));
            self::assertSame('job-' . $outage, $queue->setTimeout(1.0)->next()?->payload, sprintf('outage %d: the next poll reads the new connection', $outage));
        }

        self::assertSame(3, $client->statistics()->reconnects);
        self::assertSame(3, $transport->epoch());
        self::assertCount(7, $recorder->events, 'Connected, then Disconnected and Reconnected three times');
    }

    /**
     * The pull consumer's read is the first to notice the drop, with no message during the outage (0.5 s): once the
     * reconnect is over the read returns and the engine re-issues the pull the old server forgot right away (#120), so
     * the message the server has for the new pull reaches the handler right after the reconnect, not at the lost
     * pull's 4 s expiry.
     */
    public function testAPullConsumerWhoseOwnReadStartedTheReconnectReissuesItsPullRightAfterIt(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->lifecycleClient($watched, $recorder);
        $pulls = new class {
            /** @var list<int> The epoch of each pull. */
            public array $epochs = [];
        };
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport, $pulls): array {
            if ($replyTo === null) {
                return [];
            }
            if ($subject === 'svc.warm') {
                return $transport->replyFrame($replyTo, 'ok');
            }
            if (str_starts_with($subject, '$JS.API.CONSUMER.MSG.NEXT.')) {
                $pulls->epochs[] = $transport->epoch();
                $sid = $transport->sidFor($replyTo);
                // Only a pull on the second session finds a message.
                if ($transport->epoch() >= 1 && $sid !== null) {
                    return [ReconnectingTransport::msgFrame('evt.s', $sid, 'm-after', self::ACK_SUBJECT)];
                }
            }

            return [];
        };
        $client->request('svc.warm', 'x', 1_000)->await();

        $start = hrtime(true);
        $result = self::startOperation('pullConsumer', $client, null, 4.0);
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1 && $pulls->epochs !== []);
        $transport->refuseDials();
        $transport->dropConnection();
        $this->acceptDialsAfter($transport, 0.5);
        [$payloads, $elapsed, $error] = $this->settle($result, $start);

        self::assertNull($error, sprintf('the pull consumer threw %s', $error?->getMessage() ?? ''));
        self::assertSame(['m-after'], $payloads);
        self::assertSame([0, 1], $pulls->epochs, 'one pull on each session');
        self::assertLessThan(2.5, $elapsed, sprintf('the message came after %.3f s: right after the reconnect, not at the lost pull\'s 4 s expiry', $elapsed));
    }

    /** @return iterable<string, array{string}> */
    public static function parkedOperations(): iterable
    {
        yield 'SubscriptionQueue::next()' => ['next'];
        yield 'request()' => ['request'];
    }

    /**
     * An operation parked on the reconnect its read started, backing off 300 ms between refused dials, costs nothing
     * meanwhile: in a second, no read of the dead socket, a handful of dials and next to no CPU.
     */
    #[DataProvider('parkedOperations')]
    public function testAnOperationParkedOnTheReconnectItStartedDoesNotSpin(string $operation): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->lifecycleClient($watched, $recorder, reconnectDelayMs: 300, reconnectMaxDelayMs: 300);
        self::scriptServer($transport, '', answering: false);
        $client->request('svc.warm', 'x', 1_000)->await();
        $queue = $operation === 'next' ? $client->subscribeQueue('jobs')->await() : null;

        $result = self::startOperation($operation, $client, $queue, 3.0);
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->refuseDials();
        $transport->dropConnection();
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Connecting);
        delay(0.05);
        $readsBefore = $watched->reads;
        $dialsBefore = count($transport->connectCalls);
        $cpuBefore = self::cpuSeconds();
        delay(1.0);
        $cpu = self::cpuSeconds() - $cpuBefore;
        $reads = $watched->reads - $readsBefore;
        $dials = count($transport->connectCalls) - $dialsBefore;
        $transport->acceptDials();
        $this->settle($result, hrtime(true));
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open, 4.0);

        self::assertSame(0, $reads, 'nothing reads the dead socket while the reconnect backs off');
        self::assertLessThanOrEqual(6, $dials, sprintf('%d dials in a second of 300 ms backoffs', $dials));
        self::assertLessThan(0.15, $cpu, sprintf('%.3f s of CPU in a second parked', $cpu));
    }

    private function lifecycleClient(
        WatchedTransport $watched,
        LifecycleRecorder $recorder,
        int|float $pingIntervalSeconds = 0,
        ?\Closure $errorListener = null,
        ?\Closure $connectionListener = null,
        int $reconnectDelayMs = 5,
        int $reconnectMaxDelayMs = 20,
        int $maxReconnectAttempts = 1_000,
    ): NatsClient {
        $client = new NatsClient(
            $this->options(
                true,
                2_000,
                $maxReconnectAttempts,
                $pingIntervalSeconds,
                2,
                $connectionListener ?? $recorder->connectionListener(),
                $reconnectDelayMs,
                $reconnectMaxDelayMs,
                $errorListener ?? $recorder->errorListener(),
            ),
            $watched,
        );
        $this->opened[] = $client;
        $client->connect()->await();

        return $client;
    }

    /**
     * Scripts the server's answers on $transport: svc.warm gets "ok" at once; svc.echo gets reply-1 and the first pull
     * gets m1, each behind $lead in the same chunk, while the server answers. Returns a closure that makes it answer
     * when it did not before.
     *
     * @return \Closure(): void
     */
    private static function scriptServer(ReconnectingTransport $transport, string $lead, bool $answering = true): \Closure
    {
        $pulls = 0;
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport, $lead, &$answering, &$pulls): array {
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

            return match (true) {
                $subject === 'svc.echo' => [$lead . ReconnectingTransport::msgFrame($replyTo, $sid, 'reply-1')],
                str_starts_with($subject, '$JS.API.CONSUMER.MSG.NEXT.') => ++$pulls === 1
                    ? [$lead . ReconnectingTransport::msgFrame('evt.s', $sid, 'm1', self::ACK_SUBJECT)]
                    : [],
                default => [],
            };
        };

        return static function () use (&$answering): void {
            $answering = true;
        };
    }

    /**
     * Adds $hook to what $transport does after each write, ahead of what it did before.
     *
     * @param \Closure(string): void $hook
     */
    private static function alsoAfterWrite(ReconnectingTransport $transport, \Closure $hook): void
    {
        $previous = $transport->afterWrite;
        $transport->afterWrite = static function (string $bytes) use ($hook, $previous): void {
            $hook($bytes);
            if ($previous !== null) {
                $previous($bytes);
            }
        };
    }

    /**
     * Starts $operation with a deadline of $wait seconds. The pull consumer's pull expires after $wait seconds, and its
     * deadline is a second later, the slack it allows the server.
     *
     * @return Future<list<string>> The payloads the operation got.
     */
    private static function startOperation(string $operation, NatsClient $client, ?SubscriptionQueue $queue, float $wait): Future
    {
        $ms = (int) round($wait * 1000);

        return match ($operation) {
            'next' => async(static function () use ($queue, $wait): array {
                self::assertNotNull($queue);
                $message = $queue->setTimeout($wait)->next();

                return $message === null ? [] : [$message->payload];
            }),
            'request' => $client->request('svc.echo', 'hi', $ms)->map(static fn(NatsMessage $message): array => [$message->payload]),
            'fetchBatch' => $client->jetStream()->fetchBatch('S', 'C', 1, $ms)->map(self::payloads(...)),
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
            $payloads = $operation->await(new TimeoutCancellation(10));
        } catch (\Throwable $e) {
            $payloads = [];
            $error = $e;
        }

        return [$payloads, $this->secondsSince($start), $error];
    }

    /** Waits until a read takes the socket of $watched. */
    private static function awaitRead(WatchedTransport $watched): void
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
    private static function startApplicationReadLoop(NatsClient $client, DeferredCancellation $stop): void
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

    /** Makes $call after $hops event-loop hops: a callback queued that many times in a row. */
    private static function afterHops(int $hops, \Closure $call): void
    {
        if ($hops <= 0) {
            $call();

            return;
        }

        EventLoop::queue(static function () use ($hops, $call): void {
            self::afterHops($hops - 1, $call);
        });
    }

    /**
     * @param list<NatsMessage> $messages
     * @return list<string>
     */
    private static function payloads(array $messages): array
    {
        return array_map(static fn(NatsMessage $message): string => $message->payload, $messages);
    }

    private static function cpuSeconds(): float
    {
        $usage = getrusage();
        if ($usage === false) {
            return 0.0;
        }

        return $usage['ru_utime.tv_sec'] + $usage['ru_utime.tv_usec'] / 1e6 + $usage['ru_stime.tv_sec'] + $usage['ru_stime.tv_usec'] / 1e6;
    }
}
