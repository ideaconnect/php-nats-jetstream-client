<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Exception\SlowConsumerException;
use IDCT\NATS\JetStream\Consumers\PullConsumerIterator;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use IDCT\NATS\Transport\TransportClosedException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * What a pull consumer run does with what its pulls have received when it ends with a failure of its own waits or
 * writes (#197). The pipelined engine behind PullConsumerIterator::handle() buffers each message into the oldest pull
 * in flight and hands a pull's buffer to the handler only when it retires the pull: once its batch is full, a terminal
 * status came, or its deadline passed. A run whose read failed with the connection going (waiting for a reconnect
 * disabled, a lame-duck failover with waiting disabled, a reconnect that gave up, reconnect off, a fatal -ERR, which
 * ends a finite run whether or not the reconnect reopened the connection, and an infinite one only when the run cannot
 * go on past it, #210, see PullConsumerConnectionEndingFrameTest), or for a reason the options make the read's own
 * (handlerErrorsFailOperations, slowConsumerErrorsFailOperations), whose pull's write failed, or whose inbox the server
 * rejected, used to end with that error and drop what its pulls held: messages the server counted as delivered, which
 * came again only after the ack wait, or never on a consumer without acks. The run now hands them to the handler
 * first, in issue order, then throws that error unchanged, as fetchBatch() returns its partial batch since 2.21.0.
 * stop() still leaves them undelivered, whether it came before the failure or from the handler during that delivery,
 * and so does a close that discards them, a disconnect() or a drain() of the connection that has closed it; the
 * iterator's drain() changes nothing; a handler that throws during that delivery, one whose ack failed on a closed
 * connection say, ends it and is reported to the error listener, the run's own failure being the one thrown; a message
 * read while the handler runs goes to a pull still to be handed over, when one is left open; and a handler that throws
 * in the normal retire phase still ends the run at once, as it always did. Since #207 the close rule covers every
 * delivery of the run, as it covers what the connection itself has received: a disconnect() also ends the hand-over of
 * a pull the run retires, and the client's drain() hands over what the pulls hold instead of closing the connection
 * under them (see also PullConsumerClientDrainTest).
 *
 * Over the scripted server in tests/Support/ReconnectingTransport.php, seen through tests/Support/WatchedTransport.php,
 * which counts the reads of the socket: each message the server sends is read before the next is sent, and the failure
 * comes only once the engine's next read is on the socket, so the pulls hold what the test sent when the run fails.
 * The order of events decides every outcome; the time bounds only keep a broken run from hanging the suite. Each #197
 * test fails on 2.21.0, the handler getting nothing, or not the message the failure left, except the five declared
 * guards (one of them the disconnect() data set of an application close), which pass on both; the five #207 tests
 * (the drain() data set of an application close, a disconnect() from the handler or from another fiber while a
 * retired pull is handed over, and the two that keep a pull a disconnect() emptied from counting as an empty retire)
 * fail on 2.23.0. The #189 restart test, stop() and handle() from the handler during that delivery, fails on 2.24.6,
 * where the second handle() cleared the stop and the first handler got the rest.
 */
final class PullConsumerConnectionLossTest extends TestCase
{
    use ReconnectScenarios;

    private const PULL_SUBJECT = '$JS.API.CONSUMER.MSG.NEXT.S.C';
    private const ACK_SUBJECT = '$JS.ACK.S.C.1.1.1.0.0';
    private const STALE_CONNECTION = "-ERR 'Stale Connection'\r\n";
    /** A server's lame-duck INFO naming another server, so the connection has one to fail over to (#47). */
    private const LAME_DUCK = 'INFO {"server_id":"S1","version":"2.12.0","max_payload":1048576,"ldm":true,"connect_urls":["10.0.0.2:4222"]}' . "\r\n";

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{string, bool}> */
    public static function failures(): iterable
    {
        foreach (['waiting disabled', 'the reconnect gives up', 'reconnect off', 'fatal -ERR', 'lame duck, waiting disabled'] as $failure) {
            // An infinite run goes on past a fatal -ERR whose reconnect reopens the connection, as past a lost connection
            // (#210, PullConsumerConnectionEndingFrameTest): it ends at one where it cannot go on, here with reconnect off.
            $infinite = $failure === 'fatal -ERR' ? 'fatal -ERR, reconnect off' : $failure;
            yield 'an infinite run, ' . $infinite => [$infinite, false];
            yield 'a finite run, ' . $failure => [$failure, true];
        }
    }

    /**
     * A run with batch 3, depth 1 and a 30 s expiry, infinite or of one iteration, has received m-1 and m-2, each in a
     * read of its own: they sit in the pull's buffer, the batch not full, and the handler has seen neither. Then the
     * connection goes under the engine's next read: it drops with waiting for a reconnect disabled ("Connection is not
     * open", the reconnect going on in the background) or with dials refused and three attempts ("Reconnect attempts
     * exhausted"), or with reconnect off ("Reconnect is disabled"), or the server sends a fatal -ERR ("Server sent error
     * frame: 'Stale Connection'"), the reconnect reopening the connection under the finite run and reconnect off under
     * the infinite one, which goes on past it otherwise (#210), or the server sends a lame-duck INFO with waiting
     * disabled and the failover's dial held ("Connection is not open", the failover under way, #191). The handler gets
     * m-1 and m-2, in order, and handle() then throws the read's error, the run having pulled once. It used to throw the
     * same error with the handler never having seen them.
     */
    #[DataProvider('failures')]
    public function testARunWhoseReadFailsWithTheConnectionGoingHandsTheHandlerWhatItsPullReceivedFirst(string $failure, bool $finite): void
    {
        [$transport, $watched, $client] = $this->client($failure);
        $server = $this->pullServer($transport);
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000);
        if ($finite) {
            $iterator->setIterations(1);
        }
        $handled = self::payloadLog();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled): void {
            $handled[] = $message->payload;
        });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1', 'm-2']);
        self::assertSame([], $handled->getArrayCopy(), 'the pull holds both: its batch is not full');

        self::endTheConnection($transport, $failure);
        [$processed, $error] = self::settle($run);

        [$message, $state] = self::readError($failure);
        self::assertNull($processed, 'the run fails');
        self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame($message, $error->getMessage());
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy(), 'the handler got what the pull had received before handle() threw');
        self::assertSame($state, $client->state());
        self::assertSame([0], $server->epochs, 'one pull, on the first connection');
    }

    /**
     * Two pulls hold messages when the run fails: depth 2 and batch 2, both pulls on the wire, and the server's next
     * chunk brings m-1 and m-2, which fill the first pull, m-3, which goes into the second, and a fatal -ERR. With
     * reconnect off, the engine's read buffers the three and fails with the -ERR once the connection is closed, before
     * the engine could retire the first pull. The handler gets m-1, m-2 and m-3, in issue order, then handle() throws
     * the -ERR's error. It used to get nothing. (With the reconnect reopening the connection, the infinite run goes on
     * past the -ERR instead, #210.)
     */
    public function testWhatTwoPullsReceivedReachesTheHandlerInIssueOrder(): void
    {
        [$transport, $watched, $client] = $this->client('fatal -ERR, reconnect off');
        $server = $this->pullServer($transport);
        $handled = self::payloadLog();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled): void {
                $handled[] = $message->payload;
            });
        $this->waitUntil(static fn(): bool => count($server->epochs) === 2 && $watched->readsUnderWay === 1);

        $transport->pushFrame(self::messages((int) $server->sid, 'm-1', 'm-2', 'm-3') . self::STALE_CONNECTION);
        [, $error] = self::settle($run);

        self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame("Server sent error frame: 'Stale Connection'", $error->getMessage());
        self::assertSame(['m-1', 'm-2', 'm-3'], $handled->getArrayCopy(), 'both pulls\' messages, in issue order');
        self::assertSame(ConnectionState::Closed, $client->state());
        self::assertSame([0, 0], $server->epochs);
    }

    /** @return iterable<string, array{string, string, string}> */
    public static function writeFailures(): iterable
    {
        yield 'reconnect off' => ['reconnect off', 'Reconnect is disabled', 'The connection is gone'];
        yield 'the reconnect gives up' => ['the reconnect gives up', 'Reconnect attempts exhausted', 'Connection refused'];
    }

    /**
     * With depth 2 a pull's write can fail while an earlier pull holds messages. Batch 2: the server answers the first
     * pull with m-1, m-2 and m-3 in one chunk, so m-1 and m-2 fill the first pull and m-3 goes into the second, and the
     * third pull's write then finds the socket dead. The engine retires the first pull (the handler gets m-1 and m-2)
     * and refills the pipeline: the write of the third pull fails, the reconnect it runs is off (the write's error is
     * the cause) or gives up with dials refused. The handler gets m-3 as well, and handle() then throws the write's
     * error, the connection Closed. It used to throw with m-3 lost.
     */
    #[DataProvider('writeFailures')]
    public function testARunWhosePullWriteFailsHandsTheHandlerWhatAnEarlierPullReceivedFirst(string $failure, string $message, string $cause): void
    {
        [$transport, , $client] = $this->client($failure);
        if ($failure === 'the reconnect gives up') {
            $transport->refuseDials();
        }
        $server = $this->pullServer($transport, self::answerTheFirstPullAndFailTheThird($transport));
        $handled = self::payloadLog();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled): void {
                $handled[] = $message->payload;
            });
        [, $error] = self::settle($run);

        self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame($message, $error->getMessage());
        self::assertInstanceOf(TransportClosedException::class, $error->getPrevious());
        self::assertSame($cause, $error->getPrevious()->getMessage());
        self::assertSame(['m-1', 'm-2', 'm-3'], $handled->getArrayCopy(), 'm-3, in the second pull when the third pull\'s write failed, reached the handler too');
        self::assertSame(ConnectionState::Closed, $client->state());
        self::assertSame([0, 0], $server->epochs, 'the third pull never reached the server');
    }

    /**
     * Every failure of a pull's write counts, not only the connection going: the server's first answer brings m-1, m-2
     * and m-3 (m-3 into the second pull) and an INFO that lowers max_payload to 16 bytes, so the refill's pull request,
     * 33 bytes, is refused before it is written, with a ProtocolException, the connection staying open. The handler gets
     * m-3 after m-1 and m-2, and handle() throws the ProtocolException. A catch of ConnectionException only would let
     * it out with m-3 dropped, as 2.21.0 did.
     */
    public function testAPullWriteRefusedOnAnOpenConnectionStillHandsOverWhatAnEarlierPullReceived(): void
    {
        [$transport, , $client] = $this->client('reconnect on');
        $info = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":16,"headers":true}' . "\r\n";
        $server = $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1 ? [self::messages($sid, 'm-1', 'm-2', 'm-3') . $info] : []);
        $handled = self::payloadLog();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled): void {
                $handled[] = $message->payload;
            });
        [, $error] = self::settle($run);

        self::assertInstanceOf(ProtocolException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame('Payload size 33 exceeds server max_payload of 16', $error->getMessage());
        self::assertSame(['m-1', 'm-2', 'm-3'], $handled->getArrayCopy(), 'm-3, in the second pull when the third pull\'s write was refused, reached the handler too');
        self::assertSame(ConnectionState::Open, $client->state());
        self::assertSame([0, 0], $server->epochs, 'the third pull never reached the server');
    }

    /** @return iterable<string, array{string}> */
    public static function failuresKeepingTheConnection(): iterable
    {
        yield 'another subscription\'s handler throws (handlerErrorsFailOperations)' => ['handler'];
        yield 'another subscription\'s handler throws an Error (handlerErrorsFailOperations)' => ['handler error'];
        yield 'a SubscriptionQueue overflows (slowConsumerErrorsFailOperations)' => ['overflow'];
    }

    /**
     * Every failure of the run's read counts, not only the connection going: also one the options make the read's
     * own, on a connection that stays open. With handlerErrorsFailOperations the read that delivers a message to another
     * subscription whose handler throws fails with what it threw, an exception or an \Error; with
     * slowConsumerErrorsFailOperations, under the Error policy with room for two messages per subscription, the read
     * that brings a SubscriptionQueue a third fails with a SlowConsumerException. The pull holds m-1 and m-2 then: the
     * handler gets both, and handle() throws the read's failure, the connection still Open. The handler used to get
     * nothing.
     */
    #[DataProvider('failuresKeepingTheConnection')]
    public function testARunWhoseReadFailsOnAnOpenConnectionHandsTheHandlerWhatItsPullReceivedFirst(string $failure): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: 2_000,
            pingIntervalSeconds: 0,
            maxPendingMessagesPerSubscription: 2,
            slowConsumerPolicy: SlowConsumerPolicy::Error,
            slowConsumerErrorsFailOperations: true,
            handlerErrorsFailOperations: true,
        ), $watched);
        $this->opened[] = $client;
        $client->connect()->await();
        $otherFailure = $failure === 'handler error'
            ? new \Error('the other subscription\'s handler failed')
            : new \RuntimeException('the other subscription\'s handler failed');
        $otherSid = $failure !== 'overflow'
            ? $client->subscribe('other', static function () use ($otherFailure): void {
                throw $otherFailure;
            })->await()
            : $client->subscribeQueue('backlog')->await()->sid;
        $server = $this->pullServer($transport);
        $handled = self::payloadLog();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled): void {
                $handled[] = $message->payload;
            });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1', 'm-2']);

        $transport->pushFrame($failure !== 'overflow'
            ? ReconnectingTransport::msgFrame('other', $otherSid, 'x')
            : ReconnectingTransport::msgFrame('backlog', $otherSid, 'b1') . ReconnectingTransport::msgFrame('backlog', $otherSid, 'b2')
                . ReconnectingTransport::msgFrame('backlog', $otherSid, 'b3'));
        [, $error] = self::settle($run);

        if ($failure !== 'overflow') {
            self::assertSame($otherFailure, $error, sprintf('handle() threw %s', self::describe($error)));
        } else {
            self::assertInstanceOf(SlowConsumerException::class, $error, sprintf('handle() threw %s', self::describe($error)));
            self::assertSame($otherSid, $error->sid);
        }
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy(), 'the handler got what the pull had received before handle() threw');
        self::assertSame(ConnectionState::Open, $client->state());
    }

    /**
     * A handler that throws during that delivery ends it: the two pulls hold m-1 and m-2, and m-3, when the read fails
     * with a fatal -ERR, reconnect off (as in testWhatTwoPullsReceivedReachesTheHandlerInIssueOrder()), and the handler
     * throws on m-2. m-3 is not handed over, as the rest of a batch is not when a handler throws in the retire phase;
     * the error listener gets the handler's exception, and handle() throws the read's error, the run's failure, which
     * came first. The handler used to get nothing, and the listener nothing either.
     */
    public function testAHandlerThatThrowsDuringThatDeliveryEndsItAndIsReported(): void
    {
        /** @var \ArrayObject<int, \Throwable> $reported */
        $reported = new \ArrayObject();
        [$transport, $watched, $client] = $this->client('fatal -ERR, reconnect off', static function (\Throwable $error) use ($reported): void {
            $reported[] = $error;
        });
        $server = $this->pullServer($transport);
        $handled = self::payloadLog();
        $handlerFailure = new \RuntimeException('the handler failed on m-2');
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled, $handlerFailure): void {
                $handled[] = $message->payload;
                if ($message->payload === 'm-2') {
                    throw $handlerFailure;
                }
            });
        $this->waitUntil(static fn(): bool => count($server->epochs) === 2 && $watched->readsUnderWay === 1);

        $transport->pushFrame(self::messages((int) $server->sid, 'm-1', 'm-2', 'm-3') . self::STALE_CONNECTION);
        [, $error] = self::settle($run);

        self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame("Server sent error frame: 'Stale Connection'", $error->getMessage(), 'the run\'s own failure is thrown');
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy(), 'the delivery ends at the handler\'s failure: m-3 is not handed over');
        self::assertSame([$handlerFailure], $reported->getArrayCopy(), 'the error listener got the handler\'s exception, and only that');
    }

    /**
     * Any throwable of the handler during that delivery is reported, an \Error too: a handler with a bug, which throws
     * a TypeError on m-1 when the connection drops with reconnect off, ends the delivery, the error listener gets the
     * TypeError, and handle() throws "Reconnect is disabled". It used to throw the same with the handler never called.
     */
    public function testAnErrorTheHandlerThrowsDuringThatDeliveryIsReportedToo(): void
    {
        /** @var \ArrayObject<int, \Throwable> $reported */
        $reported = new \ArrayObject();
        [$transport, $watched, $client] = $this->client('reconnect off', static function (\Throwable $error) use ($reported): void {
            $reported[] = $error;
        });
        $server = $this->pullServer($transport);
        $handled = self::payloadLog();
        $bug = new \TypeError('a bug in the handler');
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled, $bug): void {
                $handled[] = $message->payload;

                throw $bug;
            });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1', 'm-2']);

        $transport->dropConnection();
        [, $error] = self::settle($run);

        self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame('Reconnect is disabled', $error->getMessage());
        self::assertSame(['m-1'], $handled->getArrayCopy());
        self::assertContains($bug, $reported->getArrayCopy(), 'the TypeError was reported');
    }

    /**
     * On a connection that closed, reconnect off and the connection dropped, the ack of a handler that acks fails: the
     * handler lets that exception out, which ends the delivery at m-1, the ack's failure goes to the error listener, and
     * handle() throws the read's error. m-1 and m-2 stay unacked; a handler that does not ack gets everything its
     * pulls received. The handler used to get nothing.
     */
    public function testOnAClosedConnectionThatDeliveryEndsAtTheFirstAckThatFails(): void
    {
        /** @var \ArrayObject<int, \Throwable> $reported */
        $reported = new \ArrayObject();
        [$transport, $watched, $client] = $this->client('reconnect off', static function (\Throwable $error) use ($reported): void {
            $reported[] = $error;
        });
        $server = $this->pullServer($transport);
        $handled = self::payloadLog();
        /** @var \ArrayObject<int, \Throwable> $ackFailures */
        $ackFailures = new \ArrayObject();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message, JetStreamContext $js) use ($handled, $ackFailures): void {
                $handled[] = $message->payload;
                try {
                    $js->ack($message)->await();
                } catch (\Throwable $ackFailure) {
                    $ackFailures[] = $ackFailure;

                    throw $ackFailure;
                }
            });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1', 'm-2']);

        $transport->dropConnection();
        [, $error] = self::settle($run);

        self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame('Reconnect is disabled', $error->getMessage());
        self::assertSame(['m-1'], $handled->getArrayCopy(), 'the handler got m-1, and its failed ack ended the delivery');
        self::assertCount(1, $ackFailures);
        self::assertInstanceOf(ConnectionException::class, $ackFailures[0]);
        self::assertNotSame($error, $ackFailures[0], 'the run threw its own failure, not the ack\'s');
        self::assertContains($ackFailures[0], $reported->getArrayCopy(), 'the ack\'s failure was reported');
        self::assertSame(ConnectionState::Closed, $client->state());
        self::assertSame([], $transport->controlLinesStartingWith('PUB ' . self::ACK_SUBJECT), 'no ack reached the server');
    }

    /** @return iterable<string, array{string, list<string>, ?int, int}> */
    public static function applicationCloses(): iterable
    {
        yield 'disconnect()' => ['disconnect', [], null, 0];
        yield 'drain()' => ['drain', ['m-1 (Draining)', 'ack of m-1 sent', 'm-2 (Draining)', 'ack of m-2 sent'], 2, 2];
    }

    /**
     * The application closes the connection while the run's pull holds m-1 and m-2, batch 3 with depth 1 and a 30 s
     * expiry, the engine reading for more, and the handler acks. The disconnect() data set is a guard, passing on both:
     * the close discards them, as a disconnect() discards what the connection has received and not delivered. Handed
     * over, they reached the handler only after the close had returned, the engine's fiber resuming once it had, and
     * every ack failed on the closed connection, so that a handler that acks processed the messages without acking
     * them. handle() throws "Connection is not open", the handler having got nothing, and no ack goes out. The client's
     * drain() hands them over instead (#207): once its flush is done it asks the run for what its pulls hold, and the
     * run hands m-1 and m-2 to the handler while the connection is Draining, so that both acks reach the server, before
     * drain() resolves; handle() then returns 2, as after the iterator's drain(), and no pull follows the close. On
     * 2.23.0 the drain closed the connection under the run, whose read failed and handed nothing over, as for the
     * disconnect().
     *
     * @param list<string> $expectedLog
     */
    #[DataProvider('applicationCloses')]
    public function testAnApplicationCloseDiscardsWhatThePullHoldsOnADisconnectAndHandsItOverOnADrain(string $close, array $expectedLog, ?int $expectedCount, int $expectedAcks): void
    {
        [$transport, $watched, $client] = $this->client('reconnect on');
        $server = $this->pullServer($transport);
        $log = self::payloadLog();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message, JetStreamContext $js) use ($log, $client): void {
                $log[] = $message->payload . ' (' . $client->state()->name . ')';
                try {
                    $js->ack($message)->await();
                    $log[] = 'ack of ' . $message->payload . ' sent';
                } catch (\Throwable $ackFailure) {
                    $log[] = 'ack of ' . $message->payload . ' failed: ' . $ackFailure->getMessage();
                }
            });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1', 'm-2']);

        ($close === 'disconnect' ? $client->disconnect() : $client->drain())->await(new TimeoutCancellation(2));
        $logWhenTheCloseReturned = $log->getArrayCopy();
        [$processed, $error] = self::settle($run);

        self::assertSame($expectedLog, $logWhenTheCloseReturned, 'what the handler had got when the close returned');
        self::assertSame($expectedLog, $log->getArrayCopy());
        if ($expectedCount === null) {
            self::assertNull($processed, 'the run fails');
            self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
            self::assertSame('Connection is not open', $error->getMessage());
        } else {
            self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
            self::assertSame($expectedCount, $processed);
        }
        self::assertCount($expectedAcks, $transport->controlLinesStartingWith('PUB ' . self::ACK_SUBJECT), 'the acks on the wire');
        self::assertSame([0], $server->epochs, 'one pull, none after the close');
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * A disconnect() made during that delivery ends it, as a stop() does: the two pulls hold m-1 and m-2, and m-3, when
     * the read fails with a fatal -ERR, reconnect off, and the handler calls disconnect() on m-1, which marks the close
     * as the application's. Neither m-2, in the same pull, nor m-3, in the next, reaches the handler, which would
     * otherwise run on after the close, and handle() still throws the read's error. The handler used to get nothing.
     * (The same close ends the hand-over of an infinite run that goes on past the -ERR once the reconnect reopened the
     * connection, #210: see PullConsumerConnectionEndingFrameTest.)
     */
    public function testADisconnectDuringThatDeliveryEndsIt(): void
    {
        [$transport, $watched, $client] = $this->client('fatal -ERR, reconnect off');
        $server = $this->pullServer($transport);
        $handled = self::payloadLog();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled, $client): void {
                $handled[] = $message->payload;
                $client->disconnect()->await();
            });
        $this->waitUntil(static fn(): bool => count($server->epochs) === 2 && $watched->readsUnderWay === 1);

        $transport->pushFrame(self::messages((int) $server->sid, 'm-1', 'm-2', 'm-3') . self::STALE_CONNECTION);
        [, $error] = self::settle($run);

        self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame("Server sent error frame: 'Stale Connection'", $error->getMessage(), 'the run still ends with its failure');
        self::assertSame(['m-1'], $handled->getArrayCopy(), 'the disconnect() leaves m-2 and m-3 undelivered');
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * Guard, passes on both: the close rule holds where a pull's write fails too. A disconnect() while a pull's write
     * is held up, the socket under backpressure, fails that write with the transport's own error, a
     * TransportClosedException rather than a ConnectionException, since the write's recovery leaves a close of the
     * application's alone. Depth 2 and batch 2, the first pull answered with m-1, m-2 and m-3 in one chunk, so m-3 is
     * in the second pull when the third pull's write is held up and the connection is closed under it: the close
     * discards m-3, as it discards what the connection has received and not delivered, and handle() throws the write's
     * error, the connection Closed.
     */
    public function testADisconnectWhileAPullWriteIsHeldUpLeavesWhatAnEarlierPullReceivedUndelivered(): void
    {
        [$transport, , $client] = $this->client('reconnect on');
        $this->pullServer($transport, static function (string $replyTo, int $sid, int $pull) use ($transport): array {
            if ($pull === 2) {
                $transport->stallNextWriteContaining('PUB ' . self::PULL_SUBJECT . ' ', 30.0);
            }

            return $pull === 1 ? [self::messages($sid, 'm-1', 'm-2', 'm-3')] : [];
        });
        $handled = self::payloadLog();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled): void {
                $handled[] = $message->payload;
            });
        $this->waitUntil(static fn(): bool => $transport->writesStalled() === 1);
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy(), 'the first pull was retired before the third pull\'s write');

        $client->disconnect()->await(new TimeoutCancellation(2));
        [, $error] = self::settle($run);

        self::assertInstanceOf(TransportClosedException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame('The connection is gone', $error->getMessage());
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy(), 'the close discards m-3, in the second pull when the third pull\'s write failed');
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * A disconnect() from the handler ends the hand-over of a pull the run retires, before the next message, as it ends
     * every delivery of the run (#207): batch 2 and depth 1, the server answers the pull with m-1 and m-2, which fill
     * it, so the retire phase hands it over, and the handler awaits a disconnect() on m-1. m-2 is not handed over: it
     * stays unacked, for the server to deliver again after the ack wait, where it reached the handler on the Closed
     * connection, after disconnect() had returned, with every ack failing. The run then ends with the error of its next
     * pull's write, "Connection is not open", that pull never on the wire, as before. On 2.23.0 the retire phase handed
     * the pull over whole, a rule pinned by the guard this test replaces, since a drain() lets acks out while the
     * connection is Draining, which still holds (PullConsumerClientDrainTest).
     */
    public function testADisconnectFromTheHandlerEndsTheHandOverOfARetiredPull(): void
    {
        [$transport, , $client] = $this->client('reconnect on');
        $server = $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1 ? [self::messages($sid, 'm-1', 'm-2')] : []);
        $handled = self::payloadLog();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled, $client): void {
                $handled[] = $message->payload . ' (' . $client->state()->name . ')';
                if ($message->payload === 'm-1') {
                    $client->disconnect()->await();
                }
            });
        [$processed, $error] = self::settle($run);

        self::assertNull($processed, 'the run fails');
        self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame('Connection is not open', $error->getMessage());
        self::assertNull($error->getPrevious(), 'the next pull\'s write was refused on the closed connection');
        self::assertSame(['m-1 (Open)'], $handled->getArrayCopy(), 'the disconnect() leaves m-2 undelivered');
        self::assertSame([0], $server->epochs, 'the next pull never reached the server');
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * The same for a disconnect() from another fiber while the handler holds m-1 of a pull the run retires: batch 2,
     * the pull full with m-1 and m-2, and the handler acks m-1 and then waits on a gate. The application disconnects
     * meanwhile and opens the gate once disconnect() has returned: m-2 is not handed over, no ack goes out after the
     * close (the one on the wire is m-1's, made before it), and handle() throws "Connection is not open", from the next
     * pull's write. On 2.23.0 the handler got m-2 on the Closed connection, after disconnect() had returned, and its
     * ack failed.
     */
    public function testADisconnectFromAnotherFiberDuringTheRetirePhaseLeavesTheRestUndelivered(): void
    {
        [$transport, , $client] = $this->client('reconnect on');
        $server = $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1 ? [self::messages($sid, 'm-1', 'm-2')] : []);
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        $log = self::payloadLog();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message, JetStreamContext $js) use ($log, $client, $gate): void {
                $log[] = $message->payload . ' (' . $client->state()->name . ')';
                try {
                    $js->ack($message)->await();
                    $log[] = 'ack of ' . $message->payload . ' sent';
                } catch (\Throwable $ackFailure) {
                    $log[] = 'ack of ' . $message->payload . ' failed: ' . $ackFailure->getMessage();
                }
                if ($message->payload === 'm-1') {
                    $gate->getFuture()->await();
                }
            });
        $this->waitUntil(static fn(): bool => $log->getArrayCopy() === ['m-1 (Open)', 'ack of m-1 sent']);

        $client->disconnect()->await(new TimeoutCancellation(2));
        $log[] = 'disconnect() returned';
        $gate->complete();
        [$processed, $error] = self::settle($run);

        self::assertNull($processed, 'the run fails');
        self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame('Connection is not open', $error->getMessage());
        self::assertSame(['m-1 (Open)', 'ack of m-1 sent', 'disconnect() returned'], $log->getArrayCopy(), 'm-2 is left undelivered');
        self::assertCount(1, $transport->controlLinesStartingWith('PUB ' . self::ACK_SUBJECT), 'm-1\'s ack, made before the close');
        self::assertSame([0], $server->epochs, 'the next pull never reached the server');
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * A disconnect() that discards a full pull does not end a finite run as an exhausted stream would (#207): a finite
     * run of three iterations, depth 1 and batch 2, and one chunk that brings a message on a plain subscription, whose
     * handler calls disconnect(), and m-1 and m-2, which fill the pull. The disconnect's fiber sets the close intent
     * before the engine retires the pull, so the delivery discards both; the pull brought messages, so it is not an
     * empty retire, and the run goes on to its next pull, which meets the closed connection: handle() throws, the
     * handler having got nothing. Classified as an empty retire, the discarded pull ended the finite run as one that
     * found the stream exhausted, and handle() resolved with 0. On 2.23.0 the retire phase handed both over on the
     * closing connection, and the run then failed.
     */
    public function testADisconnectThatDiscardsAFullPullDoesNotEndAFiniteRunAsAnEmptyRetire(): void
    {
        [$transport, $watched, $client] = $this->client('reconnect on');
        $server = $this->pullServer($transport);
        $client->subscribe('X', static function () use ($client): void {
            $client->disconnect()->ignore();
        })->await();
        $handled = self::payloadLog();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(1)->setExpiresMs(30_000)->setIterations(3)
            ->handle(static function (NatsMessage $message) use ($handled): void {
                $handled[] = $message->payload;
            });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $transport->pushFrame(ReconnectingTransport::msgFrame('X', (int) $transport->sidFor('X'), 'close') . self::messages((int) $server->sid, 'm-1', 'm-2'));
        [$processed, $error] = self::settle($run);

        self::assertNull($processed, sprintf('handle() resolved with %s, as for an exhausted stream', var_export($processed, true)));
        self::assertNotNull($error, 'handle() fails once the connection is closed');
        self::assertSame([], $handled->getArrayCopy(), 'the disconnect() discards what the pull held');
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * A disconnect() from the handler during the retire phase leaves a status that ended a later pull after its
     * messages unreported, as a delivery of those messages does, and handle() fails (#207): an infinite run of depth 2
     * and batch 2, one chunk bringing m-1 and m-2 (the first pull), m-3 (the second) and a 409 Consumer Deleted on the
     * second pull's reply subject. The handler awaits disconnect() on m-1: m-2 and m-3 are discarded, the second pull,
     * which brought m-3, is not an empty retire, onError gets nothing, and handle() throws "Connection is not open" at
     * the next pull's write. Classified as an empty retire, the second pull reported its 409 to onError and ended the
     * run as a terminal status does, handle() resolving with 1. On 2.23.0 the handler got m-2 and m-3 on the Closed
     * connection, and the run failed at its next pull.
     */
    public function testADisconnectDuringTheRetirePhaseDoesNotTurnALaterPullsStatusIntoTheRunsEnd(): void
    {
        [$transport, , $client] = $this->client('reconnect on');
        $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull): array => $pull === 2
            ? [self::messages($sid, 'm-1', 'm-2', 'm-3') . ReconnectingTransport::hmsgFrame($replyTo, $sid, "NATS/1.0 409 Consumer Deleted\r\n\r\n", '')]
            : []);
        $handled = self::payloadLog();
        $reported = self::payloadLog();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000)
            ->setOnError(static function (\Throwable $status) use ($reported): void {
                $reported[] = $status->getMessage();
            })
            ->handle(static function (NatsMessage $message) use ($handled, $client): void {
                $handled[] = $message->payload . ' (' . $client->state()->name . ')';
                if ($message->payload === 'm-1') {
                    $client->disconnect()->await();
                }
            });
        [$processed, $error] = self::settle($run);

        self::assertSame(['m-1 (Open)'], $handled->getArrayCopy(), 'the disconnect() discards m-2 and m-3');
        self::assertSame([], $reported->getArrayCopy(), 'no status reported for a pull that brought a message');
        self::assertNull($processed, sprintf('handle() resolved with %s', var_export($processed, true)));
        self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame('Connection is not open', $error->getMessage());
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * The same rule for a run whose inbox the server rejects mid-run, as nats-server does on a configuration reload that
     * withdraws the subscribe permission: it removes the subscription and sends "Permissions Violation for
     * Subscription to "<inbox>" (sid "<sid>")", the connection staying open. The pull holds m-1 and m-2 when the -ERR
     * comes: the handler gets both, and handle() then throws the JetStreamException saying the inbox was rejected by
     * server permissions. The handler used to get nothing.
     */
    public function testARunWhoseInboxIsRejectedHandsTheHandlerWhatItsPullReceivedFirst(): void
    {
        [$transport, $watched, $client] = $this->client('reconnect on');
        $server = $this->pullServer($transport);
        $handled = self::payloadLog();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled): void {
                $handled[] = $message->payload;
            });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1', 'm-2']);

        $transport->pushFrame(sprintf("-ERR 'Permissions Violation for Subscription to \"%s.*\" (sid \"%d\")'\r\n", $server->base, (int) $server->sid));
        [, $error] = self::settle($run);

        self::assertInstanceOf(JetStreamException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertStringContainsString('Pull consumer reply inbox "' . $server->base . '.*" was rejected by server permissions', $error->getMessage());
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy(), 'the handler got what the pull had received before handle() threw');
        self::assertSame(ConnectionState::Open, $client->state());
        self::assertSame(['UNSUB ' . $server->sid], $transport->controlLinesStartingWith('UNSUB '), 'the inbox is released once');
    }

    /**
     * A message read while the handler runs during that delivery is handed over too, after what was received before
     * it: depth 2 and batch 3, the first pull holds m-1 and m-2 and the second none when the read fails with a fatal
     * -ERR, waiting for the reconnect disabled, so that the read fails at once while the reconnect reopens the
     * connection in the background and replays the inbox. On m-1 the handler waits for that, the server sends m-3 on the
     * new connection, and the handler reads it itself: the first pull, being handed over, takes nothing more, so m-3
     * goes into the second pull and reaches the handler after m-2. Added to the buffer being handed over, it would have
     * been dropped with it. The handler used to get nothing. (With waiting enabled the infinite run goes on past the
     * -ERR instead, #210, and PullConsumerConnectionEndingFrameTest checks the same rule for that hand-over.)
     */
    public function testAMessageReadWhileTheHandlerRunsGoesToAPullStillToBeHandedOver(): void
    {
        [$transport, $watched, $client] = $this->client('fatal -ERR, waiting disabled');
        $server = $this->pullServer($transport);
        $handled = self::payloadLog();
        $framesRead = [];
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(2)->setExpiresMs(30_000)
            ->handle(function (NatsMessage $message) use ($handled, $transport, $server, $client, &$framesRead): void {
                $handled[] = $message->payload;
                if ($message->payload === 'm-1') {
                    // Once the reconnect has reopened the connection, its server answers the first pull further: the
                    // handler's own read takes it.
                    $this->waitUntilOpen($client);
                    $transport->pushFrame(self::messages((int) $server->sid, 'm-3'));
                    $framesRead[] = $client->processIncoming(new TimeoutCancellation(2))->await();
                }
            });
        $this->waitUntil(static fn(): bool => count($server->epochs) === 2 && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1', 'm-2']);

        $transport->pushFrame(self::STALE_CONNECTION);
        [, $error] = self::settle($run);

        self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame("Server sent error frame: 'Stale Connection'", $error->getMessage());
        self::assertSame([1], $framesRead, 'the handler\'s read took m-3');
        self::assertSame(1, $transport->epoch(), 'm-3 came on the new connection');
        self::assertSame(['m-1', 'm-2', 'm-3'], $handled->getArrayCopy(), 'm-3 went into the second pull, still to be handed over');
    }

    /**
     * The iterator's drain() changes nothing here: it lets the pulls in flight complete, and on a failure the run ends
     * anyway. A run drained while its pull holds m-1 and m-2 (the drain wakes the engine's read once, and the engine
     * reads on for the pull) hands both to the handler when the connection then drops with reconnect off, and handle()
     * throws "Reconnect is disabled". The handler used to get nothing.
     */
    public function testADrainedRunWhoseReadFailsStillHandsTheHandlerWhatItsPullReceived(): void
    {
        [$transport, $watched, $client] = $this->client('reconnect off');
        $server = $this->pullServer($transport);
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000);
        $handled = self::payloadLog();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled): void {
            $handled[] = $message->payload;
        });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1', 'm-2']);
        $cancelledReads = $watched->cancelledReads;
        $reads = $watched->reads;
        $iterator->drain();
        $this->waitUntil(static fn(): bool => $watched->cancelledReads > $cancelledReads && $watched->reads > $reads && $watched->readsUnderWay === 1);

        $transport->dropConnection();
        [, $error] = self::settle($run);

        self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame('Reconnect is disabled', $error->getMessage());
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertSame([0], $server->epochs, 'no pull after the drain');
    }

    /**
     * Guard, passes on both: a stop() made as the run's read fails, by the error listener when the read's EOF is
     * reported, with reconnect off, wins: the read ends as the stop's wake-up ends it, and the run ends as a stopped
     * one, returning 0 with m-1 and m-2 never handed over, and unacked. It neither throws the connection's error nor
     * hands them over.
     */
    public function testAStopAsTheReadFailsEndsTheRunAsAStoppedOne(): void
    {
        $stopper = new class {
            public ?PullConsumerIterator $iterator = null;
        };
        [$transport, $watched, $client] = $this->client('reconnect off', static function () use ($stopper): void {
            $stopper->iterator?->stop();
        });
        $server = $this->pullServer($transport);
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000);
        $handled = self::payloadLog();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled): void {
            $handled[] = $message->payload;
        });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1', 'm-2']);
        $stopper->iterator = $iterator;

        $transport->dropConnection();
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(0, $processed);
        self::assertSame([], $handled->getArrayCopy(), 'stop() leaves what the pull received undelivered');
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * Guard, passes on both: a stop() made before the delivery that precedes a failure leaves what the pulls received
     * undelivered. In the shape of testARunWhosePullWriteFailsHandsTheHandlerWhatAnEarlierPullReceivedFirst() with
     * reconnect off, a connection listener stops the run on the Closed event, which the third pull's failed write
     * raises before it throws: the handler, which had m-1 and m-2 from the retired pull, does not get m-3, and handle()
     * throws "Reconnect is disabled".
     */
    public function testAStopBeforeTheFailureLeavesWhatThePullsReceivedUndelivered(): void
    {
        $stopper = new class {
            public ?PullConsumerIterator $iterator = null;
        };
        [$transport, , $client] = $this->client('reconnect off', null, static function (ConnectionEvent $event) use ($stopper): void {
            if ($event === ConnectionEvent::Closed) {
                $stopper->iterator?->stop();
            }
        });
        $this->pullServer($transport, self::answerTheFirstPullAndFailTheThird($transport));
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000);
        $stopper->iterator = $iterator;
        $handled = self::payloadLog();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled): void {
            $handled[] = $message->payload;
        });
        [, $error] = self::settle($run);

        self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame('Reconnect is disabled', $error->getMessage());
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy(), 'm-3 is left undelivered: the run was stopped before it failed');
    }

    /**
     * A stop() from the handler during that delivery leaves the rest undelivered, as in the retire phase: the two
     * pulls hold m-1 and m-2, and m-3, when the read fails with a fatal -ERR, reconnect off, and the handler stops the
     * run on m-1. Neither m-2, in the same pull, nor m-3, in the next, is handed over, and handle() still throws the
     * read's error. The handler used to get nothing.
     */
    public function testAStopFromTheHandlerDuringThatDeliveryLeavesTheRestUndelivered(): void
    {
        [$transport, $watched, $client] = $this->client('fatal -ERR, reconnect off');
        $server = $this->pullServer($transport);
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000);
        $handled = self::payloadLog();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            $iterator->stop();
        });
        $this->waitUntil(static fn(): bool => count($server->epochs) === 2 && $watched->readsUnderWay === 1);

        $transport->pushFrame(self::messages((int) $server->sid, 'm-1', 'm-2', 'm-3') . self::STALE_CONNECTION);
        [, $error] = self::settle($run);

        self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame("Server sent error frame: 'Stale Connection'", $error->getMessage(), 'the run still ends with its failure');
        self::assertSame(['m-1'], $handled->getArrayCopy(), 'the stop leaves m-2 and m-3 undelivered');
    }

    /**
     * The restart idiom from the handler during that delivery leaves the rest undelivered as well (#189): the pull
     * (batch 3, depth 1) holds m-1 and m-2 when the server rejects the run's inbox, the connection staying open, and the
     * handler stops the run and starts another on m-1. The first handler gets m-1 only, handle() still throws the
     * rejection, and the second run, on an inbox of its own, pulls on the open connection until a stop() ends it. The
     * handle() after the stop used to clear it before the delivery checked it again: the first handler got m-2 too.
     */
    public function testARestartFromTheHandlerDuringThatDeliveryLeavesTheRestUndelivered(): void
    {
        [$transport, $watched, $client] = $this->client('reconnect on');
        $server = $this->pullServer($transport);
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000);
        $handled = self::payloadLog();
        $restarted = new class {
            /** @var Future<int>|null The run the first run's handler started. */
            public ?Future $run = null;
        };
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator, $restarted): void {
            $handled[] = 'first handler: ' . $message->payload;
            if ($message->payload === 'm-1') {
                $iterator->stop();
                $restarted->run = $iterator->handle(static function (NatsMessage $message) use ($handled): void {
                    $handled[] = 'second handler: ' . $message->payload;
                });
                // Should the test fail with the run still going, the close in tearDown ends it: nothing is to report it.
                $restarted->run->ignore();
            }
        });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1', 'm-2']);

        $transport->pushFrame(sprintf("-ERR 'Permissions Violation for Subscription to \"%s.*\" (sid \"%d\")'\r\n", $server->base, (int) $server->sid));
        [, $error] = self::settle($run);

        self::assertSame(['first handler: m-1'], $handled->getArrayCopy(), 'the stop leaves m-2 undelivered');
        self::assertInstanceOf(JetStreamException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertStringContainsString('was rejected by server permissions', $error->getMessage(), 'the run still ends with its failure');
        $second = $restarted->run;
        if ($second === null) {
            self::fail('the handler never restarted the consumer');
        }
        // The second run's pull, on the connection the rejection left open.
        $this->waitUntil(static fn(): bool => count($server->epochs) === 2);
        self::assertSame(ConnectionState::Open, $client->state());

        $iterator->stop();
        [$processed, $secondError] = self::settle($second);
        self::assertNull($secondError, sprintf('the second run threw %s', self::describe($secondError)));
        self::assertSame(0, $processed);
        self::assertSame([0, 0], $server->epochs, 'one pull per run');
    }

    /**
     * Guard, passes on both: a handler that throws in the retire phase still ends the run at once, as it always did,
     * with nothing else handed over and nothing reported. Depth 2 and batch 2, the first pull answered with m-1, m-2 and
     * m-3 in one chunk (the first pull full, m-3 in the second), and no connection failure: the handler throws on m-1,
     * and handle() throws its exception, with m-2 and m-3 never handed over, and unacked, and the error listener told
     * nothing.
     */
    public function testAHandlerThatThrowsInTheRetirePhaseStillEndsTheRunAtOnce(): void
    {
        /** @var \ArrayObject<int, \Throwable> $reported */
        $reported = new \ArrayObject();
        [$transport, , $client] = $this->client('reconnect on', static function (\Throwable $error) use ($reported): void {
            $reported[] = $error;
        });
        $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1 ? [self::messages($sid, 'm-1', 'm-2', 'm-3')] : []);
        $handled = self::payloadLog();
        $handlerFailure = new \RuntimeException('the handler failed on m-1');
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled, $handlerFailure): void {
                $handled[] = $message->payload;

                throw $handlerFailure;
            });
        [, $error] = self::settle($run);

        self::assertSame($handlerFailure, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(['m-1'], $handled->getArrayCopy(), 'm-2 and m-3 are not handed over');
        self::assertSame([], $reported->getArrayCopy());
        self::assertSame(ConnectionState::Open, $client->state());
    }

    /**
     * A connected client over a watched ReconnectingTransport, closed in tearDown, with no heartbeat, so only the
     * engine and the test read: waiting for a reconnect disabled, also for the lame duck and for a fatal -ERR whose
     * reconnect runs in the background; three reconnect attempts; reconnect off, also for a fatal -ERR that ends an
     * infinite run; or, for 'reconnect on' and 'fatal -ERR', the defaults of these tests: reconnect on, with waiting for
     * it enabled and a thousand attempts.
     *
     * @param (\Closure(\Throwable): void)|null $errorListener
     * @param (\Closure(ConnectionEvent, ?\Throwable): void)|null $connectionListener
     * @return array{ReconnectingTransport, WatchedTransport, NatsClient}
     */
    private function client(string $failure, ?\Closure $errorListener = null, ?\Closure $connectionListener = null): array
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = new NatsClient(match ($failure) {
            'waiting disabled', 'lame duck, waiting disabled', 'fatal -ERR, waiting disabled' => $this->options(false, 2_000, 1_000, 0, 2, $connectionListener, 5, 20, $errorListener),
            'the reconnect gives up' => $this->options(true, 2_000, 3, 0, 2, $connectionListener, 5, 20, $errorListener),
            'reconnect off', 'fatal -ERR, reconnect off' => new NatsOptions(
                connectTimeoutMs: 500,
                requestTimeoutMs: 2_000,
                reconnectEnabled: false,
                pingIntervalSeconds: 0,
                connectionListener: $connectionListener,
                errorListener: $errorListener,
            ),
            default => $this->options(true, 2_000, 1_000, 0, 2, $connectionListener, 5, 20, $errorListener),
        }, $watched);
        $this->opened[] = $client;
        $client->connect()->await();

        return [$transport, $watched, $client];
    }

    /**
     * The scripted server's side of the pull consumer: records the epoch of every pull and the run's inbox, its sid and
     * base, and answers each pull with what $onPull returns, given the pull's reply subject, the inbox's sid and the
     * pull's number (1 for the first); with no $onPull, or no frames, the server holds the pull, as one with no message
     * does until the pull expires.
     *
     * @param (\Closure(string, int, int): list<string>)|null $onPull
     * @return object{sid: ?int, base: ?string, epochs: list<int>}
     */
    private function pullServer(ReconnectingTransport $transport, ?\Closure $onPull = null): object
    {
        $server = new class {
            /** The sid the client subscribed the run's inbox with ("<base>.*"). */
            public ?int $sid = null;
            /** The run's inbox, without the ".<token>" each pull's reply subject adds. */
            public ?string $base = null;
            /** @var list<int> The epoch (session) each pull came on, in order. */
            public array $epochs = [];
        };
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport, $server, $onPull): array {
            if ($replyTo === null || $subject !== self::PULL_SUBJECT) {
                return [];
            }

            $server->epochs[] = $transport->epoch();
            $sid = $transport->sidFor($replyTo);
            if ($sid === null) {
                return [];
            }

            $server->sid ??= $sid;
            $server->base ??= substr($replyTo, 0, (int) strrpos($replyTo, '.'));

            return $onPull === null ? [] : $onPull($replyTo, $sid, count($server->epochs));
        };

        return $server;
    }

    /**
     * Answers the first pull with m-1, m-2 and m-3 in one chunk, and makes the third pull's write find the socket dead:
     * set while the server takes the second pull, the failing write is the next pull's.
     *
     * @return \Closure(string, int, int): list<string>
     */
    private static function answerTheFirstPullAndFailTheThird(ReconnectingTransport $transport): \Closure
    {
        return static function (string $replyTo, int $sid, int $pull) use ($transport): array {
            if ($pull === 2) {
                $transport->failNextWriteContaining('PUB ' . self::PULL_SUBJECT . ' ');
            }

            return $pull === 1 ? [self::messages($sid, 'm-1', 'm-2', 'm-3')] : [];
        };
    }

    /**
     * Sends each payload on the run's inbox in a read of its own: the next goes out once the engine has taken the
     * previous and its next read is on the socket.
     *
     * @param object{sid: ?int, base: ?string, epochs: list<int>} $server
     * @param list<string> $payloads
     */
    private function sendEachInAReadOfItsOwn(ReconnectingTransport $transport, WatchedTransport $watched, object $server, array $payloads): void
    {
        foreach ($payloads as $payload) {
            $reads = $watched->reads;
            $transport->pushFrame(self::messages((int) $server->sid, $payload));
            $this->waitUntil(static fn(): bool => $watched->reads > $reads && $watched->readsUnderWay === 1);
        }
    }

    /**
     * Ends the connection under the engine's read as $failure says: dials refused where the reconnect must not succeed,
     * and for the lame duck the failover's dial held, so that it is still under way when the run fails.
     */
    private static function endTheConnection(ReconnectingTransport $transport, string $failure): void
    {
        if ($failure === 'fatal -ERR' || $failure === 'fatal -ERR, reconnect off') {
            $transport->pushFrame(self::STALE_CONNECTION);

            return;
        }

        if ($failure === 'lame duck, waiting disabled') {
            $transport->holdNextDial();
            $transport->pushFrame(self::LAME_DUCK);

            return;
        }

        if ($failure !== 'reconnect off') {
            $transport->refuseDials();
        }
        $transport->dropConnection();
    }

    /**
     * The error the engine's read fails with for $failure, and the connection's state then.
     *
     * @return array{string, ConnectionState}
     */
    private static function readError(string $failure): array
    {
        return match ($failure) {
            'waiting disabled', 'lame duck, waiting disabled' => ['Connection is not open', ConnectionState::Connecting],
            'the reconnect gives up' => ['Reconnect attempts exhausted', ConnectionState::Closed],
            'reconnect off' => ['Reconnect is disabled', ConnectionState::Closed],
            'fatal -ERR, reconnect off' => ["Server sent error frame: 'Stale Connection'", ConnectionState::Closed],
            default => ["Server sent error frame: 'Stale Connection'", ConnectionState::Open],
        };
    }

    /** One chunk of messages on the run's inbox, each as a real server delivers a pull's message. */
    private static function messages(int $sid, string ...$payloads): string
    {
        $chunk = '';
        foreach ($payloads as $payload) {
            $chunk .= ReconnectingTransport::msgFrame('evt.s', $sid, $payload, self::ACK_SUBJECT);
        }

        return $chunk;
    }

    /** @return \ArrayObject<int, string> The payloads a handler got, in order. */
    private static function payloadLog(): \ArrayObject
    {
        return new \ArrayObject();
    }

    /**
     * Awaits the run: its count, or what it threw.
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
