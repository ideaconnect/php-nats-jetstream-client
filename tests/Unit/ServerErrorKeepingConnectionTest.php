<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\HeldUpDelivery;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\delay;

/**
 * Only a frame that says the connection is finished ends it (#171): a fatal -ERR, which the server sends
 * right before it closes the connection, and a PONG the socket would not take. 2.10.1 ended the connection
 * on every -ERR that failed a read, including the few the server sends while keeping the connection open:
 * a SUB beyond the maximum subscriptions, a permissions violation for a publish with a reply subject, an
 * invalid publish subject. A healthy connection then closed, and with reconnect on every attempt replayed the
 * rejected SUB and failed again until the reconnect gave up. Such an -ERR fails the read, as it did before
 * 2.10.1, and leaves the connection open.
 */
final class ServerErrorKeepingConnectionTest extends TestCase
{
    private const INFO = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n";

    #[DataProvider('errorsTheServerKeepsTheConnectionOpenFor')]
    public function testAnErrTheServerKeepsTheConnectionOpenForFailsTheReadButLeavesItOpen(string $error, bool $reconnect): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n", "-ERR '{$error}'\r\n"]);
        // One quick reconnect attempt: a regression that ends the connection fails at once instead of after
        // the default ten attempts with their backoff.
        $connection = new NatsConnection(new NatsOptions(
            reconnectEnabled: $reconnect,
            maxReconnectAttempts: 1,
            reconnectDelayMs: 1,
            reconnectMaxDelayMs: 1,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
        ), $transport);
        $connection->connect()->await();

        try {
            $connection->processIncoming()->await();
            self::fail('expected the read to fail with the server error');
        } catch (ConnectionException $e) {
            self::assertSame("Server sent error frame: '{$error}'", $e->getMessage());
        }

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertFalse($transport->closed, 'the socket stays open');
        self::assertCount(1, $transport->connectCalls, 'nothing reconnects');
        $connection->publish('orders', 'still open')->await();
        self::assertStringContainsString("PUB orders 10\r\nstill open", implode('', $transport->writes));
    }

    /**
     * @return iterable<string, array{string, bool}>
     */
    public static function errorsTheServerKeepsTheConnectionOpenFor(): iterable
    {
        foreach (['reconnect off' => false, 'reconnect on' => true] as $mode => $reconnect) {
            yield "maximum subscriptions, {$mode}" => ['maximum subscriptions exceeded', $reconnect];
            yield "reserved reply subject, {$mode}" => ['Permissions Violation for Publish with Reply of "_INBOX.reserved"', $reconnect];
            yield "invalid publish subject, {$mode}" => ['Invalid Publish Subject', $reconnect];
            yield "bare permissions violation, {$mode}" => ['Permissions Violation', $reconnect];
        }
    }

    /**
     * Guard: an -ERR the server closes the connection after still ends it (#171).
     */
    #[DataProvider('errorsTheServerClosesTheConnectionAfter')]
    public function testAnErrTheServerClosesTheConnectionAfterStillEndsIt(string $error): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n", "-ERR '{$error}'\r\n"]);
        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false, pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        try {
            $connection->processIncoming()->await();
            self::fail('expected the read to fail with the server error');
        } catch (ConnectionException $e) {
            self::assertSame("Server sent error frame: '{$error}'", $e->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertTrue($transport->closed);
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function errorsTheServerClosesTheConnectionAfter(): iterable
    {
        yield 'stale connection' => ['Stale Connection'];
        yield 'authorization violation' => ['Authorization Violation'];
        yield 'maximum payload' => ['Maximum Payload Violation'];
    }

    /**
     * An INFO that is valid JSON but no object is reported like broken JSON, without failing the read or
     * touching the connection or what it knows of the server. A number used to reach the INFO parser and fail
     * the read with a TypeError, which 2.10.1 then took for a connection failure; an array replaced the
     * server info with defaults, after which a publish with headers was refused.
     */
    #[DataProvider('infoPayloadsThatAreNoObject')]
    public function testAnInfoThatIsNotAJsonObjectIsReportedWithoutFailingTheRead(string $payload): void
    {
        $recorder = new LifecycleRecorder();
        $transport = new FakeTransport([self::INFO, "PONG\r\n", "INFO {$payload}\r\n"]);
        $connection = new NatsConnection(new NatsOptions(
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
        ), $transport);
        $connection->connect()->await();

        $connection->processIncoming()->await();

        self::assertNotSame([], $recorder->errorsContaining('Discarding malformed async INFO frame: INFO payload is not a JSON object'));
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertFalse($transport->closed);
        self::assertSame('S1', $connection->serverInfo()?->serverId);
        self::assertTrue($connection->serverInfo()->headersSupported);
    }

    /** @return iterable<string, array{string}> */
    public static function infoPayloadsThatAreNoObject(): iterable
    {
        yield 'a number' => ['1'];
        yield 'an array' => ['[1,2]'];
        yield 'an empty array' => ['[]'];
    }

    /**
     * At connect such an INFO fails the connect as broken JSON does, with an error that says what is wrong
     * instead of one that wraps the TypeError.
     */
    #[DataProvider('infoPayloadsThatAreNoObject')]
    public function testAnInitialInfoThatIsNotAJsonObjectFailsTheConnectAsBrokenJsonDoes(string $payload): void
    {
        $connection = new NatsConnection(
            new NatsOptions(connectTimeoutMs: 500, reconnectEnabled: false, pingIntervalSeconds: 0),
            new FakeTransport(["INFO {$payload}\r\n", "PONG\r\n"]),
        );

        try {
            $connection->connect()->await();
            self::fail('expected the connect to fail');
        } catch (ConnectionException $e) {
            self::assertSame('INFO payload is not a JSON object', $e->getMessage());
            self::assertInstanceOf(\JsonException::class, $e->getPrevious());
        }
    }

    /**
     * The server's answer to a rejected SUB can sit unread until the next read, which then also meets the
     * fatal -ERR the server sent before it closed the connection, say 'Stale Connection' after a quiet
     * period. The fatal one decides: it ends the connection, and the read fails with it, while the
     * rejection is reported.
     */
    public function testAFatalErrAfterANonClosingOneInTheSameChunkEndsTheConnection(): void
    {
        $recorder = new LifecycleRecorder();
        $transport = new FakeTransport([
            self::INFO, "PONG\r\n",
            "-ERR 'maximum subscriptions exceeded'\r\n-ERR 'Stale Connection'\r\n",
        ]);
        $connection = new NatsConnection(new NatsOptions(
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
        ), $transport);
        $connection->connect()->await();

        try {
            $connection->processIncoming()->await();
            self::fail('expected the read to fail with the fatal error');
        } catch (ConnectionException $e) {
            self::assertSame("Server sent error frame: 'Stale Connection'", $e->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertNotSame([], $recorder->errorsContaining('maximum subscriptions exceeded'), 'the rejection is reported');
    }

    /**
     * A non-closing -ERR in the same chunk as a full subscription queue is what the read fails with, whichever
     * of the two came first, and the overflow is reported: the -ERR answers something the application sent,
     * while the overflow only says that a subscriber fell behind.
     */
    #[DataProvider('overflowOrders')]
    public function testANonClosingErrOutranksAnOverflowInTheSameChunk(bool $overflowFirst): void
    {
        $recorder = new LifecycleRecorder();
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = new NatsConnection(new NatsOptions(
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            maxPendingMessagesPerSubscription: 1,
            slowConsumerPolicy: SlowConsumerPolicy::Error,
            errorListener: $recorder->errorListener(),
        ), $transport);
        $connection->connect()->await();
        $sid = $connection->subscribe('updates', static function (): void {})->await();
        $overflow = "MSG updates {$sid} 5\r\nfirst\r\nMSG updates {$sid} 6\r\nsecond\r\n";
        $error = "-ERR 'maximum subscriptions exceeded'\r\n";
        $transport->pushReadChunk($overflowFirst ? $overflow . $error : $error . $overflow);

        try {
            $connection->processIncoming()->await();
            self::fail('expected the read to fail with the -ERR');
        } catch (ConnectionException $e) {
            self::assertSame("Server sent error frame: 'maximum subscriptions exceeded'", $e->getMessage());
        }

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(["Subscription queue overflow for sid {$sid}"], $recorder->errorsContaining('overflow'), 'the overflow is reported');
    }

    /** @return iterable<string, array{bool}> */
    public static function overflowOrders(): iterable
    {
        yield 'the overflow first' => [true];
        yield 'the -ERR first' => [false];
    }

    /**
     * A replay whose answers carry a fatal -ERR after a rejected SUB fails the attempt, as a fatal -ERR alone
     * does: the rejection does not hide it, whether the two come in one chunk or the poll reads on after it
     * reported the rejection.
     *
     * @param list<string> $answers The replay's answers, one chunk each.
     */
    #[DataProvider('replayAnswersWithAFatalErrAfterARejection')]
    public function testAReconnectWhoseReplayMeetsAFatalErrAfterARejectedSubFails(array $answers): void
    {
        $transport = new FakeTransport([
            self::INFO, "PONG\r\n",
            "-ERR 'maximum subscriptions exceeded'\r\n", // the second SUB, rejected
            FakeTransport::EOF,                          // the connection drops
            self::INFO, "PONG\r\n",                     // the reconnect's handshake
            ...$answers,
        ]);
        $connection = new NatsConnection(new NatsOptions(
            reconnectEnabled: true,
            maxReconnectAttempts: 1,
            reconnectDelayMs: 1,
            reconnectMaxDelayMs: 1,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
        ), $transport);
        $connection->connect()->await();
        $connection->subscribe('first', static function (): void {})->await();
        $connection->subscribe('second', static function (): void {})->await();

        try {
            $connection->processIncoming()->await();
        } catch (ConnectionException) {
            // The first rejection; the connection stays open.
        }
        try {
            $connection->processIncoming()->await();
        } catch (\Throwable) {
            // The read that met the EOF; what matters is how the reconnect ends.
        }

        self::assertSame(ConnectionState::Closed, $connection->state(), 'the one attempt failed on the fatal -ERR');
    }

    /** @return iterable<string, array{list<string>}> */
    public static function replayAnswersWithAFatalErrAfterARejection(): iterable
    {
        yield 'in one chunk' => [["-ERR 'maximum subscriptions exceeded'\r\n-ERR 'Authorization Violation'\r\n"]];
        yield 'in two chunks' => [["-ERR 'maximum subscriptions exceeded'\r\n", "-ERR 'Authorization Violation'\r\n"]];
    }

    /**
     * The heartbeat's own read follows the same rule: an -ERR the server keeps the connection open for leaves
     * it open, and a fatal one after it ends it.
     */
    #[DataProvider('heartbeatReads')]
    public function testTheHeartbeatsReadEndsTheConnectionOnlyOnAFatalErr(string $chunk, ConnectionState $state): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false, pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();
        $transport->pushReadChunk($chunk);

        (new \ReflectionMethod($connection, 'pingTimerTick'))->invoke($connection);

        self::assertSame($state, $connection->state());
    }

    /**
     * @return iterable<string, array{string, ConnectionState}>
     */
    public static function heartbeatReads(): iterable
    {
        yield 'a rejected SUB' => ["-ERR 'maximum subscriptions exceeded'\r\n", ConnectionState::Open];
        yield 'a rejected SUB, then a fatal -ERR' => ["-ERR 'maximum subscriptions exceeded'\r\n-ERR 'Stale Connection'\r\n", ConnectionState::Closed];
    }

    /**
     * The read of a serving loop, which would only swallow what it throws (readIncomingForOperation() with
     * alwaysReport), reports such an -ERR and returns what it read, so the loop reads on.
     */
    public function testAReadForAServingLoopReportsAnErrTheServerKeepsTheConnectionOpenFor(): void
    {
        $recorder = new LifecycleRecorder();
        $transport = new FakeTransport([self::INFO, "PONG\r\n", "-ERR 'maximum subscriptions exceeded'\r\n"]);
        $connection = new NatsConnection(new NatsOptions(
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
        ), $transport);
        $connection->connect()->await();

        $read = $connection->readIncomingForOperation(alwaysReport: true)->await();

        self::assertSame(1, $read->frames);
        self::assertTrue($read->consumedBytes);
        self::assertSame(["Server sent error frame: 'maximum subscriptions exceeded'"], $recorder->errors);
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * The read of a serving loop also reports such an -ERR when a line that does not parse follows it in the
     * same chunk: the -ERR, then the corrupt line, and the read returns once the connection has recovered from
     * the corrupt stream. It used to throw the -ERR after the reconnect, and the loop swallowed it.
     */
    public function testAReadForAServingLoopReportsAnErrReadAheadOfALineThatDoesNotParse(): void
    {
        $recorder = new LifecycleRecorder();
        $transport = new FakeTransport([
            self::INFO, "PONG\r\n",
            "-ERR 'maximum subscriptions exceeded'\r\nBOGUS\r\n",
            self::INFO, "PONG\r\n", // the reconnect after the corrupt stream
        ]);
        $connection = new NatsConnection(new NatsOptions(
            reconnectEnabled: true,
            maxReconnectAttempts: 1,
            reconnectDelayMs: 1,
            reconnectMaxDelayMs: 1,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
        ), $transport);
        $connection->connect()->await();

        $read = $connection->readIncomingForOperation(alwaysReport: true)->await();

        self::assertSame(1, $read->frames);
        self::assertSame(
            ["Server sent error frame: 'maximum subscriptions exceeded'", 'Unsupported control frame: BOGUS'],
            $recorder->errors,
        );
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->connectCalls, 'the corrupt stream was recovered from');
    }

    /**
     * The read of a serving loop also reports a handler that throws while it delivers, and delivers the rest of
     * what it read: the failing subscription's messages behind it, and another subscription's.
     */
    public function testAReadForAServingLoopReportsAHandlerThatThrowsAndDeliversTheRest(): void
    {
        $recorder = new LifecycleRecorder();
        $seen = [];
        $connection = $this->connectionWithAFailingHandler($recorder, $seen);

        $read = $connection->readIncomingForOperation(alwaysReport: true)->await();

        self::assertSame(3, $read->frames);
        self::assertTrue($read->consumedBytes);
        self::assertSame(['a:x', 'a:y', 'b:z'], $seen);
        self::assertSame(['handler a'], $recorder->errors);
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /** @return iterable<string, array{bool}> */
    public static function handlerFailuresFailingOperationsOrNot(): iterable
    {
        yield 'operations report handler failures (default)' => [false];
        yield 'handler failures fail operations' => [true];
    }

    /**
     * An operation's read, such as a request's, reports a handler that throws and delivers the rest, as a serving
     * loop's read does (#173). With handlerErrorsFailOperations it still throws the handler's failure, as every
     * operation's read did before that option, once it has delivered the other subscription's message; only the
     * rest of the failing subscription stays queued (#177).
     */
    #[DataProvider('handlerFailuresFailingOperationsOrNot')]
    public function testAnOperationsReadReportsAHandlerThatThrowsUnlessConfiguredToFail(bool $failOperations): void
    {
        $recorder = new LifecycleRecorder();
        $seen = [];
        $connection = $this->connectionWithAFailingHandler($recorder, $seen, $failOperations);

        if (!$failOperations) {
            $read = $connection->readIncomingForOperation()->await();

            self::assertTrue($read->consumedBytes);
            self::assertSame(['a:x', 'a:y', 'b:z'], $seen);
            self::assertSame(['handler a'], $recorder->errors);

            return;
        }

        try {
            $connection->readIncomingForOperation()->await();
            self::fail('expected the handler failure');
        } catch (\RuntimeException $e) {
            self::assertSame('handler a', $e->getMessage());
        }

        self::assertSame(['a:x', 'b:z'], $seen, "b's message is delivered before the failure is thrown; a's next message stays queued (#177)");
        self::assertSame([], $recorder->errors);
    }

    /**
     * Your own read delivers the other subscriptions' messages before it throws a handler's exception (#177): only
     * the failing subscription's own remainder stays queued. The read of a serving loop then delivers that
     * remainder although the socket has nothing more to give it. Your read used to stop at the throwing handler
     * and leave every message behind it queued, for every subscription, until a read received bytes.
     */
    public function testYourOwnReadThrowsAfterDeliveringTheOtherSubscriptionsAndAServingReadDeliversTheRemainder(): void
    {
        $recorder = new LifecycleRecorder();
        $seen = [];
        $connection = $this->connectionWithAFailingHandler($recorder, $seen);
        try {
            $connection->processIncoming()->await();
            self::fail('expected the handler failure');
        } catch (\RuntimeException $e) {
            self::assertSame('handler a', $e->getMessage());
        }
        self::assertSame(['a:x', 'b:z'], $seen, "b's message is delivered before the exception is thrown");

        $read = $connection->readIncomingForOperation(alwaysReport: true)->await();

        self::assertSame(['a:x', 'b:z', 'a:y'], $seen, "a's remainder is delivered by the serving read");
        self::assertFalse($read->consumedBytes, 'nothing more was read');
        self::assertSame([], $recorder->errors);
    }

    /**
     * Your own next read continues that remainder itself, before it reads (#186), and its result still says what it
     * read: the socket has nothing more, so no frames and no bytes consumed, although it delivered a's second message.
     * A loop that pauses on such a result has nothing left to deliver. The read used to leave a's message queued until
     * a read received anything.
     */
    public function testYourOwnNextReadDeliversTheRemainderAndSaysItReadNothing(): void
    {
        $recorder = new LifecycleRecorder();
        $seen = [];
        $connection = $this->connectionWithAFailingHandler($recorder, $seen);
        try {
            $connection->processIncoming()->await();
            self::fail('expected the handler failure');
        } catch (\RuntimeException $e) {
            self::assertSame('handler a', $e->getMessage());
        }

        $read = $connection->readIncoming()->await();

        self::assertSame(['a:x', 'b:z', 'a:y'], $seen, "a's remainder is delivered by your next read");
        self::assertSame(0, $read->frames, 'no frame was read');
        self::assertFalse($read->consumedBytes, 'nothing was read');
        self::assertSame([], $recorder->errors);
    }

    /**
     * An operation's read does not continue that remainder (#186): it reports the other subscriptions' handler
     * failures, and its own subscription has a take of its own (#179). An operation's read that finds the socket empty
     * leaves a's second message queued, and your next read delivers it.
     */
    public function testAnOperationsReadLeavesTheRemainderToYourNextRead(): void
    {
        $recorder = new LifecycleRecorder();
        $seen = [];
        $connection = $this->connectionWithAFailingHandler($recorder, $seen);
        try {
            $connection->processIncoming()->await();
            self::fail('expected the handler failure');
        } catch (\RuntimeException $e) {
            self::assertSame('handler a', $e->getMessage());
        }

        $operationRead = $connection->readIncomingForOperation()->await();

        self::assertFalse($operationRead->consumedBytes);
        self::assertSame(['a:x', 'b:z'], $seen, "the operation's read left a's remainder queued");

        self::assertSame(0, $connection->processIncoming()->await());
        self::assertSame(['a:x', 'b:z', 'a:y'], $seen, 'your next read delivered it');
        self::assertSame([], $recorder->errors);
    }

    /**
     * A serving loop's read that has waited for another fiber's read, whose delivery is held up in a handler that
     * awaits, delivers what that read has queued and not reached before it returns, instead of leaving it to its own
     * next read, which a loop that is stopping never makes.
     */
    public function testAReadForAServingLoopThatWaitedForAnotherFibersReadDeliversWhatThatReadHasNotReached(): void
    {
        $recorder = new LifecycleRecorder();
        $transport = new ReconnectingTransport();
        $connection = new NatsConnection(new NatsOptions(
            connectTimeoutMs: 500,
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
        ), $transport);
        $connection->connect()->await();
        $seen = [];
        $hold = new HeldUpDelivery(fallbackSeconds: 2.0);
        $sidA = $connection->subscribe('a', static function (NatsMessage $message) use (&$seen, $hold): void {
            $seen[] = 'a:' . $message->payload;
            $hold->holdUp();
        })->await();
        $sidB = $connection->subscribe('b', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'b:' . $message->payload;
        })->await();
        // Another fiber's read takes the idle socket first, and the serving read waits for it.
        $other = async(static fn(): int => $connection->processIncoming()->await());
        delay(0.01);
        $serving = $connection->readIncomingForOperation(alwaysReport: true);
        delay(0.01);

        $transport->pushFrame(
            ReconnectingTransport::msgFrame('a', $sidA, 'x')
            . ReconnectingTransport::msgFrame('b', $sidB, 'z'),
        );
        $read = $serving->await();
        $heldWhenItReturned = $hold->held;
        $hold->end('the end of the test');
        // Awaited before the assertions below: had one of them failed first, this read's failure would be reported
        // in whichever test ran next.
        self::assertSame(2, $other->await(), 'the other read delivered what it read once its handler returned');

        self::assertTrue($heldWhenItReturned, "the other read's delivery was still held up in a's handler when the serving read returned");
        self::assertFalse($read->consumedBytes, 'the other fiber read the socket');
        self::assertSame(['a:x', 'b:z'], $seen, "b's message, which that read had not reached, was delivered before this one returned");
        self::assertSame([], $recorder->errors);
        $connection->disconnect()->await();
    }

    /**
     * flush() and rtt() still fail with an -ERR the server keeps the connection open for, read before their
     * PONG, where the flushes of drain() and drainSubscription() report it and read on: the server rejected
     * something sent before the PING whose answer they are to confirm. The connection stays open.
     */
    #[DataProvider('pingRoundTrips')]
    public function testFlushAndRttStillFailWithAnErrTheServerKeepsTheConnectionOpenFor(string $operation): void
    {
        $recorder = new LifecycleRecorder();
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = new NatsConnection(new NatsOptions(
            requestTimeoutMs: 1_000,
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
        ), $transport);
        $connection->connect()->await();
        $transport->enqueueOnWriteContaining = ["PING\r\n" => ["-ERR 'maximum subscriptions exceeded'\r\n", "PONG\r\n"]];

        try {
            if ($operation === 'flush') {
                $connection->flush()->await();
            } else {
                $connection->rtt()->await();
            }
            self::fail('expected the -ERR to fail ' . $operation . '()');
        } catch (ConnectionException $e) {
            self::assertSame("Server sent error frame: 'maximum subscriptions exceeded'", $e->getMessage());
        }

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame([], $recorder->errors, 'thrown, not reported');
    }

    /** @return iterable<string, array{string}> */
    public static function pingRoundTrips(): iterable
    {
        yield 'flush()' => ['flush'];
        yield 'rtt()' => ['rtt'];
    }

    /**
     * That read still throws an -ERR that ends the connection, once the connection has ended.
     */
    public function testAReadForAServingLoopStillThrowsAnErrThatEndsTheConnection(): void
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n", "-ERR 'Stale Connection'\r\n"]);
        $connection = new NatsConnection(new NatsOptions(reconnectEnabled: false, pingIntervalSeconds: 0), $transport);
        $connection->connect()->await();

        try {
            $connection->readIncomingForOperation(alwaysReport: true)->await();
            self::fail('expected the read to fail with the fatal error');
        } catch (ConnectionException $e) {
            self::assertSame("Server sent error frame: 'Stale Connection'", $e->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * A replay chunk that brings the server's rejection of a SUB and then a line that does not parse fails
     * that attempt on the parse error, and the rejection is reported instead of being lost with the attempt.
     */
    public function testARejectionTheReplayReadsAheadOfAParseErrorIsReported(): void
    {
        $recorder = new LifecycleRecorder();
        $transport = new FakeTransport([
            self::INFO, "PONG\r\n",
            FakeTransport::EOF,                                         // the connection drops
            self::INFO, "PONG\r\n",                                    // the first attempt's handshake
            "-ERR 'maximum subscriptions exceeded'\r\nBOGUS LINE\r\n",  // its replay's answer, then garbage
            self::INFO, "PONG\r\n",                                    // the second attempt, clean
        ]);
        $connection = new NatsConnection(new NatsOptions(
            reconnectEnabled: true,
            maxReconnectAttempts: 3,
            reconnectDelayMs: 1,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
        ), $transport);
        $connection->connect()->await();
        $connection->subscribe('first', static function (): void {})->await();

        $connection->processIncoming()->await();

        self::assertNotSame([], $recorder->errorsContaining('maximum subscriptions exceeded'), 'the rejection is reported');
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(3, $transport->connectCalls, 'the first attempt failed on the parse error, the second succeeded');
    }

    /**
     * A reconnect replays every subscription, including one the server rejected for exceeding the maximum
     * subscriptions. The server rejects it again, and that used to fail the attempt, and every one after it,
     * until the reconnect gave up and closed the connection. The rejection is reported instead and the
     * reconnect completes.
     */
    public function testAReconnectWhoseReplayedSubTheServerRejectsCompletes(): void
    {
        $recorder = new LifecycleRecorder();
        $transport = new FakeTransport([
            self::INFO, "PONG\r\n",
            "-ERR 'maximum subscriptions exceeded'\r\n", // the second SUB, rejected
            FakeTransport::EOF,                           // the connection drops
            self::INFO, "PONG\r\n",                       // the reconnect's handshake
            "-ERR 'maximum subscriptions exceeded'\r\n", // the replayed second SUB, rejected again
        ]);
        $connection = new NatsConnection(new NatsOptions(
            reconnectEnabled: true,
            maxReconnectAttempts: 2,
            reconnectDelayMs: 1,
            reconnectMaxDelayMs: 1,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            connectionListener: $recorder->connectionListener(),
            errorListener: $recorder->errorListener(),
        ), $transport);
        $connection->connect()->await();
        $connection->subscribe('first', static function (): void {})->await();
        $connection->subscribe('second', static function (): void {})->await();

        try {
            $connection->processIncoming()->await();
        } catch (ConnectionException) {
            // The first rejection fails this read, as it always did; the connection stays open.
        }
        try {
            $connection->processIncoming()->await();
        } catch (\Throwable) {
            // The read that met the EOF; the reconnect is what matters here.
        }

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->connectCalls, 'one reconnect, which completed');
        self::assertNotSame([], $recorder->errorsContaining('maximum subscriptions exceeded'), 'the replay rejection is reported');
    }

    /**
     * With reconnect off, a connection the heartbeat gives up on closes with nobody waiting for the recovery.
     * The Closed event now carries the reason, so that the connection listener and the log learn it (#172).
     */
    #[DataProvider('maxPingsOut')]
    public function testTheClosedEventOfAHeartbeatThatGaveUpCarriesTheReason(int $maxPingsOut, string $reason): void
    {
        $recorder = new LifecycleRecorder();
        $transport = new FakeTransport([self::INFO, "PONG\r\n"]);
        $connection = new NatsConnection(new NatsOptions(
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            maxPingsOut: $maxPingsOut,
            connectionListener: $recorder->connectionListener(),
        ), $transport);
        $connection->connect()->await();
        (new \ReflectionProperty(NatsConnection::class, 'outstandingPings'))->setValue($connection, $maxPingsOut);

        (new \ReflectionMethod($connection, 'pingTimerTick'))->invoke($connection);

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame(1, $recorder->closedEvents());
        self::assertSame([$reason], $recorder->closedErrors, 'the Closed event carries the reason');
    }

    /**
     * @return iterable<string, array{int, string}>
     */
    public static function maxPingsOut(): iterable
    {
        yield 'no PING' => [0, 'The heartbeat allows no unanswered PING (maxPingsOut is 0)'];
        yield 'one PING' => [1, 'The server did not answer the last PING'];
        yield 'two PINGs' => [2, 'The server did not answer the last 2 PINGs'];
    }

    /**
     * A connection whose next read brings two messages for "a", whose handler throws on the first, and one
     * for "b".
     *
     * @param list<string> $seen Collects what the handlers got, as "<subject>:<payload>".
     */
    private function connectionWithAFailingHandler(LifecycleRecorder $recorder, array &$seen, bool $handlerFailuresFailOperations = false): NatsConnection
    {
        $transport = new FakeTransport([self::INFO, "PONG\r\n", "MSG a 1 1\r\nx\r\nMSG a 1 1\r\ny\r\nMSG b 2 1\r\nz\r\n"]);
        $connection = new NatsConnection(new NatsOptions(
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
            handlerErrorsFailOperations: $handlerFailuresFailOperations,
        ), $transport);
        $connection->connect()->await();
        $connection->subscribe('a', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'a:' . $message->payload;
            if ($message->payload === 'x') {
                throw new \RuntimeException('handler a');
            }
        })->await();
        $connection->subscribe('b', static function (NatsMessage $message) use (&$seen): void {
            $seen[] = 'b:' . $message->payload;
        })->await();

        return $connection;
    }
}
