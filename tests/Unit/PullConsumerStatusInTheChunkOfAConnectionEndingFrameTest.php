<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\Consumers\PullConsumerIterator;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * A pull status read in the same chunk as a frame that ends the connection is classified before the pulls leave an
 * infinite run that goes on past that frame (#214), as a status read before an EOF is classified by the retire phase.
 * The pump read that meets the frame routes what its chunk brought before it recovers the connection and throws the
 * frame's error: a status of 400 or above is then on its pull, marked done, when the engine hands the pulls over
 * (#210). The pulls used to leave the run with it unread: a terminal 409 Consumer Deleted never reached onError, and
 * the run polled on, on a deleted consumer meeting the no-responders path (a 503, routine, reported once) with no end.
 * The two share a chunk when the consumer is deleted while the client is not reading: a process blocked long enough to
 * draw 'Stale Connection' finds the 409 and the -ERR in the socket together.
 *
 * Now each pull that brought no message is classified, in issue order, once every pull's messages have gone to the
 * handler: a terminal status closes the run's inbox, goes to onError once and ends the run with its count, with no pull
 * on the new connection; a 423 drops the pin the request sent; a routine status, or none, changes nothing. A pull that
 * brought messages keeps its status unclassified, as in the retire phase.
 *
 * Over the scripted server in tests/Support/ReconnectingTransport.php, seen through tests/Support/WatchedTransport.php.
 * On the new connection the server answers each pull with a 503, as nats-server answers a pull for a consumer that no
 * longer exists, unless a test says otherwise, and onError stops the run when it hears that 503: only a run that
 * missed the 409 gets there. The tests that are not declared guards fail on 2.27.0.
 */
final class PullConsumerStatusInTheChunkOfAConnectionEndingFrameTest extends TestCase
{
    use ReconnectScenarios;

    private const PULL_SUBJECT = '$JS.API.CONSUMER.MSG.NEXT.S.C';
    private const ACK_SUBJECT = '$JS.ACK.S.C.1.1.1.0.0';
    private const STALE_CONNECTION = "-ERR 'Stale Connection'\r\n";
    private const STALE_CONNECTION_ERROR = "Server sent error frame: 'Stale Connection'";
    private const CONSUMER_DELETED = '409 JetStream pull request ended with status 409: Consumer Deleted';

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{bool}> */
    public static function reconnectTimings(): iterable
    {
        yield 'the reconnect done within the read' => [false];
        yield 'the reconnect outlasting the read\'s wait' => [true];
    }

    /**
     * The issue's probe: an infinite run of batch 3 and depth 1 whose pull, empty, gets a 409 Consumer Deleted in the
     * chunk that brings a fatal -ERR 'Stale Connection'. The reconnect is done within the read, or, with a 100 ms expiry
     * and the next dial held until the -ERR's error is reported, outlasts the read's wait (1.1 s). Either way onError gets
     * the 409 once, handle() returns 0, the run pulled only on the first connection, and the error listener heard the
     * -ERR's error once. On 2.27.0 onError got the new connection's 503 instead, and the run pulled there: once with the
     * reconnect done within the read, twice when it outlasted the read's wait (the replacement of the pull the 409 ended
     * went out from the reconnect buffer).
     */
    #[DataProvider('reconnectTimings')]
    public function testATerminalStatusInTheFramesChunkReachesOnErrorAndEndsTheRun(bool $slowReconnect): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport, self::noRespondersOnTheNewConnection());
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs($slowReconnect ? 100 : 30_000);
        $reported = self::log();
        $iterator->setOnError(self::reportingUntilA503($reported, $iterator));
        $handled = self::log();
        $run = $iterator->handle(self::recording($handled));
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        if ($slowReconnect) {
            $transport->holdNextDial();
        }

        $transport->pushFrame(self::pullStatus($server->replyTos[0], (int) $server->sid, '409 Consumer Deleted') . self::STALE_CONNECTION);
        $stateMeanwhile = null;
        if ($slowReconnect) {
            // The read's wait ends at its pull's deadline, the dial still held: the engine reports the -ERR's error then.
            $this->waitUntil(static fn(): bool => $recorder->errors !== [], 5.0);
            $stateMeanwhile = $client->state();
            $transport->releaseDial();
        }
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame([self::CONSUMER_DELETED], $reported->getArrayCopy(), 'onError got the 409, once');
        self::assertSame(0, $processed);
        self::assertSame([], $handled->getArrayCopy());
        self::assertSame([0], $server->epochs, 'no pull on the new connection');
        self::assertSame([self::STALE_CONNECTION_ERROR], $recorder->errors);
        if ($slowReconnect) {
            self::assertSame(ConnectionState::Connecting, $stateMeanwhile, 'the read gave up waiting before the reconnect was done');
        }
    }

    /**
     * What the pulls held goes to the handler before the status goes to onError, as in the retire phase, and counts: depth
     * 2 and batch 2, both pulls on the wire, and one chunk brings m-1 and m-2, which fill the first pull, a 409 Consumer
     * Deleted for the second and the fatal -ERR. The handler gets m-1 and m-2, then onError the 409, handle() returns 2,
     * and the run pulls nothing on the new connection. On 2.27.0 the run pulled twice on the new connection, onError
     * getting the 503.
     */
    public function testWhatThePullsHeldReachesTheHandlerBeforeTheStatusReachesOnError(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport, self::noRespondersOnTheNewConnection());
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000);
        $events = self::log();
        $iterator->setOnError(self::reportingUntilA503($events, $iterator, 'onError: '));
        $run = $iterator->handle(self::recording($events, 'handler: '));
        $this->waitUntil(static fn(): bool => count($server->epochs) === 2 && $watched->readsUnderWay === 1);

        $transport->pushFrame(
            self::messages((int) $server->sid, 'm-1', 'm-2')
            . self::pullStatus($server->replyTos[1], (int) $server->sid, '409 Consumer Deleted')
            . self::STALE_CONNECTION,
        );
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(['handler: m-1', 'handler: m-2', 'onError: ' . self::CONSUMER_DELETED], $events->getArrayCopy());
        self::assertSame(2, $processed);
        self::assertSame([0, 0], $server->epochs, 'no pull on the new connection');
        self::assertSame([self::STALE_CONNECTION_ERROR], $recorder->errors);
    }

    /**
     * What the overflow takes during the hand-over goes to the handler after onError, as the retire phase hands over what
     * the run holds behind a terminal status: depth 2 and batch 2, one chunk brings a 409 Consumer Deleted for the first
     * pull, m-1, which goes to the second, the oldest pull still open, and the fatal -ERR. The reconnect reopens the
     * connection within the read and replays the inbox. On m-1, the last message handed over, the new connection's
     * server sends m-2 on the inbox and the handler reads it itself: every pull is marked done by then, so m-2 goes to
     * the overflow. The handler gets m-1, onError the 409, then the handler m-2; handle() returns 2, and the run pulls
     * nothing on the new connection. On 2.27.0 the overflow went out on the next pass and the run pulled on, onError
     * getting the 503.
     */
    public function testWhatTheOverflowTookDuringTheHandOverFollowsTheStatus(): void
    {
        [$transport, $watched, $client] = $this->client();
        $server = $this->pullServer($transport, self::noRespondersOnTheNewConnection());
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000);
        $events = self::log();
        $iterator->setOnError(self::reportingUntilA503($events, $iterator, 'onError: '));
        $framesRead = [];
        $run = $iterator->handle(static function (NatsMessage $message) use ($events, $transport, $server, $client, &$framesRead): void {
            $events[] = 'handler: ' . $message->payload;
            if ($message->payload === 'm-1') {
                $transport->pushFrame(self::messages((int) $server->sid, 'm-2'));
                $framesRead[] = $client->processIncoming(new TimeoutCancellation(2))->await();
            }
        });
        $this->waitUntil(static fn(): bool => count($server->epochs) === 2 && $watched->readsUnderWay === 1);

        $transport->pushFrame(
            self::pullStatus($server->replyTos[0], (int) $server->sid, '409 Consumer Deleted')
            . self::messages((int) $server->sid, 'm-1')
            . self::STALE_CONNECTION,
        );
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame([1], $framesRead, 'the handler\'s read took m-2');
        self::assertSame(['handler: m-1', 'onError: ' . self::CONSUMER_DELETED, 'handler: m-2'], $events->getArrayCopy());
        self::assertSame(2, $processed);
        self::assertSame([0, 0], $server->epochs, 'no pull on the new connection');
    }

    /**
     * A rejection of the inbox recorded while the run closes it ends the run with the rejection, onError not called, as
     * in the retire phase (#206, #211): the pull gets a 409 Consumer Deleted with the fatal -ERR, the reconnect reopens
     * the connection within the read, and as the run's UNSUB of its inbox reaches the new connection's server, a
     * permissions -ERR naming the inbox comes ahead of the PONG (held back 50 ms). handle() throws the rejection, onError
     * heard nothing, and the run pulled nothing on the new connection. On 2.27.0 the run pulled on there, onError getting
     * the 503, and handle() returned 0.
     */
    public function testARejectionOfTheInboxWhileTheRunClosesItEndsTheRunWithItInsteadOfOnError(): void
    {
        [$transport, $watched, $client] = $this->client();
        $server = $this->pullServer($transport, self::noRespondersOnTheNewConnection());
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000);
        $reported = self::log();
        $iterator->setOnError(self::reportingUntilA503($reported, $iterator));
        $run = $iterator->handle(self::recording(self::log()));
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $inbox = self::inboxOf($server->replyTos[0]);
        $transport->pongDelay = 0.05;
        $transport->afterWrite = static function (string $bytes) use ($transport, $server, $inbox): void {
            if ($transport->epoch() >= 1 && str_starts_with($bytes, 'UNSUB ' . $server->sid . "\r\n")) {
                $transport->pushFrame(self::permissionsViolationFor($inbox));
            }
        };

        $transport->pushFrame(self::pullStatus($server->replyTos[0], (int) $server->sid, '409 Consumer Deleted') . self::STALE_CONNECTION);
        [, $error] = self::settle($run);

        self::assertInstanceOf(JetStreamException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertStringContainsString('was rejected by server permissions', $error->getMessage());
        self::assertSame([], $reported->getArrayCopy(), 'onError is not called before the rejection');
        self::assertSame([0], $server->epochs, 'no pull on the new connection');
    }

    /**
     * A rejection of the inbox recorded while the overflow is handed over after onError ends the run with the rejection,
     * as one recorded during the hand-over behind a terminal status in the retire phase does (#206). As in
     * testWhatTheOverflowTookDuringTheHandOverFollowsTheStatus, m-2 reaches the overflow during the hand-over and goes to
     * the handler once onError has the 409; on m-2 the new connection's server rejects the run's inbox (a permissions
     * -ERR naming it) and the handler reads that itself. handle() throws the rejection, the handler having got m-1 and
     * m-2 and onError the 409 once. On 2.27.0 the run pulled on, onError getting the 503.
     */
    public function testARejectionOfTheInboxDuringTheOverflowsHandOverEndsTheRunWithIt(): void
    {
        [$transport, $watched, $client] = $this->client();
        $server = $this->pullServer($transport, self::noRespondersOnTheNewConnection());
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000);
        $events = self::log();
        $iterator->setOnError(self::reportingUntilA503($events, $iterator, 'onError: '));
        $run = $iterator->handle(static function (NatsMessage $message) use ($events, $transport, $server, $client): void {
            $events[] = 'handler: ' . $message->payload;
            $frame = match ($message->payload) {
                'm-1' => self::messages((int) $server->sid, 'm-2'),
                'm-2' => self::permissionsViolationFor(self::inboxOf($server->replyTos[0])),
                default => null,
            };
            if ($frame === null) {
                return;
            }
            $transport->pushFrame($frame);
            try {
                $client->processIncoming(new TimeoutCancellation(2))->await();
            } catch (\Throwable) {
                // The rejection is the run's to report; how this read ends does not matter here.
            }
        });
        $this->waitUntil(static fn(): bool => count($server->epochs) === 2 && $watched->readsUnderWay === 1);

        $transport->pushFrame(
            self::pullStatus($server->replyTos[0], (int) $server->sid, '409 Consumer Deleted')
            . self::messages((int) $server->sid, 'm-1')
            . self::STALE_CONNECTION,
        );
        [, $error] = self::settle($run);

        self::assertInstanceOf(JetStreamException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertStringContainsString('was rejected by server permissions', $error->getMessage());
        self::assertSame(['handler: m-1', 'onError: ' . self::CONSUMER_DELETED, 'handler: m-2'], $events->getArrayCopy());
        self::assertSame([0, 0], $server->epochs, 'no pull on the new connection');
    }

    /** @return iterable<string, array{string}> */
    public static function routineStatuses(): iterable
    {
        yield '404 No Messages' => ['404 No Messages'];
        yield '408 Request Timeout' => ['408 Request Timeout'];
        yield '409 Leadership Change' => ['409 Leadership Change'];
        yield '503 No Responders' => ['503 No Responders'];
    }

    /**
     * Guard, passes on both: a routine status in the frame's chunk leaves the run going. Batch 1 and depth 1, the pull
     * gets the status and the fatal -ERR in one chunk, and the server answers the pull on the new connection with m-1, on
     * which the handler stops the run: handle() returns 1, onError heard nothing, and the run pulled once on each
     * connection. A 503 read there is not reported either: the run cannot tell an outage of the old connection's
     * JetStream from the end of that connection.
     */
    #[DataProvider('routineStatuses')]
    public function testARoutineStatusInTheFramesChunkLeavesTheRunGoing(string $status): void
    {
        [$transport, $watched, $client] = $this->client();
        $server = $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull, int $epoch): array => $epoch >= 1 ? [self::messages($sid, 'm-1')] : []);
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        $reported = self::log();
        $iterator->setOnError(self::reportingUntilA503($reported, $iterator));
        $handled = self::log();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            $iterator->stop();
        });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);

        $transport->pushFrame(self::pullStatus($server->replyTos[0], (int) $server->sid, $status) . self::STALE_CONNECTION);
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(['m-1'], $handled->getArrayCopy());
        self::assertSame(1, $processed);
        self::assertSame([], $reported->getArrayCopy());
        self::assertSame([0, 1], $server->epochs, 'one pull on each connection');
    }

    /**
     * A 423 in the frame's chunk drops the pin its request sent, and the group acquires its pin serially again, as after
     * a 423 in the retire phase: a grouped run of batch 2 and depth 2 whose first pull, serial while the group bootstraps,
     * brought m-1 with pin-1 and m-1b, so that the group fans out two pulls carrying pin-1; the first of them gets a 423
     * and the fatal -ERR in one chunk. The run's pull on the new connection goes out without a pin, and the server
     * answers it with m-2, carrying pin-2: the run, acquiring its pin, pulls no more until that pull retires, and the
     * server then sends m-3, on which the handler stops the run. On 2.27.0 the pull on the new connection carried pin-1.
     */
    public function testA423InTheFramesChunkDropsThePinItsRequestSent(): void
    {
        [$transport, $watched, $client] = $this->client();
        $server = $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull, int $epoch): array => $pull === 1
            ? [ReconnectingTransport::hmsgFrame('evt.s', $sid, "NATS/1.0\r\nNats-Pin-Id: pin-1\r\n\r\n", 'm-1', self::ACK_SUBJECT) . self::messages($sid, 'm-1b')]
            : []);
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setGroup('workers')->setBatching(2)->setDepth(2)->setExpiresMs(30_000);
        $handled = self::log();
        $run = $this->resources()->ownOperation($iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-3') {
                $iterator->stop();
            }
        }), $iterator->stop(...), 'pull consumer run');
        $this->waitUntil(static fn(): bool => count($server->epochs) === 3 && $watched->readsUnderWay === 1);

        $transport->pushFrame(self::pullStatus($server->replyTos[1], (int) $server->sid, '423 Nats-Pin-Id Mismatch') . self::STALE_CONNECTION);
        $this->waitUntil(static fn(): bool => count($server->epochs) >= 4 && $watched->readsUnderWay === 1);
        // m-2 brings pin-2 in a read of its own: the engine goes round, and would fan out again were the group not
        // acquiring its pin, before its next read is on the socket.
        $reads = $watched->reads;
        $transport->pushFrame(ReconnectingTransport::hmsgFrame('evt.s', (int) $server->sid, "NATS/1.0\r\nNats-Pin-Id: pin-2\r\n\r\n", 'm-2', self::ACK_SUBJECT));
        $this->waitUntil(static fn(): bool => $watched->reads > $reads && $watched->readsUnderWay === 1);
        $transport->pushFrame(self::messages((int) $server->sid, 'm-3'));
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(['m-1', 'm-1b', 'm-2', 'm-3'], $handled->getArrayCopy());
        self::assertSame(4, $processed);
        self::assertSame([0, 0, 0, 1], $server->epochs, 'one pull on the new connection while the group acquires its pin');
        self::assertSame([null, 'pin-1', 'pin-1', null], array_map(self::pinOfRequest(...), $server->requests), 'the pull on the new connection went out without the pin the 423 rejected');
    }

    /**
     * Guard, passes on both: a pull that brought a message keeps its status unclassified, as in the retire phase. Batch 3
     * and depth 1, one chunk brings m-1, a 409 Consumer Deleted for the same pull and the fatal -ERR; the server answers
     * the pull on the new connection with m-2 to m-4, and the handler stops the run on m-2. The handler gets m-1, then
     * m-2, handle() returns 2, and onError heard nothing.
     */
    public function testAPullThatBroughtAMessageKeepsItsStatusUnclassified(): void
    {
        [$transport, $watched, $client] = $this->client();
        $server = $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull, int $epoch): array => $epoch >= 1 ? [self::messages($sid, 'm-2', 'm-3', 'm-4')] : []);
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000);
        $reported = self::log();
        $iterator->setOnError(self::reportingUntilA503($reported, $iterator));
        $handled = self::log();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-2') {
                $iterator->stop();
            }
        });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);

        $transport->pushFrame(
            self::messages((int) $server->sid, 'm-1')
            . self::pullStatus($server->replyTos[0], (int) $server->sid, '409 Consumer Deleted')
            . self::STALE_CONNECTION,
        );
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertSame(2, $processed);
        self::assertSame([], $reported->getArrayCopy());
        self::assertSame([0, 1], $server->epochs);
    }

    /**
     * Guard, passes on both: a stop() from the handler during the hand-over keeps its precedence over a terminal status
     * behind it, as it does in the retire phase. Depth 2 and batch 2, one chunk brings m-1 and m-2 for the first pull, a
     * 409 Consumer Deleted for the second and the fatal -ERR, and the handler stops the run on m-1: handle() returns 1,
     * m-2 stays undelivered, onError heard nothing, and the run pulled nothing on the new connection.
     */
    public function testAStopDuringTheHandOverKeepsItsPrecedenceOverTheStatus(): void
    {
        [$transport, $watched, $client] = $this->client();
        $server = $this->pullServer($transport, self::noRespondersOnTheNewConnection());
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000);
        $reported = self::log();
        $iterator->setOnError(self::reportingUntilA503($reported, $iterator));
        $handled = self::log();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            $iterator->stop();
        });
        $this->waitUntil(static fn(): bool => count($server->epochs) === 2 && $watched->readsUnderWay === 1);

        $transport->pushFrame(
            self::messages((int) $server->sid, 'm-1', 'm-2')
            . self::pullStatus($server->replyTos[1], (int) $server->sid, '409 Consumer Deleted')
            . self::STALE_CONNECTION,
        );
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(1, $processed);
        self::assertSame(['m-1'], $handled->getArrayCopy());
        self::assertSame([], $reported->getArrayCopy());
        self::assertSame([0, 0], $server->epochs);
    }

    /**
     * A connected client over a watched ReconnectingTransport, closed in tearDown, with no heartbeat, so only the engine
     * and the test read: reconnect on, with waiting for it enabled and a thousand attempts.
     *
     * @return array{ReconnectingTransport, WatchedTransport, NatsClient, LifecycleRecorder}
     */
    private function client(): array
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $client = $this->own(new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: 2_000,
            reconnectEnabled: true,
            maxReconnectAttempts: 1_000,
            reconnectDelayMs: 5,
            reconnectMaxDelayMs: 20,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            connectionListener: $recorder->connectionListener(),
            errorListener: $recorder->errorListener(),
            waitForReconnect: true,
        ), $watched));
        $this->opened[] = $client;
        $client->connect()->await();

        return [$transport, $watched, $client, $recorder];
    }

    /**
     * The scripted server's side of the pull consumer: records the epoch (session), the JSON request and the reply subject
     * of every pull, and the sid of the run's inbox, and answers each pull with what $onPull returns, given the pull's
     * reply subject, the inbox's sid, the pull's number (1 for the first) and its epoch; with no frames the server holds
     * the pull, as one with no message does until it expires.
     *
     * @param \Closure(string, int, int, int): list<string> $onPull
     * @return object{sid: ?int, epochs: list<int>, requests: list<string>, replyTos: list<string>}
     */
    private function pullServer(ReconnectingTransport $transport, \Closure $onPull): object
    {
        $server = new class {
            /** The sid the client subscribed the run's inbox with. */
            public ?int $sid = null;
            /** @var list<int> The epoch each pull came on, in order. */
            public array $epochs = [];
            /** @var list<string> The JSON body of each pull request, in order. */
            public array $requests = [];
            /** @var list<string> The reply subject of each pull, in order. */
            public array $replyTos = [];
        };
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport, $server, $onPull): array {
            if ($replyTo === null || $subject !== self::PULL_SUBJECT) {
                return [];
            }

            $server->epochs[] = $transport->epoch();
            $server->requests[] = $payload;
            $server->replyTos[] = $replyTo;
            $sid = $transport->sidFor($replyTo);
            if ($sid === null) {
                return [];
            }

            $server->sid ??= $sid;

            return $onPull($replyTo, $sid, count($server->epochs), $transport->epoch());
        };

        return $server;
    }

    /**
     * Holds every pull on the first connection and answers each pull on a new one with a 503 status, as nats-server
     * answers a pull for a consumer that no longer exists.
     *
     * @return \Closure(string, int, int, int): list<string>
     */
    private static function noRespondersOnTheNewConnection(): \Closure
    {
        return static fn(string $replyTo, int $sid, int $pull, int $epoch): array => $epoch >= 1 ? [self::pullStatus($replyTo, $sid, '503')] : [];
    }

    /**
     * An onError that logs each status as "<code> <message>", after $prefix, and stops the run on a 503: only a run that
     * missed the status in the frame's chunk pulls on the new connection, where every pull gets one.
     *
     * @param \ArrayObject<int, string> $log
     * @return \Closure(\Throwable): void
     */
    private static function reportingUntilA503(\ArrayObject $log, PullConsumerIterator $iterator, string $prefix = ''): \Closure
    {
        return static function (\Throwable $status) use ($log, $iterator, $prefix): void {
            $log[] = $prefix . $status->getCode() . ' ' . $status->getMessage();
            if ($status->getCode() === 503) {
                $iterator->stop();
            }
        };
    }

    /**
     * A handler that logs each payload, after $prefix.
     *
     * @param \ArrayObject<int, string> $log
     * @return \Closure(NatsMessage): void
     */
    private static function recording(\ArrayObject $log, string $prefix = ''): \Closure
    {
        return static function (NatsMessage $message) use ($log, $prefix): void {
            $log[] = $prefix . $message->payload;
        };
    }

    /** A pull status, "<code>" or "<code> <description>", on the pull's reply subject, as JetStream sends one. */
    private static function pullStatus(string $replyTo, int $sid, string $status): string
    {
        return ReconnectingTransport::hmsgFrame($replyTo, $sid, 'NATS/1.0 ' . $status . "\r\n\r\n", '');
    }

    /** The subject the run subscribed its inbox with, "<base>.*", from one of its pulls' reply subjects. */
    private static function inboxOf(string $replyTo): string
    {
        return substr($replyTo, 0, (int) strrpos($replyTo, '.')) . '.*';
    }

    /** The server's rejection of a subscription to $subject for permissions. */
    private static function permissionsViolationFor(string $subject): string
    {
        return sprintf("-ERR 'Permissions Violation for Subscription to \"%s\"'\r\n", $subject);
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

    /** The pin id a pull request carried, or null for none. */
    private static function pinOfRequest(string $request): ?string
    {
        $decoded = json_decode($request, true, 512, JSON_THROW_ON_ERROR);
        $pin = is_array($decoded) ? ($decoded['id'] ?? null) : null;

        return is_string($pin) ? $pin : null;
    }

    /** @return \ArrayObject<int, string> */
    private static function log(): \ArrayObject
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
