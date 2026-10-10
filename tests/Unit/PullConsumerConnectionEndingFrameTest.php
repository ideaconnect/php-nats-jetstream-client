<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\Consumers\PullConsumerIterator;
use IDCT\NATS\Tests\Support\DrainFlushWatcher;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use IDCT\NATS\Transport\TransportClosedException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;
use Psr\Log\LoggerInterface;

use function Amp\async;

/**
 * An infinite pull consumer run outlives a connection that a frame ended as it outlives a lost one (#210). With
 * reconnect on and waiting for it enabled, the pump read that meets a fatal -ERR ('Stale Connection' after the event
 * loop was blocked for a few of the server's ping intervals, 'User Authentication Expired' for a short-lived JWT that
 * the jwtProvider renews on the reconnect) or a PING whose PONG the socket will not take recovers the connection and
 * throws the frame's error once the reconnect is done, or once its own wait ends (#171). The engine took that as the
 * end of the run: it handed over what its pulls held (#197) and handle() threw, the connection Open on the new socket,
 * where after an EOF the run pulled again on the new connection. The run now goes on: the frame's error goes to the
 * error listener and the logger, what the pulls held goes to the handler as a delivery of the run, counted in its
 * total, a stop() and a close that discards ending it as they end the hand-over before a failure (#197, #207) and a
 * group pin captured from it as in the retire phase, and the run pulls on the new connection; while a reconnect slower
 * than the read's wait is still under way, it goes round as after an EOF whose reconnect outlasts that wait, the pulls
 * still open kept in flight. A finite run, waiting disabled, reconnect off, a reconnect that gives up while the read
 * that met the frame still waits for it, a close of the application's under way, and every failure that leaves the
 * connection open still end the run as in 2.22.0; a reconnect that gives up later ends it as after an EOF, with the
 * reconnect's error.
 *
 * Over the scripted server in tests/Support/ReconnectingTransport.php, seen through tests/Support/WatchedTransport.php,
 * which counts the reads of the socket: each message the server sends is read before the next is sent, and the frame
 * comes only once the engine's next read is on the socket, so the pulls hold what the test sent when the connection
 * ends. The order of events decides every outcome, except where a test needs a pull's deadline to pass while a
 * reconnect is held, which it stages with a 100 ms expiry (and, for two deadlines, a half-second margin between them);
 * the other time bounds only keep a broken run from hanging the suite. Each test fails on 2.22.0, where handle() threw
 * the frame's error, except the EOF control data sets and the declared guards, which pass on both. A drain() of the
 * client still Draining lets the hand-over deliver what the pulls held, the run issuing no pull while it drains (#207):
 * the two tests of that fail on 2.23.0 as well, where any close ended the hand-over. The two #189 restart data sets fail
 * on 2.24.6, where the handle() after the stop cleared it: the first handler got the rest of the hand-over, and the
 * first run pulled on the new connection next to the second.
 */
final class PullConsumerConnectionEndingFrameTest extends TestCase
{
    use ReconnectScenarios;

    private const PULL_SUBJECT = '$JS.API.CONSUMER.MSG.NEXT.S.C';
    private const ACK_SUBJECT = '$JS.ACK.S.C.1.1.1.0.0';
    private const STALE_CONNECTION = "-ERR 'Stale Connection'\r\n";
    private const STALE_CONNECTION_ERROR = "Server sent error frame: 'Stale Connection'";
    private const PONG_FAILURE = 'a PING whose PONG write fails';

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{string, bool, string}> */
    public static function connectionEndings(): iterable
    {
        $endings = [
            'EOF' => 'Socket closed by peer (EOF)',
            "-ERR 'Stale Connection'" => self::STALE_CONNECTION_ERROR,
            "-ERR 'User Authentication Expired'" => "Server sent error frame: 'User Authentication Expired'",
            self::PONG_FAILURE => 'The connection is gone',
        ];
        foreach ($endings as $ending => $reported) {
            yield $ending . ', nothing buffered' => [$ending, false, $reported];
            yield $ending . ', the pull holds m-1' => [$ending, true, $reported];
        }
    }

    /**
     * An infinite run of batch 3, depth 1 and a 30 s expiry, whose pull holds m-1 or nothing, meets the end of the
     * connection under its pump read, the reconnect reopening it at once: an EOF (the control), a fatal -ERR, or a PING
     * whose PONG write fails. The server answers the pull on the new connection with m-2 to m-4, and the handler stops
     * the run on m-2. The run goes on after each of them: the handler gets m-1, when the pull held it, then m-2, handle()
     * returns the number of messages the handler got, the run pulled once on each connection, the connection is Open
     * after one Disconnected and one Reconnected, and the error listener and the logger heard why the connection ended:
     * the connection reports an EOF itself, and the engine reports the frame's error, which only its read got. After an
     * EOF the reset at the top of the loop drops what the pull held (#187), which this test leaves alone: it checks only
     * that the handler got m-2 last. On 2.22.0 each frame ended the run, handle() throwing the frame's error with the
     * connection Open, the run having pulled only on the first connection and the error reported to nobody.
     */
    #[DataProvider('connectionEndings')]
    public function testAnInfiniteRunGoesOnAfterAFrameEndsTheConnectionAsAfterAnEof(string $ending, bool $buffered, string $reported): void
    {
        $logger = new class extends AbstractLogger {
            /** @var list<string> */
            public array $messages = [];

            /** @param array<mixed> $context */
            public function log($level, string|\Stringable $message, array $context = []): void
            {
                $this->messages[] = (string) $message;
            }
        };
        [$transport, $watched, $client, $recorder] = $this->client(logger: $logger);
        $server = $this->pullServer($transport, self::answerOnTheNewConnection(true, 'm-2', 'm-3', 'm-4'));
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000);
        $handled = self::payloadLog();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-2') {
                $iterator->stop();
            }
        });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        if ($buffered) {
            $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1']);
        }

        self::endTheConnection($transport, $ending);
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame('m-2', $handled[count($handled) - 1] ?? null, 'the run pulled on the new connection and the handler got m-2');
        self::assertSame(count($handled), $processed, 'handle() returned the number of messages the handler got');
        if ($ending !== 'EOF') {
            self::assertSame($buffered ? ['m-1', 'm-2'] : ['m-2'], $handled->getArrayCopy(), 'what the pull held went to the handler first');
        }
        self::assertSame([0, 1], $server->epochs, 'one pull on each connection');
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Disconnected, ConnectionEvent::Reconnected], $recorder->events);
        self::assertSame(ConnectionState::Open, $client->state());
        self::assertSame([$reported], $recorder->errors, 'the error listener heard why the connection ended, once');
        self::assertContains('NATS connection error: ' . $reported, $logger->messages, 'and so did the logger');
    }

    /**
     * What two pulls held when a frame ended the connection goes to the handler in issue order and counts towards the
     * run: depth 2 and batch 2, both pulls on the wire, and one chunk brings m-1 and m-2, which fill the first pull, m-3,
     * which goes into the second, and a fatal -ERR, so the engine's read buffers the three and fails before the engine
     * could retire the first pull. The server answers the first pull on the new connection with m-4 and m-5, and the
     * handler stops the run on m-4: it got m-1, m-2, m-3 and m-4, in that order, and handle() returns 4. Left out of
     * the run's count, the hand-over made handle() return 1. On 2.22.0 handle() threw the -ERR's error once the handler
     * had m-1, m-2 and m-3.
     */
    public function testWhatThePullsHeldGoesToTheHandlerInIssueOrderAndCountsTowardsTheRun(): void
    {
        [$transport, $watched, $client] = $this->client();
        $server = $this->pullServer($transport, self::answerOnTheNewConnection(false, 'm-4', 'm-5'));
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000);
        $handled = self::payloadLog();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-4') {
                $iterator->stop();
            }
        });
        $this->waitUntil(static fn(): bool => count($server->epochs) === 2 && $watched->readsUnderWay === 1);

        $transport->pushFrame(self::messages((int) $server->sid, 'm-1', 'm-2', 'm-3') . self::STALE_CONNECTION);
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(['m-1', 'm-2', 'm-3', 'm-4'], $handled->getArrayCopy(), 'both pulls\' messages in issue order, then the new connection\'s');
        self::assertSame(4, $processed, 'the messages handed over count towards the run');
        self::assertSame([0, 0, 1, 1], $server->epochs, 'two pulls on each connection');
    }

    /**
     * A pinned_client group's pin belongs to the consumer, not the connection: a grouped run of batch 3 and depth 1 whose
     * pull holds m-1, which carries the server's Nats-Pin-Id, when a fatal -ERR ends the connection captures the pin
     * from m-1 as it hands it over, as the retire phase captures one, and its pull on the new connection carries it. The
     * handler gets m-1, then m-2 from that pull. On 2.22.0 handle() threw the -ERR's error once the handler had m-1, and
     * the run never pulled on the new connection.
     */
    public function testAPinnedGroupPullsOnTheNewConnectionWithThePinTheHandedOverMessageBrought(): void
    {
        [$transport, $watched, $client] = $this->client();
        $server = $this->pullServer($transport, self::answerOnTheNewConnection(true, 'm-2', 'm-3', 'm-4'));
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setGroup('workers')->setBatching(3)->setDepth(1)->setExpiresMs(30_000);
        $handled = self::payloadLog();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-2') {
                $iterator->stop();
            }
        });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $reads = $watched->reads;
        $transport->pushFrame(ReconnectingTransport::hmsgFrame('evt.s', (int) $server->sid, "NATS/1.0\r\nNats-Pin-Id: pin-1\r\n\r\n", 'm-1', self::ACK_SUBJECT));
        $this->waitUntil(static fn(): bool => $watched->reads > $reads && $watched->readsUnderWay === 1);

        $transport->pushFrame(self::STALE_CONNECTION);
        [, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertSame([0, 1], $server->epochs);
        self::assertSame([null, 'pin-1'], array_map(self::pinOfRequest(...), $server->requests), 'the pull on the new connection carried the pin m-1 brought');
    }

    /** @return iterable<string, array{bool, list<string>, list<int>}> */
    public static function groupBootstraps(): iterable
    {
        yield 'the pull holds m-1: the group has delivered and fans out' => [true, ['m-1', 'm-2'], [0, 1, 1]];
        yield 'nothing buffered: the group has not delivered and pulls one at a time' => [false, ['m-2'], [0, 1]];
    }

    /**
     * The hand-over is a delivery of the run, so it ends a group's bootstrap as a delivery in the retire phase does, and
     * only when it delivered: a grouped run of depth 2 pulls serially until its first delivery, so that a pinned_client
     * group captures its pin before it fans out. Its first pull holds m-1, which carries no Nats-Pin-Id, as an overflow
     * or prioritized group's messages never do, or nothing, when a fatal -ERR ends the connection. With m-1 handed over
     * and no pin captured, the run is a group that has delivered without pinning, and it fans out on the new connection,
     * two pulls at once; with nothing handed over it still pulls one at a time there. The handler gets m-2 on the new
     * connection either way. Not counted as the run's first delivery, the hand-over of m-1 left the run pulling one at a
     * time. On 2.22.0 handle() threw the -ERR's error.
     *
     * @param list<string> $expectedHandled
     * @param list<int> $expectedEpochs
     */
    #[DataProvider('groupBootstraps')]
    public function testAnUnpinnedGroupFansOutOnTheNewConnectionOnlyOnceTheHandOverDelivered(bool $buffered, array $expectedHandled, array $expectedEpochs): void
    {
        [$transport, $watched, $client] = $this->client();
        $server = $this->pullServer($transport, self::answerOnTheNewConnection(false, 'm-2', 'm-3', 'm-4'));
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setGroup('workers')->setBatching(3)->setDepth(2)->setExpiresMs(30_000);
        $handled = self::payloadLog();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-2') {
                $iterator->stop();
            }
        });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        if ($buffered) {
            $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1']);
        }

        $transport->pushFrame(self::STALE_CONNECTION);
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame($expectedHandled, $handled->getArrayCopy());
        self::assertSame(count($expectedHandled), $processed);
        self::assertSame($expectedEpochs, $server->epochs, 'one pull while bootstrapping, then as many as the group may on the new connection');
        self::assertSame(array_fill(0, count($expectedEpochs), null), array_map(self::pinOfRequest(...), $server->requests), 'no pin was captured');
    }

    /**
     * The hand-over re-arms the one-shot no-responders signal as a delivery in the retire phase does: JetStream answered
     * with data again, so a later outage is reported once more. The first pull is answered with 503, which onError hears
     * once; the next pull holds m-1 when a fatal -ERR ends the connection, and m-1 is handed over; the first pull on the
     * new connection is answered with 503 again, which onError hears again, and the next one brings m-2, on which the
     * handler stops the run. Not re-armed by the hand-over, the second outage went unreported. On 2.22.0 handle() threw
     * the -ERR's error once the handler had m-1.
     */
    public function testTheHandOverReArmsTheNoRespondersSignalAsADeliveryDoes(): void
    {
        [$transport, $watched, $client] = $this->client();
        $server = $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull, int $epoch): array => match ($pull) {
            1, 3 => [ReconnectingTransport::hmsgFrame($replyTo, $sid, "NATS/1.0 503 No Responders\r\n\r\n", '')],
            4 => [self::messages($sid, 'm-2', 'm-3', 'm-4')],
            default => [],
        });
        /** @var \ArrayObject<int, int> $outages */
        $outages = new \ArrayObject();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000)
            ->setOnError(static function (JetStreamException $error) use ($outages): void {
                $outages[] = $error->getCode();
            });
        $handled = self::payloadLog();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-2') {
                $iterator->stop();
            }
        });
        $this->waitUntil(static fn(): bool => count($server->epochs) === 2 && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1']);

        $transport->pushFrame(self::STALE_CONNECTION);
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertSame(2, $processed);
        self::assertSame([503, 503], $outages->getArrayCopy(), 'each outage was reported once');
        self::assertSame([0, 0, 1, 1], $server->epochs);
    }

    /**
     * A handler that throws while what the pulls held is handed over ends the run with its own exception, as a handler
     * that throws in the retire phase does: the run goes on as a run, not as one that fails. The pull holds m-1 when a
     * fatal -ERR ends the connection, and the handler throws on m-1: handle() throws the handler's exception, the run
     * pulls nothing on the new connection, and the error listener heard the -ERR's error, which the engine reported
     * before the hand-over, and not the handler's. On 2.22.0 handle() threw the -ERR's error, the handler's exception
     * going to the error listener.
     */
    public function testAHandlerThatThrowsDuringTheHandOverEndsTheRunWithItsOwnException(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport, self::answerOnTheNewConnection(true, 'm-2'));
        $handlerFailure = new \RuntimeException('the handler failed on m-1');
        $handled = self::payloadLog();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled, $handlerFailure): void {
                $handled[] = $message->payload;

                throw $handlerFailure;
            });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1']);

        $transport->pushFrame(self::STALE_CONNECTION);
        [, $error] = self::settle($run);

        self::assertSame($handlerFailure, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(['m-1'], $handled->getArrayCopy());
        self::assertSame([self::STALE_CONNECTION_ERROR], $recorder->errors, 'the error listener heard the -ERR\'s error, and only that');
        self::assertSame([0], $server->epochs, 'no pull on the new connection');
        self::assertSame(ConnectionState::Open, $client->state());
    }

    /**
     * A stop() from the handler during that hand-over leaves the rest undelivered, and the run ends as a stopped run,
     * returning its count: depth 2 and batch 2, the two pulls hold m-1 and m-2, and m-3, when a fatal -ERR ends the
     * connection, and the handler stops the run on m-1. Neither m-2, in the same pull, nor m-3, in the next, is handed
     * over, handle() returns 1, and the run pulls nothing on the new connection. On 2.22.0 handle() threw the -ERR's
     * error.
     */
    public function testAStopFromTheHandlerDuringTheHandOverEndsTheRunWithItsCount(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport, self::answerOnTheNewConnection(true, 'm-4', 'm-5'));
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000);
        $handled = self::payloadLog();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            $iterator->stop();
        });
        $this->waitUntil(static fn(): bool => count($server->epochs) === 2 && $watched->readsUnderWay === 1);

        $transport->pushFrame(self::messages((int) $server->sid, 'm-1', 'm-2', 'm-3') . self::STALE_CONNECTION);
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(1, $processed);
        self::assertSame(['m-1'], $handled->getArrayCopy(), 'the stop leaves m-2 and m-3 undelivered');
        self::assertSame([0, 0], $server->epochs, 'no pull on the new connection');
        self::assertSame([self::STALE_CONNECTION_ERROR], $recorder->errors);
    }

    /** @return iterable<string, array{bool}> */
    public static function restartsDuringTheHandOver(): iterable
    {
        yield 'the handler restarts its consumer' => [false];
        yield 'another fiber restarts it while the handler waits' => [true];
    }

    /**
     * The restart idiom during that hand-over leaves the rest undelivered, and the run ends as a stopped run, returning
     * its count, with no pull on the new connection (#189): depth 2 and batch 2, the two pulls hold m-1 and m-2, and m-3,
     * when a fatal -ERR ends the connection, the reconnect reopening it, and on m-1 the run is stopped and another
     * started, by the handler itself, or by another fiber while the handler waits on m-1. The first handler gets m-1
     * only, handle() returns 1, and only the second run pulls on the new connection, on an inbox of its own, until a
     * stop() ends it. The handle() after the stop used to clear it: the first handler got m-2 and m-3 as well, and the
     * first run went on pulling on the new connection next to the second.
     */
    #[DataProvider('restartsDuringTheHandOver')]
    public function testARestartDuringTheHandOverEndsTheRunWithItsCountAndNoPullOnTheNewConnection(bool $fromAnotherFiber): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport, static fn(): array => []);
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000);
        $handled = self::payloadLog();
        $restarted = new class {
            /** @var Future<int>|null The run started on m-1. */
            public ?Future $run = null;
        };
        $restart = static function () use ($iterator, $handled, $restarted): void {
            $iterator->stop();
            $restarted->run = $iterator->handle(static function (NatsMessage $message) use ($handled): void {
                $handled[] = 'second handler: ' . $message->payload;
            });
            // Should the test fail with the run still going, the close in tearDown ends it: nothing is to report it.
            $restarted->run->ignore();
        };
        /** @var DeferredFuture<null> $holding */
        $holding = new DeferredFuture();
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $restart, $fromAnotherFiber, $holding, $gate): void {
            $handled[] = 'first handler: ' . $message->payload;
            if ($message->payload !== 'm-1') {
                return;
            }
            if (!$fromAnotherFiber) {
                $restart();

                return;
            }
            $holding->complete();
            $gate->getFuture()->await(new TimeoutCancellation(5));
        });
        $this->waitUntil(static fn(): bool => count($server->epochs) === 2 && $watched->readsUnderWay === 1);

        $transport->pushFrame(self::messages((int) $server->sid, 'm-1', 'm-2', 'm-3') . self::STALE_CONNECTION);
        if ($fromAnotherFiber) {
            $holding->getFuture()->await(new TimeoutCancellation(5));
            $restart();
            $gate->complete();
        }
        [$processed, $error] = self::settle($run);

        self::assertSame(['first handler: m-1'], $handled->getArrayCopy(), 'the stop leaves m-2 and m-3 undelivered');
        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(1, $processed);
        $second = $restarted->run;
        if ($second === null) {
            self::fail('the consumer was never restarted');
        }
        // The second run's generation, on the new connection.
        $this->waitUntil(static fn(): bool => count($server->epochs) === 4);
        $firstInbox = (int) $server->sid;
        self::assertSame([0, 0, 1, 1], $server->epochs);
        self::assertSame([$firstInbox, $firstInbox], array_slice($server->sids, 0, 2), 'the first run pulled on the old connection only');
        self::assertNotContains($firstInbox, array_slice($server->sids, 2), 'the pulls on the new connection are the second run\'s');
        self::assertSame([self::STALE_CONNECTION_ERROR], $recorder->errors);

        $iterator->stop();
        [$secondProcessed, $secondError] = self::settle($second);
        self::assertNull($secondError, sprintf('the second run threw %s', self::describe($secondError)));
        self::assertSame(0, $secondProcessed);
        self::assertCount(4, $server->epochs, 'no pull after the stop');
    }

    /**
     * The iterator's drain() while the engine's read waits for the reconnect ends the run with its count once what the
     * pull held is handed over: the pull holds m-1 and m-2 when a fatal -ERR ends the connection, and the reconnect's
     * dial is held, so the read that met the -ERR waits for it; the drain() wakes that wait, the read throws the -ERR's
     * error, and the run hands both messages over and, drained, pulls no more. handle() returns 2 with the reconnect
     * still under way and the -ERR's error reported, and the connection comes back once the dial is let through. On
     * 2.22.0 handle() threw the -ERR's error after the handler had m-1 and m-2.
     */
    public function testADrainOfTheIteratorDuringTheReconnectWaitEndsTheRunWithItsCount(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport, self::answerOnTheNewConnection(true, 'm-3'));
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000);
        $handled = self::payloadLog();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled): void {
            $handled[] = $message->payload;
        });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1', 'm-2']);
        $transport->holdNextDial();

        $transport->pushFrame(self::STALE_CONNECTION);
        // The reconnect is dialling, held, and the read that met the -ERR waits for it.
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
        $iterator->drain();
        [$processed, $error] = self::settle($run);
        $stateWhenTheRunEnded = $client->state();
        $transport->releaseDial();
        $this->waitUntilOpen($client);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(2, $processed);
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertSame(ConnectionState::Connecting, $stateWhenTheRunEnded, 'the run ended with the reconnect still under way');
        self::assertSame([0], $server->epochs, 'no pull after the drain');
        self::assertSame([self::STALE_CONNECTION_ERROR], $recorder->errors);
    }

    /**
     * A disconnect() of the application's during that hand-over ends it, as it ends the hand-over before a run fails
     * (#197), and the run then ends with the close's error, as any run whose connection the application closes: the two
     * pulls hold m-1 and m-2, and m-3, when a fatal -ERR ends the connection, the reconnect reopening it, and the handler
     * closes the connection on m-1. Neither m-2 nor m-3 reaches the handler, which would otherwise run on after the
     * close, and handle() throws "Connection is not open" from the run's next pull, which never reached the server. On
     * 2.22.0 handle() threw the -ERR's error.
     */
    public function testADisconnectDuringTheHandOverEndsItAndTheRunWithTheClosesError(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport, self::answerOnTheNewConnection(true, 'm-4', 'm-5'));
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
        self::assertSame('Connection is not open', $error->getMessage(), 'the run ends where it meets the close');
        self::assertSame(['m-1'], $handled->getArrayCopy(), 'the disconnect() leaves m-2 and m-3 undelivered');
        self::assertSame([0, 0], $server->epochs, 'no pull reached the new connection');
        self::assertSame(ConnectionState::Closed, $client->state());
        self::assertSame([self::STALE_CONNECTION_ERROR], $recorder->errors);
    }

    /**
     * A message read while the handler runs during that hand-over goes to a pull still to be handed over: depth 2 and
     * batch 3, the first pull holds m-1 and m-2 and the second none when a fatal -ERR ends the connection, the reconnect
     * reopening it and replaying the inbox. On m-1 the server sends m-3 on the new connection and the handler reads it
     * itself: the first pull, being handed over, takes nothing more, so m-3 goes into the second pull and reaches the
     * handler after m-2. Then the run pulls on the new connection, gets m-4, and the handler stops it there. Added to the
     * buffer being handed over, m-3 would have been dropped with it. On 2.22.0 handle() threw the -ERR's error once the
     * handler had m-1, m-2 and m-3.
     */
    public function testAMessageReadWhileTheHandlerRunsDuringTheHandOverGoesToAPullStillToBeHandedOver(): void
    {
        [$transport, $watched, $client] = $this->client();
        $server = $this->pullServer($transport, self::answerOnTheNewConnection(false, 'm-4', 'm-5', 'm-6'));
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(2)->setExpiresMs(30_000);
        $handled = self::payloadLog();
        $framesRead = [];
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $transport, $server, $client, $iterator, &$framesRead): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-1') {
                // The new connection's server sends m-3 on the replayed inbox: the handler's own read takes it.
                $transport->pushFrame(self::messages((int) $server->sid, 'm-3'));
                $framesRead[] = $client->processIncoming(new TimeoutCancellation(2))->await();
            }
            if ($message->payload === 'm-4') {
                $iterator->stop();
            }
        });
        $this->waitUntil(static fn(): bool => count($server->epochs) === 2 && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1', 'm-2']);

        $transport->pushFrame(self::STALE_CONNECTION);
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame([1], $framesRead, 'the handler\'s read took m-3');
        self::assertSame(['m-1', 'm-2', 'm-3', 'm-4'], $handled->getArrayCopy(), 'm-3 went into the second pull, still to be handed over');
        self::assertSame(4, $processed);
        self::assertSame([0, 0, 1, 1], $server->epochs);
    }

    /**
     * A reconnect slower than the pump read's wait still leaves the run going: with a 100 ms expiry the read waits for
     * the reconnect up to its pull's deadline, 1.1 s after the pull, and the reconnect's dial is held past it, so the read
     * throws the -ERR's error with the reconnect still under way. The run goes round as after an EOF whose reconnect
     * outlasted that wait: it reports the error, hands m-1 over, and its next read waits for the reconnect. Once the test
     * lets the dial through, the run pulls on the new connection and the handler gets m-2 there. The pull the run issues
     * while the reconnect is under way goes out from the reconnect buffer on the new connection, where the reset at the
     * top of the loop drops it from the run (#187), so this test checks only the first pull's connection and the last
     * one's. On 2.22.0 handle() threw the -ERR's error at the read's deadline, the handler having m-1.
     */
    public function testAReconnectSlowerThanThePumpReadsWaitStillLeavesTheRunGoing(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport, self::answerOnTheNewConnection(true, 'm-2', 'm-3', 'm-4'));
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(100);
        $handled = self::payloadLog();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-2') {
                $iterator->stop();
            }
        });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1']);
        $transport->holdNextDial();

        $transport->pushFrame(self::STALE_CONNECTION);
        // The read's wait ends at its pull's deadline, the dial still held: the engine then reports the -ERR's error and
        // goes round, or, on 2.22.0, the run ends.
        $this->waitUntil(static fn(): bool => $run->isComplete() || $recorder->errorsContaining('Stale Connection') !== [], 5.0);
        $wentOnDuringTheReconnect = !$run->isComplete();
        $stateMeanwhile = $client->state();
        $transport->releaseDial();
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertTrue($wentOnDuringTheReconnect, 'the run was still going when its read gave up waiting for the reconnect');
        self::assertSame(ConnectionState::Connecting, $stateMeanwhile, 'the read gave up waiting before the reconnect was done');
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertSame(2, $processed);
        self::assertSame(0, $server->epochs[0] ?? null, 'the first pull came on the first connection');
        self::assertSame(1, $server->epochs[count($server->epochs) - 1] ?? null, 'the run pulled on the new connection');
        self::assertSame(ConnectionState::Open, $client->state());
        self::assertSame([self::STALE_CONNECTION_ERROR], $recorder->errors);
    }

    /**
     * A reconnect slower than the pump read's wait replaces only the pulls whose deadline passed, as after an EOF whose
     * reconnect outlasted that wait: depth 2, batch 1 and a 100 ms expiry, the first pull answered with m-1 half a
     * second in, so that its replacement's deadline comes half a second after the second pull's. The reconnect's dial
     * is held, and the logger lets it through as the engine reports the -ERR's error, which it does once its read's
     * wait ends at the second pull's deadline, so that the reconnect is still under way while the engine goes round.
     * The run keeps the third pull in flight, emptied, replaces only the second, and once the reconnect is done pulls a
     * fresh generation: the new connection gets three pulls, as after an EOF, where a whole generation issued during
     * the reconnect sent it four, the extra one dropped from the run by the reset at the top of the loop while the
     * server still serves it (#187). The handler gets m-1, then m-9 on the new connection, where it stops the run.
     */
    public function testAReconnectSlowerThanThePumpReadsWaitReplacesOnlyThePullsWhoseDeadlinePassed(): void
    {
        $logger = new class extends AbstractLogger {
            public ?ReconnectingTransport $transport = null;

            /** @param array<mixed> $context */
            public function log($level, string|\Stringable $message, array $context = []): void
            {
                if (str_contains((string) $message, 'Stale Connection')) {
                    $this->transport?->releaseDial();
                }
            }
        };
        [$transport, $watched, $client, $recorder] = $this->client(logger: $logger);
        $logger->transport = $transport;
        $server = $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull, int $epoch): array => $epoch >= 1 ? [self::messages($sid, 'm-9')] : []);
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(100);
        $handled = self::payloadLog();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-9') {
                $iterator->stop();
            }
        });
        $this->waitUntil(static fn(): bool => count($server->epochs) === 2 && $watched->readsUnderWay === 1);
        // Half a second between the second pull's deadline and its replacement's: a margin no loaded runner eats.
        \Amp\delay(0.5);
        $transport->pushFrame(self::messages((int) $server->sid, 'm-1'));
        $this->waitUntil(static fn(): bool => count($server->epochs) === 3 && $watched->readsUnderWay === 1);
        $transport->holdNextDial();

        $transport->pushFrame(self::STALE_CONNECTION);
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(['m-1', 'm-9'], $handled->getArrayCopy());
        self::assertSame(2, $processed);
        self::assertSame([0, 0, 0, 1, 1, 1], $server->epochs, 'one pull replaced during the reconnect, then a fresh generation');
        self::assertSame([self::STALE_CONNECTION_ERROR], $recorder->errors);
    }

    /** @return iterable<string, array{string, string}> */
    public static function runsThatEndAtTheFrame(): iterable
    {
        foreach (['a finite run', 'waiting for the reconnect disabled', 'reconnect off', 'the reconnect gives up'] as $run) {
            yield $run . ", -ERR 'Stale Connection'" => [$run, "-ERR 'Stale Connection'"];
            yield $run . ', ' . self::PONG_FAILURE => [$run, self::PONG_FAILURE];
        }
    }

    /**
     * Guard, passes on both: a run that cannot go on past the frame still ends with its error, having handed over what
     * its pull held (#197), unchanged by #210, as each of them ends after an EOF: a finite run of one iteration with the
     * reconnect reopening the connection; an infinite one with waiting for the reconnect disabled, whose read throws at
     * once while the reconnect, its dial held, is under way; one with reconnect off, the connection closed for good; and
     * one whose reconnect gives up while the read still waits for it, three attempts with dials refused (one that gives
     * up later ends the run as after an EOF instead, with the reconnect's error,
     * testAReconnectThatGivesUpAfterThePumpReadStoppedWaitingEndsTheRunAsAfterAnEof). The pull holds m-1 when a fatal
     * -ERR, or a PING whose PONG write fails, ends the connection: the handler gets m-1, handle() throws the frame's
     * error, the ConnectionException of the -ERR or the TransportClosedException of the failed write, the run pulled
     * once, and the engine reports nothing, since the run throws the error.
     */
    #[DataProvider('runsThatEndAtTheFrame')]
    public function testARunThatCannotGoOnPastTheFrameStillHandsOverAndThrowsItsError(string $run, string $ending): void
    {
        [$transport, $watched, $client, $recorder] = match ($run) {
            'waiting for the reconnect disabled' => $this->client(waitForReconnect: false),
            'reconnect off' => $this->client(reconnect: false),
            'the reconnect gives up' => $this->client(maxReconnectAttempts: 3),
            default => $this->client(),
        };
        $server = $this->pullServer($transport, self::answerOnTheNewConnection(true, 'm-2'));
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000);
        if ($run === 'a finite run') {
            $iterator->setIterations(1);
        }
        $handled = self::payloadLog();
        $handling = $iterator->handle(static function (NatsMessage $message) use ($handled): void {
            $handled[] = $message->payload;
        });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1']);
        if ($run === 'waiting for the reconnect disabled') {
            $transport->holdNextDial();
        } elseif ($run === 'the reconnect gives up') {
            $transport->refuseDials();
        }

        self::endTheConnection($transport, $ending);
        [$processed, $error] = self::settle($handling);

        [$class, $message] = $ending === self::PONG_FAILURE
            ? [TransportClosedException::class, 'The connection is gone']
            : [ConnectionException::class, self::STALE_CONNECTION_ERROR];
        self::assertNull($processed, 'the run fails');
        self::assertInstanceOf($class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame($message, $error->getMessage());
        self::assertSame(['m-1'], $handled->getArrayCopy(), 'the handler got what the pull held before handle() threw');
        self::assertSame([0], $server->epochs, 'one pull, on the first connection');
        self::assertSame(match ($run) {
            'a finite run' => ConnectionState::Open,
            'waiting for the reconnect disabled' => ConnectionState::Connecting,
            default => ConnectionState::Closed,
        }, $client->state());
        self::assertSame([], $recorder->errorsContaining($message), 'the run threw the frame\'s error rather than report it');
    }

    /**
     * Guard, passes on both: a failure of the read that leaves the connection open still ends an infinite run, whatever
     * the options: an -ERR the server keeps the connection open for ('Invalid Publish Subject', #209) fails the read, and
     * the run hands over m-1, which its pull held, and throws that error, the connection Open with nothing reconnected
     * and nothing reported. Only a frame that ended the connection lets the run go on.
     */
    public function testAnErrKeepingTheConnectionOpenStillEndsTheRun(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport, self::answerOnTheNewConnection(true, 'm-2'));
        $handled = self::payloadLog();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled): void {
                $handled[] = $message->payload;
            });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1']);

        $transport->pushFrame("-ERR 'Invalid Publish Subject'\r\n");
        [, $error] = self::settle($run);

        self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame("Server sent error frame: 'Invalid Publish Subject'", $error->getMessage());
        self::assertSame(['m-1'], $handled->getArrayCopy());
        self::assertSame(ConnectionState::Open, $client->state());
        self::assertSame(0, $client->statistics()->reconnects);
        self::assertSame([], $recorder->errors);
    }

    /**
     * A close of the application's under way when the frame's error reaches the engine still ends the run with that
     * error, as it did in 2.22.0, and a drain() of the client still Draining then lets the hand-over before the run
     * fails deliver what the pull held, as it delivers what the connection holds (#207). The pull holds m-1 when a
     * fatal -ERR ends the connection, the reconnect's dial is held, so the engine's read waits for it, and the
     * application drains the connection, which waits for the reconnect too. Once the dial is let through, the drain
     * takes the reopened connection over, and the engine's read then throws the -ERR's error with the connection
     * Draining: the run does not go on, the handler gets m-1 while the connection is Draining, the engine reports
     * nothing, and handle() throws that error. On 2.23.0 the hand-over ended at any close, a drain() still Draining
     * included, and the handler got nothing.
     */
    public function testAConnectionDrainUnderWayWhenTheFramesErrorReachesTheEngineEndsTheRunAfterTheHandOver(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport, self::answerOnTheNewConnection(true, 'm-2'));
        $handled = self::payloadLog();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled, $client): void {
                $handled[] = $message->payload . ' (' . $client->state()->name . ')';
            });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1']);
        $transport->holdNextDial();

        $transport->pushFrame(self::STALE_CONNECTION);
        // The reconnect is dialling, held, and the read that met the -ERR waits for it.
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
        // The drain starts before the dial is let through, so it waits for the reconnect and takes it over first.
        $drain = $client->drain();
        $transport->releaseDial();
        [$processed, $error] = self::settle($run);
        $drain->await(new TimeoutCancellation(5));

        self::assertNull($processed, 'the run fails');
        self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(self::STALE_CONNECTION_ERROR, $error->getMessage());
        self::assertSame(['m-1 (Draining)'], $handled->getArrayCopy(), 'a drain still Draining hands over what the pull held');
        self::assertSame([0], $server->epochs, 'no pull on the new connection');
        self::assertSame(ConnectionState::Closed, $client->state());
        self::assertSame([], $recorder->errorsContaining('Stale Connection'));
    }

    /** @return iterable<string, array{bool}> */
    public static function clientDrainsDuringTheHandOver(): iterable
    {
        yield 'the drain asks for the rest while the handler holds m-1' => [true];
        yield 'the drain still flushing when the hand-over ends' => [false];
    }

    /**
     * A drain() of the client during that hand-over lets it go on while the connection is Draining, and the run then
     * ends with its count, with no pull written onto the inbox the drain has unsubscribed (#207). The pull holds m-1
     * and m-2 when a fatal -ERR ends the connection, the reconnect reopening it at once, and the handler holds m-1 on a
     * gate while the application drains the client. In the first data set the drain's flush is answered, and the drain,
     * which reads its own PONG with the engine in the handler, asks the run for the rest and waits for it; in the
     * second the server does not answer the drain's PING until the run has ended, so that the drain is still flushing
     * when the hand-over ends. Either way the test opens the gate once the drain has got that far: the handler gets m-2
     * while the connection is Draining, the run writes no pull, handle() returns 2, and drain() resolves, the engine
     * having reported the -ERR's error once. On 2.23.0 the hand-over ended at the drain's close intent, m-2
     * undelivered, and the run then wrote its next pull, after the drain's UNSUB of its inbox where the drain was still
     * flushing, and failed with "Connection is not open" once the drain had closed the connection.
     */
    #[DataProvider('clientDrainsDuringTheHandOver')]
    public function testAClientDrainDuringTheHandOverLetsItFinishAndTheRunWritesNoPullAfter(bool $flushAnswered): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport, self::answerOnTheNewConnection(true, 'm-3'));
        $flush = new DrainFlushWatcher($transport, $watched, $client);
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        $handled = self::payloadLog();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled, $client, $gate): void {
                $handled[] = $message->payload . ' (' . $client->state()->name . ')';
                if ($message->payload === 'm-1') {
                    $gate->getFuture()->await();
                }
            });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1', 'm-2']);
        $transport->pushFrame(self::STALE_CONNECTION);
        // The reconnect reopens the connection at once, and the hand-over holds m-1 in the handler.
        $this->waitUntil(static fn(): bool => $handled->getArrayCopy() === ['m-1 (Open)']);

        $transport->answerPings = $flushAnswered;
        $drain = $client->drain();
        $this->waitUntil(static fn(): bool => $flushAnswered ? $flush->flushDone() || $drain->isComplete() : $flush->pingWritten());
        $gate->complete();
        [$processed, $error] = self::settle($run);
        if (!$flushAnswered) {
            $transport->pushFrame("PONG\r\n");
        }
        $drain->await(new TimeoutCancellation(5));

        self::assertSame(['m-1 (Open)', 'm-2 (Draining)'], $handled->getArrayCopy());
        self::assertTrue($flush->unsubscribed((int) $server->sid));
        self::assertSame([], $flush->pullsAfterTheUnsubscribeOf((int) $server->sid, self::PULL_SUBJECT), 'no pull after the drain\'s UNSUB of the inbox');
        self::assertSame([0], $server->epochs, 'no pull on the new connection');
        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(2, $processed);
        self::assertSame([self::STALE_CONNECTION_ERROR], $recorder->errors);
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * Guard, passes on both: a fatal -ERR that another fiber's read meets, while the engine's read waits for that one,
     * leaves the run going, as it always did: only the read that met the -ERR fails with it. The server answers the first
     * pull, batch 1, with m-1, and the handler awaits a gate on it, so nothing reads; the application then reads, its read
     * on the socket, the gate opens, and the engine's next pull goes out and its read waits for the application's. The
     * -ERR comes: the application's read fails with it once the reconnect is done, and the run pulls on the new
     * connection, where the handler gets m-2 and stops it. handle() returns 2, and the engine reports nothing about the
     * -ERR, which its own read did not meet.
     */
    public function testAFatalErrThatAnotherFibersReadMeetsLeavesTheRunGoing(): void
    {
        [$transport, $watched, $client, $recorder] = $this->client();
        $server = $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull, int $epoch): array => match (true) {
            $epoch >= 1 => [self::messages($sid, 'm-2')],
            $pull === 1 => [self::messages($sid, 'm-1')],
            default => [],
        });
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        $handled = self::payloadLog();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $gate, $iterator): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-1') {
                $gate->getFuture()->await();
            }
            if ($message->payload === 'm-2') {
                $iterator->stop();
            }
        });
        $this->waitUntil(static fn(): bool => $handled->getArrayCopy() === ['m-1']);
        $application = async(static function () use ($client): ?\Throwable {
            try {
                $client->processIncoming()->await();
            } catch (\Throwable $failure) {
                return $failure;
            }

            return null;
        });
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $gate->complete();
        $this->waitUntil(static fn(): bool => count($server->epochs) === 2);

        $transport->pushFrame(self::STALE_CONNECTION);
        $applicationFailure = $application->await(new TimeoutCancellation(5));
        [$processed, $error] = self::settle($run);

        self::assertInstanceOf(ConnectionException::class, $applicationFailure, 'the application\'s read met the -ERR');
        self::assertSame(self::STALE_CONNECTION_ERROR, $applicationFailure->getMessage());
        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(2, $processed);
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertSame([0, 0, 1], $server->epochs);
        self::assertSame([], $recorder->errorsContaining('Stale Connection'), 'the engine reports no failure its own read did not meet');
    }

    /** @return iterable<string, array{string, string}> */
    public static function endingsWhoseReconnectGivesUpLater(): iterable
    {
        yield 'an EOF, the control' => ['EOF', 'Socket closed by peer (EOF)'];
        yield "-ERR 'Stale Connection'" => ["-ERR 'Stale Connection'", self::STALE_CONNECTION_ERROR];
        yield self::PONG_FAILURE => [self::PONG_FAILURE, 'The connection is gone'];
    }

    /**
     * A reconnect that gives up only after the pump read stopped waiting for it ends the run as after a lost
     * connection, with the reconnect's error, not the frame's: with a 100 ms expiry the read waits for the reconnect up
     * to its pull's deadline, 1.1 s after the pull, and the reconnect's first dial is held past it, so the read throws
     * the frame's error with the reconnect still under way and the run goes on, as after an EOF whose reconnect
     * outlasted that wait (a reconnect to a server that stays down gives up, with the default options, after backoffs
     * of about 43 s alone, where the read waits at most 4 s with the default expiry). Once the run has handed m-1 over,
     * the test refuses the dials and lets the held one through, and the reconnect gives up after three attempts.
     * handle() throws "Reconnect attempts exhausted", the error listener having heard why the connection ended first,
     * and once, the connection is Closed, and no pull reached a new connection: the one the run issued meanwhile waited
     * in the reconnect buffer, which the give-up discarded. On 2.22.0 the frame data sets failed, handle() throwing the
     * frame's error at the read's deadline; the EOF control passes on both.
     */
    #[DataProvider('endingsWhoseReconnectGivesUpLater')]
    public function testAReconnectThatGivesUpAfterThePumpReadStoppedWaitingEndsTheRunAsAfterAnEof(string $ending, string $reported): void
    {
        [$transport, $watched, $client, $recorder] = $this->client(maxReconnectAttempts: 3);
        $server = $this->pullServer($transport, self::answerOnTheNewConnection(true, 'm-2', 'm-3', 'm-4'));
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(100);
        $handled = self::payloadLog();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled): void {
            $handled[] = $message->payload;
        });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1']);
        $transport->holdNextDial();

        self::endTheConnection($transport, $ending);
        // The read's wait ends at its pull's deadline with the dial still held: the run then hands m-1 over and goes on,
        // or, on 2.22.0 after a frame, ends.
        $this->waitUntil(static fn(): bool => $run->isComplete() || $handled->getArrayCopy() === ['m-1'], 5.0);
        $wentOnDuringTheReconnect = !$run->isComplete();
        $transport->refuseDials();
        $transport->releaseDial();
        [, $error] = self::settle($run);

        self::assertTrue($wentOnDuringTheReconnect, 'the run was still going when its read stopped waiting for the reconnect');
        self::assertInstanceOf(ConnectionException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame('Reconnect attempts exhausted', $error->getMessage(), 'the run ends with the reconnect\'s error, as after an EOF');
        self::assertSame(['m-1'], $handled->getArrayCopy());
        self::assertSame([0], $server->epochs, 'no pull reached a new connection');
        self::assertSame(ConnectionState::Closed, $client->state());
        self::assertSame($reported, $recorder->errors[0] ?? null, 'the error listener heard first why the connection ended');
        self::assertCount(1, $recorder->errorsContaining($reported), 'once');
    }

    /**
     * The error of the frame goes to the error listener before the hand-over, so that a listener that stops the run when
     * it hears it ends the run as a stop() does anywhere else: what the pull held stays undelivered and the run pulls no
     * more. The pull holds m-1 when a fatal -ERR ends the connection, the reconnect reopening it; the error listener
     * stops the iterator on the -ERR's error. handle() returns 0, the handler got nothing, and no pull went to the new
     * connection, as a listener that stops the run on an EOF ends it
     * (PullConsumerConnectionLossTest::testAStopAsTheReadFailsEndsTheRunAsAStoppedOne). Reported only after the hand-over,
     * the error let m-1 reach the handler first. On 2.22.0 handle() threw the -ERR's error, which nothing reported.
     */
    public function testAnErrorListenerThatStopsTheRunOnTheFramesErrorEndsItBeforeTheHandOver(): void
    {
        $listening = new class {
            /** The run the error listener stops. */
            public ?PullConsumerIterator $iterator = null;
        };
        [$transport, $watched, $client, $recorder] = $this->client(alsoOnError: static function (\Throwable $error) use ($listening): void {
            if (str_contains($error->getMessage(), 'Stale Connection')) {
                $listening->iterator?->stop();
            }
        });
        $server = $this->pullServer($transport, self::answerOnTheNewConnection(true, 'm-2', 'm-3', 'm-4'));
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000);
        $listening->iterator = $iterator;
        $handled = self::payloadLog();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled): void {
            $handled[] = $message->payload;
        });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $this->sendEachInAReadOfItsOwn($transport, $watched, $server, ['m-1']);

        $transport->pushFrame(self::STALE_CONNECTION);
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(0, $processed);
        self::assertSame([], $handled->getArrayCopy(), 'the stop came before the hand-over');
        self::assertSame([0], $server->epochs, 'no pull on the new connection');
        self::assertSame([self::STALE_CONNECTION_ERROR], $recorder->errors);
    }

    /**
     * A pin captured from the hand-over marks the run as a pinned_client group's, as one captured in the retire phase
     * does (#170): when the pin is lost later, the run pulls one at a time until it has a pin again, rather than fan out
     * pulls without one. A grouped run of depth 2 and batch 3 whose pull holds m-1, carrying pin-1, when a fatal -ERR
     * ends the connection: pinned, it fans out two pulls carrying pin-1 on the new connection, and the server answers
     * both with 423, the pin lost; the run then pulls once without a pin, and the server answers that pull with m-2
     * (carrying pin-2), m-3 and m-4, the handler stopping the run on m-2. Only one pull went out without a pin. Captured
     * without marking the run as a pinned group's, the pin's loss made the run fan out pulls without a pin again. On
     * 2.22.0 handle() threw the -ERR's error once the handler had m-1.
     */
    public function testAPinCapturedInTheHandOverMakesALaterPinLossPullOneAtATime(): void
    {
        [$transport, $watched, $client] = $this->client();
        $server = $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull, int $epoch): array => match (true) {
            $epoch < 1 => [],
            $pull === 2, $pull === 3 => [ReconnectingTransport::hmsgFrame($replyTo, $sid, "NATS/1.0 423 Nats-Pin-Id Mismatch\r\n\r\n", '')],
            $pull === 4 => [ReconnectingTransport::hmsgFrame('evt.s', $sid, "NATS/1.0\r\nNats-Pin-Id: pin-2\r\n\r\n", 'm-2', self::ACK_SUBJECT) . self::messages($sid, 'm-3', 'm-4')],
            default => [],
        });
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setGroup('workers')->setBatching(3)->setDepth(2)->setExpiresMs(30_000);
        $handled = self::payloadLog();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-2') {
                $iterator->stop();
            }
        });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1);
        $reads = $watched->reads;
        $transport->pushFrame(ReconnectingTransport::hmsgFrame('evt.s', (int) $server->sid, "NATS/1.0\r\nNats-Pin-Id: pin-1\r\n\r\n", 'm-1', self::ACK_SUBJECT));
        $this->waitUntil(static fn(): bool => $watched->reads > $reads && $watched->readsUnderWay === 1);

        $transport->pushFrame(self::STALE_CONNECTION);
        [, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertSame([null, 'pin-1', 'pin-1', null], array_map(self::pinOfRequest(...), $server->requests), 'after the pin loss, one pull without a pin');
        self::assertSame([0, 1, 1, 1], $server->epochs);
    }

    /**
     * A connected client over a watched ReconnectingTransport, closed in tearDown, with no heartbeat, so only the engine
     * and the test read: reconnect on, with waiting for it enabled and a thousand attempts, unless a guard asks otherwise.
     * The recorder hears the connection's events and errors, then $alsoOnError, and $logger what the connection logs.
     *
     * @param (\Closure(\Throwable): void)|null $alsoOnError
     * @return array{ReconnectingTransport, WatchedTransport, NatsClient, LifecycleRecorder}
     */
    private function client(
        bool $reconnect = true,
        bool $waitForReconnect = true,
        int $maxReconnectAttempts = 1_000,
        ?LoggerInterface $logger = null,
        ?\Closure $alsoOnError = null,
    ): array {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $recorder = new LifecycleRecorder();
        $errorListener = $recorder->errorListener();
        if ($alsoOnError !== null) {
            $recordingListener = $errorListener;
            $errorListener = static function (\Throwable $error) use ($recordingListener, $alsoOnError): void {
                $recordingListener($error);
                $alsoOnError($error);
            };
        }
        $client = $this->own(new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: 2_000,
            reconnectEnabled: $reconnect,
            maxReconnectAttempts: $maxReconnectAttempts,
            reconnectDelayMs: 5,
            reconnectMaxDelayMs: 20,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            connectionListener: $recorder->connectionListener(),
            errorListener: $errorListener,
            logger: $logger,
            waitForReconnect: $waitForReconnect,
        ), $watched));
        $this->opened[] = $client;
        $client->connect()->await();

        return [$transport, $watched, $client, $recorder];
    }

    /**
     * The scripted server's side of the pull consumer: records the epoch (session), the JSON request and the inbox's sid
     * of every pull, and the sid of the first run's inbox, and answers each pull with what $onPull returns, given the
     * pull's reply subject, the inbox's sid, the pull's number (1 for the first) and its epoch; with no frames the
     * server holds the pull, as one with no message does until it expires.
     *
     * @param \Closure(string, int, int, int): list<string> $onPull
     * @return object{sid: ?int, epochs: list<int>, requests: list<string>, sids: list<?int>}
     */
    private function pullServer(ReconnectingTransport $transport, \Closure $onPull): object
    {
        $server = new class {
            /** The sid the client subscribed the (first) run's inbox with. */
            public ?int $sid = null;
            /** @var list<int> The epoch each pull came on, in order. */
            public array $epochs = [];
            /** @var list<string> The JSON body of each pull request, in order. */
            public array $requests = [];
            /** @var list<?int> The sid of the inbox each pull's reply subject belongs to, null where none was subscribed. */
            public array $sids = [];
        };
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport, $server, $onPull): array {
            if ($replyTo === null || $subject !== self::PULL_SUBJECT) {
                return [];
            }

            $server->epochs[] = $transport->epoch();
            $server->requests[] = $payload;
            $sid = $transport->sidFor($replyTo);
            $server->sids[] = $sid;
            if ($sid === null) {
                return [];
            }

            $server->sid ??= $sid;

            return $onPull($replyTo, $sid, count($server->epochs), $transport->epoch());
        };

        return $server;
    }

    /**
     * Answers the pulls on the new connection (epoch 1 on) with $payloads in one chunk, every one of them or only the
     * first, and holds every other pull.
     *
     * @return \Closure(string, int, int, int): list<string>
     */
    private static function answerOnTheNewConnection(bool $everyPull, string ...$payloads): \Closure
    {
        $answered = false;

        return static function (string $replyTo, int $sid, int $pull, int $epoch) use ($everyPull, $payloads, &$answered): array {
            if ($epoch < 1 || ($answered && !$everyPull)) {
                return [];
            }

            $answered = true;

            return [self::messages($sid, ...$payloads)];
        };
    }

    /**
     * Sends each payload on the run's inbox in a read of its own: the next goes out once the engine has taken the
     * previous and its next read is on the socket.
     *
     * @param object{sid: ?int, epochs: list<int>, requests: list<string>} $server
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
     * Ends the connection under the engine's read: the socket drops (EOF), the server sends the given -ERR, or it sends a
     * PING whose PONG the socket will not take.
     */
    private static function endTheConnection(ReconnectingTransport $transport, string $ending): void
    {
        if ($ending === 'EOF') {
            $transport->dropConnection();

            return;
        }

        if ($ending === self::PONG_FAILURE) {
            $transport->failNextWriteContaining('PONG');
            $transport->pushFrame("PING\r\n");

            return;
        }

        $transport->pushFrame($ending . "\r\n");
    }

    /** The pin id a pull request carried, or null for none. */
    private static function pinOfRequest(string $request): ?string
    {
        $decoded = json_decode($request, true, 512, JSON_THROW_ON_ERROR);
        $pin = is_array($decoded) ? ($decoded['id'] ?? null) : null;

        return is_string($pin) ? $pin : null;
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
