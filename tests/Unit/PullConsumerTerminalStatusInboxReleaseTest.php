<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\JetStream\Consumers\PullConsumerIterator;
use IDCT\NATS\Tests\Support\DeletedAndRecreatedConsumer;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\delay;

/**
 * An infinite pull consumer run that ends on its head pull's terminal status collects what the server sent the pulls
 * behind it before it reports the status and hands those pulls over (#211). The case: the consumer is deleted and
 * recreated under the same name while the handler works on a batch. The waiting pull's "409 Consumer Deleted" sits
 * unread on the socket; the refill the run issues when the handler returns reaches the recreated consumer, which serves
 * it at once. The engine then reads the 409 and ends the run. Since #187 it hands over what the pulls behind the status
 * hold, which covers the refill's messages when they came in the same read as the 409; when they came a read later (the
 * likelier timing: the 409 waits on the socket, the refill's answer comes a round trip after the refill), they were read
 * after the run's UNSUB and dropped: gone on a consumer with ack_policy none or max_deliver 1.
 *
 * The run now closes its inbox before onError (NatsClient::retirePullInbox(), #212): UNSUB and a PING in one write, what
 * the server sent before the UNSUB routed into the pulls until that PING's PONG, within one request-timeout budget, the
 * routing sealed, the rejection observer kept until the run's final unsubscribe. Then the run's lifecycle rules apply in
 * their order: a stop or a discarding close returns the count; a drain's request for the hand-over, or a rejection of the
 * inbox, goes back to the loop's top, onError not called; otherwise onError gets the status once, the pulls behind it and
 * the overflow are handed over and counted, and the run returns. No path pulls again.
 *
 * Over the scripted server in tests/Support/ReconnectingTransport.php, seen through tests/Support/WatchedTransport.php.
 * Each test fails on 2.25.2, run over its JetStreamContext.php, but testTheRoutingIsSealedBeforeOnErrorRuns, a guard of
 * the order; the same-read timing and the status with nothing behind it, whose outcome is unchanged, only at their check
 * that the release went out with its PING.
 */
final class PullConsumerTerminalStatusInboxReleaseTest extends TestCase
{
    use ReconnectScenarios;

    private const PULL_SUBJECT = '$JS.API.CONSUMER.MSG.NEXT.S.C';
    private const ACK_SUBJECT = '$JS.ACK.S.C.1.1.1.0.0';

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{bool}> */
    public static function timings(): iterable
    {
        yield 'the refill\'s answer a read after the 409' => [false];
        yield 'the refill\'s answer in the same read as the 409' => [true];
    }

    /**
     * The issue's probe: an infinite run with batch 1, depth 2 and a 30 s expiry; the server answers pull 1 with m-1 and
     * holds pull 2. While the handler works on m-1, the consumer is deleted: pull 2's "409 Consumer Deleted" is queued
     * unread. The refill, pull 3, is answered by the recreated consumer with m-2, a read after the 409 or in the same
     * read. The handler gets m-1 and m-2, onError the 409 once, handle() returns 2, the run has pulled three times, and
     * the inbox is released once, with a PING behind its UNSUB. With the answer a read later the handler used to get m-1
     * only; with it in the same read the outcome was already right (#187), and only the release's PING is new.
     */
    #[DataProvider('timings')]
    public function testWhatARefillBringsAfterTheHeadsTerminalStatusReachesTheHandler(bool $oneRead): void
    {
        [$transport, , $client] = $this->client();
        $server = $this->deletedAndRecreated($transport, $oneRead, ['m-2']);
        /** @var \ArrayObject<int, string> $handled */
        $handled = new \ArrayObject();
        /** @var \ArrayObject<int, string> $reported */
        $reported = new \ArrayObject();
        $run = $this->iterator($client, $reported)->handle(static function (NatsMessage $message) use ($handled, $server): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-1') {
                $server->deleteTheConsumer();
            }
        });
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertSame(['409 JetStream pull request ended with status 409: Consumer Deleted'], $reported->getArrayCopy());
        self::assertSame(2, $processed);
        self::assertCount(3, $server->replyTos, 'three pulls, no refill after the status');
        self::assertSame(['UNSUB ' . $server->sid], $transport->controlLinesStartingWith('UNSUB '));
        self::assertSame(1, self::writesOf($transport, 'UNSUB ' . $server->sid . "\r\nPING\r\n"));
    }

    /**
     * What none of the pulls behind the status has room for goes to the overflow and is handed over and counted too: the
     * recreated consumer answers the refill (batch 1) with m-2 and m-3 a read after the 409, m-3 more than the refill
     * asked for. The handler gets m-1 to m-3, and handle() returns 3. Both used to be dropped.
     */
    public function testWhatNoPullHasRoomForGoesToTheOverflowAndIsCounted(): void
    {
        [$transport, , $client] = $this->client();
        $server = $this->deletedAndRecreated($transport, false, ['m-2', 'm-3']);
        /** @var \ArrayObject<int, string> $handled */
        $handled = new \ArrayObject();
        /** @var \ArrayObject<int, string> $reported */
        $reported = new \ArrayObject();
        $run = $this->iterator($client, $reported)->handle(static function (NatsMessage $message) use ($handled, $server): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-1') {
                $server->deleteTheConsumer();
            }
        });
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(['m-1', 'm-2', 'm-3'], $handled->getArrayCopy());
        self::assertSame(3, $processed);
        self::assertCount(1, $reported);
    }

    /** @return iterable<string, array{bool}> */
    public static function withAndWithoutOnError(): iterable
    {
        yield 'with onError' => [true];
        yield 'without onError' => [false];
    }

    /**
     * A terminal status with nothing behind it still ends the run at once, not at any pull's deadline: depth 1, the pull
     * answered with a 409 Consumer Deleted, a 30 s expiry. handle() returns 0 well within a second, onError (when set)
     * gets the status once, and the release went out, UNSUB and PING in one write: an empty tail does not prove that
     * nothing is on its way. Only that last check is new; the rest passes on 2.25.2 as well.
     */
    #[DataProvider('withAndWithoutOnError')]
    public function testATerminalStatusWithNothingBehindItStillEndsTheRunAtOnce(bool $onError): void
    {
        [$transport, , $client] = $this->client();
        $server = $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1
            ? [self::statusFrame($replyTo, $sid, 'Consumer Deleted')]
            : []);
        /** @var \ArrayObject<int, string> $reported */
        $reported = new \ArrayObject();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        if ($onError) {
            $iterator->setOnError(static function (\Throwable $status) use ($reported): void {
                $reported[] = $status->getCode() . ' ' . $status->getMessage();
            });
        }
        $started = hrtime(true);
        [$processed, $error] = self::settle($iterator->handle(static function (): void {
            self::fail('the server sends no message');
        }));

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(0, $processed);
        self::assertLessThan(1.0, $this->secondsSince($started));
        self::assertSame($onError ? ['409 JetStream pull request ended with status 409: Consumer Deleted'] : [], $reported->getArrayCopy());
        self::assertCount(1, $server->replyTos);
        self::assertSame(1, self::writesOf($transport, 'UNSUB ' . $server->sid . "\r\nPING\r\n"));
    }

    /**
     * A stop() made while the release waits for its PONG (PINGs unanswered from the status on, a 5 s budget) ends the
     * release at once, and the run with it: handle() returns 1, the count delivered so far, onError never called, and m-2,
     * which the release read into the refill's pull, left undelivered, as a stop() leaves it anywhere.
     */
    public function testAStopDuringTheCollectionReturnsTheCountWithoutOnErrorOrTheTail(): void
    {
        [$transport, , $client] = $this->client(5_000);
        $server = $this->deletedAndRecreated($transport, false, ['m-2']);
        /** @var \ArrayObject<int, string> $handled */
        $handled = new \ArrayObject();
        /** @var \ArrayObject<int, string> $reported */
        $reported = new \ArrayObject();
        $iterator = $this->iterator($client, $reported);
        $transport->afterWrite = static function (string $bytes) use ($server, $iterator): void {
            if ($server->sid !== null && str_starts_with($bytes, 'UNSUB ' . $server->sid . "\r\n")) {
                EventLoop::delay(0.05, $iterator->stop(...));
            }
        };
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $server, $transport): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-1') {
                // From here the server answers no PING: the release's PONG never comes.
                $transport->answerPings = false;
                $server->deleteTheConsumer();
            }
        });
        $started = hrtime(true);
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(1, $processed);
        self::assertSame(['m-1'], $handled->getArrayCopy());
        self::assertSame([], $reported->getArrayCopy(), 'onError is not called once the run is stopped');
        self::assertLessThan(2.0, $this->secondsSince($started), 'the stop ended the release, not its 5 s budget');
    }

    /**
     * A rejection of the inbox recorded while the release waits for its PONG ends the run with the rejection: the
     * permissions -ERR naming the inbox comes as the UNSUB reaches the server, ahead of the PONG. The handler gets m-1 and
     * m-2 (the hand-over before a failure, #197), handle() throws the rejection, and onError is never called with the
     * 409. The run used to report the 409 and return 1.
     */
    public function testARejectionDuringTheCollectionEndsTheRunWithItAndOnErrorIsNotCalled(): void
    {
        [$transport, , $client] = $this->client();
        $server = $this->deletedAndRecreated($transport, false, ['m-2']);
        /** @var \ArrayObject<int, string> $handled */
        $handled = new \ArrayObject();
        /** @var \ArrayObject<int, string> $reported */
        $reported = new \ArrayObject();
        $transport->pongDelay = 0.05;
        $transport->afterWrite = static function (string $bytes) use ($transport, $server): void {
            if ($server->sid !== null && str_starts_with($bytes, 'UNSUB ' . $server->sid . "\r\n")) {
                $transport->pushFrame(sprintf("-ERR 'Permissions Violation for Subscription to \"%s.*\"'\r\n", $server->base));
            }
        };
        $run = $this->iterator($client, $reported)->handle(static function (NatsMessage $message) use ($handled, $server): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-1') {
                $server->deleteTheConsumer();
            }
        });
        [, $error] = self::settle($run);

        self::assertInstanceOf(JetStreamException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertStringContainsString('was rejected by server permissions', $error->getMessage());
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertSame([], $reported->getArrayCopy(), 'onError is not called before the rejection');
    }

    /**
     * Guard, passes on 2.25.2 as well. The routing is sealed before onError runs: an onError that reads the socket itself, the server having sent another
     * message on the run's inbox, routes nothing more to the run. The handler gets m-1 and m-2, handle() returns 2, and
     * m-3, read by onError's own processIncoming(), reaches no pull.
     */
    public function testTheRoutingIsSealedBeforeOnErrorRuns(): void
    {
        [$transport, , $client] = $this->client();
        $server = $this->deletedAndRecreated($transport, false, ['m-2']);
        /** @var \ArrayObject<int, string> $handled */
        $handled = new \ArrayObject();
        $reads = [];
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(30_000)
            ->setOnError(static function () use ($client, $transport, $server, &$reads): void {
                $transport->pushFrame(ReconnectingTransport::msgFrame('evt.s', (int) $server->sid, 'm-3', self::ACK_SUBJECT));
                $reads[] = $client->processIncoming(new TimeoutCancellation(1))->await();
            });
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $server): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-1') {
                $server->deleteTheConsumer();
            }
        });
        [$processed, $error] = self::settle($run);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame([1], $reads, 'onError\'s read took m-3');
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertSame(2, $processed);
    }

    /**
     * A PONG that never comes ends the release at the request timeout (300 ms) and the run still ends as on any terminal
     * status: m-2, which came before the PING was even written, is handed over, onError gets the 409 once, handle()
     * returns 2 well before the pulls' 30 s expiry, the run pulls no more, and the timeout is reported.
     */
    public function testAPongThatNeverComesStillEndsTheRunAsOnATerminalStatus(): void
    {
        /** @var \ArrayObject<int, \Throwable> $errors */
        $errors = new \ArrayObject();
        [$transport, , $client] = $this->client(300, static function (\Throwable $error) use ($errors): void {
            $errors[] = $error;
        });
        $server = $this->deletedAndRecreated($transport, false, ['m-2']);
        /** @var \ArrayObject<int, string> $handled */
        $handled = new \ArrayObject();
        /** @var \ArrayObject<int, string> $reported */
        $reported = new \ArrayObject();
        $run = $this->iterator($client, $reported)->handle(static function (NatsMessage $message) use ($handled, $server, $transport): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-1') {
                // From here the server answers no PING: the release's PONG never comes.
                $transport->answerPings = false;
                $server->deleteTheConsumer();
            }
        });
        $started = hrtime(true);
        [$processed, $error] = self::settle($run);
        $this->waitUntil(static fn(): bool => self::countTimeouts($errors) === 1);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(2, $processed);
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertCount(1, $reported);
        self::assertCount(3, $server->replyTos);
        self::assertLessThan(3.0, $this->secondsSince($started));
    }

    /**
     * A connection that a fatal -ERR ends while the release waits for its PONG does not send the terminal run back to
     * pulling on the new connection: onError gets the 409 once, m-2 is handed over, handle() returns 2, and no pull goes
     * out on the new session.
     */
    public function testAConnectionEndedDuringTheCollectionDoesNotPullAgain(): void
    {
        [$transport, , $client] = $this->client();
        $server = $this->deletedAndRecreated($transport, false, ['m-2']);
        $transport->afterWrite = static function (string $bytes) use ($transport, $server): void {
            if ($server->sid !== null && str_starts_with($bytes, 'UNSUB ' . $server->sid . "\r\n")) {
                $transport->pushFrame("-ERR 'Stale Connection'\r\n");
            }
        };
        /** @var \ArrayObject<int, string> $handled */
        $handled = new \ArrayObject();
        /** @var \ArrayObject<int, string> $reported */
        $reported = new \ArrayObject();
        $run = $this->iterator($client, $reported)->handle(static function (NatsMessage $message) use ($handled, $server, $transport): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-1') {
                // The release's PONG never comes on this connection: the -ERR ends it first (a reconnect's handshake
                // PING is answered all the same).
                $transport->answerPings = false;
                $server->deleteTheConsumer();
            }
        });
        [$processed, $error] = self::settle($run);
        $this->waitUntil(static fn(): bool => $transport->epoch() === 1);
        delay(0.1);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(2, $processed);
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertCount(1, $reported);
        self::assertSame([0, 0, 0], $server->epochs, 'no pull on the new connection');
    }

    /**
     * A client drain() started while the run's own release waits for its PONG keeps that release authoritative: the
     * inbox, out of the drain's scan, gets no second UNSUB, the run's PONG (each PONG held back 50 ms, in order) comes
     * ahead of the drain's, onError gets the status once, m-2 is handed over, handle() returns 2 with no further pull,
     * and the drain, which waited for the run, completes.
     */
    public function testAClientDrainStartedDuringTheCollectionKeepsTheRunsOwnReleaseAndSendsNoSecondUnsub(): void
    {
        [$transport, , $client] = $this->client();
        $server = $this->deletedAndRecreated($transport, false, ['m-2']);
        /** @var \ArrayObject<int, string> $handled */
        $handled = new \ArrayObject();
        /** @var \ArrayObject<int, string> $reported */
        $reported = new \ArrayObject();
        /** @var \ArrayObject<int, Future<void>> $drains */
        $drains = new \ArrayObject();
        $transport->afterWrite = static function (string $bytes) use ($transport, $server, $client, $drains): void {
            if ($drains->count() === 0 && $server->sid !== null && str_starts_with($bytes, 'UNSUB ' . $server->sid . "\r\n")) {
                $transport->pongDelay = 0.05;
                $drains[] = $client->drain();
            }
        };
        $run = $this->iterator($client, $reported)->handle(static function (NatsMessage $message) use ($handled, $server): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-1') {
                $server->deleteTheConsumer();
            }
        });
        [$processed, $error] = self::settle($run);
        self::assertCount(1, $drains, 'the drain started during the release');
        $drain = $drains[0];
        self::assertInstanceOf(Future::class, $drain);
        $drain->await(new TimeoutCancellation(5));

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertSame(2, $processed);
        self::assertCount(1, $reported);
        self::assertCount(3, $server->replyTos);
        self::assertSame(['UNSUB ' . $server->sid], array_values(array_filter(
            $transport->controlLinesStartingWith('UNSUB '),
            static fn(string $line): bool => $line === 'UNSUB ' . $server->sid,
        )), 'one UNSUB for the run\'s inbox');
    }

    /**
     * A client drain() under way when the run reads the terminal status keeps its precedence over that status: the drain
     * starts right after the refill is written, so the run meets the 409 on a Draining connection; the release waits
     * for the drain's request for the hand-over instead of writing an UNSUB of its own, and the run then ends as a drain
     * ends it: the handler gets m-1 and m-2, onError is never called, handle() returns 2 with no further pull, the inbox
     * gets the drain's UNSUB only, and the drain completes.
     */
    public function testADrainUnderWayWhenTheTerminalStatusIsReadHandsTheTailOverWithoutOnError(): void
    {
        [$transport, , $client] = $this->client();
        $server = $this->deletedAndRecreated($transport, false, ['m-2']);
        /** @var \ArrayObject<int, string> $handled */
        $handled = new \ArrayObject();
        /** @var \ArrayObject<int, string> $reported */
        $reported = new \ArrayObject();
        /** @var \ArrayObject<int, Future<void>> $drains */
        $drains = new \ArrayObject();
        $transport->afterWrite = static function (string $bytes) use ($server, $client, $drains): void {
            if ($drains->count() === 0 && count($server->replyTos) === 3 && str_contains($bytes, self::PULL_SUBJECT)) {
                $drains[] = $client->drain();
            }
        };
        $run = $this->iterator($client, $reported)->handle(static function (NatsMessage $message) use ($handled, $server): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-1') {
                $server->deleteTheConsumer();
            }
        });
        [$processed, $error] = self::settle($run);
        self::assertCount(1, $drains, 'the drain started behind the refill');
        $drain = $drains[0];
        self::assertInstanceOf(Future::class, $drain);
        $drain->await(new TimeoutCancellation(5));

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertSame(2, $processed);
        self::assertSame([], $reported->getArrayCopy(), 'the drain\'s hand-over, not the terminal status');
        self::assertCount(3, $server->replyTos);
        self::assertSame(0, self::writesOf($transport, 'UNSUB ' . $server->sid . "\r\nPING\r\n"), 'the drain owns the UNSUB');
    }

    /**
     * An onError that throws still ends the run with its own exception, after the release, as before: handle() throws
     * it, the tail it abandons (m-2) is not handed over, and no further pull goes out.
     */
    public function testAnOnErrorThatThrowsStillEndsTheRunWithItsOwnException(): void
    {
        [$transport, , $client] = $this->client();
        $server = $this->deletedAndRecreated($transport, false, ['m-2']);
        /** @var \ArrayObject<int, string> $handled */
        $handled = new \ArrayObject();
        $thrown = new \RuntimeException('onError gives up');
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(30_000)
            ->setOnError(static function () use ($thrown): void {
                throw $thrown;
            })
            ->handle(static function (NatsMessage $message) use ($handled, $server): void {
                $handled[] = $message->payload;
                if ($message->payload === 'm-1') {
                    $server->deleteTheConsumer();
                }
            });
        [, $error] = self::settle($run);

        self::assertSame($thrown, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(['m-1'], $handled->getArrayCopy());
        self::assertCount(3, $server->replyTos);
        self::assertSame(1, self::writesOf($transport, 'UNSUB ' . $server->sid . "\r\nPING\r\n"));
    }

    /**
     * A connected client over a watched ReconnectingTransport, closed in tearDown, with no heartbeat and reconnect on.
     *
     * @param (\Closure(\Throwable): void)|null $errorListener
     * @return array{ReconnectingTransport, WatchedTransport, NatsClient}
     */
    private function client(int $requestTimeoutMs = 2_000, ?\Closure $errorListener = null): array
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: $requestTimeoutMs,
            reconnectDelayMs: 5,
            reconnectMaxDelayMs: 20,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            errorListener: $errorListener,
        ), $watched);
        $this->opened[] = $client;
        $client->connect()->await();

        return [$transport, $watched, $client];
    }

    /**
     * The probe's iterator: infinite, batch 1, depth 2, a 30 s expiry, onError recording each status.
     *
     * @param \ArrayObject<int, string> $reported
     */
    private function iterator(NatsClient $client, \ArrayObject $reported): PullConsumerIterator
    {
        return $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(30_000)
            ->setOnError(static function (\Throwable $status) use ($reported): void {
                $reported[] = $status->getCode() . ' ' . $status->getMessage();
            });
    }

    /**
     * The scripted server of the issue's probe ({@see DeletedAndRecreatedConsumer}), answering the refill with $refill
     * in the same read as the 409 ($oneRead), or in a read of its own after it.
     *
     * @param list<string> $refill
     */
    private function deletedAndRecreated(ReconnectingTransport $transport, bool $oneRead, array $refill): DeletedAndRecreatedConsumer
    {
        $server = new DeletedAndRecreatedConsumer($transport, self::PULL_SUBJECT, self::ACK_SUBJECT, $oneRead, $refill);
        $server->serve();

        return $server;
    }

    /**
     * A scripted pull server answering each pull with what $onPull returns, given its reply subject, the inbox's sid and
     * the pull's number.
     *
     * @param \Closure(string, int, int): list<string> $onPull
     * @return object{sid: ?int, replyTos: list<string>}
     */
    private function pullServer(ReconnectingTransport $transport, \Closure $onPull): object
    {
        $server = new class {
            public ?int $sid = null;
            /** @var list<string> */
            public array $replyTos = [];
        };
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport, $server, $onPull): array {
            if ($replyTo === null || $subject !== self::PULL_SUBJECT) {
                return [];
            }
            $server->replyTos[] = $replyTo;
            $sid = (int) $transport->sidFor($replyTo);
            $server->sid ??= $sid;

            return $onPull($replyTo, $sid, count($server->replyTos));
        };

        return $server;
    }

    private static function statusFrame(string $replyTo, int $sid, string $description): string
    {
        return ReconnectingTransport::hmsgFrame($replyTo, $sid, 'NATS/1.0 409 ' . $description . "\r\n\r\n", '');
    }

    /** How many writes the client made whose bytes are exactly $bytes. */
    private static function writesOf(ReconnectingTransport $transport, string $bytes): int
    {
        return count(array_filter($transport->writes, static fn(array $write): bool => $write['bytes'] === $bytes));
    }

    /** @param \ArrayObject<int, \Throwable> $errors */
    private static function countTimeouts(\ArrayObject $errors): int
    {
        $count = 0;
        foreach ($errors as $error) {
            if ($error instanceof TimeoutException && str_contains($error->getMessage(), 'Releasing the pull consumer inbox')) {
                ++$count;
            }
        }

        return $count;
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
