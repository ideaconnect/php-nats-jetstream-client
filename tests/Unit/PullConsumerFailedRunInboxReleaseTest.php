<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\ScriptedPullStream;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\delay;

/**
 * A pull consumer run that ends with a failure of its own closes its inbox to new deliveries before it hands what its
 * pulls hold to the handler (#212). Since 2.22.0 such a run hands its buffers over first (#197), and it released its
 * inbox only in its finally, after that hand-over: the server, which knows nothing of the failure, went on serving the
 * run's pulls for as long as the hand-over lasted, and what it delivered then was dropped, read after the run's UNSUB,
 * or as a straggler, unacked: gone on a consumer without acks or with max_deliver 1, delivered again after the ack wait
 * otherwise. The run now writes UNSUB and a PING in one write first, reads what the server sent before the UNSUB into
 * its pulls until that PING's PONG, within one request-timeout budget, removes the inbox's local state, and only then
 * hands over and throws its failure. The server stops serving the pulls, and what is published during the hand-over
 * stays in the stream for the next pull.
 *
 * Over the scripted server in tests/Support/ReconnectingTransport.php, seen through tests/Support/WatchedTransport.php.
 * The scripted server models a subscription's interest (an UNSUB removes it) but not a waiting pull's, so the tests
 * assert the order of events: the UNSUB on the wire before the first message of the hand-over; what the server sent
 * before the UNSUB, in a chunk of its own ahead of the PONG, still reaching the handler; what a test publishes during
 * the hand-over going to a waiting pull only while its inbox has interest. The time bounds keep a broken run from
 * hanging the suite, and bound the cleanup: a PONG that never comes, a stalled UNSUB, a stop, and a handler of another
 * subscription that the cleanup's read runs and that suspends end it within the request timeout. Every test fails on
 * 2.25.1, where the UNSUB came after the hand-over: checked by running them over its JetStreamContext.php.
 */
final class PullConsumerFailedRunInboxReleaseTest extends TestCase
{
    use ReconnectScenarios;

    private const PULL_SUBJECT = '$JS.API.CONSUMER.MSG.NEXT.S.C';
    private const ACK_SUBJECT = '$JS.ACK.S.C.1.1.1.0.0';

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{string}> */
    public static function failureSites(): iterable
    {
        yield 'the pump read fails (another subscription\'s handler, handlerErrorsFailOperations)' => ['read'];
        yield 'a pull\'s write is refused (an INFO lowered max_payload)' => ['write'];
        yield 'the server rejects the inbox (a permissions violation)' => ['rejection'];
    }

    /**
     * At each of the three sites where a run ends with a failure of its own, the connection staying open, the inbox's
     * UNSUB, with a PING behind it in the same write, goes out before the first message of the hand-over reaches the
     * handler, whose acks follow it; the run still throws its own failure, and the inbox is released once, the run's
     * final unsubscribe writing nothing more. The read: batch 3, depth 1, the pull holding m-1 and m-2 when another
     * subscription's handler throws in the engine's read. The write: batch 2, depth 2, the first pull's answer bringing
     * m-1, m-2 and m-3 and an INFO lowering max_payload to 16 bytes, so that m-1 and m-2 go out through the retire phase
     * and the refill's pull is refused with m-3 in the second pull. The rejection: as the read, ended by the server's
     * permissions -ERR naming the inbox. The UNSUB used to come after the hand-over, the server serving the pulls until
     * then.
     */
    #[DataProvider('failureSites')]
    public function testTheInboxIsReleasedBeforeTheHandOverAtEachFailureSite(string $site): void
    {
        [$transport, $watched, $client] = $this->client();
        $otherSid = $client->subscribe('other', static function (): void {
            throw new \RuntimeException('the other subscription\'s handler failed');
        })->await();
        $info = 'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":16,"headers":true}' . "\r\n";
        $server = $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull): array => match (true) {
            $site === 'write' && $pull === 1 => [self::messages($sid, 'm-1', 'm-2', 'm-3') . $info],
            $site !== 'write' && $pull === 1 => [self::messages($sid, 'm-1', 'm-2')],
            default => [],
        });
        $log = $this->watchWire($transport);
        $run = $client->jetStream()->pullConsumer('S', 'C')
            ->setBatching($site === 'write' ? 2 : 3)->setDepth($site === 'write' ? 2 : 1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message, JetStreamContext $js) use ($log): void {
                $log[] = 'handler: ' . $message->payload;
                $js->ack($message)->await();
            });

        if ($site !== 'write') {
            $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1 && $watched->reads >= 2);
            $transport->pushFrame($site === 'read'
                ? ReconnectingTransport::msgFrame('other', $otherSid, 'x')
                : sprintf("-ERR 'Permissions Violation for Subscription to \"%s.*\" (sid \"%d\")'\r\n", $server->base, (int) $server->sid));
        }
        [, $error] = self::settle($run);

        $release = 'wire: UNSUB ' . $server->sid . ' | PING';
        if ($site === 'write') {
            self::assertInstanceOf(ProtocolException::class, $error, sprintf('handle() threw %s', self::describe($error)));
            self::assertSame(
                ['handler: m-1', 'wire: ACK', 'handler: m-2', 'wire: ACK', $release, 'handler: m-3', 'wire: ACK'],
                $log->getArrayCopy(),
            );
        } else {
            if ($site === 'read') {
                self::assertInstanceOf(\RuntimeException::class, $error, sprintf('handle() threw %s', self::describe($error)));
                self::assertSame('the other subscription\'s handler failed', $error->getMessage());
            } else {
                self::assertInstanceOf(JetStreamException::class, $error, sprintf('handle() threw %s', self::describe($error)));
                self::assertStringContainsString('was rejected by server permissions', $error->getMessage());
            }
            self::assertSame([$release, 'handler: m-1', 'wire: ACK', 'handler: m-2', 'wire: ACK'], $log->getArrayCopy());
        }
        self::assertSame(['UNSUB ' . $server->sid], $transport->controlLinesStartingWith('UNSUB '), 'the inbox is released once');
        self::assertFalse($client->isSubscriptionActive((int) $server->sid));
        self::assertSame(ConnectionState::Open, $client->state());
    }

    /** @return iterable<string, array{int, list<string>, list<string>}> */
    public static function preUnsubDeliveries(): iterable
    {
        yield 'depth 1: into the pull being handed over' => [1, ['m-3'], ['m-1', 'm-2', 'm-3']];
        yield 'depth 2: m-3 fills the first pull, m-4 goes into the second' => [2, ['m-3', 'm-4'], ['m-1', 'm-2', 'm-3', 'm-4']];
    }

    /**
     * The release is a fence, not a plain unsubscribe: what the server sent the inbox before it had the UNSUB, in a
     * chunk of its own on the socket ahead of the PING's PONG (held back 50 ms), is read and routed into the pulls
     * before the hand-over, and reaches the handler after what they held, in order. Batch 3, the first pull holding m-1
     * and m-2 when another subscription's handler throws in the engine's read; the server sends m-3 (and m-4) as the
     * UNSUB reaches it. With depth 2, m-3 fills the first pull and m-4 goes into the second. A plain UNSUB before the
     * hand-over would drop them as frames for a sid the client no longer has; the UNSUB after the hand-over left them
     * unread.
     *
     * @param list<string> $late
     * @param list<string> $expected
     */
    #[DataProvider('preUnsubDeliveries')]
    public function testWhatTheServerSentBeforeTheUnsubStillReachesTheHandlerInOrder(int $depth, array $late, array $expected): void
    {
        [$transport, $watched, $client] = $this->client();
        $otherSid = $client->subscribe('other', static function (): void {
            throw new \RuntimeException('the other subscription\'s handler failed');
        })->await();
        $server = $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1 ? [self::messages($sid, 'm-1', 'm-2')] : []);
        /** @var \ArrayObject<int, string> $handled */
        $handled = new \ArrayObject();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth($depth)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled): void {
                $handled[] = $message->payload;
            });
        $this->waitUntil(static fn(): bool => count($server->epochs) === $depth && $watched->readsUnderWay === 1 && $watched->reads >= 2);

        $transport->pongDelay = 0.05;
        $transport->afterWrite = static function (string $bytes) use ($transport, $server, $late): void {
            if (str_contains($bytes, 'UNSUB ' . $server->sid . "\r\n")) {
                $transport->pushFrame(self::messages((int) $server->sid, ...$late));
            }
        };
        $transport->pushFrame(ReconnectingTransport::msgFrame('other', $otherSid, 'x'));
        [, $error] = self::settle($run);

        self::assertInstanceOf(\RuntimeException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame($expected, $handled->getArrayCopy());
    }

    /** @return iterable<string, array{bool, string}> */
    public static function runsAndAcks(): iterable
    {
        foreach ([false, true] as $finite) {
            foreach (['ack', 'ackSync'] as $ack) {
                yield ($finite ? 'a finite run' : 'an infinite run') . ', ' . $ack . '()' => [$finite, $ack];
            }
        }
    }

    /**
     * What is published while the handler works through the hand-over stays in the stream for the next pull. The
     * scripted server keeps a stream: m-1 and m-2 stored, and a pull waiting for the rest of its batch is served what
     * is published only while its inbox has interest, as nats-server drops a waiting pull whose reply subject has none.
     * A run, infinite or of one iteration, batch 3 and depth 1, holds m-1 and m-2 when another subscription's handler
     * throws in its read; on m-1 the handler has m-3 published, and acks each message with ack() or ackSync(). The
     * handler gets m-1 and m-2, and m-3 stays in the stream; a second run then gets it. The server used to serve m-3 to
     * the failed run's pull, which dropped it: read after the run's UNSUB, or as a straggler.
     */
    #[DataProvider('runsAndAcks')]
    public function testWhatIsPublishedDuringTheHandOverStaysForTheNextPull(bool $finite, string $ack): void
    {
        [$transport, $watched, $client] = $this->client();
        $otherSid = $client->subscribe('other', static function (): void {
            throw new \RuntimeException('the other subscription\'s handler failed');
        })->await();
        $stream = $this->streamServer($transport, ['m-1', 'm-2']);
        /** @var \ArrayObject<int, string> $handled */
        $handled = new \ArrayObject();
        $consumer = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000);
        if ($finite) {
            $consumer->setIterations(1);
        }
        $run = $consumer->handle(static function (NatsMessage $message, JetStreamContext $js) use ($handled, $stream, $ack): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-1') {
                $stream->publish('m-3');
            }
            $ack === 'ackSync' ? $js->ackSync($message)->await() : $js->ack($message)->await();
        });
        $this->waitUntil(static fn(): bool => $stream->waiting !== [] && $watched->readsUnderWay === 1 && $watched->reads >= 2);

        $transport->pushFrame(ReconnectingTransport::msgFrame('other', $otherSid, 'x'));
        [, $error] = self::settle($run);

        self::assertInstanceOf(\RuntimeException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertSame(['m-3'], $stream->stored, 'm-3 stayed in the stream');

        /** @var \ArrayObject<int, string> $next */
        $next = new \ArrayObject();
        $second = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000)->setIterations(1)
            ->handle(static function (NatsMessage $message) use ($next): void {
                $next[] = $message->payload;
            });
        [$processed, $secondError] = self::settle($second);

        self::assertNull($secondError, sprintf('the second run threw %s', self::describe($secondError)));
        self::assertSame(1, $processed);
        self::assertSame(['m-3'], $next->getArrayCopy(), 'the next pull got m-3');
    }

    /**
     * The cleanup is bounded: a server that never answers the PING behind the UNSUB holds it to the request timeout
     * (300 ms here), not to the pull's 30 s expiry. The handler still gets m-1 and m-2, handle() throws the run's own
     * failure, the timeout is reported to the error listener as a warning, and the inbox is released once.
     */
    public function testAPongThatNeverComesEndsTheReleaseAtTheRequestTimeout(): void
    {
        /** @var \ArrayObject<int, \Throwable> $reported */
        $reported = new \ArrayObject();
        [$transport, $watched, $client] = $this->client(300, static function (\Throwable $error) use ($reported): void {
            $reported[] = $error;
        });
        $otherSid = $client->subscribe('other', static function (): void {
            throw new \RuntimeException('the other subscription\'s handler failed');
        })->await();
        $server = $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1 ? [self::messages($sid, 'm-1', 'm-2')] : []);
        /** @var \ArrayObject<int, string> $handled */
        $handled = new \ArrayObject();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled): void {
                $handled[] = $message->payload;
            });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1 && $watched->reads >= 2);

        $transport->answerPings = false;
        $transport->pushFrame(ReconnectingTransport::msgFrame('other', $otherSid, 'x'));
        $started = hrtime(true);
        [, $error] = self::settle($run);
        $elapsed = $this->secondsSince($started);
        $this->waitUntil(static fn(): bool => self::timeoutsReported($reported) === 1);

        self::assertInstanceOf(\RuntimeException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame('the other subscription\'s handler failed', $error->getMessage());
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertLessThan(2.0, $elapsed, 'the cleanup ended at the request timeout');
        self::assertSame(['UNSUB ' . $server->sid], $transport->controlLinesStartingWith('UNSUB '));
    }

    /**
     * A stalled UNSUB (a peer that stopped reading) holds the cleanup to the request timeout too: the handler gets m-1
     * and m-2, handle() throws the run's own failure within it, and once the stalled write lands no second UNSUB
     * follows.
     */
    public function testAStalledUnsubEndsTheReleaseAtTheRequestTimeout(): void
    {
        [$transport, $watched, $client] = $this->client(300);
        $otherSid = $client->subscribe('other', static function (): void {
            throw new \RuntimeException('the other subscription\'s handler failed');
        })->await();
        $server = $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1 ? [self::messages($sid, 'm-1', 'm-2')] : []);
        /** @var \ArrayObject<int, string> $handled */
        $handled = new \ArrayObject();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled): void {
                $handled[] = $message->payload;
            });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1 && $watched->reads >= 2);

        $transport->stallNextWriteContaining('UNSUB ' . $server->sid, 5.0);
        $transport->pushFrame(ReconnectingTransport::msgFrame('other', $otherSid, 'x'));
        $started = hrtime(true);
        [, $error] = self::settle($run);
        $elapsed = $this->secondsSince($started);

        self::assertInstanceOf(\RuntimeException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertLessThan(2.0, $elapsed, 'the cleanup ended at the request timeout');
        self::assertSame(1, $transport->writesStalled(), 'the UNSUB is still stalled');

        $transport->releaseStalledWrites();
        $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith('UNSUB ') !== []);
        delay(0.05);
        self::assertSame(['UNSUB ' . $server->sid], $transport->controlLinesStartingWith('UNSUB '), 'no second UNSUB');
    }

    /**
     * A stop() made while the cleanup waits for its PONG ends the cleanup at once and leaves what the pulls hold
     * undelivered, as a stop() leaves it anywhere; handle() still throws the run's own failure, which came first, not
     * a CancelledException of the cleanup.
     */
    public function testAStopDuringTheReleaseLeavesTheRestUndeliveredAndKeepsTheFailure(): void
    {
        [$transport, $watched, $client] = $this->client(5_000);
        $otherSid = $client->subscribe('other', static function (): void {
            throw new \RuntimeException('the other subscription\'s handler failed');
        })->await();
        $server = $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1 ? [self::messages($sid, 'm-1', 'm-2')] : []);
        /** @var \ArrayObject<int, string> $handled */
        $handled = new \ArrayObject();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(3)->setDepth(1)->setExpiresMs(30_000);
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled): void {
            $handled[] = $message->payload;
        });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1 && $watched->reads >= 2);

        $transport->answerPings = false;
        $transport->afterWrite = static function (string $bytes) use ($iterator, $server): void {
            if (str_contains($bytes, 'UNSUB ' . $server->sid . "\r\n")) {
                EventLoop::delay(0.05, $iterator->stop(...));
            }
        };
        $transport->pushFrame(ReconnectingTransport::msgFrame('other', $otherSid, 'x'));
        $started = hrtime(true);
        [, $error] = self::settle($run);

        self::assertInstanceOf(\RuntimeException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame('the other subscription\'s handler failed', $error->getMessage());
        self::assertSame([], $handled->getArrayCopy(), 'the stop left m-1 and m-2 undelivered');
        self::assertLessThan(2.0, $this->secondsSince($started), 'the stop ended the cleanup, not its 5 s budget');
        self::assertSame(['UNSUB ' . $server->sid], $transport->controlLinesStartingWith('UNSUB '));
    }

    /**
     * The cleanup owns its wait, not the read it drives: a handler of another subscription that the cleanup's read
     * runs, and that waits on a gate, holds that read, and with it the PONG behind it, yet the run settles at the
     * request timeout (300 ms) with its own failure, the gate still closed, its inbox sealed. m-3, which came in the
     * same chunk as the gated subscription's message, behind it in delivery order (the gated subscription's sid is
     * lower), was queued for the inbox: the seal routes it into the pull, and the handler gets it after m-1 and m-2.
     * Opening the gate afterwards routes nothing more to the run.
     */
    public function testAHandlerOfAnotherSubscriptionThatTheReleaseRunsCannotHoldTheRun(): void
    {
        [$transport, $watched, $client] = $this->client(300);
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        $gatedSid = $client->subscribe('gated', static function () use ($gate): void {
            $gate->getFuture()->await();
        })->await();
        $otherSid = $client->subscribe('other', static function (): void {
            throw new \RuntimeException('the other subscription\'s handler failed');
        })->await();
        $server = $this->pullServer($transport, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1 ? [self::messages($sid, 'm-1', 'm-2')] : []);
        /** @var \ArrayObject<int, string> $handled */
        $handled = new \ArrayObject();
        $run = $client->jetStream()->pullConsumer('S', 'C')->setBatching(4)->setDepth(1)->setExpiresMs(30_000)
            ->handle(static function (NatsMessage $message) use ($handled): void {
                $handled[] = $message->payload;
            });
        $this->waitUntil(static fn(): bool => $server->sid !== null && $watched->readsUnderWay === 1 && $watched->reads >= 2);

        $transport->pongDelay = 0.05;
        $transport->afterWrite = static function (string $bytes) use ($transport, $server, $gatedSid): void {
            if (str_contains($bytes, 'UNSUB ' . $server->sid . "\r\n")) {
                $transport->pushFrame(ReconnectingTransport::msgFrame('gated', $gatedSid, 'g') . self::messages((int) $server->sid, 'm-3'));
            }
        };
        $transport->pushFrame(ReconnectingTransport::msgFrame('other', $otherSid, 'x'));
        $started = hrtime(true);
        try {
            [, $error] = self::settle($run);

            self::assertFalse($gate->isComplete());
            self::assertInstanceOf(\RuntimeException::class, $error, sprintf('handle() threw %s', self::describe($error)));
            self::assertSame('the other subscription\'s handler failed', $error->getMessage());
            self::assertLessThan(2.0, $this->secondsSince($started), 'the run settled at the request timeout');
            self::assertSame(['m-1', 'm-2', 'm-3'], $handled->getArrayCopy());
            self::assertFalse($client->isSubscriptionActive((int) $server->sid), 'the inbox is sealed');
        } finally {
            $gate->complete();
        }
        $transport->pushFrame(self::messages((int) $server->sid, 'm-4'));
        $client->processIncoming(new TimeoutCancellation(0.2))->await();
        self::assertSame(['m-1', 'm-2', 'm-3'], $handled->getArrayCopy(), 'nothing is routed to the run after the seal');
    }

    /**
     * A connected client over a watched ReconnectingTransport, closed in tearDown, with no heartbeat, reconnect on and
     * handlerErrorsFailOperations, so that another subscription's handler that throws in the engine's read fails the
     * run, the connection staying open.
     *
     * @param (\Closure(\Throwable): void)|null $errorListener
     * @return array{ReconnectingTransport, WatchedTransport, NatsClient}
     */
    private function client(int $requestTimeoutMs = 2_000, ?\Closure $errorListener = null): array
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->own(new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: $requestTimeoutMs,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            errorListener: $errorListener,
            handlerErrorsFailOperations: true,
        ), $watched));
        $this->opened[] = $client;
        $client->connect()->await();

        return [$transport, $watched, $client];
    }

    /**
     * The scripted server's side of the pull consumer: records the epoch of every pull and the run's inbox, and answers
     * each pull with what $onPull returns, given the pull's reply subject, the inbox's sid and the pull's number.
     * Each +ACK request is confirmed.
     *
     * @param \Closure(string, int, int): list<string> $onPull
     * @return object{sid: ?int, base: ?string, epochs: list<int>}
     */
    private function pullServer(ReconnectingTransport $transport, \Closure $onPull): object
    {
        $server = new class {
            public ?int $sid = null;
            public ?string $base = null;
            /** @var list<int> */
            public array $epochs = [];
        };
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport, $server, $onPull): array {
            if ($replyTo !== null && str_starts_with($subject, '$JS.ACK.')) {
                return $transport->replyFrame($replyTo, '');
            }
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

            return $onPull($replyTo, $sid, count($server->epochs));
        };

        return $server;
    }

    /**
     * The scripted stream behind consumer C ({@see ScriptedPullStream}), holding $stored, as the transport's server.
     *
     * @param list<string> $stored
     */
    private function streamServer(ReconnectingTransport $transport, array $stored): ScriptedPullStream
    {
        $stream = new ScriptedPullStream($transport, self::PULL_SUBJECT, self::ACK_SUBJECT, $stored);
        $stream->serve();

        return $stream;
    }

    /**
     * Logs, in one list with what the handlers log, each write of the inbox's release ("wire: UNSUB <sid> | PING") and
     * each +ACK ("wire: ACK"), in the order they reach the wire.
     *
     * @return \ArrayObject<int, string>
     */
    private function watchWire(ReconnectingTransport $transport): \ArrayObject
    {
        /** @var \ArrayObject<int, string> $log */
        $log = new \ArrayObject();
        $transport->afterWrite = static function (string $bytes) use ($log): void {
            if (str_starts_with($bytes, 'UNSUB ')) {
                $log[] = 'wire: ' . str_replace("\r\n", ' | ', trim($bytes));
            } elseif (str_starts_with($bytes, 'PUB ' . self::ACK_SUBJECT)) {
                $log[] = 'wire: ACK';
            }
        };

        return $log;
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

    /** @param \ArrayObject<int, \Throwable> $reported */
    private static function timeoutsReported(\ArrayObject $reported): int
    {
        $count = 0;
        foreach ($reported as $error) {
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
