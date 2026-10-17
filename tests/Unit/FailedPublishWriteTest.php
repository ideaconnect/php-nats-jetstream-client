<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\ByteStream\ClosedException;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\HeldFailingWriteTransport;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\SlowLogger;
use IDCT\NATS\Tests\Support\ThrowingLogger;
use IDCT\NATS\Transport\TransportClosedException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Revolt\EventLoop;

use function Amp\delay;

/**
 * An ordinary publish whose write fails on an Open connection (#244): its frame goes to the reconnect buffer, the
 * publish resolves once the frame is accepted there, and the recovery runs in a fiber of its own, over the scripted
 * server in `tests/Support/ReconnectingTransport.php`. The publish used to run the whole recovery inline and write
 * again afterwards: with the default policy, about 100 s for a server that did not come back.
 *
 * The recovery's next dial is held where a test needs the publish to be done before it: the publish is awaited while
 * the dial is held, and the wire is checked once it is let through. A timeout given to an await only ends a broken test
 * ({@see self::WATCHDOG}); no test passes by the clock.
 */
final class FailedPublishWriteTest extends TestCase
{
    use OwnsTestResources;

    /** Seconds after which a wait that should have ended fails the test. */
    private const WATCHDOG = 3;

    /** The frame of publish('orders', 'x'): 17 bytes. */
    private const FRAME = "PUB orders 1\r\nx\r\n";

    protected function tearDown(): void
    {
        $this->releaseOwnedResources();
    }

    /** @return iterable<string, array{string, bool}> */
    public static function failedPublishes(): iterable
    {
        foreach (['waiting for a reconnect' => true, 'not waiting for a reconnect' => false] as $policy => $wait) {
            yield 'publish(), ' . $policy => ['publish', $wait];
            yield 'publishWithHeaders(), ' . $policy => ['publishWithHeaders', $wait];
            yield 'a JetStream ack(), ' . $policy => ['ack', $wait];
            yield 'a header block segment, ' . $policy => ['header block', $wait];
        }
    }

    /**
     * The publish resolves while the recovery's dial is still held, whether or not operations wait for a reconnect, and
     * nothing is written meanwhile. Once the dial is let through, the frame goes out on the new connection after its
     * CONNECT and the subscription replay, once, and each message it carries is counted once. A plain ack() is the
     * issue's measurement: through the Symfony Messenger transport it returned after 103 s.
     */
    #[DataProvider('failedPublishes')]
    public function testAFailedPublishResolvesWhileTheReconnectDialIsStillHeld(string $kind, bool $wait): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, wait: $wait);
        $client->subscribe('updates', static function (): void {})->await();
        [$publish, $needle, $expected] = self::failedPublish($kind);
        $this->holdTheReconnectDial($transport);
        $transport->failNextWriteContaining($needle);

        $publish($client)->await(new TimeoutCancellation(self::WATCHDOG));
        self::assertSame(ConnectionState::Connecting, $client->state());
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
        self::assertSame([], self::publishLines($transport), 'nothing is written while the dial is held');

        $transport->releaseDial();
        $this->waitUntilOpen($client);
        $client->flush()->await(new TimeoutCancellation(self::WATCHDOG));

        $lines = $transport->controlLines(1);
        self::assertStringStartsWith('CONNECT ', $lines[0]);
        self::assertSame($expected, self::publishLines($transport));
        self::assertLessThan(array_search($expected[0], $lines, true), array_search('SUB updates 1', $lines, true), 'the replay comes first');
        self::assertSame(count($expected), $client->statistics()->outMsgs);
        self::assertCount(2, $transport->connectCalls);
    }

    /**
     * A second publish whose write fails on the old socket while the first one's recovery is under way puts its frame
     * into that recovery's buffer, behind the first: it does not wait for the recovery, and starts no second one.
     */
    public function testASecondWriteFailingOnTheOldSocketJoinsTheRecoveryBuffer(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport);
        $this->holdTheReconnectDial($transport);
        $transport->failNextWriteContaining('PUB orders.a');

        // The write of orders.b waits behind that of orders.a, and fails with the session it was made on.
        $first = $client->publish('orders.a', 'a');
        $second = $client->publish('orders.b', 'b');
        $first->await(new TimeoutCancellation(self::WATCHDOG));
        $second->await(new TimeoutCancellation(self::WATCHDOG));
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
        self::assertSame([], self::publishLines($transport));

        $transport->releaseDial();
        $this->waitUntilOpen($client);
        $client->flush()->await(new TimeoutCancellation(self::WATCHDOG));

        self::assertSame(['PUB orders.a 1', 'PUB orders.b 1'], self::publishLines($transport, 1));
        self::assertCount(2, $transport->connectCalls, 'one recovery owns the outage');
        self::assertSame(2, $client->statistics()->outMsgs);
    }

    /** @return iterable<string, array{int}> */
    public static function roomOrNone(): iterable
    {
        yield 'room in the buffer' => [8_388_608];
        yield 'buffering disabled' => [0];
    }

    /**
     * A write that fails once another fiber's read has met the lost connection and runs its recovery: the frame goes
     * straight into that recovery's buffer, the publish resolving while its dial is held, and out on the new connection
     * once it is let through; with buffering disabled the publish fails with "Connection is not open", the write's
     * error its previous, and the recovery reopens the connection without it.
     */
    #[DataProvider('roomOrNone')]
    public function testAWriteFailingWhileAnotherFibersRecoveryRunsGoesIntoItsBuffer(int $bufferSize): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, bufferSize: $bufferSize);
        // The write is held past the end of its session, and fails once the test lets it go.
        $transport->stallNextWriteContaining('PUB orders', 30, outlivesSession: true);
        $publish = $client->publish('orders', 'x');
        $publish->ignore();
        $this->waitUntil(static fn(): bool => $transport->writesStalled() === 1);
        $this->holdTheReconnectDial($transport);
        $transport->dropConnection();
        // The application's read meets the drop and runs the recovery, which waits on its held dial.
        $read = $this->resources()->ownOperation($client->processIncoming());
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);

        $transport->releaseStalledWrites();
        $failure = self::failureOf($publish);
        if ($bufferSize > 0) {
            self::assertNull($failure);
        } else {
            self::assertInstanceOf(ConnectionException::class, $failure);
            self::assertSame('Connection is not open', $failure->getMessage());
            self::assertSame(0, $failure->getCode());
            self::assertInstanceOf(TransportClosedException::class, $failure->getPrevious());
        }
        self::assertSame([], self::publishLines($transport));

        $transport->releaseDial();
        $read->await(new TimeoutCancellation(self::WATCHDOG));
        $this->waitUntilOpen($client);
        $client->flush()->await(new TimeoutCancellation(self::WATCHDOG));

        self::assertSame($bufferSize > 0 ? ['PUB orders 1'] : [], self::publishLines($transport, 1));
        self::assertCount(2, $transport->connectCalls, 'the read\'s recovery is the only one');
    }

    /** @return iterable<string, array{int, list<string>}> The buffer's size, and what the new connection gets. */
    public static function roomForTheListenersPublish(): iterable
    {
        yield 'room for one frame' => [strlen(self::FRAME), ['PUB orders 1']];
        yield 'room for both' => [2 * strlen(self::FRAME), ['PUB orders 1', 'PUB later 1']];
    }

    /**
     * A Disconnected listener that publishes cannot take the room of the frame whose failed write started the
     * recovery: the frame was accepted before the listener was called. With room for one frame only, the listener's
     * publish is refused; with room for both, both go out, in the order they were accepted.
     *
     * @param list<string> $expected
     */
    #[DataProvider('roomForTheListenersPublish')]
    public function testTheFailedFrameIsAcceptedBeforeTheDisconnectedListenerPublishes(int $bufferSize, array $expected): void
    {
        $transport = new ReconnectingTransport();
        $holder = new class {
            public ?NatsClient $client = null;
            public ?\Throwable $laterFailure = null;
            public bool $listenerRan = false;
        };
        $client = $this->client($transport, bufferSize: $bufferSize, listener: static function (ConnectionEvent $event) use ($holder): void {
            if ($event !== ConnectionEvent::Disconnected || $holder->client === null) {
                return;
            }

            $holder->listenerRan = true;
            try {
                $holder->client->publish('later', 'x')->await();
            } catch (\Throwable $failure) {
                $holder->laterFailure = $failure;
            }
        });
        $holder->client = $client;
        $this->holdTheReconnectDial($transport);
        $transport->failNextWriteContaining('PUB orders');

        $client->publish('orders', 'x')->await(new TimeoutCancellation(self::WATCHDOG));
        // The dial starts once the listener has returned.
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
        self::assertTrue($holder->listenerRan);
        if (count($expected) === 1) {
            self::assertInstanceOf(ConnectionException::class, $holder->laterFailure);
            self::assertSame('Connection is not open', $holder->laterFailure->getMessage());
        } else {
            self::assertNull($holder->laterFailure);
        }

        $transport->releaseDial();
        $this->waitUntilOpen($client);
        $client->flush()->await(new TimeoutCancellation(self::WATCHDOG));

        self::assertSame($expected, self::publishLines($transport, 1));
        self::assertSame(count($expected), $client->statistics()->outMsgs);
    }

    /**
     * A Disconnected listener that closes the connection: the publish had been accepted already, and resolves; the
     * close discards its frame with the rest of the buffer, without a report, as disconnect() discards every buffered
     * publish; and a later connect() does not bring the frame back.
     */
    public function testADisconnectedListenerThatClosesFindsTheFrameAcceptedAndDiscardsIt(): void
    {
        $transport = new ReconnectingTransport();
        $errors = [];
        $closed = new DeferredFuture();
        $holder = new class {
            public ?NatsClient $client = null;
        };
        $client = $this->client(
            $transport,
            listener: static function (ConnectionEvent $event) use ($holder, $closed): void {
                if ($event === ConnectionEvent::Disconnected && $holder->client !== null) {
                    $holder->client->disconnect()->await();
                    $closed->complete();
                }
            },
            errorListener: static function (\Throwable $error) use (&$errors): void {
                $errors[] = $error->getMessage();
            },
        );
        $holder->client = $client;
        $transport->failNextWriteContaining('PUB orders');

        $client->publish('orders', 'x')->await(new TimeoutCancellation(self::WATCHDOG));
        $closed->getFuture()->await(new TimeoutCancellation(self::WATCHDOG));
        self::assertSame(ConnectionState::Closed, $client->state());

        $holder->client = null;
        $client->connect()->await(new TimeoutCancellation(self::WATCHDOG));
        $client->flush()->await(new TimeoutCancellation(self::WATCHDOG));

        self::assertSame([], self::publishLines($transport), 'nothing resurrected on the new connection');
        self::assertSame([], $errors, 'disconnect() reports no discarded publish');
        self::assertSame(1, $client->statistics()->outMsgs, 'the publish was accepted');
        self::assertCount(2, $transport->connectCalls, 'the recovery did not dial');
    }

    /** @return iterable<string, array{int, bool}> The buffer's size, and whether the frame fits. */
    public static function bufferSizes(): iterable
    {
        yield 'buffering disabled' => [0, false];
        yield 'an exact fit' => [strlen(self::FRAME), true];
        yield 'one byte short' => [strlen(self::FRAME) - 1, false];
    }

    /**
     * The frame is accepted when it fits in the reconnect buffer, to the byte. One that does not fails the publish at
     * once with "Connection is not open", the write's error its previous, and nothing of it is buffered; either way
     * the recovery goes on and reopens the connection.
     */
    #[DataProvider('bufferSizes')]
    public function testTheFrameIsAcceptedOnlyWhenItFitsAndTheRecoveryGoesOnEitherWay(int $bufferSize, bool $fits): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, bufferSize: $bufferSize);
        $writeError = new TransportClosedException('Connection reset by peer');
        $transport->failNextWriteContaining('PUB orders', $writeError);

        $failure = self::failureOf($client->publish('orders', 'x'));
        if ($fits) {
            self::assertNull($failure);
        } else {
            self::assertInstanceOf(ConnectionException::class, $failure);
            self::assertSame('Connection is not open', $failure->getMessage());
            self::assertSame(0, $failure->getCode());
            self::assertSame($writeError, $failure->getPrevious());
        }

        $this->waitUntilOpen($client);
        $client->flush()->await(new TimeoutCancellation(self::WATCHDOG));

        self::assertSame($fits ? ['PUB orders 1'] : [], self::publishLines($transport, 1));
        self::assertSame($fits ? 1 : 0, $client->statistics()->outMsgs);
        self::assertSame(1, $client->statistics()->reconnects);
    }

    /**
     * A buffer the first failed frame filled refuses the second, whose write failed on the old socket as well: the
     * second publish fails with "Connection is not open", the first frame goes out.
     */
    public function testAFullBufferRefusesTheFrameOfASecondFailedWrite(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, bufferSize: strlen("PUB orders.a 1\r\na\r\n"));
        $this->holdTheReconnectDial($transport);
        $transport->failNextWriteContaining('PUB orders.a');

        $first = $client->publish('orders.a', 'a');
        $second = $client->publish('orders.b', 'b');
        self::assertNull(self::failureOf($first));
        $failure = self::failureOf($second);
        self::assertInstanceOf(ConnectionException::class, $failure);
        self::assertSame('Connection is not open', $failure->getMessage());
        self::assertSame(0, $failure->getCode());
        self::assertInstanceOf(TransportClosedException::class, $failure->getPrevious());

        $transport->releaseDial();
        $this->waitUntilOpen($client);
        $client->flush()->await(new TimeoutCancellation(self::WATCHDOG));

        self::assertSame(['PUB orders.a 1'], self::publishLines($transport, 1));
        self::assertSame(1, $client->statistics()->outMsgs);
    }

    /**
     * A header block segment larger than the reconnect buffer is refused whole: the block's publish fails, nothing of
     * the segment is buffered, and the recovery goes on.
     */
    public function testASegmentLargerThanTheBufferIsRefusedWhole(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, bufferSize: 40);
        $transport->failNextWriteContaining('HPUB orders.1');

        $failure = self::failureOf($client->publishHeaderBlock([
            ['subject' => 'orders.1', 'payload' => 'a', 'headers' => ['K' => 'v']],
            ['subject' => 'orders.2', 'payload' => 'b', 'headers' => ['K' => 'v']],
        ]));

        self::assertInstanceOf(ConnectionException::class, $failure);
        self::assertSame('Connection is not open', $failure->getMessage());
        $this->waitUntilOpen($client);
        $client->flush()->await(new TimeoutCancellation(self::WATCHDOG));
        self::assertSame([], self::publishLines($transport));
        self::assertSame(0, $client->statistics()->outMsgs);
    }

    /**
     * A frame whose every write fails - on every new connection - fails every flush of the reconnect, which uses up
     * its attempts once, without starting a fresh budget for the same frame. The publish had resolved; its bytes are
     * kept from attempt to attempt and reported discarded once, before the Closed event.
     */
    public function testAFrameEveryFlushFailsExhaustsTheReconnectOnceAndIsReportedDiscarded(): void
    {
        $transport = new ReconnectingTransport();
        $log = [];
        $client = $this->client(
            $transport,
            maxReconnectAttempts: 2,
            listener: static function (ConnectionEvent $event) use (&$log): void {
                $log[] = $event->name;
            },
            errorListener: static function (\Throwable $error) use (&$log): void {
                $log[] = $error->getMessage();
            },
        );
        // Every session takes the CONNECT and fails the frame: the first write of the publish and both flushes.
        $transport->afterWrite = static function (string $bytes) use ($transport): void {
            if (str_contains($bytes, 'CONNECT ')) {
                $transport->failNextWriteContaining('PUB orders');
            }
        };
        $transport->failNextWriteContaining('PUB orders');

        $client->publish('orders', 'x')->await(new TimeoutCancellation(self::WATCHDOG));
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Closed);

        self::assertSame([
            'Connected',
            'Disconnected',
            'Reconnect exhausted: 17 bytes of buffered publishes were discarded',
            'Closed',
        ], $log);
        self::assertCount(3, $transport->connectCalls, 'the first connection, and the two attempts of one reconnect');
        self::assertSame([], self::publishLines($transport));
        self::assertSame(1, $client->statistics()->outMsgs);
    }

    /**
     * A flush that wrote the failed frame and then failed on a frame buffered behind it keeps that frame for the next
     * attempt: each goes out once, the first on the connection whose flush failed, the second on the next one.
     */
    public function testAFlushThatFailsPartwayKeepsWhatItHadNotWritten(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport);
        $this->holdTheReconnectDial($transport);
        $transport->failNextWriteContaining('PUB orders.1');

        $client->publish('orders.1', 'a')->await(new TimeoutCancellation(self::WATCHDOG));
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
        // The flush writes orders.1 and is held there, while orders.2 is buffered behind it; its own write then fails.
        $transport->holdCompletionOfNextWriteContaining('PUB orders.1', 30);
        $transport->releaseDial();
        $this->waitUntil(static fn(): bool => $transport->completionsHeld() === 1);
        $client->publish('orders.2', 'b')->await(new TimeoutCancellation(self::WATCHDOG));
        $transport->failNextWriteContaining('PUB orders.2');
        $transport->releaseHeldCompletions();
        $this->waitUntilOpen($client);
        $client->flush()->await(new TimeoutCancellation(self::WATCHDOG));

        self::assertSame(['PUB orders.1 1'], self::publishLines($transport, 1));
        self::assertSame(['PUB orders.2 1'], self::publishLines($transport, 2));
        self::assertSame(2, $client->statistics()->outMsgs);
    }

    /**
     * A flush whose write fails after the server took its bytes - an ambiguous failure - sends the frame again on the
     * next connection: a duplicate rather than a loss.
     */
    public function testAFlushWhoseWriteFailsAfterTheServerTookItSendsTheFrameAgain(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport);
        $ambiguous = true;
        $transport->afterWrite = static function (string $bytes) use ($transport, &$ambiguous): void {
            if ($ambiguous && $transport->epoch() === 1 && str_contains($bytes, 'PUB orders')) {
                $ambiguous = false;
                $transport->dropConnection();

                throw new TransportClosedException('Connection reset by peer');
            }
        };
        $transport->failNextWriteContaining('PUB orders');

        $client->publish('orders', 'x')->await(new TimeoutCancellation(self::WATCHDOG));
        $this->waitUntil(static fn(): bool => $transport->epoch() === 2 && $client->state() === ConnectionState::Open);
        $client->flush()->await(new TimeoutCancellation(self::WATCHDOG));

        self::assertSame(['PUB orders 1'], self::publishLines($transport, 1));
        self::assertSame(['PUB orders 1'], self::publishLines($transport, 2));
        self::assertSame(1, $client->statistics()->outMsgs);
    }

    /**
     * A reconnect that the server refuses as unauthorized: the publish had resolved and stays resolved; the reconnect
     * reports the frame discarded, then the authentication error, and closes the connection once.
     */
    public function testAReconnectRefusedAuthenticationReportsTheAcceptedFrameDiscarded(): void
    {
        $transport = new ReconnectingTransport();
        $log = [];
        $client = $this->client(
            $transport,
            listener: static function (ConnectionEvent $event) use (&$log): void {
                $log[] = $event->name;
            },
            errorListener: static function (\Throwable $error) use (&$log): void {
                $log[] = $error->getMessage();
            },
        );
        $this->holdTheReconnectDial($transport);
        $transport->failNextWriteContaining('PUB orders');

        $publish = $client->publish('orders', 'x');
        $publish->await(new TimeoutCancellation(self::WATCHDOG));
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
        $transport->rejectAuthentication = true;
        $transport->releaseDial();
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Closed);

        self::assertNull(self::failureOf($publish), 'the publish stays resolved');
        self::assertSame('Reconnect failed authentication: 17 bytes of buffered publishes were discarded', $log[2] ?? null);
        self::assertSame(['Connected', 'Disconnected'], array_slice($log, 0, 2));
        self::assertSame('Closed', $log[count($log) - 1]);
        self::assertCount(1, array_keys($log, 'Closed', true));
        self::assertSame([], self::publishLines($transport));
    }

    /** @return iterable<string, array{string}> */
    public static function slowOrBrokenCallbacks(): iterable
    {
        yield 'a slow Disconnected listener' => ['slow listener'];
        yield 'a throwing Disconnected listener' => ['throwing listener'];
        yield 'a slow logger' => ['slow logger'];
        yield 'a throwing logger' => ['throwing logger'];
    }

    /**
     * What the recovery calls once the frame is accepted - the Disconnected listener, the logger - neither delays the
     * publish nor changes its outcome: a slow one is still under way when the publish has resolved, a throwing one is
     * contained, and the recovery sends the frame.
     */
    #[DataProvider('slowOrBrokenCallbacks')]
    public function testSlowOrThrowingCallbacksOfTheRecoveryNeitherDelayNorFailThePublish(string $callback): void
    {
        $transport = new ReconnectingTransport();
        $slowListener = new class {
            public bool $suspended = false;
        };
        $listener = match ($callback) {
            'slow listener' => static function (ConnectionEvent $event) use ($slowListener): void {
                if ($event === ConnectionEvent::Disconnected) {
                    $slowListener->suspended = true;
                    delay(0.2);
                    $slowListener->suspended = false;
                }
            },
            'throwing listener' => static function (ConnectionEvent $event): void {
                if ($event === ConnectionEvent::Disconnected) {
                    throw new \RuntimeException('listener failed');
                }
            },
            default => null,
        };
        $logger = match ($callback) {
            'slow logger' => new SlowLogger('NATS connection Disconnected', 0.2),
            'throwing logger' => new ThrowingLogger('NATS connection'),
            default => null,
        };
        $client = $this->client($transport, listener: $listener, logger: $logger);
        $transport->failNextWriteContaining('PUB orders');

        $client->publish('orders', 'x')->await(new TimeoutCancellation(self::WATCHDOG));
        if ($callback === 'slow listener') {
            self::assertTrue($slowListener->suspended, 'the listener is still under way');
        } elseif ($logger instanceof SlowLogger) {
            self::assertTrue($logger->suspended, 'the logger is still under way');
        }
        $this->waitUntilOpen($client);
        $client->flush()->await(new TimeoutCancellation(self::WATCHDOG));

        self::assertSame(['PUB orders 1'], self::publishLines($transport, 1));
    }

    /**
     * A close that the application makes in the tick the write fails, before the recovery's fiber has claimed the
     * connection: the close wins, the recovery does not start, and the publish fails with its write's error, as an
     * error of this library - a raw stream error is wrapped in a TransportClosedException.
     */
    public function testACloseMadeBeforeTheRecoveryClaimsTheConnectionWins(): void
    {
        $inner = new ReconnectingTransport();
        $transport = new HeldFailingWriteTransport($inner);
        $connection = $this->connection($transport);
        $writeError = new ClosedException('The stream is not writable');
        $transport->holdNextWriteContaining('PUB orders');
        $publish = $connection->publish('orders', 'x');
        $publish->ignore();
        $this->waitUntil(static fn(): bool => $transport->writeHeld());

        $inner->dropConnection();
        $transport->failHeldWrite($writeError);
        // disconnect() sets close-intent in a fiber of its own: started from here it runs right after the writer's
        // catch, ahead of the recovery's fiber that catch queued.
        $disconnect = null;
        self::rightBeforeTheWriterCatches(static function () use ($connection, &$disconnect): void {
            $disconnect = $connection->disconnect();
        });
        $failure = self::failureOf($publish);
        self::assertInstanceOf(Future::class, $disconnect);
        $disconnect->await(new TimeoutCancellation(self::WATCHDOG));

        self::assertInstanceOf(TransportClosedException::class, $failure);
        self::assertSame('Transport is not connected', $failure->getMessage());
        self::assertSame(0, $failure->getCode());
        self::assertSame($writeError, $failure->getPrevious());
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertCount(1, $inner->connectCalls, 'no reconnect');
        self::assertSame(0, $connection->statistics()->outMsgs);
    }

    /** @return iterable<string, array{string}> */
    public static function whatTheRecoveryFiberFinds(): iterable
    {
        yield 'a recovery another fiber runs' => ['recovery'];
        yield "that recovery's sealed flush" => ['sealed flush'];
        yield "that recovery's terminal close" => ['closed'];
        yield 'a close and a reopen' => ['reopened'];
        yield 'a connect() of the application dialling' => ['connect'];
    }

    /**
     * What the recovery's fiber finds when it runs, a tick after the write failed, decides where the frame goes: into
     * the buffer of a recovery another fiber started meanwhile, the publish not waiting for that recovery; behind its
     * sealed flush (#165), the publish waiting for the flush to end and then written once to the connection it opened;
     * nowhere once the connection is closed, or was closed and reopened, the publish failing with its write's error;
     * and not into a connect() of the application's, whose dial no buffer is flushed by. The stand-in recovery ends
     * only once the publish has, so a publish that waited for it would fail the test.
     */
    #[DataProvider('whatTheRecoveryFiberFinds')]
    public function testWhatTheRecoveryFiberFindsDecidesWhereTheFrameGoes(string $found): void
    {
        $inner = new ReconnectingTransport();
        $transport = new HeldFailingWriteTransport($inner);
        $connection = $this->connection($transport);
        $state = new \ReflectionProperty(NatsConnection::class, 'state');
        $reconnecting = new \ReflectionProperty(NatsConnection::class, 'reconnecting');
        $connecting = new \ReflectionProperty(NatsConnection::class, 'connecting');
        $flushGate = new \ReflectionProperty(NatsConnection::class, 'reconnectFlushGate');
        $terminalCloses = new \ReflectionProperty(NatsConnection::class, 'terminalCloses');
        $generation = new \ReflectionProperty(NatsConnection::class, 'connectionGeneration');
        $buffer = new \ReflectionProperty(NatsConnection::class, 'reconnectBuffer');
        /** @var DeferredFuture<void> $standIn */
        $standIn = new DeferredFuture();
        /** @var DeferredFuture<void> $gate */
        $gate = new DeferredFuture();
        $writeError = new TransportClosedException('Connection reset by peer');
        $transport->holdNextWriteContaining('PUB orders');
        $publish = $connection->publish('orders', 'x');
        $publish->ignore();
        $this->waitUntil(static fn(): bool => $transport->writeHeld());

        $transport->failHeldWrite($writeError);
        $seen = new class {
            /** Whether something had claimed the connection when the state was changed; null until then. */
            public ?bool $claimed = null;
        };
        self::rightAfterTheWriterCatches(static function () use ($found, $connection, $state, $reconnecting, $connecting, $flushGate, $terminalCloses, $standIn, $gate, $seen): void {
            // Nothing has claimed the connection yet: the recovery's fiber runs next.
            $seen->claimed = $reconnecting->getValue($connection) !== null || $state->getValue($connection) !== ConnectionState::Open;
            if ($found === 'connect') {
                $connecting->setValue($connection, $standIn);
                $state->setValue($connection, ConnectionState::Connecting);

                return;
            }

            $reconnecting->setValue($connection, $standIn);
            match ($found) {
                'closed' => $state->setValue($connection, ConnectionState::Closed),
                'reopened' => $terminalCloses->setValue($connection, $terminalCloses->getValue($connection) + 1),
                default => $state->setValue($connection, ConnectionState::Connecting),
            };
            if ($found === 'sealed flush') {
                $flushGate->setValue($connection, $gate);
            }
        });

        try {
            $this->waitUntil(static fn(): bool => $seen->claimed !== null);
            self::assertFalse($seen->claimed);
            if ($found === 'sealed flush') {
                delay(0.05);
                self::assertFalse($publish->isComplete(), 'the publish waits for the sealed flush');
                // The recovery ends with the connection Open on a new socket.
                $generation->setValue($connection, $generation->getValue($connection) + 1);
                $state->setValue($connection, ConnectionState::Open);
                $reconnecting->setValue($connection, null);
                $flushGate->setValue($connection, null);
                $gate->complete();
            }
            $failure = self::failureOf($publish);

            match ($found) {
                'recovery' => self::assertSame(self::FRAME, $buffer->getValue($connection)),
                'sealed flush' => self::assertSame(['PUB orders 1'], self::publishLines($inner)),
                default => self::assertSame('', $buffer->getValue($connection)),
            };
            if ($found === 'recovery' || $found === 'sealed flush') {
                self::assertNull($failure);
            } elseif ($found === 'connect') {
                self::assertInstanceOf(ConnectionException::class, $failure);
                self::assertSame('Connection is not open', $failure->getMessage());
                self::assertSame(0, $failure->getCode());
                self::assertSame($writeError, $failure->getPrevious());
            } else {
                self::assertSame($writeError, $failure);
            }
            self::assertCount(1, $inner->connectCalls, 'no recovery of its own');
        } finally {
            // The stand-ins end as what they stand for would: the connection is left as it was before the write.
            $buffer->setValue($connection, '');
            $reconnecting->setValue($connection, null);
            $connecting->setValue($connection, null);
            $flushGate->setValue($connection, null);
            $state->setValue($connection, ConnectionState::Open);
            $standIn->complete();
        }
    }

    /** @return iterable<string, array{bool, list<string>}> Whether operations wait for a reconnect, and what comes first. */
    public static function yieldOrNot(): iterable
    {
        yield 'waiting for a reconnect' => [true, ['a callback deferred meanwhile', 'the publish']];
        yield 'not waiting for a reconnect' => [false, ['the publish', 'a callback deferred meanwhile']];
    }

    /**
     * A failed write that puts its frame into the buffer of a recovery already in flight then yields one event-loop
     * tick when operations wait for a reconnect, as a publish buffered during the reconnect does: a callback deferred
     * in the tick the write fails runs before the publish resolves. With waiting disabled it yields no tick: the
     * publish resolves first.
     *
     * @param list<string> $expected
     */
    #[DataProvider('yieldOrNot')]
    public function testAFrameTakenIntoARunningRecoverysBufferYieldsOneTickOnlyWhenWaiting(bool $wait, array $expected): void
    {
        $inner = new ReconnectingTransport();
        $transport = new HeldFailingWriteTransport($inner);
        $connection = $this->connection($transport, $wait);
        $state = new \ReflectionProperty(NatsConnection::class, 'state');
        $reconnecting = new \ReflectionProperty(NatsConnection::class, 'reconnecting');
        $buffer = new \ReflectionProperty(NatsConnection::class, 'reconnectBuffer');
        /** @var DeferredFuture<void> $standIn */
        $standIn = new DeferredFuture();
        $transport->holdNextWriteContaining('PUB orders');
        $publish = $connection->publish('orders', 'x');
        $publish->ignore();
        $this->waitUntil(static fn(): bool => $transport->writeHeld());
        $order = new class {
            /** @var list<string> What happened, in order. */
            public array $events = [];
        };

        // As a recovery another fiber runs holds the connection.
        $reconnecting->setValue($connection, $standIn);
        $state->setValue($connection, ConnectionState::Connecting);
        try {
            $resolved = $publish->map(static function () use ($order): void {
                $order->events[] = 'the publish';
            });
            $transport->failHeldWrite(new TransportClosedException('Connection reset by peer'));
            EventLoop::defer(static function () use ($order): void {
                $order->events[] = 'a callback deferred meanwhile';
            });
            $resolved->await(new TimeoutCancellation(self::WATCHDOG));
            $this->waitUntil(static fn(): bool => count($order->events) === 2);

            self::assertSame($expected, $order->events);
            self::assertSame(self::FRAME, $buffer->getValue($connection));
        } finally {
            $buffer->setValue($connection, '');
            $reconnecting->setValue($connection, null);
            $state->setValue($connection, ConnectionState::Open);
            $standIn->complete();
        }
    }

    /**
     * A failed publish that finds the reconnect's flush sealed (#165) - the one wait left besides its original write -
     * waits for the flush to end and then goes where the reconnect left the connection: written once to the connection
     * it opened, or failed with its write's error when it closed the connection.
     */
    #[DataProvider('sealedFlushOutcomes')]
    public function testAFailedPublishFindingTheFlushSealedWaitsForIt(bool $reopened): void
    {
        $inner = new ReconnectingTransport();
        $transport = new HeldFailingWriteTransport($inner);
        $connection = $this->connection($transport);
        $state = new \ReflectionProperty(NatsConnection::class, 'state');
        $reconnecting = new \ReflectionProperty(NatsConnection::class, 'reconnecting');
        $flushGate = new \ReflectionProperty(NatsConnection::class, 'reconnectFlushGate');
        $generation = new \ReflectionProperty(NatsConnection::class, 'connectionGeneration');
        /** @var DeferredFuture<void> $standIn */
        $standIn = new DeferredFuture();
        /** @var DeferredFuture<void> $gate */
        $gate = new DeferredFuture();
        $writeError = new TransportClosedException('Connection reset by peer');
        $transport->holdNextWriteContaining('PUB orders');
        $publish = $connection->publish('orders', 'x');
        $publish->ignore();
        $this->waitUntil(static fn(): bool => $transport->writeHeld());

        // As a reconnect holds the connection while it flushes a sealed buffer.
        $reconnecting->setValue($connection, $standIn);
        $state->setValue($connection, ConnectionState::Connecting);
        $flushGate->setValue($connection, $gate);
        $transport->failHeldWrite($writeError);
        try {
            delay(0.05);
            self::assertFalse($publish->isComplete(), 'the publish waits for the sealed flush');

            $reconnecting->setValue($connection, null);
            $flushGate->setValue($connection, null);
            if ($reopened) {
                $generation->setValue($connection, $generation->getValue($connection) + 1);
                $state->setValue($connection, ConnectionState::Open);
            } else {
                $state->setValue($connection, ConnectionState::Closed);
            }
            $gate->complete();
            $failure = self::failureOf($publish);

            self::assertSame($reopened ? null : $writeError, $failure);
            self::assertSame($reopened ? ['PUB orders 1'] : [], self::publishLines($inner));
            self::assertSame($reopened ? 1 : 0, $connection->statistics()->outMsgs);
        } finally {
            $reconnecting->setValue($connection, null);
            $flushGate->setValue($connection, null);
            $state->setValue($connection, ConnectionState::Open);
            $standIn->complete();
        }
    }

    /** @return iterable<string, array{bool}> */
    public static function sealedFlushOutcomes(): iterable
    {
        yield 'the reconnect opens the connection' => [true];
        yield 'the reconnect closes the connection' => [false];
    }

    /**
     * A drain() that waits for the reconnect the failed publish started: the reconnect's flush sends the frame before
     * the drain takes the connection over, unsubscribes and closes it. The frame used to be lost when the drain was
     * over before the publish's retry.
     */
    public function testADrainWaitingForTheReconnectGetsTheFrameSentFirst(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport);
        $client->subscribe('updates', static function (): void {})->await();
        $this->holdTheReconnectDial($transport);
        $transport->failNextWriteContaining('PUB orders');

        $client->publish('orders', 'x')->await(new TimeoutCancellation(self::WATCHDOG));
        $drain = $client->drain();
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
        $transport->releaseDial();
        $drain->await(new TimeoutCancellation(self::WATCHDOG));

        $lines = $transport->controlLines(1);
        self::assertSame(['PUB orders 1'], self::publishLines($transport, 1));
        self::assertLessThan(array_search('UNSUB 1', $lines, true), array_search('PUB orders 1', $lines, true));
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * A write that fails once the connection drains: the frame is not buffered, and no reconnect starts - the drain is
     * closing the connection. It goes to the socket the drain holds, within the drain's budget, as any publish made
     * during the drain does, and the drain then closes the connection as usual.
     */
    public function testAWriteFailingWhileTheConnectionDrainsGoesToTheSocketTheDrainHolds(): void
    {
        $inner = new ReconnectingTransport();
        $transport = new HeldFailingWriteTransport($inner);
        $connection = $this->connection($transport);
        $connection->subscribe('updates', static function (): void {})->await();
        $transport->holdNextWriteContaining('PUB orders');
        $publish = $connection->publish('orders', 'x');
        $publish->ignore();
        $this->waitUntil(static fn(): bool => $transport->writeHeld());
        // The drain's flush waits for the PONG of its PING, which the test sends.
        $inner->answerPings = false;
        $drain = $connection->drain();
        $this->waitUntil(static fn(): bool => $inner->controlLinesStartingWith('UNSUB') !== []);

        $transport->failHeldWrite(new TransportClosedException('Connection reset by peer'));
        self::assertNull(self::failureOf($publish));
        $inner->pushFrame("PONG\r\n");
        $drain->await(new TimeoutCancellation(self::WATCHDOG));

        $lines = $inner->controlLines(0);
        self::assertSame(['PUB orders 1'], self::publishLines($inner));
        self::assertLessThan(array_search('PUB orders 1', $lines, true), array_search('UNSUB 1', $lines, true), 'written during the drain');
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertCount(1, $inner->connectCalls, 'no reconnect');
        self::assertSame(1, $connection->statistics()->outMsgs);
    }

    /**
     * An atomic batch whose intermediate segment's write fails: the segment waits in the reconnect buffer, and the
     * commit - a request, which waits for the reconnect - goes out behind it on the new connection, so the server gets
     * the batch in order and its PubAck is what the commit returns. When the segment does not fit in the buffer, the
     * commit fails with "Connection is not open" and is never sent.
     */
    #[DataProvider('segmentFits')]
    public function testABatchWhoseIntermediatesFailedIsCommittedBehindThem(bool $fits): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport): array {
            if ($replyTo === null) {
                return [];
            }

            return $transport->replyFrame($replyTo, $subject === 'orders.c' ? '{"stream":"ORDERS","seq":3,"batch":"b1","count":3}' : '');
        };
        $client = $this->client($transport, bufferSize: $fits ? 8_388_608 : 64);
        $transport->failNextWriteContaining('HPUB orders.b');

        $commit = $client->jetStream()->batch('b1')
            ->add('orders.a', 'a')
            ->add('orders.b', 'b')
            ->add('orders.c', 'c')
            ->commit();
        $failure = null;
        $ack = null;
        try {
            $ack = $commit->await(new TimeoutCancellation(self::WATCHDOG));
        } catch (\Throwable $e) {
            $failure = $e;
        }
        $this->waitUntilOpen($client);

        $subjects = array_map(static fn(string $line): string => explode(' ', $line)[1], self::publishLines($transport));
        if (!$fits) {
            self::assertInstanceOf(ConnectionException::class, $failure);
            self::assertSame('Connection is not open', $failure->getMessage());
            self::assertSame(['orders.a'], $subjects, 'neither the intermediate nor the commit was sent');

            return;
        }

        self::assertNull($failure, sprintf('the commit failed with %s', $failure?->getMessage()));
        self::assertNotNull($ack);
        self::assertSame(3, $ack->batchCount);
        self::assertSame(['orders.a', 'orders.b', 'orders.c'], $subjects);
        self::assertCount(2, self::publishLines($transport, 1), 'the intermediate, then the commit, on the new connection');
    }

    /** @return iterable<string, array{bool}> */
    public static function segmentFits(): iterable
    {
        yield 'the segment fits in the buffer' => [true];
        yield 'the segment does not fit' => [false];
    }

    /**
     * Runs $then right before the writer whose held write {@see HeldFailingWriteTransport::failHeldWrite()} has just
     * failed catches the failure: the write's future resumes its awaiting fiber through two queued callbacks - the
     * future's, then the suspension's - and $then is queued behind the first. A fiber $then starts with async() runs
     * right after the writer's catch, ahead of the fiber that catch starts.
     *
     * @param \Closure(): void $then
     */
    private static function rightBeforeTheWriterCatches(\Closure $then): void
    {
        EventLoop::queue($then);
    }

    /**
     * Runs $then right after that writer's catch, before what the catch queued - a fiber started with async() - runs:
     * queued behind the suspension's callback, which the future's queues.
     *
     * @param \Closure(): void $then
     */
    private static function rightAfterTheWriterCatches(\Closure $then): void
    {
        EventLoop::queue(static function () use ($then): void {
            EventLoop::queue($then);
        });
    }

    /**
     * The publish, the needle its write fails on, and the control lines the new connection gets.
     *
     * @return array{\Closure(NatsClient): Future<void>, string, list<string>}
     */
    private static function failedPublish(string $kind): array
    {
        return match ($kind) {
            'publish' => [
                static fn(NatsClient $client): Future => $client->publish('orders', 'x'),
                'PUB orders',
                ['PUB orders 1'],
            ],
            'publishWithHeaders' => [
                static fn(NatsClient $client): Future => $client->publishWithHeaders('orders', 'x', ['K' => 'v']),
                'HPUB orders',
                ['HPUB orders 17 18'],
            ],
            'ack' => [
                static fn(NatsClient $client): Future => $client->jetStream()->ack(new NatsMessage('orders', 1, '$JS.ACK.ORDERS.worker.1.1.1.0.0', 'x')),
                'PUB $JS.ACK',
                ['PUB $JS.ACK.ORDERS.worker.1.1.1.0.0 4'],
            ],
            default => [
                static fn(NatsClient $client): Future => $client->publishHeaderBlock([
                    ['subject' => 'orders.1', 'payload' => 'a', 'headers' => ['K' => 'v']],
                    ['subject' => 'orders.2', 'payload' => 'b', 'headers' => ['K' => 'v']],
                ]),
                'HPUB orders.1',
                ['HPUB orders.1 17 18', 'HPUB orders.2 17 18'],
            ],
        };
    }

    /**
     * The PUB and HPUB control lines the client wrote, optionally only on one connection.
     *
     * @return list<string>
     */
    private static function publishLines(ReconnectingTransport $transport, ?int $epoch = null): array
    {
        return array_values(array_filter(
            $transport->controlLines($epoch),
            static fn(string $line): bool => str_starts_with($line, 'PUB ') || str_starts_with($line, 'HPUB '),
        ));
    }

    /**
     * What $future failed with, null when it resolved.
     *
     * @param Future<mixed> $future
     */
    private static function failureOf(Future $future): ?\Throwable
    {
        try {
            $future->await(new TimeoutCancellation(self::WATCHDOG));
        } catch (\Throwable $failure) {
            return $failure;
        }

        return null;
    }

    private function holdTheReconnectDial(ReconnectingTransport $transport): void
    {
        $transport->holdNextDial();
        $this->resources()->onRelease($transport->releaseDial(...), 'the held reconnect dial');
    }

    private function client(
        ReconnectingTransport $transport,
        bool $wait = true,
        int $bufferSize = 8_388_608,
        ?\Closure $listener = null,
        ?\Closure $errorListener = null,
        int $maxReconnectAttempts = 1_000,
        ?LoggerInterface $logger = null,
    ): NatsClient {
        $client = $this->own(new NatsClient(self::options($wait, $bufferSize, $listener, $errorListener, $maxReconnectAttempts, $logger), $transport));
        $client->connect()->await();

        return $client;
    }

    private function connection(HeldFailingWriteTransport $transport, bool $wait = true): NatsConnection
    {
        $connection = $this->own(new NatsConnection(self::options($wait), $transport));
        $connection->connect()->await();

        return $connection;
    }

    private static function options(
        bool $wait = true,
        int $bufferSize = 8_388_608,
        ?\Closure $listener = null,
        ?\Closure $errorListener = null,
        int $maxReconnectAttempts = 1_000,
        ?LoggerInterface $logger = null,
    ): NatsOptions {
        return new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: 2_000,
            reconnectEnabled: true,
            maxReconnectAttempts: $maxReconnectAttempts,
            reconnectDelayMs: 1,
            reconnectMaxDelayMs: 2,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            reconnectBufferSize: $bufferSize,
            connectionListener: $listener,
            errorListener: $errorListener,
            waitForReconnect: $wait,
            logger: $logger,
        );
    }

    private function waitUntil(\Closure $condition): void
    {
        $deadline = hrtime(true) + self::WATCHDOG * 1_000_000_000;
        while (!$condition()) {
            if (hrtime(true) > $deadline) {
                self::fail('condition not reached within ' . self::WATCHDOG . ' s');
            }

            delay(0.001);
        }
    }

    private function waitUntilOpen(NatsClient $client): void
    {
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open);
    }
}
