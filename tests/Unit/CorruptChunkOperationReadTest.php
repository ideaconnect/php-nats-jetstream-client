<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;

/**
 * The read of one of the library's operations that gets a chunk which fails to parse recovers the connection the way a
 * read that met a lost socket does (#195, #178): the recovery runs in a fiber of its own, and the operation waits for
 * it only within its own deadline and wake-up, or fails at once with waiting disabled. Each case holds the recovery's
 * first dial until the operation has returned, so the order of events decides: the operation ends while the
 * connection is still Connecting. Up to 2.26.0 such a read ran the recovery inline and returned only once the dial was
 * let through and the connection was Open again. Your own read still runs it inline.
 */
final class CorruptChunkOperationReadTest extends TestCase
{
    use ReconnectScenarios;

    private const BOGUS = "BOGUS\r\n";
    private const LAME_DUCK = 'INFO {"server_id":"S1","ldm":true,"connect_urls":["127.0.0.1:4223"]}' . "\r\n";

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /**
     * A request whose reply is a corrupt chunk times out at its own deadline, with the recovery's dial still held: it
     * used to wait for the whole reconnect, past its deadline. The parse error is reported, and once the dial is let
     * through the connection reconnects once.
     */
    public function testARequestWhoseReadGetsACorruptChunkTimesOutAtItsDeadline(): void
    {
        $transport = new ReconnectingTransport();
        $errors = new Issue195Errors();
        $client = $this->client($transport, true, $errors);
        $this->answerEcho($client, $transport, [self::BOGUS]);
        $this->holdTheRecoveryDial($transport);

        [$reply, $error] = self::outcome($client->request('svc.echo', 'x', 300));

        self::assertNull($reply);
        self::assertInstanceOf(TimeoutException::class, $error);
        self::assertSame('Request timed out for subject svc.echo', $error->getMessage());
        self::assertSame(ConnectionState::Connecting, $client->state(), 'the request ended with the recovery\'s dial still held');
        self::assertCount(2, $transport->connectCalls);
        self::assertSame(['Unsupported control frame: BOGUS'], $errors->messages);

        $this->letTheRecoveryThrough($client, $transport);
        self::assertSame(1, $client->statistics()->reconnects);
    }

    /**
     * With waiting for a reconnect disabled the request fails at once with "Connection is not open", the parse error
     * as its previous, as one whose read met a lost socket does; the recovery carries on in its own fiber. It used to
     * wait for the whole reconnect and then time out.
     */
    public function testWithWaitingDisabledTheRequestFailsAtOnce(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, false);
        $this->answerEcho($client, $transport, [self::BOGUS]);
        $this->holdTheRecoveryDial($transport);

        [$reply, $error] = self::outcome($client->request('svc.echo', 'x', 30_000));

        self::assertNull($reply);
        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertSame('Connection is not open', $error->getMessage());
        self::assertInstanceOf(ProtocolException::class, $error->getPrevious());
        self::assertSame('Unsupported control frame: BOGUS', $error->getPrevious()->getMessage());
        self::assertSame(ConnectionState::Connecting, $client->state());

        $this->letTheRecoveryThrough($client, $transport);
    }

    /** @return iterable<string, array{bool}> */
    public static function waiting(): iterable
    {
        yield 'waiting enabled' => [true];
        yield 'waiting disabled' => [false];
    }

    /**
     * The reply comes in the same chunk as the bad bytes, ahead of them: it is delivered first, and the request returns
     * it at once, with the recovery's dial still held. It used to return it only once the reconnect was over.
     */
    #[DataProvider('waiting')]
    public function testAReplyAheadOfTheBadBytesIsReturnedAtOnce(bool $waitForReconnect): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->client($transport, $waitForReconnect);
        $this->answerEcho($client, $transport, null);
        $this->holdTheRecoveryDial($transport);

        [$reply, $error] = self::outcome($client->request('svc.echo', 'x', 30_000));

        self::assertNull($error, $error === null ? '' : $error::class . ': ' . $error->getMessage());
        self::assertSame('reply-1', $reply?->payload);
        self::assertSame(ConnectionState::Connecting, $client->state());

        $this->letTheRecoveryThrough($client, $transport);
    }

    /** @return iterable<string, array{string, ?string}> */
    public static function polls(): iterable
    {
        yield 'a corrupt chunk' => [self::BOGUS, null];
        yield 'its message ahead of the bad bytes' => ['MESSAGE', 'job'];
    }

    /**
     * An own-sid operation, a SubscriptionQueue poll, gets the chunk: with nothing for it, it returns nothing at its
     * deadline; with its message ahead of the bad bytes, it returns the message. Either way the recovery's dial is
     * still held.
     */
    #[DataProvider('polls')]
    public function testAPollWhoseReadGetsACorruptChunkKeepsToItsWait(string $chunk, ?string $expected): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->client($watched, true);
        $queue = $client->subscribeQueue('jobs')->await();
        $this->holdTheRecoveryDial($transport);

        $poll = async(static fn(): ?NatsMessage => $queue->setTimeout($expected === null ? 0.3 : 30.0)->next());
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->pushFrame(($chunk === 'MESSAGE' ? ReconnectingTransport::msgFrame('jobs', $queue->sid, 'job') : '') . self::BOGUS);
        [$message, $error] = self::outcome($poll);

        self::assertNull($error, $error === null ? '' : $error::class . ': ' . $error->getMessage());
        self::assertSame($expected, $message?->payload);
        self::assertSame(ConnectionState::Connecting, $client->state());

        $this->letTheRecoveryThrough($client, $transport);
    }

    /**
     * A lame-duck INFO and bad bytes behind it in one chunk: the INFO, dispatched ahead of the bad bytes, fails over in
     * a fiber of its own, as from a chunk that parses (#191), and the parse error's recovery joins that failover within
     * the poll's wait. The poll returns at its deadline, the failover's dial to the server the INFO names still held.
     * The read used to run the failover inline, past the poll's deadline.
     */
    public function testALameDuckInfoAheadOfTheBadBytesFailsOverInAFiberOfItsOwn(): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->client($watched, true);
        $queue = $client->subscribeQueue('jobs')->await();
        $this->holdTheRecoveryDial($transport);

        $poll = async(static fn(): ?NatsMessage => $queue->setTimeout(0.3)->next());
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->pushFrame(self::LAME_DUCK . self::BOGUS);
        [$message, $error] = self::outcome($poll);

        self::assertNull($error, $error === null ? '' : $error::class . ': ' . $error->getMessage());
        self::assertNull($message);
        self::assertSame(ConnectionState::Connecting, $client->state());
        self::assertCount(2, $transport->connectCalls, 'one failover, the parse error\'s recovery joined it');
        self::assertStringContainsString('4223', $transport->connectCalls[1], 'the failover dials the server the INFO names');

        $this->letTheRecoveryThrough($client, $transport);
        self::assertSame(1, $client->statistics()->reconnects);
    }

    /**
     * Guard: your own processIncoming() that gets a corrupt chunk still runs the recovery inline and waits for all of
     * it, also with waiting for a reconnect disabled, which concerns the library's operations only: it is still
     * pending, past a checkpoint, while the dial is held, and returns once the connection is back.
     */
    #[DataProvider('waiting')]
    public function testAnApplicationReadStillWaitsForTheWholeRecovery(bool $waitForReconnect): void
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        $client = $this->client($watched, $waitForReconnect);
        $this->holdTheRecoveryDial($transport);

        $read = $this->resources()->ownOperation($client->processIncoming(), label: 'the application read');
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $transport->pushFrame(self::BOGUS);
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
        $checkpoint = new DeferredFuture();
        $this->resources()->ownCallback(EventLoop::defer(static fn() => $checkpoint->complete()), 'the checkpoint');
        $checkpoint->getFuture()->await(new TimeoutCancellation(3));
        self::assertFalse($read->isComplete(), 'the application read waits for the held dial');

        $transport->releaseDial();
        $read->await(new TimeoutCancellation(3));
        self::assertSame(ConnectionState::Open, $client->state());
        self::assertSame(1, $client->statistics()->reconnects);
    }


    private function client(ReconnectingTransport|WatchedTransport $transport, bool $waitForReconnect, ?Issue195Errors $errors = null): NatsClient
    {
        $client = $this->own(new NatsClient($this->options(
            $waitForReconnect,
            5_000,
            1_000,
            0,
            2,
            null,
            5,
            20,
            $errors === null ? null : static function (\Throwable $error) use ($errors): void {
                $errors->messages[] = $error->getMessage();
            },
        ), $transport));
        $client->connect()->await(new TimeoutCancellation(3));

        return $client;
    }

    /**
     * Sets up the request inbox with a first request, so that the request under test only waits for its reply, then
     * answers `svc.echo` with $answer, or, given null, with the reply `reply-1` and the bad bytes behind it in one chunk.
     *
     * @param list<string>|null $answer
     */
    private function answerEcho(NatsClient $client, ReconnectingTransport $transport, ?array $answer): void
    {
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport, $answer): array {
            if ($replyTo === null) {
                return [];
            }
            if ($subject === 'svc.warm') {
                return $transport->replyFrame($replyTo, 'warm');
            }

            return $answer ?? [$transport->replyFrame($replyTo, 'reply-1')[0] . self::BOGUS];
        };
        self::assertSame('warm', $client->request('svc.warm', '', 3_000)->await(new TimeoutCancellation(3))->payload);
    }

    /** Holds the recovery's first dial until the test lets it through, after the operation has returned. */
    private function holdTheRecoveryDial(ReconnectingTransport $transport): void
    {
        $transport->holdNextDial();
        $this->resources()->onRelease($transport->releaseDial(...), 'the held recovery dial');
    }

    /**
     * Lets the held dial through once the recovery has made it - with waiting disabled the operation can fail before
     * the recovery's fiber dials - and waits for the connection to be back.
     */
    private function letTheRecoveryThrough(NatsClient $client, ReconnectingTransport $transport): void
    {
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
        $transport->releaseDial();
        $this->waitUntilOpen($client);
    }

    /**
     * @param Future<mixed> $operation
     * @return array{mixed, ?\Throwable}
     */
    private static function outcome(Future $operation): array
    {
        try {
            // A watchdog for a broken test only: the operation must end while the dial is held.
            return [$operation->await(new TimeoutCancellation(3)), null];
        } catch (\Throwable $error) {
            return [null, $error];
        }
    }
}

/** The error listener's log of {@see CorruptChunkOperationReadTest}. */
final class Issue195Errors
{
    /** @var list<string> */
    public array $messages = [];
}
