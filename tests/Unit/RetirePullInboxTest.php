<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredCancellation;
use Amp\NullCancellation;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\Enum\InboxRetirement;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Tests\Support\HeldDrainParticipant;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

/**
 * The connection-owned release of a pull consumer run's inbox, NatsClient::retirePullInbox() (#212), state by state:
 * what it writes, what it routes to the inbox's router before the inbox goes, and how it reports the way it ended
 * ({@see InboxRetirement}), so that a completed fence is never mistaken for a best-effort release. The pull consumer
 * engine calls it before a failing run's hand-over (PullConsumerFailedRunInboxReleaseTest); here it is driven directly,
 * over the scripted server in tests/Support/ReconnectingTransport.php, on a guarded, unbounded inbox as the engine
 * subscribes it, whose router only records what it gets.
 */
final class RetirePullInboxTest extends TestCase
{
    use ReconnectScenarios;

    private const INBOX = '_INBOX.JS.PULL.TEST.*';

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /**
     * Open: UNSUB and PING go out in one write, what the server sent the inbox before the UNSUB, in a chunk of its own
     * ahead of the PONG (held back 50 ms), reaches the router, and the operation ends Fenced, the inbox no longer
     * registered. A plain unsubscribe afterwards writes nothing more.
     */
    public function testOnAnOpenConnectionItFencesWhatTheServerSentBeforeTheUnsub(): void
    {
        [$transport, $client] = $this->client();
        [$sid, $routed] = $this->inbox($client);
        $transport->pongDelay = 0.05;
        $transport->afterWrite = static function (string $bytes) use ($transport, $sid): void {
            if (str_starts_with($bytes, 'UNSUB ' . $sid . "\r\n")) {
                $transport->pushFrame(ReconnectingTransport::msgFrame('evt.s', $sid, 'late'));
            }
        };

        $outcome = $client->retirePullInbox($sid, new NullCancellation())->await(new TimeoutCancellation(5));

        self::assertSame(InboxRetirement::Fenced, $outcome);
        self::assertSame(['late'], $routed->getArrayCopy());
        self::assertFalse($client->isSubscriptionActive($sid));
        self::assertSame(1, self::writesOf($transport, 'UNSUB ' . $sid . "\r\nPING\r\n"));
        $client->unsubscribe($sid)->await();
        self::assertSame(['UNSUB ' . $sid], $transport->controlLinesStartingWith('UNSUB '));
    }

    /** A sid with no registration and no UNSUB owed for it: Gone, nothing written. */
    public function testAnUnknownSidIsGone(): void
    {
        [$transport, $client] = $this->client();

        self::assertSame(InboxRetirement::Gone, $client->retirePullInbox(999, new NullCancellation())->await());
        self::assertSame([], $transport->controlLinesStartingWith('UNSUB '));
    }

    /**
     * A stop already made when the release starts ends the collection before it begins: what the inbox's queue holds is
     * not routed, the release goes out as a bare UNSUB without a PING, and the operation ends Released.
     */
    public function testAStopAlreadyMadeReleasesWithABareUnsubAndRoutesNothing(): void
    {
        [$transport, $client] = $this->client();
        [$sid, $routed] = $this->inbox($client);
        $stop = new DeferredCancellation();
        $stop->cancel();

        $outcome = $client->retirePullInbox($sid, $stop->getCancellation())->await(new TimeoutCancellation(5));
        $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith('UNSUB ') !== []);

        self::assertSame(InboxRetirement::Released, $outcome);
        self::assertSame([], $routed->getArrayCopy());
        self::assertFalse($client->isSubscriptionActive($sid));
        self::assertSame(['UNSUB ' . $sid], $transport->controlLinesStartingWith('UNSUB '));
        self::assertSame(0, self::writesOf($transport, 'UNSUB ' . $sid . "\r\nPING\r\n"), 'no PING behind a bare UNSUB');
    }

    /**
     * A server that never answers the PING ends the release at the request timeout (200 ms): Released, the timeout
     * reported to the error listener as a TimeoutException, the inbox gone all the same.
     */
    public function testAPongThatNeverComesEndsItReleasedAtTheRequestTimeout(): void
    {
        /** @var \ArrayObject<int, \Throwable> $reported */
        $reported = new \ArrayObject();
        [$transport, $client] = $this->client(200, static function (\Throwable $error) use ($reported): void {
            $reported[] = $error;
        });
        [$sid] = $this->inbox($client);
        $transport->answerPings = false;

        $started = hrtime(true);
        $outcome = $client->retirePullInbox($sid, new NullCancellation())->await(new TimeoutCancellation(5));
        $elapsed = $this->secondsSince($started);
        $this->waitUntil(static fn(): bool => $reported->count() > 0);

        self::assertSame(InboxRetirement::Released, $outcome);
        self::assertLessThan(1.5, $elapsed);
        self::assertInstanceOf(TimeoutException::class, $reported[0]);
        self::assertStringContainsString('Releasing the pull consumer inbox (sid ' . $sid . ')', $reported[0]->getMessage());
        self::assertFalse($client->isSubscriptionActive($sid));
    }

    /**
     * A reconnect under way: the inbox leaves the replay at once, what its queue holds is routed, and the operation
     * ends Released without waiting for the reconnect; once the reconnect is done the new session holds no SUB for it.
     */
    public function testWhileAReconnectIsUnderWayItTakesTheInboxOutOfTheReplay(): void
    {
        [$transport, $client] = $this->client();
        [$sid] = $this->inbox($client);
        $reader = $this->startRecoveryHeldMidDial($client, $transport);
        self::assertSame(ConnectionState::Connecting, $client->state());

        $outcome = $client->retirePullInbox($sid, new NullCancellation())->await(new TimeoutCancellation(1));
        $transport->releaseDial();
        $reader->await(new TimeoutCancellation(5));
        $this->waitUntilOpen($client);

        self::assertSame(InboxRetirement::Released, $outcome);
        self::assertFalse($client->isSubscriptionActive($sid));
        self::assertSame([], array_values(array_filter(
            $transport->controlLinesStartingWith('SUB ', 1),
            static fn(string $line): bool => str_starts_with($line, 'SUB ' . self::INBOX),
        )));
    }

    /**
     * A connection closed for good (reconnect off) released every subscription with its runtime state: the operation
     * finds nothing registered and nothing owed, ends Gone, and writes nothing.
     */
    public function testOnAConnectionClosedForGoodThereIsNothingLeftToRelease(): void
    {
        [$transport, $client] = $this->client(2_000, null, false);
        [$sid] = $this->inbox($client);
        $transport->dropConnection();
        $this->waitUntil(static function () use ($client): bool {
            try {
                $client->processIncoming(new TimeoutCancellation(0.05))->await();
            } catch (\Throwable) {
                // The EOF, with reconnect off.
            }

            return $client->state() === ConnectionState::Closed;
        });

        self::assertSame(InboxRetirement::Gone, $client->retirePullInbox($sid, new NullCancellation())->await());
        self::assertFalse($client->isSubscriptionActive($sid));
        self::assertSame([], $transport->controlLinesStartingWith('UNSUB '));
    }

    /**
     * While the client drains, the drain owns the UNSUB and the flush: the operation writes nothing of its own and waits
     * for the drain's request for the hand-over ($drainReady), then ends DrainFlushed, the inbox gone. A drain
     * participant keeps the drain from closing meanwhile.
     */
    public function testWhileTheClientDrainsItWaitsForTheDrainsRequestForTheHandOver(): void
    {
        [$transport, $client] = $this->client();
        [$sid] = $this->inbox($client);
        $participant = new HeldDrainParticipant();
        $client->addDrainParticipant($participant);
        $drain = $client->drain();
        $this->waitUntil(static fn(): bool => $participant->asked === 1);
        self::assertSame(ConnectionState::Draining, $client->state());
        $ready = new DeferredCancellation();
        EventLoop::delay(0.05, static function () use ($ready): void {
            $ready->cancel();
        });

        $outcome = $client->retirePullInbox($sid, new NullCancellation(), $ready->getCancellation())->await(new TimeoutCancellation(1));
        $participant->release();
        $drain->await(new TimeoutCancellation(5));

        self::assertSame(InboxRetirement::DrainFlushed, $outcome);
        self::assertSame(['UNSUB ' . $sid], $transport->controlLinesStartingWith('UNSUB '), 'only the drain\'s UNSUB');
    }

    /**
     * A guarded inbox the client treated as rejected for the subscription limit: its state went with the -ERR, and its
     * UNSUB is owed. The release writes that UNSUB, once, and ends Released; the plain unsubscribe after it writes
     * nothing more.
     */
    public function testAnInboxRejectedForTheSubscriptionLimitGetsItsOwedUnsubOnce(): void
    {
        [$transport, $client] = $this->client();
        $transport->answerPings = false;
        [$sid] = $this->inbox($client);
        $transport->pushFrame("-ERR 'maximum subscriptions exceeded'\r\n");
        try {
            $client->processIncoming(new TimeoutCancellation(1))->await();
        } catch (\Throwable) {
            // The -ERR the server keeps the connection open for.
        }
        $transport->answerPings = true;
        self::assertFalse($client->isSubscriptionActive($sid));

        $outcome = $client->retirePullInbox($sid, new NullCancellation())->await(new TimeoutCancellation(5));
        $client->unsubscribe($sid)->await();

        self::assertSame(InboxRetirement::Released, $outcome);
        self::assertSame(['UNSUB ' . $sid], $transport->controlLinesStartingWith('UNSUB '));
    }

    /**
     * A connection ended by a fatal -ERR while the release reads for its PONG: the PONG dies with the socket, the
     * operation ends Released, and the reconnect does not subscribe the inbox again.
     */
    public function testAConnectionEndingUnderTheFenceEndsItReleasedWithoutAReplay(): void
    {
        [$transport, $client] = $this->client();
        [$sid] = $this->inbox($client);
        $transport->answerPings = false;
        $transport->afterWrite = static function (string $bytes) use ($transport, $sid): void {
            if (str_starts_with($bytes, 'UNSUB ' . $sid . "\r\n")) {
                $transport->answerPings = true;
                $transport->pushFrame("-ERR 'Stale Connection'\r\n");
            }
        };

        $outcome = $client->retirePullInbox($sid, new NullCancellation())->await(new TimeoutCancellation(5));
        $this->waitUntilOpen($client);

        self::assertSame(InboxRetirement::Released, $outcome);
        self::assertSame(1, $transport->epoch());
        self::assertFalse($client->isSubscriptionActive($sid));
        self::assertSame([], array_values(array_filter(
            $transport->controlLinesStartingWith('SUB ', 1),
            static fn(string $line): bool => str_starts_with($line, 'SUB ' . self::INBOX),
        )));
    }

    /**
     * A drain whose request for the hand-over was made already when the release starts: the operation does not wait,
     * and ends DrainFlushed at once, the inbox gone.
     */
    public function testADrainThatAskedForTheHandOverAlreadyEndsItDrainFlushedAtOnce(): void
    {
        [, $client] = $this->client();
        [$sid] = $this->inbox($client);
        $participant = new HeldDrainParticipant();
        $client->addDrainParticipant($participant);
        $drain = $client->drain();
        $this->waitUntil(static fn(): bool => $participant->asked === 1);
        $ready = new DeferredCancellation();
        $ready->cancel();

        $started = hrtime(true);
        $outcome = $client->retirePullInbox($sid, new NullCancellation(), $ready->getCancellation())->await(new TimeoutCancellation(1));
        $elapsed = $this->secondsSince($started);
        $participant->release();
        $drain->await(new TimeoutCancellation(5));

        self::assertSame(InboxRetirement::DrainFlushed, $outcome);
        self::assertLessThan(0.1, $elapsed);
        self::assertFalse($client->isSubscriptionActive($sid));
    }

    /**
     * A drain that never asks for the hand-over within the request timeout (200 ms): the operation stops waiting at its
     * budget and ends Released, not DrainFlushed, the inbox gone all the same.
     */
    public function testADrainThatNeverAsksForTheHandOverEndsItReleasedAtTheRequestTimeout(): void
    {
        [, $client] = $this->client(200);
        [$sid] = $this->inbox($client);
        $participant = new HeldDrainParticipant();
        $client->addDrainParticipant($participant);
        $drain = $client->drain();
        $this->waitUntil(static fn(): bool => $participant->asked === 1);
        $never = new DeferredCancellation();

        $started = hrtime(true);
        $outcome = $client->retirePullInbox($sid, new NullCancellation(), $never->getCancellation())->await(new TimeoutCancellation(2));
        $elapsed = $this->secondsSince($started);
        $participant->release();
        try {
            $drain->await(new TimeoutCancellation(5));
        } catch (\Throwable) {
            // The drain's own budget may have run out meanwhile; it closes the connection all the same.
        }

        self::assertSame(InboxRetirement::Released, $outcome);
        self::assertGreaterThanOrEqual(0.15, $elapsed);
        self::assertLessThan(1.5, $elapsed);
        self::assertFalse($client->isSubscriptionActive($sid));
    }

    /**
     * A rejected inbox whose UNSUB is owed, released with a stop already made: the operation does not wait for the
     * write, ends Released, and the owed UNSUB still goes out, once.
     */
    public function testAnOwedUnsubStillGoesOutWhenTheStopWasMadeAlready(): void
    {
        [$transport, $client] = $this->client();
        $transport->answerPings = false;
        [$sid] = $this->inbox($client);
        $transport->pushFrame("-ERR 'maximum subscriptions exceeded'\r\n");
        self::readTheErr($client);
        $transport->answerPings = true;
        $stop = new DeferredCancellation();
        $stop->cancel();

        $outcome = $client->retirePullInbox($sid, $stop->getCancellation())->await(new TimeoutCancellation(5));
        $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith('UNSUB ') !== []);

        self::assertSame(InboxRetirement::Released, $outcome);
        self::assertSame(['UNSUB ' . $sid], $transport->controlLinesStartingWith('UNSUB '));
    }

    /**
     * The write of UNSUB and PING fails on a socket that died: no PONG can come, the operation ends Released at once
     * rather than at its budget, the write's failure is reported to the error listener, and the inbox is gone.
     */
    public function testAReleaseWhoseWriteFailsEndsItReleasedAndReportsTheFailure(): void
    {
        /** @var \ArrayObject<int, \Throwable> $reported */
        $reported = new \ArrayObject();
        [$transport, $client] = $this->client(5_000, static function (\Throwable $error) use ($reported): void {
            $reported[] = $error;
        });
        [$sid] = $this->inbox($client);
        $failure = new \RuntimeException('the socket died under the release');
        $transport->failNextWriteContaining('UNSUB ' . $sid . "\r\nPING\r\n", $failure);

        $started = hrtime(true);
        $outcome = $client->retirePullInbox($sid, new NullCancellation())->await(new TimeoutCancellation(5));
        $elapsed = $this->secondsSince($started);
        $this->waitUntil(static fn(): bool => in_array($failure, $reported->getArrayCopy(), true));

        self::assertSame(InboxRetirement::Released, $outcome);
        self::assertLessThan(1.0, $elapsed);
        self::assertFalse($client->isSubscriptionActive($sid));
    }

    /**
     * @param (\Closure(\Throwable): void)|null $errorListener
     * @return array{ReconnectingTransport, NatsClient}
     */
    private function client(int $requestTimeoutMs = 2_000, ?\Closure $errorListener = null, bool $reconnect = true): array
    {
        $transport = new ReconnectingTransport();
        $client = new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: $requestTimeoutMs,
            reconnectEnabled: $reconnect,
            reconnectDelayMs: 5,
            reconnectMaxDelayMs: 20,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            errorListener: $errorListener,
        ), $transport);
        $this->opened[] = $client;
        $client->connect()->await();

        return [$transport, $client];
    }

    /**
     * A guarded, unbounded inbox as the pull consumer engine subscribes it, whose router records the payloads it gets.
     *
     * @return array{int, \ArrayObject<int, string>}
     */
    private function inbox(NatsClient $client): array
    {
        /** @var \ArrayObject<int, string> $routed */
        $routed = new \ArrayObject();
        $sid = $client->subscribeGuarded(self::INBOX, static function (NatsMessage $message) use ($routed): void {
            $routed[] = $message->payload;
        }, static function (): void {})->await();
        $client->markSubscriptionUnbounded($sid);

        return [$sid, $routed];
    }

    /** Reads the -ERR pushed last, which the server keeps the connection open for. */
    private static function readTheErr(NatsClient $client): void
    {
        try {
            $client->processIncoming(new TimeoutCancellation(1))->await();
        } catch (\Throwable) {
            // Your own read throws such an -ERR.
        }
    }

    /** How many writes the client made whose bytes are exactly $bytes. */
    private static function writesOf(ReconnectingTransport $transport, string $bytes): int
    {
        return count(array_filter($transport->writes, static fn(array $write): bool => $write['bytes'] === $bytes));
    }
}
