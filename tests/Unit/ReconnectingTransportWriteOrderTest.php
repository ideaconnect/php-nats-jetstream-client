<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Transport\TransportClosedException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\delay;

/**
 * The write order of the scripted reconnect server, `tests/Support/ReconnectingTransport.php` (#192). Bytes reach a
 * server in the order they were written, and a write held up before the server has it - a socket under backpressure -
 * holds the later ones behind it; the production transports hand their bytes over in the caller's fiber, so wire order
 * is call order. The double used to run each write on its own and hold up only the one matching a stall, so a write
 * made meanwhile overtook it: a heartbeat PING overtaking a held-up SUB + PING got the PONG, which the connection, tying
 * PONGs to PINGs by order, took for the held-up one's, and confirmed an inbox whose SUB the server had not received.
 *
 * Covered here: the order behind a stall, a successor's stall starting only in its turn, the end of a session (drop,
 * close, a dial replacing it) failing the writes waiting their turn without waiting for a head that outlives its
 * session, a failing head, a write made just before the session ended, a replacement dial that is refused or cancelled
 * leaving the session and its queue alone, and a writer's completion held after acceptance - by the double or by an
 * afterWrite callback - holding no other write. The last case shows the connection-level consequence with the
 * heartbeat answered. With the write order taken out of the double, every case fails except the ones marked as
 * guards: they check that the order holds no more than it should.
 */
final class ReconnectingTransportWriteOrderTest extends TestCase
{
    use OwnsTestResources;

    private const PULL_SUBJECT = '$JS.API.CONSUMER.MSG.NEXT.S.C';

    /** @var list<ReconnectingTransport> */
    private array $transports = [];

    protected function tearDown(): void
    {
        // #183: the clients first; then what a bare transport still holds.
        $this->releaseOwnedResources();

        foreach ($this->transports as $transport) {
            $transport->releaseStalledWrites();
            $transport->releaseHeldCompletions();
            $transport->dropConnection();
        }
        $this->transports = [];
    }

    /**
     * A compound SUB + PING held up before the session has it, then a PING and a PUB: neither overtakes it. Once it
     * is let through, all three reach the session in the order they were made.
     */
    public function testWritesMadeBehindAStalledOneWaitAndThenKeepTheirOrder(): void
    {
        $transport = $this->session();
        $transport->stallNextWriteContaining('SUB a', 5.0);

        $first = self::write($transport, "SUB a 1\r\nPING\r\n");
        $second = self::write($transport, "PING\r\n");
        $third = self::write($transport, "PUB b 1\r\nx\r\n");
        $this->waitUntil(static fn(): bool => $transport->writesStalled() === 1);
        delay(0.02);

        self::assertSame(['CONNECT {}', 'PING'], $transport->controlLines(), 'nothing overtook the stalled write');
        self::assertFalse($second->isComplete());
        self::assertFalse($third->isComplete());

        $transport->releaseStalledWrites();
        self::assertSame([null, null, null], self::settleAll($first, $second, $third));
        self::assertSame(['CONNECT {}', 'PING', 'SUB a 1', 'PING', 'PING', 'PUB b 1'], $transport->controlLines());
    }

    /**
     * Two writes, each with a stall of its own, and a third behind them: only the first stall runs. Releasing it lets
     * the first write through, and the second then starts its stall, which holds the third; releaseStalledWrites()
     * ends the stalls running, not the ones still to come.
     */
    public function testASuccessorsStallStartsOnlyInItsTurn(): void
    {
        $transport = $this->session();
        $transport->stallNextWriteContaining('PUB a', 5.0);
        $transport->stallNextWriteContaining('PUB b', 5.0);

        $a = self::write($transport, "PUB a 1\r\nx\r\n");
        $b = self::write($transport, "PUB b 1\r\nx\r\n");
        $c = self::write($transport, "PUB c 1\r\nx\r\n");
        $this->waitUntil(static fn(): bool => $transport->writesStalled() === 1);
        delay(0.02);
        self::assertSame(1, $transport->writesStalled(), "the second write's stall has not started");

        $transport->releaseStalledWrites();
        $this->waitUntil(static fn(): bool => $a->isComplete() && $transport->writesStalled() === 1);
        self::assertSame(['PUB a 1'], $transport->controlLinesStartingWith('PUB'));
        delay(0.02);
        self::assertFalse($c->isComplete(), 'the third write waits behind the second, now stalled');

        $transport->releaseStalledWrites();
        self::assertSame([null, null, null], self::settleAll($a, $b, $c));
        self::assertSame(['PUB a 1', 'PUB b 1', 'PUB c 1'], $transport->controlLinesStartingWith('PUB'));
    }

    /** @return iterable<string, array{string}> */
    public static function sessionEnds(): iterable
    {
        yield 'dropConnection()' => ['drop'];
        yield 'close()' => ['close'];
    }

    /**
     * The session ends while a write is stalled and two wait behind it: all three fail with the closed connection,
     * none reaches the session, and the next session's handshake goes through at once.
     */
    #[DataProvider('sessionEnds')]
    public function testTheWritesWaitingTheirTurnFailWithTheirSession(string $end): void
    {
        $transport = $this->session();
        $transport->stallNextWriteContaining('PUB a', 10.0);
        $writes = [
            self::write($transport, "PUB a 1\r\nx\r\n"),
            self::write($transport, "PUB b 1\r\nx\r\n"),
            self::write($transport, "PING\r\n"),
        ];
        $this->waitUntil(static fn(): bool => $transport->writesStalled() === 1);

        if ($end === 'drop') {
            $transport->dropConnection();
        } else {
            $transport->close()->await(new TimeoutCancellation(1));
        }

        foreach (self::settleAll(...$writes) as $error) {
            self::assertInstanceOf(TransportClosedException::class, $error);
        }
        self::assertSame(0, $transport->writesStalled());
        self::assertSame(['CONNECT {}', 'PING'], $transport->controlLines(), 'none of them reached the session');

        $this->handshake($transport);
        self::assertSame(1, $transport->epoch());
    }

    /**
     * The write at the head of the queue finds the socket dead and fails with the error the test gave; the two behind
     * it fail with the closed connection, and none of the three reaches the session.
     */
    public function testAFailingHeadKeepsItsErrorAndTheWritesBehindItFailWithTheSession(): void
    {
        $transport = $this->session();
        $transport->failNextWriteContaining('PUB a', new \RuntimeException('the raw stream error'));

        [$head, $second, $third] = self::settleAll(
            self::write($transport, "PUB a 1\r\nx\r\n"),
            self::write($transport, "PUB b 1\r\nx\r\n"),
            self::write($transport, "PUB c 1\r\nx\r\n"),
        );

        self::assertInstanceOf(\RuntimeException::class, $head);
        self::assertSame('the raw stream error', $head->getMessage());
        self::assertInstanceOf(TransportClosedException::class, $second);
        self::assertInstanceOf(TransportClosedException::class, $third);
        self::assertSame([], $transport->controlLinesStartingWith('PUB'));
    }

    /**
     * A write made on a live session, with nothing ahead of it, whose session ends before its turn comes: it fails and
     * does not reach the session. The double used to record it on the dead session and complete it.
     */
    public function testAWriteMadeJustBeforeTheSessionEndsFailsEvenWithNothingAheadOfIt(): void
    {
        $transport = $this->session();
        self::write($transport, "PUB a 1\r\nx\r\n")->await(new TimeoutCancellation(1));

        $late = self::write($transport, "PUB b 1\r\nx\r\n");
        $transport->dropConnection();

        self::assertInstanceOf(TransportClosedException::class, self::settleAll($late)[0]);
        self::assertSame(['PUB a 1'], $transport->controlLinesStartingWith('PUB'));
    }

    /** Guard: a write to a session already known to be gone fails at once, as before. */
    public function testAWriteOnASessionAlreadyGoneFailsAtOnce(): void
    {
        $transport = $this->session();
        $transport->dropConnection();

        $write = self::write($transport, "PUB a 1\r\nx\r\n");

        self::assertTrue($write->isComplete());
        self::assertInstanceOf(TransportClosedException::class, self::settleAll($write)[0]);
    }

    /**
     * A head whose stall outlives its session keeps its full time - the session's end does not release it - but the
     * write behind it fails at once, and the next session's handshake does not wait for it. Released later, the head
     * fails on its old session.
     */
    public function testAHeadOutlivingItsSessionHoldsNeitherItsFollowersNorTheNextSession(): void
    {
        $transport = $this->session();
        $transport->stallNextWriteContaining('PUB a', 10.0, outlivesSession: true);
        $head = self::write($transport, "PUB a 1\r\nx\r\n");
        $follower = self::write($transport, "PUB b 1\r\nx\r\n");
        $this->waitUntil(static fn(): bool => $transport->writesStalled() === 1);

        $transport->dropConnection();
        self::assertInstanceOf(TransportClosedException::class, self::settleAll($follower)[0]);
        self::assertSame(1, $transport->writesStalled(), 'the head still runs its course');
        self::assertFalse($head->isComplete());

        $this->handshake($transport);

        $transport->releaseStalledWrites();
        self::assertInstanceOf(TransportClosedException::class, self::settleAll($head)[0]);
        self::assertSame([], $transport->controlLinesStartingWith('PUB'));
    }

    /**
     * A dial that succeeds while a session is live replaces it without a drop first: the stalled write and the one
     * behind it fail by the time the new session is up, no stall is left, and the new session's writes go through.
     */
    public function testASuccessfulDialReplacingALiveSessionEndsItsPendingWrites(): void
    {
        $transport = $this->session();
        $transport->stallNextWriteContaining('PUB a', 10.0);
        $head = self::write($transport, "PUB a 1\r\nx\r\n");
        $follower = self::write($transport, "PUB b 1\r\nx\r\n");
        $this->waitUntil(static fn(): bool => $transport->writesStalled() === 1);

        $this->handshake($transport);

        self::assertTrue($head->isComplete() && $follower->isComplete(), 'both ended with their session');
        foreach (self::settleAll($head, $follower) as $error) {
            self::assertInstanceOf(TransportClosedException::class, $error);
        }
        self::assertSame(0, $transport->writesStalled());
        self::write($transport, "PUB c 1\r\nx\r\n")->await(new TimeoutCancellation(1));
        self::assertSame(['PUB c 1'], $transport->controlLinesStartingWith('PUB'));
    }

    /** @return iterable<string, array{string}> */
    public static function failedReplacements(): iterable
    {
        yield 'refused' => ['refused'];
        yield 'cancelled while held' => ['cancelled'];
    }

    /**
     * A replacement dial that fails - refused, or held and cancelled - leaves the live session and its queue alone:
     * once released, the stalled write and the one behind it reach that session, in order.
     */
    #[DataProvider('failedReplacements')]
    public function testAFailedReplacementDialLeavesTheLiveSessionAndItsQueueAlone(string $how): void
    {
        $transport = $this->session();
        $transport->stallNextWriteContaining('PUB a', 10.0);
        $head = self::write($transport, "PUB a 1\r\nx\r\n");
        $follower = self::write($transport, "PUB b 1\r\nx\r\n");
        $this->waitUntil(static fn(): bool => $transport->writesStalled() === 1);

        if ($how === 'refused') {
            $transport->refuseDials();
            $dialError = self::settleAll($transport->connect('nats://127.0.0.1:4222', 500))[0];
            $transport->acceptDials();
            self::assertInstanceOf(TransportClosedException::class, $dialError);
        } else {
            $transport->holdNextDial();
            $stop = new DeferredCancellation();
            $dial = $transport->connect('nats://127.0.0.1:4222', 500, $stop->getCancellation());
            delay(0.01);
            $stop->cancel();
            self::assertInstanceOf(CancelledException::class, self::settleAll($dial)[0]);
            self::assertSame(1, $transport->dialsCancelled);
        }

        self::assertTrue($transport->sessionLive());
        self::assertSame(0, $transport->epoch());
        self::assertSame(1, $transport->writesStalled());
        self::assertFalse($follower->isComplete());

        $transport->releaseStalledWrites();
        self::assertSame([null, null], self::settleAll($head, $follower));
        self::assertSame(['PUB a 1', 'PUB b 1'], $transport->controlLinesStartingWith('PUB', 0));
    }

    /**
     * A writer's completion held after the session accepted its bytes holds no other write: the write behind it
     * completes while it is held, and the held one completes once released. Unlike a stall, which holds the later
     * writes too. Guard: nothing held later writes before the change either.
     */
    public function testAHeldCompletionHoldsOnlyItsOwnWriter(): void
    {
        $transport = $this->session();
        $transport->holdCompletionOfNextWriteContaining('PUB a', 5.0);

        $held = self::write($transport, "PUB a 1\r\nx\r\n");
        $this->waitUntil(static fn(): bool => $transport->completionsHeld() === 1);
        self::write($transport, "PUB b 1\r\nx\r\n")->await(new TimeoutCancellation(1));

        self::assertSame(['PUB a 1', 'PUB b 1'], $transport->controlLinesStartingWith('PUB'));
        self::assertFalse($held->isComplete());

        $transport->releaseHeldCompletions();
        self::assertSame([null], self::settleAll($held));
        self::assertSame(0, $transport->completionsHeld());
    }

    /** Guard: a held completion ends with its session, successfully - the bytes had reached it. */
    public function testAHeldCompletionEndsWithItsSession(): void
    {
        $transport = $this->session();
        $transport->holdCompletionOfNextWriteContaining('PUB a', 5.0);
        $held = self::write($transport, "PUB a 1\r\nx\r\n");
        $this->waitUntil(static fn(): bool => $transport->completionsHeld() === 1);

        $transport->dropConnection();

        self::assertSame([null], self::settleAll($held));
        self::assertSame(0, $transport->completionsHeld());
    }

    /**
     * Guard: an afterWrite callback runs once its write has let the next one through, so one that suspends holds only
     * its own writer, and one that writes again and waits for that write does not wait for itself.
     */
    public function testAnAfterWriteCallbackThatSuspendsHoldsOnlyItsOwnWriterAndMayWriteAgain(): void
    {
        $transport = $this->session();
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        $transport->afterWrite = static function (string $bytes) use ($transport, $gate): void {
            if (str_starts_with($bytes, 'PUB a ')) {
                self::write($transport, "PUB nested 1\r\nx\r\n")->await(new TimeoutCancellation(1));
                $gate->getFuture()->await(new TimeoutCancellation(2));
            }
        };

        try {
            $held = self::write($transport, "PUB a 1\r\nx\r\n");
            self::write($transport, "PUB b 1\r\nx\r\n")->await(new TimeoutCancellation(1));
            $this->waitUntil(static fn(): bool => count($transport->controlLinesStartingWith('PUB')) === 3);
            self::assertFalse($held->isComplete());

            $gate->complete();
            self::assertSame([null], self::settleAll($held));
            self::assertSame(['PUB a 1', 'PUB b 1', 'PUB nested 1'], $transport->controlLinesStartingWith('PUB'));
        } finally {
            $transport->afterWrite = null;
            if (!$gate->isComplete()) {
                $gate->complete();
            }
        }
    }

    /**
     * The connection-level consequence, with the heartbeat's PINGs answered: a fetch's guarded inbox whose SUB + PING
     * is held up before the server has it stays unconfirmed however many times the heartbeat ticks, and its SUB has
     * not reached the server; let through, the PONG behind the SUB confirms it and the fetch gets its message. On the
     * double before the change, a heartbeat PING overtook the held-up write, and its PONG confirmed the inbox before the
     * server had its SUB.
     */
    public function testAHeartbeatNeverConfirmsAGuardedInboxWhoseSubIsStillHeldUp(): void
    {
        $transport = new ReconnectingTransport();
        $this->transports[] = $transport;
        $client = $this->own(new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            reconnectEnabled: false,
            pingIntervalSeconds: 0.05,
            // The heartbeat ticks through the hold-up without declaring the connection stale.
            maxPingsOut: 100,
        ), $transport));
        $client->connect()->await();
        $transport->responder = static fn(string $subject, ?string $replyTo): array => $subject === self::PULL_SUBJECT && $replyTo !== null
            ? [ReconnectingTransport::msgFrame('evt.s', (int) $transport->sidFor($replyTo), 'm1', '$JS.ACK.S.C.1.1.1.0.0')]
            : [];
        $pingsBefore = count($transport->controlLinesStartingWith('PING'));

        $transport->stallNextWriteContaining('SUB _INBOX.JS.FETCH', 5.0);
        $fetch = $client->jetStream()->fetchBatch('S', 'C', 1, 2_000);
        $fetch->ignore();
        $this->waitUntil(static fn(): bool => $transport->writesStalled() === 1);
        // Several heartbeat ticks.
        delay(0.2);

        self::assertCount(1, self::unconfirmedSids(self::connectionOf($client)), 'the inbox is still unconfirmed');
        self::assertSame([], $transport->controlLinesStartingWith('SUB _INBOX.JS.FETCH'), 'its SUB has not reached the server');
        self::assertCount($pingsBefore, $transport->controlLinesStartingWith('PING'), 'no PING went out past it');

        $transport->releaseStalledWrites();
        $messages = $fetch->await(new TimeoutCancellation(3));

        self::assertSame(['m1'], array_map(static fn(NatsMessage $message): string => $message->payload, $messages));
        self::assertSame([], self::unconfirmedSids(self::connectionOf($client)));
        self::assertSame(ConnectionState::Open, $client->state());
    }

    /** A transport with a live session whose handshake is done, closed at tearDown. */
    private function session(): ReconnectingTransport
    {
        $transport = new ReconnectingTransport();
        $this->transports[] = $transport;
        $this->handshake($transport);

        return $transport;
    }

    /**
     * A write, marked as handled: a case that fails before it awaits one leaves no unhandled failure for the next.
     *
     * @return Future<null>
     */
    private static function write(ReconnectingTransport $transport, string $bytes): Future
    {
        $write = $transport->write($bytes);
        $write->ignore();

        return $write;
    }

    /** Dials a new session and writes its handshake, which must complete at once. */
    private function handshake(ReconnectingTransport $transport): void
    {
        $transport->connect('nats://127.0.0.1:4222', 500)->await(new TimeoutCancellation(1));
        self::write($transport, "CONNECT {}\r\nPING\r\n")->await(new TimeoutCancellation(1));
    }

    /**
     * Awaits each write, within 2 s: what each threw, null for one that completed.
     *
     * @param Future<mixed> ...$futures
     * @return list<?\Throwable>
     */
    private static function settleAll(Future ...$futures): array
    {
        $errors = [];
        foreach ($futures as $future) {
            try {
                $future->await(new TimeoutCancellation(2));
                $errors[] = null;
            } catch (\Throwable $e) {
                $errors[] = $e;
            }
        }

        return $errors;
    }

    /** @param \Closure(): bool $condition */
    private function waitUntil(\Closure $condition): void
    {
        $deadline = hrtime(true) + 2_000_000_000;
        while (!$condition()) {
            if (hrtime(true) > $deadline) {
                self::fail('condition not reached within 2 s');
            }

            delay(0.001);
        }
    }

    private static function connectionOf(NatsClient $client): NatsConnection
    {
        $connection = (new \ReflectionProperty(NatsClient::class, 'connection'))->getValue($client);
        self::assertInstanceOf(NatsConnection::class, $connection);

        return $connection;
    }

    /** @return array<array-key, mixed> */
    private static function unconfirmedSids(NatsConnection $connection): array
    {
        $value = (new \ReflectionProperty(NatsConnection::class, 'unconfirmedSids'))->getValue($connection);
        self::assertIsArray($value);

        return $value;
    }
}
