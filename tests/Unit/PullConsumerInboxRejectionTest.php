<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\Consumers\PullConsumerIterator;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\SubscriptionLimitServer;
use IDCT\NATS\Tests\Support\WatchedTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\delay;

/**
 * A pull consumer run whose reply inbox the server rejects while the engine is not reading for it (#206): a
 * configuration reload that withdraws the subscribe permission (nats-server then sends "Permissions Violation for
 * Subscription to "<inbox>"", the connection staying open), or a reconnect whose replay of the inbox the new server
 * refuses for the subscription limit. Whichever fiber's read meets the -ERR records the rejection: the handler's own
 * read, an application's processIncoming() loop, or the recovery of a reconnect. The engine looked at it at the top of
 * its loop only, so a rejection recorded while the handler, or onError, waited was followed by a refill on the dead
 * inbox and a pump read that only the new pull's deadline ended, setExpiresMs() plus a second later; a finite run with
 * no pull left, and a run that a later pull's terminal status ended, returned their count without the error; and a
 * pull whose write failed was retried after the recovery whose replay the new server rejected. On a server with leaf
 * nodes or gateways, nats-server serves a pull younger than 2 s whose reply subject has no local interest, so such a
 * pull can take a batch nobody receives, lost under ack_policy none or max_deliver 1.
 *
 * The run now looks again after every wait that can let another fiber run (before each pull it retires, after the
 * retire phase and a terminal status's hand-over, after the issue phase's writes), issues nothing more, wakes from its
 * pump read and idle backoff at once, and guards each pull's publication, so that no new pull, nor a retry, goes out
 * once it knows its inbox is rejected. It hands what its pulls hold to the handler first and then throws the rejection,
 * as before (#197); a stop() keeps its precedence, and a handler or onError that throws still ends the run with its own
 * exception.
 *
 * Over the scripted server in tests/Support/ReconnectingTransport.php seen through tests/Support/WatchedTransport.php,
 * and over tests/Support/SubscriptionLimitServer.php for the limit. The pulls expire after 30 s, so a pull's deadline
 * (31 s) is far from both outcomes, and the order of events decides: each pull the server receives is recorded with
 * whether the rejection had been reported by then, and the run is to have ended within the 5 s the test waits for it.
 * The rejection is reported through the error listener in the same dispatch that records it, before any other fiber
 * can run. Each test fails on 2.25.0 unless it is declared a guard, which passes on both.
 */
final class PullConsumerInboxRejectionTest extends TestCase
{
    use ReconnectScenarios;

    private const PULL_SUBJECT = '$JS.API.CONSUMER.MSG.NEXT.S.C';
    private const ACK_SUBJECT = '$JS.ACK.S.C.1.1.1.0.0';
    private const PERMISSIONS = "was rejected by server permissions: 'Permissions Violation for Subscription to";
    private const LIMIT = 'may have been rejected by the server because the connection is at its subscription limit';

    /** @var list<DeferredFuture<null>> Gates a handler waits on, opened in tearDown for a test that failed early. */
    private array $gates = [];

    protected function tearDown(): void
    {
        foreach ($this->gates as $gate) {
            if (!$gate->isComplete()) {
                $gate->complete();
            }
        }
        $this->gates = [];
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{string, ?int}> */
    public static function readersAndRuns(): iterable
    {
        foreach (['another fiber' => 'another fiber', 'the handler' => 'the handler'] as $name => $reader) {
            yield $name . ', an infinite run' => [$reader, null];
            yield $name . ', a run of two iterations' => [$reader, 2];
            yield $name . ', a finite run on its last iteration' => [$reader, 1];
        }
    }

    /**
     * The issue's shape: batch 1, depth 1, the first pull answered with m-1, and the run's inbox rejected by a
     * permissions violation while the handler waits on m-1, the -ERR met by another fiber's processIncoming() or by
     * the handler's own. Once the handler returns, handle() throws the rejection, with no pull written after it, the
     * inbox released once and the connection still Open. The run used to write a second pull on the dead inbox and wait
     * for its deadline, 31 s later, and a finite run on its last iteration returned 1 without the error, which a caller
     * learnt only from its next run.
     */
    #[DataProvider('readersAndRuns')]
    public function testARejectionReadWhileTheHandlerWaitsEndsTheRunWithoutAnotherPull(string $reader, ?int $iterations): void
    {
        [$transport, , $client, $rejected] = $this->client();
        $server = $this->pullServer($transport, $rejected, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1
            ? [self::messages($sid, 'm-1')]
            : []);
        $handled = self::log();
        $gate = $this->gate();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        if ($iterations !== null) {
            $iterator->setIterations($iterations);
        }
        $run = $iterator->handle(function (NatsMessage $message) use ($handled, $gate, $reader, $client, $transport, $server, $rejected): void {
            $handled[] = $message->payload;
            if ($reader === 'the handler') {
                $this->rejectInbox($client, $transport, $server, $rejected);
            }
            $handled[] = 'waits';
            $gate->getFuture()->await();
        });
        $this->waitUntil(static fn(): bool => $handled->getArrayCopy() === ['m-1', 'waits']);
        if ($reader === 'another fiber') {
            $this->rejectInbox($client, $transport, $server, $rejected);
        }
        self::assertTrue($rejected->reported, 'the rejection was recorded while the handler waited');

        $gate->complete();
        [$processed, $error] = $this->settle($run, $iterator);

        self::assertNull($processed, sprintf('handle() returned %s', var_export($processed, true)));
        self::assertInstanceOf(JetStreamException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertStringContainsString('Pull consumer reply inbox "' . self::baseOf($server) . '.*" ' . self::PERMISSIONS, $error->getMessage());
        self::assertSame(['m-1', 'waits'], $handled->getArrayCopy());
        self::assertSame([false], self::afterRejection($server), 'no pull after the rejection');
        self::assertSame(['UNSUB ' . self::sidOf($server)], $transport->controlLinesStartingWith('UNSUB '), 'the inbox is released once');
        self::assertSame(ConnectionState::Open, $client->state());
    }

    /** @return iterable<string, array{?int}> */
    public static function runsAcrossTheReplay(): iterable
    {
        yield 'a run of two iterations' => [2];
        yield 'an infinite run (guard: #187 sees the reconnect)' => [null];
    }

    /**
     * The limit's shape: the connection holds app.one and the run's inbox at a limit of 2; while the handler waits on
     * m-1 the limit drops to 1 and the connection drops, and another fiber's read runs the reconnect, whose replay of
     * the inbox the new server rejects ahead of the PONG behind it. Once the handler returns, handle() throws the limit
     * error, with no pull written on the new connection, where app.one is all the server holds. A run of two
     * iterations used to write its second pull there, on the rejected inbox, and wait for its deadline. Guard for an
     * infinite run: its reconnect check sends it to the top of its loop, which sees the rejection first (#187).
     */
    #[DataProvider('runsAcrossTheReplay')]
    public function testAReplayTheNewServerRejectsWhileTheHandlerWaitsEndsTheRunWithoutAPullOnTheNewConnection(?int $iterations): void
    {
        $limited = new SubscriptionLimitServer();
        $limited->limit = 2;
        [$rejected, $listener] = self::rejectionListener();
        $client = new NatsClient($this->options(true, 2_000, 3, 0, 2, null, 1, 1, $listener), $limited);
        $this->opened[] = $client;
        $client->connect()->await();
        $client->subscribe('app.one', static function (): void {})->await();
        /** @var \ArrayObject<int, int> $pullEpochs */
        $pullEpochs = new \ArrayObject();
        $limited->responder = static function (string $subject, ?string $replyTo) use ($limited, $pullEpochs): array {
            if ($subject !== self::PULL_SUBJECT || $replyTo === null) {
                return [];
            }
            $pullEpochs[] = $limited->epoch();
            $sid = self::sidHeldFor($limited, $replyTo);

            return $sid !== null && count($pullEpochs) === 1 ? [self::messages($sid, 'm-1')] : [];
        };
        $handled = self::log();
        $gate = $this->gate();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        if ($iterations !== null) {
            $iterator->setIterations($iterations);
        }
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $gate): void {
            $handled[] = $message->payload;
            $gate->getFuture()->await();
        });
        $this->waitUntil(static fn(): bool => $handled->getArrayCopy() === ['m-1']);

        $limited->limit = 1;
        $limited->dropConnection();
        $client->processIncoming(new TimeoutCancellation(3))->await();
        self::assertTrue($rejected->reported, 'the new server rejected the replayed inbox');
        self::assertSame(1, $limited->epoch());

        $gate->complete();
        [$processed, $error] = $this->settle($run, $iterator);

        self::assertNull($processed, sprintf('handle() returned %s', var_export($processed, true)));
        self::assertInstanceOf(JetStreamException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertStringContainsString(self::LIMIT, $error->getMessage());
        self::assertSame([0], $pullEpochs->getArrayCopy(), 'no pull on the new connection');
        self::assertSame(['app.one'], array_values($limited->heldSubscriptions()));
    }

    /**
     * What the pulls hold still reaches the handler, in order, before the rejection is thrown: batch 2, depth 2, the
     * first pull answered with m-1 and m-2, the second with m-3, which leaves it open; the inbox is rejected while the
     * handler waits on m-1. The handler gets m-1, m-2 and m-3, and handle() throws the rejection, with two pulls, none
     * after it. The run used to refill the pull it had retired and read for its deadline, m-3 still held.
     */
    public function testWhatAPullStillOpenHoldsReachesTheHandlerBeforeTheRejectionIsThrown(): void
    {
        [$transport, , $client, $rejected] = $this->client();
        $server = $this->pullServer($transport, $rejected, static fn(string $replyTo, int $sid, int $pull): array => match ($pull) {
            1 => [self::messages($sid, 'm-1', 'm-2')],
            2 => [self::messages($sid, 'm-3')],
            default => [],
        });
        $handled = self::log();
        $gate = $this->gate();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000);
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $gate): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-1') {
                $gate->getFuture()->await();
            }
        });
        $this->waitUntil(static fn(): bool => $handled->getArrayCopy() === ['m-1'] && count($server->pulls) === 2);
        $this->rejectInbox($client, $transport, $server, $rejected);

        $gate->complete();
        [, $error] = $this->settle($run, $iterator);

        self::assertInstanceOf(JetStreamException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertStringContainsString(self::PERMISSIONS, $error->getMessage());
        self::assertSame(['m-1', 'm-2', 'm-3'], $handled->getArrayCopy());
        self::assertSame([false, false], self::afterRejection($server), 'no pull after the rejection');
    }

    /**
     * The same, the handler throwing on m-3, which the hand-over before the rejection delivers: as for any failure of
     * the run (#197), the handler's exception is reported through the error listener and the rejection thrown. The run
     * used to refill instead, m-3 still held.
     */
    public function testAHandlerFailingInTheHandOverBeforeTheRejectionIsReportedAndTheRejectionThrown(): void
    {
        $recorder = new LifecycleRecorder();
        [$transport, , $client, $rejected] = $this->client($recorder);
        $server = $this->pullServer($transport, $rejected, static fn(string $replyTo, int $sid, int $pull): array => match ($pull) {
            1 => [self::messages($sid, 'm-1', 'm-2')],
            2 => [self::messages($sid, 'm-3')],
            default => [],
        });
        $handled = self::log();
        $gate = $this->gate();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(2)->setDepth(2)->setExpiresMs(30_000);
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $gate): void {
            $handled[] = $message->payload;
            if ($message->payload === 'm-1') {
                $gate->getFuture()->await();
            }
            if ($message->payload === 'm-3') {
                throw new \RuntimeException('the handler failed on m-3');
            }
        });
        $this->waitUntil(static fn(): bool => $handled->getArrayCopy() === ['m-1'] && count($server->pulls) === 2);
        $this->rejectInbox($client, $transport, $server, $rejected);

        $gate->complete();
        [, $error] = $this->settle($run, $iterator);

        self::assertInstanceOf(JetStreamException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertStringContainsString(self::PERMISSIONS, $error->getMessage());
        self::assertSame(['m-1', 'm-2', 'm-3'], $handled->getArrayCopy());
        self::assertSame(['the handler failed on m-3'], $recorder->errorsContaining('the handler failed'));
        self::assertSame([false, false], self::afterRejection($server), 'no pull after the rejection');
    }

    /**
     * A later pull's terminal status does not hide the rejection: batch 1, depth 2, the first pull answered with m-1,
     * the second with a 409 Consumer Deleted, and the inbox rejected while the handler waits on m-1. handle() throws
     * the rejection rather than ending on the status, which onError never sees, the rejection coming first. The run
     * used to retire the second pull next, its status ending the run through onError, and handle() returned 1 without
     * the error.
     */
    public function testALaterPullsTerminalStatusDoesNotHideTheRejection(): void
    {
        [$transport, , $client, $rejected] = $this->client();
        $server = $this->pullServer($transport, $rejected, static fn(string $replyTo, int $sid, int $pull): array => match ($pull) {
            1 => [self::messages($sid, 'm-1')],
            2 => [self::statusFrame($replyTo, $sid, 409, 'Consumer Deleted')],
            default => [],
        });
        $handled = self::log();
        /** @var \ArrayObject<int, string> $reported */
        $reported = new \ArrayObject();
        $gate = $this->gate();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(30_000)
            ->setOnError(static function (\Throwable $error) use ($reported): void {
                $reported[] = $error->getMessage();
            });
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $gate): void {
            $handled[] = $message->payload;
            $gate->getFuture()->await();
        });
        $this->waitUntil(static fn(): bool => $handled->getArrayCopy() === ['m-1'] && count($server->pulls) === 2);
        $this->rejectInbox($client, $transport, $server, $rejected);

        $gate->complete();
        [$processed, $error] = $this->settle($run, $iterator);

        self::assertNull($processed, sprintf('handle() returned %s', var_export($processed, true)));
        self::assertInstanceOf(JetStreamException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertStringContainsString(self::PERMISSIONS, $error->getMessage());
        self::assertSame([], $reported->getArrayCopy(), 'the status of a pull on the rejected inbox is not reported');
        self::assertSame([false, false], self::afterRejection($server));
    }

    /** @return iterable<string, array{?int}> */
    public static function infiniteAndFinite(): iterable
    {
        yield 'an infinite run' => [null];
        yield 'a finite run' => [1];
    }

    /**
     * A rejection that onError's own read meets, as it reports a terminal status, ends the run with it: depth 1, the
     * first pull answered with a 409 Consumer Deleted, and onError reading the -ERR. handle() throws the rejection, with
     * one pull. The run used to end as on any terminal status, returning 0 without the error.
     */
    #[DataProvider('infiniteAndFinite')]
    public function testARejectionReadByOnErrorOnATerminalStatusEndsTheRunWithIt(?int $iterations): void
    {
        [$transport, , $client, $rejected] = $this->client();
        $server = $this->pullServer($transport, $rejected, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1
            ? [self::statusFrame($replyTo, $sid, 409, 'Consumer Deleted')]
            : []);
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000)
            ->setOnError(function () use ($client, $transport, $server, $rejected): void {
                $this->rejectInbox($client, $transport, $server, $rejected);
            });
        if ($iterations !== null) {
            $iterator->setIterations($iterations);
        }
        $run = $iterator->handle(static function (): void {
            self::fail('the server sends no message');
        });
        [$processed, $error] = $this->settle($run, $iterator);

        self::assertTrue($rejected->reported, 'onError read the rejection');
        self::assertNull($processed, sprintf('handle() returned %s', var_export($processed, true)));
        self::assertInstanceOf(JetStreamException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertStringContainsString(self::PERMISSIONS, $error->getMessage());
        self::assertSame([false], self::afterRejection($server));
    }

    /**
     * A rejection recorded before a terminal status's hand-over makes it the hand-over before a failure (#197): batch
     * 1, depth 2, the first pull answered with a 409 Consumer Deleted and the second with m-1, and onError, reporting the
     * 409, reads the rejection. The handler gets m-1 before the rejection is thrown, and throws: its exception is
     * reported through the error listener, and handle() throws the rejection. The run used to hand m-1 over as at the
     * end of a run, and to end with the handler's exception.
     */
    public function testAHandlerFailingInTheHandOverAfterOnErrorReadTheRejectionIsReportedAndTheRejectionThrown(): void
    {
        $recorder = new LifecycleRecorder();
        [$transport, , $client, $rejected] = $this->client($recorder);
        $server = $this->pullServer($transport, $rejected, static fn(string $replyTo, int $sid, int $pull): array => match ($pull) {
            1 => [self::statusFrame($replyTo, $sid, 409, 'Consumer Deleted')],
            2 => [self::messages($sid, 'm-1')],
            default => [],
        });
        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(30_000)
            ->setOnError(function () use ($client, $transport, $server, $rejected): void {
                $this->rejectInbox($client, $transport, $server, $rejected);
            });
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled): void {
            $handled[] = $message->payload;

            throw new \RuntimeException('the handler failed on ' . $message->payload);
        });
        [, $error] = $this->settle($run, $iterator);

        self::assertTrue($rejected->reported, 'onError read the rejection');
        self::assertInstanceOf(JetStreamException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertStringContainsString(self::PERMISSIONS, $error->getMessage());
        self::assertSame(['m-1'], $handled->getArrayCopy());
        self::assertSame(['the handler failed on m-1'], $recorder->errorsContaining('the handler failed'));
        self::assertSame([false, false], self::afterRejection($server));
    }

    /**
     * A rejection read while the handler waits in a terminal status's hand-over ends the run with it, after that
     * hand-over: batch 1, depth 2, the answer to the second pull bringing the first pull's 409 Consumer Deleted and m-1,
     * which goes to the second pull, so that the status ends the run and the hand-over of the pulls behind it gives the
     * handler m-1 (#187); the inbox is rejected while the handler waits on it. The handler gets m-1 once, and handle()
     * throws the rejection. The run used to return 1. Guard, with the handler stopping the run before it returns: the
     * stop keeps its precedence, and handle() returns 1.
     */
    #[DataProvider('stoppedOrNot')]
    public function testARejectionReadDuringATerminalStatusHandOverEndsTheRunAfterIt(bool $stopped): void
    {
        [$transport, , $client, $rejected] = $this->client();
        $first = new class {
            /** The first pull's reply subject, which its status comes on. */
            public string $replyTo = '';
        };
        // One chunk answers the second pull: the first pull's terminal status, then m-1, which goes to the second.
        $server = $this->pullServer($transport, $rejected, static function (string $replyTo, int $sid, int $pull) use ($first): array {
            if ($pull === 1) {
                $first->replyTo = $replyTo;
            }

            return $pull === 2 ? [self::statusFrame($first->replyTo, $sid, 409, 'Consumer Deleted') . self::messages($sid, 'm-1')] : [];
        });
        $handled = self::log();
        $gate = $this->gate();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(30_000);
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $gate, $iterator, $stopped): void {
            $handled[] = $message->payload;
            $gate->getFuture()->await();
            if ($stopped) {
                $iterator->stop();
            }
        });
        $this->waitUntil(static fn(): bool => $handled->getArrayCopy() === ['m-1']);
        $this->rejectInbox($client, $transport, $server, $rejected);

        $gate->complete();
        [$processed, $error] = $this->settle($run, $iterator);

        self::assertSame(['m-1'], $handled->getArrayCopy(), 'm-1 is delivered once');
        self::assertSame([false, false], self::afterRejection($server));
        if ($stopped) {
            self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
            self::assertSame(1, $processed);

            return;
        }
        self::assertNull($processed, sprintf('handle() returned %s', var_export($processed, true)));
        self::assertInstanceOf(JetStreamException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertStringContainsString(self::PERMISSIONS, $error->getMessage());
    }

    /** @return iterable<string, array{bool}> */
    public static function stoppedOrNot(): iterable
    {
        yield 'the handler returning' => [false];
        yield 'the handler stopping the run (guard)' => [true];
    }

    /**
     * Guard: a stop() keeps its precedence over a rejection recorded while the handler waits: the handler stops the run
     * once the inbox is rejected, and handle() returns 1, with no pull after the rejection, as a stopped run does.
     */
    public function testAStopAfterTheRejectionStillEndsTheRunWithItsCount(): void
    {
        [$transport, , $client, $rejected] = $this->client();
        $server = $this->pullServer($transport, $rejected, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1
            ? [self::messages($sid, 'm-1')]
            : []);
        $gate = $this->gate();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        $handled = self::log();
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $gate, $iterator): void {
            $handled[] = $message->payload;
            $gate->getFuture()->await();
            $iterator->stop();
        });
        $this->waitUntil(static fn(): bool => $handled->getArrayCopy() === ['m-1']);
        $this->rejectInbox($client, $transport, $server, $rejected);

        $gate->complete();
        [$processed, $error] = $this->settle($run, $iterator);

        self::assertNull($error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame(1, $processed);
        self::assertSame([false], self::afterRejection($server));
    }

    /**
     * Guard: a handler that throws once the inbox is rejected still ends the run with its own exception, as anywhere in
     * the retire phase, with no pull after the rejection.
     */
    public function testAHandlerThatThrowsAfterTheRejectionStillEndsTheRunWithItsOwnException(): void
    {
        [$transport, , $client, $rejected] = $this->client();
        $server = $this->pullServer($transport, $rejected, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1
            ? [self::messages($sid, 'm-1')]
            : []);
        $gate = $this->gate();
        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $gate): void {
            $handled[] = $message->payload;
            $gate->getFuture()->await();

            throw new \RuntimeException('the handler failed');
        });
        $this->waitUntil(static fn(): bool => $handled->getArrayCopy() === ['m-1']);
        $this->rejectInbox($client, $transport, $server, $rejected);

        $gate->complete();
        [, $error] = $this->settle($run, $iterator);

        self::assertInstanceOf(\RuntimeException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertSame('the handler failed', $error->getMessage());
        self::assertSame([false], self::afterRejection($server));
    }

    /**
     * A rejection read while the issue phase awaits a pull's write ends the generation there: batch 1, depth 3, the
     * second pull's write held up, like a socket under backpressure, while another fiber reads the -ERR. The held write,
     * which the transport had taken, completes, but no third pull goes out, and handle() throws the rejection once the
     * write is released, with m-1, which the first pull brought, handed over first. The run used to write the third
     * pull too, after the rejection, and read for its deadline.
     */
    public function testARejectionReadWhileAPullsWriteIsHeldUpIssuesNoFurtherPull(): void
    {
        [$transport, , $client, $rejected] = $this->client();
        $server = $this->pullServer($transport, $rejected, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1
            ? [self::messages($sid, 'm-1')]
            : []);
        $transport->afterWrite = static function (string $bytes) use ($transport, $server): void {
            if (str_starts_with($bytes, 'PUB ' . self::PULL_SUBJECT . ' ') && count($server->pulls) === 1) {
                $transport->stallNextWriteContaining('PUB ' . self::PULL_SUBJECT . ' ', 30.0);
            }
        };
        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(3)->setExpiresMs(30_000);
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled): void {
            $handled[] = $message->payload;
        });
        $this->waitUntil(static fn(): bool => $transport->writesStalled() === 1);
        $this->rejectInbox($client, $transport, $server, $rejected);

        $transport->releaseStalledWrites();
        [, $error] = $this->settle($run, $iterator);

        self::assertInstanceOf(JetStreamException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertStringContainsString(self::PERMISSIONS, $error->getMessage());
        self::assertSame(['m-1'], $handled->getArrayCopy());
        self::assertSame([false, true], self::afterRejection($server), 'the held write completed; no third pull');
    }

    /**
     * A rejection read while the issue phase's last write is held up ends the run once that write is done, rather than
     * reading for the pulls in flight: batch 1, depth 2, both pulls held by the server, the second one's write held up
     * while another fiber reads the -ERR. handle() throws the rejection once the write is released, with the two pulls
     * written. The run used to read for them until their deadline, 31 s later.
     */
    public function testARejectionReadWhileTheLastPullsWriteIsHeldUpEndsTheRunOnceTheWriteIsDone(): void
    {
        [$transport, , $client, $rejected] = $this->client();
        $server = $this->pullServer($transport, $rejected, static fn(): array => []);
        $transport->afterWrite = static function (string $bytes) use ($transport, $server): void {
            if (str_starts_with($bytes, 'PUB ' . self::PULL_SUBJECT . ' ') && count($server->pulls) === 1) {
                $transport->stallNextWriteContaining('PUB ' . self::PULL_SUBJECT . ' ', 30.0);
            }
        };
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(2)->setExpiresMs(30_000);
        $run = $iterator->handle(static function (): void {
            self::fail('the server sends no message');
        });
        $this->waitUntil(static fn(): bool => $transport->writesStalled() === 1);
        $this->rejectInbox($client, $transport, $server, $rejected);

        $transport->releaseStalledWrites();
        [, $error] = $this->settle($run, $iterator);

        self::assertInstanceOf(JetStreamException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertStringContainsString(self::PERMISSIONS, $error->getMessage());
        self::assertSame([false, true], self::afterRejection($server), 'the held write completed');
    }

    /**
     * A rejection that a reconnect's replay records while the pump read waits for that reconnect ends the run at once:
     * the run's pull is held, the connection drops while the pump read is on the socket, and the reconnect that read
     * starts replays the inbox, which the new server rejects, and then flushes a publish made during the outage, whose
     * write is held up, like a socket under backpressure. handle() throws the rejection while that write is still held
     * up, the reconnect not over, with no pull written on the new connection. The pump read used to wait for the
     * reconnect to be over, which held it to that write.
     */
    public function testARejectionTheReplayRecordsWhileThePumpReadWaitsForTheReconnectEndsTheRunAtOnce(): void
    {
        [$transport, $watched, $client, $rejected] = $this->client();
        $server = $this->pullServer($transport, $rejected, static fn(): array => []);
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        $run = $iterator->handle(static function (): void {
            self::fail('the server sends no message');
        });
        $this->waitUntil(static fn(): bool => count($server->pulls) === 1 && $watched->readsUnderWay === 1);
        $base = self::baseOf($server);
        $transport->afterWrite = static function (string $bytes) use ($transport, $base): void {
            if ($transport->epoch() === 1 && str_contains($bytes, 'SUB ' . $base . '.* ')) {
                $transport->pushFrame(self::permissionsViolation($base));
            }
        };
        $transport->holdNextDial();
        $transport->dropConnection();
        // The pump read met the loss and started the reconnect, whose dial is held: a publish made now is buffered.
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Connecting);
        $client->publish('during.the.outage', 'x')->await();
        $transport->stallNextWriteContaining('PUB during.the.outage', 30.0);

        $transport->releaseDial();
        [, $error] = $this->settle($run, $iterator);

        self::assertTrue($rejected->reported, 'the new server rejected the replayed inbox');
        self::assertInstanceOf(JetStreamException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertStringContainsString(self::PERMISSIONS, $error->getMessage());
        self::assertSame(ConnectionState::Connecting, $client->state(), 'the reconnect was not over');
        self::assertSame([false], self::afterRejection($server), 'no pull on the new connection');
        // The reconnect goes on to its flush, held up for 30 s, which the pump read would have waited for.
        $this->waitUntil(static fn(): bool => $transport->writesStalled() === 1);
        $transport->releaseStalledWrites();
        $this->waitUntilOpen($client);
    }

    /**
     * A rejection read while the engine waits in its idle backoff ends that wait at once: a no_wait run whose pulls are
     * all answered empty at once backs off for 10, 20, 40, 80, 160, 320 and then 500 ms, and another fiber reads the
     * -ERR 10 ms into the seventh backoff, no read of the socket under way. handle() throws the rejection within 0.25 s,
     * with seven pulls. The run used to wait out the backoff first, about 490 ms.
     */
    public function testARejectionReadDuringTheIdleBackoffEndsItAtOnce(): void
    {
        [$transport, $watched, $client, $rejected] = $this->client();
        $server = $this->pullServer($transport, $rejected, static fn(string $replyTo, int $sid): array => [self::statusFrame($replyTo, $sid, 404, 'No Messages')]);
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setNoWait()->setExpiresMs(30_000);
        $run = $iterator->handle(static function (): void {
            self::fail('every pull is answered empty');
        });
        $this->waitUntil(static fn(): bool => count($server->pulls) === 7);
        delay(0.01);
        self::assertSame(0, $watched->readsUnderWay, 'the run is in its seventh backoff, not in a read');
        self::assertCount(7, $server->pulls);

        $this->rejectInbox($client, $transport, $server, $rejected);
        $rejectedAt = hrtime(true);
        try {
            $run->await(new TimeoutCancellation(0.25));
            self::fail('expected handle() to throw the rejection');
        } catch (JetStreamException $error) {
            self::assertStringContainsString(self::PERMISSIONS, $error->getMessage());
        } catch (CancelledException) {
            $run->ignore();
            $iterator->stop();
            self::fail(sprintf('handle() was still waiting %.3f s after the rejection: the 500 ms backoff was not woken', $this->secondsSince($rejectedAt)));
        }
        self::assertCount(7, $server->pulls, 'no pull after the rejection');
    }

    /**
     * Guard: a rejection that another fiber's read records while the engine's pump read waits for the socket behind it
     * ends the run at once, as it did. While the handler waits on m-1, the application's processIncoming() takes the
     * socket; the handler returns, the run refills and its pump read waits for the socket behind that read, which then
     * meets the -ERR and, in the same chunk, a message whose handler holds the read up. The run throws the rejection
     * without waiting for that read to end, with no pull after the rejection.
     */
    public function testARejectionAnotherReaderRecordsWhileThePumpWaitsBehindItEndsTheRunAtOnce(): void
    {
        [$transport, $watched, $client, $rejected] = $this->client();
        $server = $this->pullServer($transport, $rejected, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1
            ? [self::messages($sid, 'm-1')]
            : []);
        $readerGate = $this->gate();
        $application = new class {
            /** Whether the application's read is held up in the unrelated subscription's handler. */
            public bool $heldUp = false;
        };
        $appSid = $client->subscribe('app.blocked', static function () use ($readerGate, $application): void {
            $application->heldUp = true;
            $readerGate->getFuture()->await();
        })->await();
        $handled = self::log();
        $gate = $this->gate();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled, $gate): void {
            $handled[] = $message->payload;
            $gate->getFuture()->await();
        });
        $this->waitUntil(static fn(): bool => $handled->getArrayCopy() === ['m-1']);
        $read = $client->processIncoming(new TimeoutCancellation(4));
        $read->ignore();
        $this->waitUntil(static fn(): bool => $watched->readsUnderWay === 1);
        $gate->complete();
        $this->waitUntil(static fn(): bool => count($server->pulls) === 2);
        // The pump read has had the time to wait behind the application's.
        delay(0.01);

        $transport->pushFrame(self::permissionsViolation(self::baseOf($server)) . ReconnectingTransport::msgFrame('app.blocked', $appSid, 'hold'));
        $this->waitUntil(static fn(): bool => $application->heldUp);
        [, $error] = $this->settle($run, $iterator);

        self::assertInstanceOf(JetStreamException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertStringContainsString(self::PERMISSIONS, $error->getMessage());
        self::assertSame([false, false], self::afterRejection($server));
        $readerGate->complete();
        $read->await(new TimeoutCancellation(3));
    }

    /**
     * A pull whose write fails is not retried once the recovery's replay of the inbox is rejected: the second pull's
     * write meets a dead socket and runs the reconnect inline, and the new server answers the replayed SUB of the inbox
     * with a permissions violation, which the recovery's read meets. handle() throws the rejection, nothing written on
     * the new connection after it, and the connection is Open. The write used to be retried there, on the rejected
     * inbox, after the recovery.
     */
    public function testAPullWhoseWriteFailedIsNotRetriedOnceTheReplayOfItsInboxIsRejected(): void
    {
        [$transport, , $client, $rejected] = $this->client();
        $server = $this->pullServer($transport, $rejected, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1
            ? [self::messages($sid, 'm-1')]
            : []);
        $transport->afterWrite = static function (string $bytes) use ($transport, $server): void {
            if (str_starts_with($bytes, 'PUB ' . self::PULL_SUBJECT . ' ') && count($server->pulls) === 1) {
                $transport->failNextWriteContaining('PUB ' . self::PULL_SUBJECT . ' ');
            }
            if ($transport->epoch() === 1 && str_starts_with($bytes, 'SUB ' . self::baseOf($server) . '.* ')) {
                $transport->pushFrame(self::permissionsViolation(self::baseOf($server)));
            }
        };
        $handled = self::log();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        $run = $iterator->handle(static function (NatsMessage $message) use ($handled): void {
            $handled[] = $message->payload;
        });
        [, $error] = $this->settle($run, $iterator);

        self::assertTrue($rejected->reported, 'the new server rejected the replayed inbox');
        self::assertInstanceOf(JetStreamException::class, $error, sprintf('handle() threw %s', self::describe($error)));
        self::assertStringContainsString(self::PERMISSIONS, $error->getMessage());
        self::assertSame(['m-1'], $handled->getArrayCopy());
        self::assertSame([], $transport->controlLinesStartingWith('PUB ' . self::PULL_SUBJECT . ' ', epoch: 1), 'no pull on the new connection');
        self::assertSame(1, $client->statistics()->reconnects);
        self::assertSame(ConnectionState::Open, $client->state());
    }

    /**
     * A rejection ends only the run whose inbox it names (#189's runs of one iterator each have their own inbox and
     * rejection): two runs started on one iterator, the first one's handler waiting on m-1 while its inbox is
     * rejected, the second with its pull held by the server. The first run throws its rejection, with one pull; the
     * second goes on, issuing nothing more, and handles m-2 once the server answers its pull, ending with 1 on a stop().
     * The first run used to wait for its second pull's deadline.
     */
    public function testARejectionEndsOnlyTheRunWhoseInboxItNames(): void
    {
        [$transport, , $client, $rejected] = $this->client();
        $server = $this->pullServer($transport, $rejected, static fn(string $replyTo, int $sid, int $pull): array => $pull === 1
            ? [self::messages($sid, 'm-1')]
            : []);
        $handled = self::log();
        $gate = $this->gate();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        $first = $iterator->handle(static function (NatsMessage $message) use ($handled, $gate): void {
            $handled[] = 'first: ' . $message->payload;
            $gate->getFuture()->await();
        });
        $this->waitUntil(static fn(): bool => $handled->getArrayCopy() === ['first: m-1']);
        $second = $iterator->handle(static function (NatsMessage $message) use ($handled): void {
            $handled[] = 'second: ' . $message->payload;
        });
        $second->ignore();
        $this->waitUntil(static fn(): bool => count($server->pulls) === 2);
        $firstSid = self::sidOf($server);
        $secondSid = $server->pulls[1]['sid'];
        self::assertNotSame($firstSid, $secondSid, 'each run has an inbox of its own');
        $this->rejectInbox($client, $transport, $server, $rejected);

        $gate->complete();
        [, $error] = $this->settle($first, $iterator);

        self::assertInstanceOf(JetStreamException::class, $error, sprintf('the first run threw %s', self::describe($error)));
        self::assertStringContainsString('Pull consumer reply inbox "' . self::baseOf($server) . '.*" ' . self::PERMISSIONS, $error->getMessage());
        self::assertSame([false, false], self::afterRejection($server), 'neither run pulled after the rejection');

        $transport->pushFrame(self::messages($secondSid, 'm-2'));
        $this->waitUntil(static fn(): bool => in_array('second: m-2', $handled->getArrayCopy(), true));
        $iterator->stop();
        [$processed, $secondError] = $this->settle($second, $iterator);

        self::assertNull($secondError, sprintf('the second run threw %s', self::describe($secondError)));
        self::assertSame(1, $processed);
        self::assertSame(['first: m-1', 'second: m-2'], $handled->getArrayCopy());
        $sids = array_map(static fn(array $pull): int => $pull['sid'], $server->pulls);
        self::assertSame([$firstSid], array_values(array_filter($sids, static fn(int $sid): bool => $sid === $firstSid)), 'the first run pulled once');
        self::assertContains('UNSUB ' . $firstSid, $transport->controlLinesStartingWith('UNSUB '));
        self::assertContains('UNSUB ' . $secondSid, $transport->controlLinesStartingWith('UNSUB '));
        self::assertSame(ConnectionState::Open, $client->state());
    }

    /**
     * A run started once a rejected one has ended gets an inbox and a rejection of its own: the first run throws the
     * rejection read while its handler waited, and a handle() on the same iterator then subscribes a new inbox, pulls on
     * it and handles m-2, ending with 1 on its handler's stop(). The first run used to wait for its second pull's
     * deadline, 31 s, before the iterator could be started again.
     */
    public function testARunStartedAfterARejectedOnePullsOnAnInboxOfItsOwn(): void
    {
        [$transport, , $client, $rejected] = $this->client();
        $server = $this->pullServer($transport, $rejected, static fn(string $replyTo, int $sid, int $pull): array => match ($pull) {
            1 => [self::messages($sid, 'm-1')],
            2 => [self::messages($sid, 'm-2')],
            default => [],
        });
        $handled = self::log();
        $gate = $this->gate();
        $iterator = $client->jetStream()->pullConsumer('S', 'C')->setBatching(1)->setDepth(1)->setExpiresMs(30_000);
        $first = $iterator->handle(static function (NatsMessage $message) use ($handled, $gate): void {
            $handled[] = $message->payload;
            $gate->getFuture()->await();
        });
        $this->waitUntil(static fn(): bool => $handled->getArrayCopy() === ['m-1']);
        $this->rejectInbox($client, $transport, $server, $rejected);
        $gate->complete();
        [, $error] = $this->settle($first, $iterator);
        self::assertInstanceOf(JetStreamException::class, $error, sprintf('the first run threw %s', self::describe($error)));

        $second = $iterator->handle(static function (NatsMessage $message) use ($handled, $iterator): void {
            $handled[] = $message->payload;
            $iterator->stop();
        });
        [$processed, $secondError] = $this->settle($second, $iterator);

        self::assertNull($secondError, sprintf('the second run threw %s', self::describe($secondError)));
        self::assertSame(1, $processed);
        self::assertSame(['m-1', 'm-2'], $handled->getArrayCopy());
        self::assertCount(2, $server->pulls);
        self::assertNotSame($server->pulls[0]['sid'], $server->pulls[1]['sid'], 'the second run pulled on an inbox of its own');
        self::assertCount(2, $transport->controlLinesStartingWith('UNSUB '), 'each run released its inbox');
    }

    /**
     * A connected client over a watched scripted server, with no heartbeat, so that only the engine and the test read,
     * closed in tearDown; the error listener records the inbox's rejection, and $recorder whatever else it reports.
     *
     * @return array{ReconnectingTransport, WatchedTransport, NatsClient, object{reported: bool}}
     */
    private function client(?LifecycleRecorder $recorder = null): array
    {
        $transport = new ReconnectingTransport();
        $watched = new WatchedTransport($transport);
        [$rejected, $listener] = self::rejectionListener($recorder);
        $client = new NatsClient($this->options(true, 2_000, 3, 0, 2, null, 1, 1, $listener), $watched);
        $this->opened[] = $client;
        $client->connect()->await();

        return [$transport, $watched, $client, $rejected];
    }

    /**
     * What records the inbox's rejection: an error listener that marks, in the same dispatch that records the rejection
     * in the run, a permissions violation or a subscription-limit -ERR the connection reports, and passes every error
     * on to $recorder; and the mark, whether the connection has reported it yet.
     *
     * @return array{object{reported: bool}, \Closure(\Throwable): void}
     */
    private static function rejectionListener(?LifecycleRecorder $recorder = null): array
    {
        $rejected = new class {
            public bool $reported = false;
        };
        $passOn = $recorder?->errorListener();

        return [$rejected, static function (\Throwable $error) use ($rejected, $passOn): void {
            if (str_contains($error->getMessage(), 'Permissions Violation') || str_contains($error->getMessage(), 'maximum subscriptions exceeded')) {
                $rejected->reported = true;
            }
            if ($passOn !== null) {
                $passOn($error);
            }
        }];
    }

    /**
     * The scripted server's side of the pull consumer: every pull to the consumer is recorded, with the sid of the run
     * inbox its reply subject belongs to, its reply subject, and whether the rejection had been reported when the
     * server received it, and answered by $onPull, given the reply subject, that sid and the pull's number (1 for the
     * first). The first pull's inbox is the one the tests reject.
     *
     * @param object{reported: bool} $rejected
     * @param \Closure(string, int, int): list<string> $onPull
     * @return object{pulls: list<array{sid: int, replyTo: string, afterRejection: bool}>}
     */
    private function pullServer(ReconnectingTransport $transport, object $rejected, \Closure $onPull): object
    {
        $server = new class {
            /** @var list<array{sid: int, replyTo: string, afterRejection: bool}> Each pull, in order. */
            public array $pulls = [];
        };
        $transport->responder = static function (string $subject, ?string $replyTo) use ($transport, $rejected, $onPull, $server): array {
            if ($replyTo === null || $subject !== self::PULL_SUBJECT) {
                return [];
            }

            $sid = $transport->sidFor($replyTo);
            if ($sid === null) {
                return [];
            }

            $server->pulls[] = ['sid' => $sid, 'replyTo' => $replyTo, 'afterRejection' => $rejected->reported];

            return $onPull($replyTo, $sid, count($server->pulls));
        };

        return $server;
    }

    /**
     * The inbox of the first pull's run, without its ".*".
     *
     * @param object{pulls: list<array{sid: int, replyTo: string, afterRejection: bool}>} $server
     */
    private static function baseOf(object $server): string
    {
        self::assertNotSame([], $server->pulls, 'the server received no pull');
        $replyTo = $server->pulls[0]['replyTo'];

        return substr($replyTo, 0, (int) strrpos($replyTo, '.'));
    }

    /**
     * The sid of the first pull's run inbox.
     *
     * @param object{pulls: list<array{sid: int, replyTo: string, afterRejection: bool}>} $server
     */
    private static function sidOf(object $server): int
    {
        self::assertNotSame([], $server->pulls, 'the server received no pull');

        return $server->pulls[0]['sid'];
    }

    /**
     * For each pull, whether the rejection had been reported when the server received it.
     *
     * @param object{pulls: list<array{sid: int, replyTo: string, afterRejection: bool}>} $server
     * @return list<bool>
     */
    private static function afterRejection(object $server): array
    {
        return array_map(static fn(array $pull): bool => $pull['afterRejection'], $server->pulls);
    }

    /**
     * The server rejects the inbox of the first pull's run with a permissions violation naming it, as on a configuration
     * reload that withdraws the permission, and the caller's own reads meet the -ERR, until the connection has reported
     * it: the reads of whichever fiber calls this, the handler's or onError's included.
     *
     * @param object{pulls: list<array{sid: int, replyTo: string, afterRejection: bool}>} $server
     * @param object{reported: bool} $rejected
     */
    private function rejectInbox(NatsClient $client, ReconnectingTransport $transport, object $server, object $rejected): void
    {
        $transport->pushFrame(self::permissionsViolation(self::baseOf($server), self::sidOf($server)));
        $budget = new TimeoutCancellation(2);
        while (!$rejected->reported) {
            $client->processIncoming($budget)->await();
        }
    }

    /** The -ERR nats-server sends when it rejects the subscription to "<base>.*", naming its sid when given. */
    private static function permissionsViolation(string $base, ?int $sid = null): string
    {
        return $sid === null
            ? sprintf("-ERR 'Permissions Violation for Subscription to \"%s.*\"'\r\n", $base)
            : sprintf("-ERR 'Permissions Violation for Subscription to \"%s.*\" (sid \"%d\")'\r\n", $base, $sid);
    }

    /**
     * What $run returned and what it threw. It is to end within 5 s; one still going then is stopped, ignored, and the
     * test failed, so that no run outlives the test (the close in tearDown ends one the stop does not reach in time).
     *
     * @param Future<int> $run
     * @return array{?int, ?\Throwable}
     */
    private function settle(Future $run, PullConsumerIterator $iterator): array
    {
        try {
            return [$run->await(new TimeoutCancellation(5)), null];
        } catch (CancelledException) {
            $run->ignore();
            $iterator->stop();
            self::fail('handle() was still waiting 5 s later, with the pull\'s deadline 31 s away');
        } catch (\Throwable $error) {
            return [null, $error];
        }
    }

    /** @return DeferredFuture<null> A gate a handler waits on, opened in tearDown at the latest. */
    private function gate(): DeferredFuture
    {
        /** @var DeferredFuture<null> $gate */
        $gate = new DeferredFuture();
        $this->gates[] = $gate;

        return $gate;
    }

    /** @return \ArrayObject<int, string> */
    private static function log(): \ArrayObject
    {
        /** @var \ArrayObject<int, string> $log */
        $log = new \ArrayObject();

        return $log;
    }

    /** One chunk of messages on a run's inbox, each as a real server delivers a pull's message. */
    private static function messages(int $sid, string ...$payloads): string
    {
        $chunk = '';
        foreach ($payloads as $payload) {
            $chunk .= ReconnectingTransport::msgFrame('evt.s', $sid, $payload, self::ACK_SUBJECT);
        }

        return $chunk;
    }

    /** A pull status frame, on the pull's reply subject as the server sends it. */
    private static function statusFrame(string $replyTo, int $sid, int $code, string $description): string
    {
        return ReconnectingTransport::hmsgFrame($replyTo, $sid, sprintf("NATS/1.0 %d %s\r\n\r\n", $code, $description), '');
    }

    /** The sid of the subscription the live session of $server holds for $subject: the exact one, or a "<base>.*" wildcard. */
    private static function sidHeldFor(SubscriptionLimitServer $server, string $subject): ?int
    {
        foreach ($server->heldSubscriptions() as $sid => $held) {
            if ($held === $subject || (str_ends_with($held, '.*') && str_starts_with($subject, substr($held, 0, -1)))) {
                return $sid;
            }
        }

        return null;
    }

    private static function describe(?\Throwable $error): string
    {
        return $error === null ? 'nothing' : $error::class . ': ' . $error->getMessage();
    }
}
