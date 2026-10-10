<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\SlowConsumerException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Tests\Support\HeldDrainParticipant;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use IDCT\NATS\Tests\Support\SubscriptionLimitServer;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;
use function Amp\Future\awaitAll;

/**
 * The request mux inbox when the server rejects its SUB, and its set-up. A rejection for the connection's
 * subscription limit names no subject and leaves the connection open, and the client used to keep the rejected mux
 * as if it were in place: every later request was sent with a reply subject nobody held and waited out its whole
 * timeout, until a terminal close. The client now drops a mux the server is not known to hold - the PONG of the
 * PING written right behind its SUB confirms it, as does a delivery on it - and the next request subscribes it
 * again, waiting for that confirmation before it is sent. The client records the mux before its SUB is written, so
 * whichever read meets the server's answer knows it is the mux's, the #167 permissions latch included, and a
 * terminal close during the set-up leaves nothing of the closed connection's mux behind.
 */
final class MuxInboxRejectionTest extends TestCase
{
    use OwnsTestResources;
    private const LIMIT_ERROR = "Server sent error frame: 'maximum subscriptions exceeded'";
    private const MUX_DROPPED = 'the server may have rejected the shared reply-inbox subscription';
    private const CLOSED = '/^Connection (is not open|was closed while the reply inbox was being set up)$/';

    /** @var list<NatsConnection|NatsClient> */
    private array $opened = [];

    protected function tearDown(): void
    {
        // Each connection is registered before it connects; the shutdown of #183 closes them, joins their closes and
        // checks that nothing is left running.
        foreach ($this->opened as $connection) {
            $this->own($connection);
        }
        $this->opened = [];
        $this->releaseOwnedResources();
    }

    /**
     * The main case: the first request fails with the server's -ERR, and once a slot is free a later request
     * subscribes the mux again, in a write that also unsubscribes the dropped one, and gets its reply. It used
     * to be sent on the rejected mux and time out.
     */
    public function testALaterRequestSubscribesTheMuxAgainOnceASlotIsFree(): void
    {
        $server = $this->echoServer(limit: 1);
        $connection = $this->connect($server);
        $appSid = $this->subscribe($connection, 'app.updates');

        try {
            $connection->request('svc.echo', 'one', 1_000)->await();
            self::fail('expected the request to fail: the server rejected its reply inbox');
        } catch (ConnectionException $e) {
            self::assertSame(self::LIMIT_ERROR, $e->getMessage());
        }
        self::assertSame(ConnectionState::Open, $connection->state());

        $connection->unsubscribe($appSid)->await();

        self::assertSame('echo:two', $connection->request('svc.echo', 'two', 1_000)->await()->payload);
        $installs = self::muxInstallWrites($server);
        self::assertCount(2, $installs, 'the second request subscribed the mux again');
        self::assertMatchesRegularExpression('/^SUB _INBOX\.[0-9a-f]{24}\.\* 2\r\nPING\r\n$/', $installs[0]);
        self::assertMatchesRegularExpression('/^UNSUB 2\r\nSUB _INBOX\.[0-9a-f]{24}\.\* 3\r\nPING\r\n$/', $installs[1]);
    }

    public function testALaterRequestManySubscribesTheMuxAgainOnceASlotIsFree(): void
    {
        $server = $this->echoServer(limit: 1);
        $connection = $this->connect($server);
        $appSid = $this->subscribe($connection, 'app.updates');

        try {
            $connection->requestMany('svc.echo', 'one', null, 1, 1_000)->await();
            self::fail('expected the requestMany to fail: the server rejected its reply inbox');
        } catch (ConnectionException $e) {
            self::assertSame(self::LIMIT_ERROR, $e->getMessage());
        }

        $connection->unsubscribe($appSid)->await();

        $replies = $connection->requestMany('svc.echo', 'two', null, 1, 1_000)->await();
        self::assertSame(['echo:two'], self::payloads($replies));
    }

    /**
     * While the limit holds, a later request fails fast instead of waiting out its timeout, and is not sent:
     * after the drop it waits for the server to confirm the new mux, and the server rejects that SUB again.
     */
    public function testWhileTheLimitHoldsALaterRequestFailsFastWithoutBeingSent(): void
    {
        $server = $this->echoServer(limit: 1);
        $connection = $this->connect($server);
        $this->subscribe($connection, 'app.updates');
        try {
            $connection->request('svc.echo', 'one', 2_000)->await();
            self::fail('expected the first request to fail');
        } catch (ConnectionException) {
            // The server rejected its reply inbox.
        }

        $start = hrtime(true);
        try {
            $connection->request('svc.echo', 'two', 10_000)->await();
            self::fail('expected the second request to fail');
        } catch (ConnectionException $e) {
            self::assertStringContainsString(self::MUX_DROPPED, $e->getMessage());
            self::assertStringContainsString('(maximum subscriptions exceeded)', $e->getMessage());
        }

        self::assertLessThan(2.0, self::secondsSince($start), 'it did not wait out its ten-second timeout');
        self::assertCount(2, self::muxInstallWrites($server), 'it tried to subscribe the mux again');
        self::assertCount(1, $server->controlLines('PUB svc.echo'), 'only the first request, sent before the server answered its SUB, went out');
    }

    /**
     * A caller that retries at once on every failure: failing fast without a fence would send each attempt, many
     * times as often as when every attempt waited out its timeout. After the first, none is sent.
     */
    public function testACallerRetryingWhileTheLimitHoldsIsNotSentOnEveryAttempt(): void
    {
        $server = $this->echoServer(limit: 1);
        $connection = $this->connect($server);
        $this->subscribe($connection, 'app.updates');

        $failures = [];
        for ($attempt = 0; $attempt < 10; $attempt++) {
            try {
                $connection->request('svc.echo', 'x', 250)->await();
            } catch (ConnectionException|TimeoutException $e) {
                $failures[] = $e::class;
            }
        }

        self::assertSame(array_fill(0, 10, ConnectionException::class), $failures);
        self::assertCount(1, $server->controlLines('PUB svc.echo'), 'only the first attempt was sent');
        self::assertCount(10, self::muxInstallWrites($server), 'each attempt tried to subscribe the mux');
    }

    /**
     * A request whose own read meets the server's renewed rejection, while it waits for the new mux, fails with the
     * dropped-inbox error, the -ERR as its previous exception, and the -ERR is not reported on top: a caller that
     * retries while the limit holds does not get a report per attempt.
     */
    public function testARenewedRejectionFailsTheRequestAndIsNotReportedOnTop(): void
    {
        $server = $this->echoServer(limit: 1);
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($server, recorder: $recorder);
        $this->subscribe($connection, 'app.updates');
        try {
            $connection->request('svc.echo', 'one', 1_000)->await();
            self::fail('expected the first request to fail');
        } catch (ConnectionException) {
            // The server rejected its reply inbox.
        }

        for ($attempt = 0; $attempt < 3; $attempt++) {
            try {
                $connection->request('svc.echo', 'again', 1_000)->await();
                self::fail('expected the request to fail while the limit holds');
            } catch (ConnectionException $e) {
                self::assertStringContainsString(self::MUX_DROPPED, $e->getMessage());
                self::assertSame(self::LIMIT_ERROR, $e->getPrevious()?->getMessage());
            }
        }

        self::assertSame([], $recorder->errorsContaining('maximum subscriptions exceeded'));
    }

    /**
     * With slowConsumerErrorsFailOperations, another subscription's overflow fails a request that waits for the new
     * mux to be confirmed, as it fails one that waits for its reply, and the request is not sent.
     */
    public function testAnOverflowWhileARequestWaitsForTheNewMuxFailsItWhenConfiguredTo(): void
    {
        $server = $this->echoServer(limit: 2);
        $client = new NatsClient(new NatsOptions(
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            maxPendingMessagesPerSubscription: 2,
            slowConsumerPolicy: SlowConsumerPolicy::Error,
            slowConsumerErrorsFailOperations: true,
        ), $server);
        $this->opened[] = $client;
        $client->connect()->await();
        $queue = $client->subscribeQueue('backlog')->await();
        $server->pushFrame("MSG backlog {$queue->sid} 2\r\nb1\r\nMSG backlog {$queue->sid} 2\r\nb2\r\n");
        $client->processIncoming()->await();
        // The queue and this subscription hold both slots, so the first request's mux is rejected.
        $appSid = $client->subscribe('app.one', static function (): void {})->await();
        try {
            $client->request('svc.echo', 'one', 1_000)->await();
            self::fail('expected the first request to fail');
        } catch (ConnectionException) {
            // The server rejected its reply inbox.
        }
        $client->unsubscribe($appSid)->await();

        // The server's answers to the new mux SUB are held while a third message for the full queue arrives.
        $server->holdAnswers();
        $request = $client->request('svc.echo', 'two', 1_000);
        delay(0.02);
        $server->pushFrame("MSG backlog {$queue->sid} 2\r\nb3\r\n");
        delay(0.02);
        $server->releaseAnswers();

        try {
            $request->await();
            self::fail('expected the request to fail with the overflow');
        } catch (SlowConsumerException $e) {
            self::assertSame($queue->sid, $e->sid);
        }
        self::assertCount(1, $server->controlLines('PUB svc.echo'), 'the second request was not sent');
    }

    /** @return iterable<string, array{bool}> */
    public static function handlerFailuresFailingOperationsOrNot(): iterable
    {
        yield 'operations report handler failures (default)' => [false];
        yield 'handler failures fail operations' => [true];
    }

    /**
     * Another subscription's handler that throws while a request waits for the new mux to be confirmed is reported,
     * and the request is sent once the mux is confirmed and gets its reply (#173). With handlerErrorsFailOperations
     * it fails the request, as it fails one that waits for its reply, and the request is not sent.
     */
    #[DataProvider('handlerFailuresFailingOperationsOrNot')]
    public function testAHandlerFailureWhileARequestWaitsForTheNewMuxIsReportedUnlessConfiguredToFail(bool $failOperations): void
    {
        $server = $this->echoServer(limit: 2);
        $recorder = new LifecycleRecorder();
        $client = new NatsClient(new NatsOptions(
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
            handlerErrorsFailOperations: $failOperations,
        ), $server);
        $this->opened[] = $client;
        $client->connect()->await();
        $poison = $client->subscribe('poison', static function (): void {
            throw new \RuntimeException('poison handler');
        })->await();
        // This subscription and the poisoned one hold both slots, so the first request's mux is rejected.
        $appSid = $client->subscribe('app.one', static function (): void {})->await();
        try {
            $client->request('svc.echo', 'one', 1_000)->await();
            self::fail('expected the first request to fail');
        } catch (ConnectionException) {
            // The server rejected its reply inbox.
        }
        $client->unsubscribe($appSid)->await();

        // The server's answers to the new mux SUB are held while a message for the poisoned subscription arrives.
        $server->holdAnswers();
        $request = $client->request('svc.echo', 'two', 1_000);
        delay(0.02);
        $server->pushFrame("MSG poison {$poison} 2\r\np1\r\n");
        delay(0.02);
        $server->releaseAnswers();

        if ($failOperations) {
            try {
                $request->await();
                self::fail('expected the request to fail with the handler failure');
            } catch (\RuntimeException $e) {
                self::assertSame('poison handler', $e->getMessage());
            }
            self::assertCount(1, $server->controlLines('PUB svc.echo'), 'the second request was not sent');
            self::assertSame([], $recorder->errorsContaining('poison handler'));

            return;
        }

        self::assertSame('echo:two', $request->await()->payload);
        self::assertSame(['poison handler'], $recorder->errorsContaining('poison handler'));
    }

    /**
     * Another -ERR the server keeps the connection open for, read while a request waits for the new mux, is not
     * taken for the mux's rejection: the request gets its reply or fails with that -ERR, as any read does, never with
     * the dropped-inbox error, and the next request is sent on the same mux.
     */
    public function testAnotherErrWhileARequestWaitsForTheNewMuxIsNotTakenForItsRejection(): void
    {
        $server = $this->echoServer(limit: 1);
        $connection = $this->connect($server);
        $appSid = $this->subscribe($connection, 'app.updates');
        try {
            $connection->request('svc.echo', 'one', 1_000)->await();
            self::fail('expected the first request to fail');
        } catch (ConnectionException) {
            // The server rejected its reply inbox.
        }
        $connection->unsubscribe($appSid)->await();

        $server->holdAnswers();
        $request = $connection->request('svc.echo', 'two', 1_000);
        delay(0.02);
        $server->pushFrame("-ERR 'Invalid Publish Subject'\r\n");
        delay(0.02);
        $server->releaseAnswers();

        try {
            $outcome = $request->await()->payload;
        } catch (ConnectionException $e) {
            $outcome = $e->getMessage();
        }
        self::assertContains($outcome, ['echo:two', "Server sent error frame: 'Invalid Publish Subject'"]);
        self::assertSame('echo:three', $connection->request('svc.echo', 'three', 1_000)->await()->payload);
        self::assertCount(2, self::muxInstallWrites($server), 'the mux was kept');
    }

    /**
     * The read a request makes while it waits for the new mux brings the server's renewed rejection and then an -ERR
     * after which the server closes the connection: the request fails with that -ERR, which says why the connection
     * is gone, rather than with the dropped inbox and a promise that the next request subscribes it again.
     */
    public function testAnErrEndingTheConnectionWhileARequestWaitsForTheNewMuxFailsItWithThatErr(): void
    {
        $server = $this->echoServer(limit: 1);
        $connection = $this->connect($server);
        $appSid = $this->subscribe($connection, 'app.updates');
        try {
            $connection->request('svc.echo', 'one', 1_000)->await();
            self::fail('expected the first request to fail');
        } catch (ConnectionException) {
            // The server rejected its reply inbox.
        }
        $connection->unsubscribe($appSid)->await();

        $server->holdAnswers();
        $request = $connection->request('svc.echo', 'two', 1_000);
        delay(0.02);
        $server->pushFrame("-ERR 'maximum subscriptions exceeded'\r\n-ERR 'Stale Connection'\r\n");

        try {
            $request->await();
            self::fail('expected the request to fail');
        } catch (ConnectionException $e) {
            self::assertSame("Server sent error frame: 'Stale Connection'", $e->getMessage());
        }
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * A request waiting for the new mux over a transport whose reads return at once when nothing has arrived sleeps
     * between those reads, as it does while it waits for its reply, so that the timer bringing the server's answer
     * gets to run.
     */
    public function testARequestWaitingForTheNewMuxSleepsBetweenIdleReads(): void
    {
        $server = $this->echoServer(limit: 1);
        $connection = $this->connect($server);
        $appSid = $this->subscribe($connection, 'app.updates');
        try {
            $connection->request('svc.echo', 'one', 1_000)->await();
            self::fail('expected the first request to fail');
        } catch (ConnectionException) {
            // The server rejected its reply inbox.
        }
        $connection->unsubscribe($appSid)->await();

        $server->readsReturnWhenIdle = true;
        $server->holdAnswers();
        EventLoop::delay(0.03, static function () use ($server): void {
            $server->releaseAnswers();
        });

        self::assertSame('echo:two', $connection->request('svc.echo', 'two', 1_000)->await()->payload);
    }

    /**
     * A request waiting for the new mux to be confirmed is sent as soon as the PONG that confirms it is in, whenever
     * another fiber's read - an application's processIncoming() - starts around it and takes that PONG. The wait checks
     * for the confirmation before each read, and its read runs on a fiber of its own; another fiber's read that started
     * just before that fiber took the PONG and freed the read slot, and the wait's read then waited on a socket with
     * nothing more to come. The request failed when its budget ran out, "while waiting for the reply inbox to be set
     * up", although the server held the mux. Where the window falls depends on how many event-loop hops each call
     * takes, so the other read starts after each of 0 to 16 hops, before the request and after it.
     */
    public function testARequestWaitingForTheNewMuxIsSentOnceAnotherFibersReadBringsItsConfirmation(): void
    {
        for ($hops = -16; $hops <= 16; $hops++) {
            $server = $this->echoServer(limit: 1);
            $connection = $this->connect($server);
            $appSid = $this->subscribe($connection, 'app.updates');
            try {
                $connection->request('svc.echo', 'one', 1_000)->await();
                self::fail('expected the first request to fail');
            } catch (ConnectionException) {
                // The server rejected its reply inbox.
            }
            $connection->unsubscribe($appSid)->await();

            $calls = [
                'request' => static fn(): Future => $connection->request('svc.echo', 'two', 10_000),
                'read' => static fn(): Future => $connection->processIncoming(new TimeoutCancellation(2)),
            ];
            // A negative count makes the other read first and the request $hops hops after it.
            [$first, $second] = $hops < 0 ? ['read', 'request'] : ['request', 'read'];
            $start = hrtime(true);
            /** @var array<string, Future<mixed>> $futures */
            $futures = [$first => $calls[$first]()];
            $made = new DeferredFuture();
            self::afterHops(abs($hops), static function () use (&$futures, $calls, $second, $made): void {
                $futures[$second] = $calls[$second]();
                $made->complete();
            });
            $made->getFuture()->await();
            $futures['read']->ignore();

            $reply = $futures['request']->await();
            self::assertInstanceOf(NatsMessage::class, $reply);
            self::assertSame('echo:two', $reply->payload);
            self::assertLessThan(
                2.0,
                self::secondsSince($start),
                sprintf('with the other read %d hop(s) %s the request, it waited out its ten-second budget', abs($hops), $hops < 0 ? 'before' : 'after'),
            );
            $connection->disconnect()->await();
        }
    }

    /** Two first requests race: one subscribes the mux, the other joins it, and both fail fast. */
    public function testConcurrentFirstRequestsBothFailFast(): void
    {
        $server = $this->echoServer(limit: 1);
        $connection = $this->connect($server);
        $this->subscribe($connection, 'app.updates');

        $start = hrtime(true);
        [$errors] = awaitAll([
            $connection->request('svc.echo', 'a', 2_000),
            $connection->request('svc.echo', 'b', 2_000),
        ]);

        self::assertCount(2, $errors);
        foreach ($errors as $error) {
            self::assertInstanceOf(ConnectionException::class, $error, $error::class . ': ' . $error->getMessage());
            self::assertStringContainsString('maximum subscriptions exceeded', $error->getMessage());
        }
        self::assertLessThan(1.0, self::secondsSince($start));
        self::assertCount(1, self::muxInstallWrites($server), 'one SUB for both');
    }

    /**
     * An application loop owns the read and gets the -ERR: the request still fails fast, and once a slot is free
     * the next request subscribes again. The request used to time out, the -ERR having gone to the other fiber.
     */
    public function testARejectionReadByAnotherFiberFailsTheRequestFast(): void
    {
        $server = $this->echoServer(limit: 1);
        $connection = $this->connect($server);
        $appSid = $this->subscribe($connection, 'app.updates');
        [$stop, $loop] = $this->startReadLoop($connection);

        try {
            $start = hrtime(true);
            try {
                $connection->request('svc.echo', 'one', 10_000)->await();
                self::fail('expected the request to fail');
            } catch (ConnectionException $e) {
                self::assertStringContainsString(self::MUX_DROPPED, $e->getMessage());
            }
            self::assertLessThan(2.0, self::secondsSince($start), 'it did not wait out its ten-second timeout');

            $connection->unsubscribe($appSid)->await();
            self::assertSame('echo:two', $connection->request('svc.echo', 'two', 1_000)->await()->payload);
        } finally {
            $stop->cancel();
            $loop->await();
        }
    }

    /**
     * The server's answer arrives after the request timed out, and the heartbeat reads it: the mux is dropped all
     * the same, and the next request subscribes it again. It used to be sent on the rejected mux and time out.
     */
    public function testARejectionReadByTheHeartbeatLetsTheNextRequestSubscribeAgain(): void
    {
        $server = $this->echoServer(limit: 1);
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($server, pingIntervalSeconds: 0.05, recorder: $recorder);
        $appSid = $this->subscribe($connection, 'app.updates');

        $server->holdAnswers();
        try {
            $connection->request('svc.echo', 'one', 30)->await();
            self::fail('expected the request to time out: the server has not answered yet');
        } catch (TimeoutException) {
            // The -ERR rejecting its reply inbox is still on its way.
        }
        $server->releaseAnswers();
        $this->waitUntil(static fn(): bool => $recorder->errorsContaining('maximum subscriptions exceeded') !== []);

        $connection->unsubscribe($appSid)->await();
        self::assertSame('echo:two', $connection->request('svc.echo', 'two', 1_000)->await()->payload);
        self::assertCount(2, self::muxInstallWrites($server));
    }

    /**
     * A service's loop owns the read and reports the -ERR instead of throwing it: a request from another fiber
     * still fails fast, and once a slot is free the next one subscribes the mux again.
     */
    public function testARejectionReadByAServiceLoopFailsTheRequestFast(): void
    {
        $server = $this->echoServer();
        $recorder = new LifecycleRecorder();
        $client = new NatsClient(new NatsOptions(
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            errorListener: $recorder->errorListener(),
        ), $server);
        $this->opened[] = $client;
        $client->connect()->await();
        $service = $client->service('work', '1.0.0')
            ->addEndpoint('work', 'svc.work', static fn(NatsMessage $message): string => 'done');
        $service->start()->await();
        // The service holds every slot the connection gets.
        $server->limit = count($server->heldSubscriptions());
        $stop = new DeferredCancellation();
        $running = $service->run(cancellation: $stop->getCancellation());
        delay(0.01);

        try {
            $start = hrtime(true);
            try {
                $client->request('svc.echo', 'one', 10_000)->await();
                self::fail('expected the request to fail');
            } catch (ConnectionException $e) {
                self::assertStringContainsString(self::MUX_DROPPED, $e->getMessage());
            }
            self::assertLessThan(2.0, self::secondsSince($start), 'it did not wait out its ten-second timeout');
            self::assertNotSame([], $recorder->errorsContaining('maximum subscriptions exceeded'), 'the loop reported the -ERR');

            $server->limit = PHP_INT_MAX;
            self::assertSame('echo:two', $client->request('svc.echo', 'two', 1_000)->await()->payload);
        } finally {
            $stop->cancel();
            $running->await();
        }
    }

    /**
     * The PONG of the PING behind the mux SUB confirms the mux before anything is delivered on it. Another
     * subscription's rejection then leaves it alone: requests in flight on it, waiting for a slow service, get
     * their replies, read here by an application loop.
     */
    public function testRequestsInFlightSurviveAnotherSubscriptionsRejectionReadByAnotherFiber(): void
    {
        [$server, $pending] = $this->slowServer(limit: 2);
        $connection = $this->connect($server);
        $this->subscribe($connection, 'app.one');
        [$stop, $loop] = $this->startReadLoop($connection);

        try {
            $requests = [];
            for ($i = 0; $i < 3; $i++) {
                $requests[] = $connection->request('svc.slow', (string) $i, 2_000);
            }
            delay(0.05);
            // Rejected: app.one and the mux hold both slots.
            $this->subscribe($connection, 'app.two');
            delay(0.05);
            $this->answerPending($server, $pending);

            [$errors, $replies] = awaitAll($requests);
        } finally {
            $stop->cancel();
            $loop->await();
        }

        self::assertSame([], $errors);
        self::assertSame(['late', 'late', 'late'], self::payloads($replies));
        self::assertCount(1, self::muxInstallWrites($server));
        self::assertCount(1, self::heldMuxes($server));
    }

    /**
     * As above, with the requests reading for themselves: the one whose read meets the -ERR fails with it, as any
     * read does, and the others get their replies.
     */
    public function testRequestsInFlightReadingForThemselvesSurviveAnotherSubscriptionsRejection(): void
    {
        [$server, $pending] = $this->slowServer(limit: 2);
        $connection = $this->connect($server);
        $this->subscribe($connection, 'app.one');

        $requests = [];
        for ($i = 0; $i < 3; $i++) {
            $requests[] = $connection->request('svc.slow', (string) $i, 2_000);
        }
        delay(0.05);
        $this->subscribe($connection, 'app.two');
        delay(0.05);
        $this->answerPending($server, $pending);

        [$errors, $replies] = awaitAll($requests);
        self::assertGreaterThanOrEqual(2, count($replies), 'the requests that did not read the -ERR got their replies');
        foreach ($errors as $error) {
            self::assertSame(self::LIMIT_ERROR, $error->getMessage());
        }
        self::assertCount(1, self::muxInstallWrites($server));
    }

    /** @return iterable<string, array{bool}> */
    public static function drainsDuringTheSetUp(): iterable
    {
        yield 'while the first mux SUB is written' => [false];
        yield 'while the request waits for a new mux to be confirmed' => [true];
    }

    /**
     * A drain() that begins while a request sets the mux up - while its SUB is written, held up by backpressure, or
     * while the request waits for a new mux to be confirmed - keeps that mux subscribed while it delivers (#213): the
     * request is sent once its set-up is over and gets its reply, the drain's delivery phase held open by a
     * participant still handing over. The drain unsubscribes the mux once that phase is over, behind the request. The
     * rejection protection holds: after a drop the request is still sent only once the server has confirmed the new
     * mux. Until #213 the drain unsubscribed the mux at once and the request failed with "Connection is not open"
     * without being sent; a handler's ackSync() during the drain failed the same way.
     */
    #[DataProvider('drainsDuringTheSetUp')]
    public function testARequestWhoseMuxSetUpADrainOvertakesIsSentWhileTheDrainDelivers(bool $afterADrop): void
    {
        $server = $this->echoServer(limit: 1);
        $connection = $this->connect($server);
        $participant = new HeldDrainParticipant();
        $connection->addDrainParticipant($participant);
        $sent = $afterADrop ? $this->dropTheMuxAndFreeASlot($connection, $server) : 0;
        if (!$afterADrop) {
            $server->stallNextWriteContaining('SUB _INBOX.', 30.0);
        }

        $request = $connection->request('svc.echo', 'two', 3_000);
        if ($afterADrop) {
            // The request has written the new mux SUB and the PING behind it, and waits for the PONG.
            $this->waitUntil(static fn(): bool => count(self::muxInstallWrites($server)) === 2);
        } else {
            $this->waitUntil(static fn(): bool => $server->writesStalled() === 1);
        }
        $drain = $connection->drain();
        if ($afterADrop) {
            // The handshake's, the two mux SUBs' and the drain's.
            $this->waitUntil(static fn(): bool => count($server->controlLines('PING')) === 4);
            // The PONG for the mux's PING, then the one for the drain's.
            $server->pushFrame("PONG\r\n");
            delay(0.02);
            $server->pushFrame("PONG\r\n");
        } else {
            // The drain's PING waits behind the mux SUB, as bytes on a socket do.
            delay(0.02);
            $server->releaseStalledWrites();
        }

        self::assertSame('echo:two', $request->await(new TimeoutCancellation(2))->payload, 'sent and answered while the drain delivers');
        $this->waitUntil(static fn(): bool => $participant->asked === 1);
        self::assertFalse($drain->isComplete(), 'the participant holds the delivery phase open');
        $participant->release();
        $drain->await(new TimeoutCancellation(2));

        self::assertCount($sent + 1, $server->controlLines('PUB svc.echo'));
        $mux = self::muxSidOf(self::muxInstallWrites($server)[$sent]);
        $lines = $server->controlLines();
        $pubs = array_keys(array_filter($lines, static fn(string $line): bool => str_starts_with($line, 'PUB svc.echo ')));
        $releases = array_keys(array_filter($lines, static fn(string $line): bool => $line === 'UNSUB ' . $mux));
        self::assertCount(1, $releases, 'the drain released the mux once');
        self::assertGreaterThan(max([-1, ...$pubs]), $releases[0], 'the mux was released behind the request');
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * The same set-ups, outlasting the drain's delivery phase, which its deadline ends (a budget of 300 ms): the mux SUB
     * still held up, or the new mux still unconfirmed, the server never answering the drain's PING. The request ends with
     * the phase, wherever it waits, with "Connection is not open", and is not sent; the drain closes within its budget.
     * A SUB still being written is not followed by an UNSUB that could overtake it: the close releases what the server
     * holds.
     */
    #[DataProvider('drainsDuringTheSetUp')]
    public function testARequestWhoseMuxSetUpOutlastsTheDrainsDeliveryIsNotSent(bool $afterADrop): void
    {
        $server = $this->echoServer(limit: 1);
        $connection = $this->connect($server, requestTimeoutMs: 300);
        $sent = $afterADrop ? $this->dropTheMuxAndFreeASlot($connection, $server) : 0;
        if (!$afterADrop) {
            $server->stallNextWriteContaining('SUB _INBOX.', 30.0);
        }

        $request = $connection->request('svc.echo', 'two', 5_000);
        if ($afterADrop) {
            $this->waitUntil(static fn(): bool => count(self::muxInstallWrites($server)) === 2);
        } else {
            $this->waitUntil(static fn(): bool => $server->writesStalled() === 1);
        }
        $unsubscribes = count($server->controlLines('UNSUB'));
        $start = hrtime(true);
        $connection->drain()->await(new TimeoutCancellation(5));
        $drainedIn = self::secondsSince($start);

        try {
            $request->await(new TimeoutCancellation(1));
            self::fail('expected the request to fail: the drain\'s delivery phase ended before it could be sent');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }
        self::assertLessThan(2.0, $drainedIn, sprintf('the drain took %.3f s of its 300 ms budget', $drainedIn));
        self::assertCount($sent, $server->controlLines('PUB svc.echo'), 'the request was not sent');
        $released = array_slice($server->controlLines('UNSUB'), $unsubscribes);
        if ($afterADrop) {
            // The new mux's SUB is out: the drain's release of it may follow it, when the drain's flush ended a hair
            // before its deadline as the event loop measures it, the delivery phase then over with time to spare.
            self::assertContains($released, [[], ['UNSUB ' . self::muxSidOf(self::muxInstallWrites($server)[1])]]);
        } else {
            self::assertSame([], $released, 'no UNSUB ahead of the SUB still on its way');
        }
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /** @return iterable<string, array{string}> */
    public static function flushesBesideTheFirstRequest(): iterable
    {
        yield 'flush()' => ['flush'];
        yield 'rtt()' => ['rtt'];
        yield 'drainSubscription()' => ['drainSubscription'];
        yield "a service's drain()" => ['service'];
    }

    /**
     * A flush started just before the first request - flush(), rtt(), drainSubscription() or a service's drain() -
     * does not leave the mux open to another subscription's rejection. The flush writes its PING from a fiber of its
     * own, and its pong slot used to be queued before that fiber ran: when the request wrote the mux SUB and the PING
     * behind it meanwhile, that PING reached the wire first and its PONG completed the flush's slot. The mux then
     * stayed unconfirmed, and the rejection of a SUB written between the two PINGs dropped the mux the server held:
     * the request failed with the dropped-inbox error, and its reply was discarded. What the flush unsubscribes is not
     * held by the server, so the mux always takes the connection's last slot. Where the window falls depends on how
     * many event-loop hops the flush takes before its PING, so the request starts after each count of hops, from 0
     * until it comes after the flush's PING. A request whose own read meets the -ERR fails with it, as any read does.
     * The rejected SUB is written right behind the mux SUB and its PING: the mux SUB goes out from a writer of its own,
     * within the request's budget (#194, #213), so a subscribe made in the same step as the request would be written
     * ahead of it.
     */
    #[DataProvider('flushesBesideTheFirstRequest')]
    public function testAFlushStartedBesideTheFirstRequestDoesNotLeaveTheMuxOpenToAnotherSubscriptionsRejection(string $flush): void
    {
        for ($hops = 0; $hops <= 500; $hops++) {
            $server = $this->echoServer(limit: 1);
            $client = new NatsClient(new NatsOptions(connectTimeoutMs: 500, reconnectEnabled: false, pingIntervalSeconds: 0), $server);
            $this->opened[] = $client;
            $client->connect()->await();
            $client->subscribe('app.updates', static function (): void {})->await();
            [$stop, $loop] = $this->startReadLoop($client);

            try {
                // Rejected at the limit of 1, so the UNSUBs of the flush below free no slot.
                $drainedSid = $flush === 'drainSubscription' ? $client->subscribe('app.drained', static function (): void {})->await() : 0;
                $service = $flush === 'service'
                    ? $client->service('work', '1.0.0')->addEndpoint('work', 'svc.work', static fn(NatsMessage $message): string => 'done')
                    : null;
                $service?->start()->await();
                $this->flushPastTheRejections($client);
                $server->limit = 2;
                $writesBefore = count($server->writes);

                if ($service !== null) {
                    $flushing = $service->drain();
                } elseif ($flush === 'drainSubscription') {
                    $flushing = $client->drainSubscription($drainedSid);
                } else {
                    $flushing = $flush === 'rtt' ? $client->rtt() : $client->flush();
                }
                $issued = new class {
                    /** @var Future<NatsMessage>|null */
                    public ?Future $request = null;
                    /** @var Future<int>|null */
                    public ?Future $rejected = null;
                };
                // A SUB the server rejects, right behind the mux SUB and the PING written with it.
                $server->afterWrite = static function (string $bytes) use ($server, $client, $issued): void {
                    if (preg_match('/^SUB _INBOX\.\S+\.\* \d+\r$/m', $bytes) === 1) {
                        $server->afterWrite = null;
                        $issued->rejected = $client->subscribe('app.rejected', static function (): void {});
                    }
                };
                self::afterHops($hops, static function () use ($client, $issued): void {
                    $issued->request = $client->request('svc.echo', 'hello', 1_000);
                });
                $this->waitUntil(static fn(): bool => $issued->request !== null && $issued->rejected !== null);
                $request = $issued->request;
                $rejected = $issued->rejected;
                if ($request === null || $rejected === null) {
                    self::fail('the request or the rejected subscribe was never made');
                }
                $flushing->await();
                try {
                    $rejected->await();
                } catch (\Throwable) {
                    // The read that met the -ERR failed with it; the subscribe itself does not wait for an answer.
                }

                try {
                    self::assertSame('echo:hello', $request->await()->payload, sprintf('after %d hop(s)', $hops));
                } catch (ConnectionException $e) {
                    self::assertSame(self::LIMIT_ERROR, $e->getMessage(), sprintf('after %d hop(s): only its own read meeting the -ERR fails it', $hops));
                }
                self::assertCount(1, self::muxInstallWrites($server), sprintf('after %d hop(s): the mux was kept', $hops));
                self::assertCount(1, self::heldMuxes($server), sprintf('after %d hop(s)', $hops));
                self::assertSame('echo:again', $client->request('svc.echo', 'again', 1_000)->await()->payload);
            } finally {
                $stop->cancel();
                $loop->await();
            }

            $client->disconnect()->await();
            if (self::requestCameAfterTheFlushsPing($server, $writesBefore)) {
                // Later starts only move further past it.
                return;
            }
        }

        self::fail('the request never came after the flush\'s PING');
    }

    /** Another subscription's -ERR and the mux's first reply in one read: the reply behind the -ERR is delivered. */
    public function testTheFirstReplyBehindAnotherSubscriptionsRejectionInTheSameReadIsDelivered(): void
    {
        [$server, $pending] = $this->slowServer(limit: 10);
        $connection = $this->connect($server);
        [$stop, $loop] = $this->startReadLoop($connection);

        try {
            $request = $connection->request('svc.slow', 'a', 1_000);
            delay(0.05);
            $replyTos = $pending->getArrayCopy();
            self::assertCount(1, $replyTos);
            $server->pushFrame("-ERR 'maximum subscriptions exceeded'\r\n" . implode('', $server->replyFrame($replyTos[0], 'reply')));

            self::assertSame('reply', $request->await()->payload);
        } finally {
            $stop->cancel();
            $loop->await();
        }
    }

    /**
     * The mux's first reply and then another subscription's rejection in one read, while the server has not answered
     * the PING behind the mux SUB (it never does here): the reply confirms the mux as its frame is handled, ahead of
     * the -ERR, which then leaves the mux alone, and the reply is delivered.
     */
    public function testAFirstReplyAheadOfAnotherSubscriptionsRejectionInTheSameReadConfirmsTheMux(): void
    {
        [$server, $pending] = $this->slowServer(limit: 10);
        $connection = $this->connect($server);
        $server->answerPings = false;
        [$stop, $loop] = $this->startReadLoop($connection);

        try {
            $request = $connection->request('svc.slow', 'a', 1_000);
            delay(0.05);
            $replyTos = $pending->getArrayCopy();
            self::assertCount(1, $replyTos);
            $server->pushFrame(implode('', $server->replyFrame($replyTos[0], 'reply')) . "-ERR 'maximum subscriptions exceeded'\r\n");

            self::assertSame('reply', $request->await()->payload);
        } finally {
            $stop->cancel();
            $loop->await();
        }
        self::assertSame('echo:b', $connection->request('svc.echo', 'b', 1_000)->await()->payload);
        self::assertCount(1, self::muxInstallWrites($server), 'the mux was kept');
    }

    /**
     * An application that keeps retrying a subscribe the server rejects, faster than a service answers: dropping a
     * mux that has delivered nothing yet would drop each new one before it could, and request/reply would never
     * recover. The mux the server confirmed stays. A request whose own read meets one of the subscriber's -ERRs
     * fails with it, as any read does; none fails because its mux was dropped.
     */
    public function testASubscriberRetryingARejectedSubscribeDoesNotStarveRequests(): void
    {
        $server = new SubscriptionLimitServer();
        $server->limit = 2;
        /** @var \ArrayObject<int, string> $pending */
        $pending = new \ArrayObject();
        $server->responder = static function (string $subject, ?string $replyTo) use ($pending): array {
            if ($replyTo !== null) {
                $pending[] = $replyTo;
            }

            return [];
        };
        $connection = $this->connect($server);
        $this->subscribe($connection, 'app.one');
        [$stop, $loop] = $this->startReadLoop($connection);
        // The service answers each request 50 ms after it arrived.
        $service = async(function () use ($server, $pending, $stop): void {
            while (!$stop->getCancellation()->isRequested()) {
                if (count($pending) > 0) {
                    $replyTos = $pending->getArrayCopy();
                    $pending->exchangeArray([]);
                    EventLoop::delay(0.05, static function () use ($server, $replyTos): void {
                        foreach ($replyTos as $replyTo) {
                            foreach ($server->replyFrame($replyTo, 'late') as $frame) {
                                $server->pushFrame($frame);
                            }
                        }
                    });
                }
                delay(0.005);
            }
        });
        $subscriber = null;

        try {
            $first = $connection->request('svc.slow', '0', 1_000);
            // The mux is subscribed, and the server answered the PING behind it, before the first rejection.
            delay(0.01);
            $subscriber = async(function () use ($connection, $stop): void {
                while (!$stop->getCancellation()->isRequested()) {
                    $sid = $this->subscribe($connection, 'app.retry');
                    delay(0.01);
                    $connection->unsubscribe($sid)->await();
                    delay(0.01);
                }
            });

            $outcomes = [];
            for ($i = 0; $i < 5; $i++) {
                try {
                    $outcomes[] = ($i === 0 ? $first : $connection->request('svc.slow', (string) $i, 1_000))->await()->payload;
                } catch (ConnectionException $e) {
                    $outcomes[] = $e->getMessage();
                }
            }
        } finally {
            $stop->cancel();
            $loop->await();
            $service->await();
            $subscriber?->await();
        }

        foreach ($outcomes as $outcome) {
            self::assertContains($outcome, ['late', self::LIMIT_ERROR]);
        }
        self::assertContains('late', $outcomes);
        self::assertGreaterThan(1, count($server->controlLines('SUB app.retry')), 'the server rejected the subscriber more than once');
        self::assertCount(1, self::muxInstallWrites($server));
    }

    /**
     * A delivery on the mux confirms it too, here before the server answers the PING behind its SUB (it never
     * does): another subscription's rejection afterwards leaves the mux alone.
     */
    public function testADeliveryConfirmsTheMuxBeforeTheServerAnswersThePingBehindItsSub(): void
    {
        $server = $this->echoServer(limit: 2);
        $connection = $this->connect($server);
        $server->answerPings = false;
        $this->subscribe($connection, 'app.one');
        self::assertSame('echo:a', $connection->request('svc.echo', 'a', 1_000)->await()->payload);

        // Rejected: app.one and the mux hold both slots.
        $this->subscribe($connection, 'app.two');
        try {
            $connection->processIncoming()->await();
            self::fail('expected the read to fail with the rejection');
        } catch (ConnectionException $e) {
            self::assertSame(self::LIMIT_ERROR, $e->getMessage());
        }

        self::assertSame('echo:b', $connection->request('svc.echo', 'b', 1_000)->await()->payload);
        self::assertCount(1, self::muxInstallWrites($server), 'the mux was kept');
    }

    /**
     * Only the subscription limit's -ERR can be the mux's rejection: another -ERR the server keeps the connection
     * open for leaves even a mux it has not confirmed yet alone.
     */
    #[DataProvider('otherErrsTheServerKeepsTheConnectionOpenFor')]
    public function testAnotherErrTheServerKeepsTheConnectionOpenForLeavesAnUnconfirmedMuxAlone(string $error): void
    {
        [$server, $pending] = $this->slowServer(limit: PHP_INT_MAX);
        $connection = $this->connect($server);
        // Unconfirmed: the server does not answer the PING behind the mux SUB, and nothing is delivered yet.
        $server->answerPings = false;
        [$stop, $loop] = $this->startReadLoop($connection);

        try {
            $request = $connection->request('svc.slow', 'a', 1_000);
            delay(0.05);
            $server->pushFrame("-ERR '{$error}'\r\n");
            delay(0.05);
            $this->answerPending($server, $pending);

            self::assertSame('late', $request->await()->payload);
        } finally {
            $stop->cancel();
            $loop->await();
        }
        self::assertCount(1, self::muxInstallWrites($server));
    }

    /** @return iterable<string, array{string}> */
    public static function otherErrsTheServerKeepsTheConnectionOpenFor(): iterable
    {
        yield 'invalid publish subject' => ['Invalid Publish Subject'];
        yield 'reserved reply subject' => ['Permissions Violation for Publish with Reply of "_INBOX.reserved"'];
    }

    /**
     * Another subscription's rejection can still arrive ahead of the PONG behind the mux SUB, while the server
     * took the mux: then the client drops a mux the server holds. The next request's write unsubscribes it ahead
     * of the new SUB, so the server holds one mux and the slot the dropped one held is not lost.
     */
    public function testAMuxDroppedForAnotherSubscriptionsRejectionGivesItsSlotBackWithTheNextSub(): void
    {
        $server = $this->echoServer(limit: 2);
        $connection = $this->connect($server);
        $this->subscribe($connection, 'app.one');
        $twoSid = $this->subscribe($connection, 'app.two');
        // Rejected, its -ERR still unread; then a slot frees up, which the mux SUB takes.
        $this->subscribe($connection, 'app.three');
        $connection->unsubscribe($twoSid)->await();

        try {
            $connection->request('svc.echo', 'one', 1_000)->await();
            self::fail('expected the request to fail: the -ERR came before the PONG behind its mux SUB');
        } catch (ConnectionException $e) {
            self::assertSame(self::LIMIT_ERROR, $e->getMessage());
        }

        self::assertSame('echo:two', $connection->request('svc.echo', 'two', 1_000)->await()->payload);
        $installs = self::muxInstallWrites($server);
        self::assertCount(2, $installs);
        self::assertMatchesRegularExpression('/^UNSUB 4\r\nSUB _INBOX\.[0-9a-f]{24}\.\* 5\r\nPING\r\n$/', $installs[1]);
        self::assertCount(1, self::heldMuxes($server), 'the server holds one mux');
    }

    /**
     * Another subscription's rejection arrives while the first mux SUB still waits behind earlier writes, so the
     * client drops that mux before the server has seen it. A request issued then does not join the set-up of the
     * dropped mux and wait, until its timeout, for a confirmation nothing will send: it subscribes a new one, in a
     * write that follows the dropped one's on the socket and unsubscribes it, and gets its reply. The mux SUB is
     * held up until the second request has been made.
     */
    public function testARequestAfterTheMuxIsDroppedDuringItsSubWriteSubscribesANewOne(): void
    {
        $server = $this->echoServer();
        $connection = $this->connect($server);
        [$stop, $loop] = $this->startReadLoop($connection);
        $server->stallNextWriteContaining('SUB _INBOX.', 30.0);

        try {
            $first = $connection->request('svc.echo', 'one', 10_000);
            delay(0.02);
            // Read by the loop while the mux SUB is still held up.
            $server->pushFrame("-ERR 'maximum subscriptions exceeded'\r\n");
            delay(0.02);

            $start = hrtime(true);
            $second = $connection->request('svc.echo', 'two', 10_000);
            delay(0.01);
            $server->releaseStalledWrites();
            [$errors, $replies] = awaitAll(['first' => $first, 'second' => $second]);
            $seconds = self::secondsSince($start);
        } finally {
            $stop->cancel();
            $loop->await();
        }

        self::assertSame(['second'], array_keys($replies));
        self::assertSame('echo:two', $replies['second']->payload);
        self::assertLessThan(2.0, $seconds, 'the second request did not wait out its ten-second timeout');
        self::assertSame(['first'], array_keys($errors), 'the first request failed: its mux was dropped');
        self::assertStringContainsString(self::MUX_DROPPED, $errors['first']->getMessage());
        $installs = self::muxInstallWrites($server);
        self::assertCount(2, $installs);
        self::assertMatchesRegularExpression('/^UNSUB 1\r\nSUB _INBOX\.[0-9a-f]{24}\.\* 2\r\nPING\r\n$/', $installs[1]);
        self::assertCount(1, self::heldMuxes($server), 'the server holds one mux');
        self::assertCount(1, $server->controlLines('PUB svc.echo'), 'only the second request was sent');
    }

    /** Guard: a healthy mux is subscribed once and reused; its confirmation costs one PING and no wait. */
    public function testAHealthyMuxIsSubscribedOnceAndReused(): void
    {
        $server = $this->echoServer();
        $connection = $this->connect($server);

        for ($i = 0; $i < 3; $i++) {
            self::assertSame('echo:' . $i, $connection->request('svc.echo', (string) $i, 1_000)->await()->payload);
        }

        self::assertCount(1, self::muxInstallWrites($server));
        self::assertSame([], $server->controlLines('UNSUB'));
        self::assertSame(['PING', 'PING'], $server->controlLines('PING'), "the handshake's, and the one behind the mux SUB");
    }

    /**
     * A terminal close while the first request's mux SUB is still being written: the request reports the closed
     * connection, not a rejected inbox, and after a new connect() the next request subscribes the mux on the new
     * connection. A write that completed after the close used to leave the mux recorded, so that no request on the
     * next connection subscribed it and every one timed out.
     */
    #[DataProvider('closesDuringTheMuxSubWrite')]
    public function testACloseWhileTheMuxSubIsWrittenReportsTheClosedConnection(bool $completesAfterClose): void
    {
        $server = $this->echoServer();
        $connection = $this->connect($server);
        $server->stallNextWriteContaining('SUB _INBOX.', 0.05, $completesAfterClose);

        $request = $connection->request('svc.echo', 'one', 1_000);
        delay(0.01);
        $connection->disconnect()->await();

        try {
            $request->await();
            self::fail('expected the request to fail with the closed connection');
        } catch (ConnectionException $e) {
            self::assertMatchesRegularExpression(self::CLOSED, $e->getMessage());
        }

        $connection->connect()->await();
        self::assertSame('echo:two', $connection->request('svc.echo', 'two', 1_000)->await()->payload);
        self::assertCount(1, self::muxInstallWrites($server, epoch: 1), 'the mux was subscribed on the new connection');
    }

    /** @return iterable<string, array{bool}> */
    public static function closesDuringTheMuxSubWrite(): iterable
    {
        yield 'the write fails at the close' => [false];
        yield 'the write completes after the close' => [true];
    }

    /**
     * A request on a new connection does not wait for a mux set-up still under way for the connection a terminal
     * close ended: it subscribes the mux on the new connection. The request whose set-up outlived the close reports
     * the closed connection and is not sent on the new one. That set-up is held up until the new request is done.
     * The disconnect() ends the first request as it begins, with the request lifetime of its connection (#213), rather
     * than once its SUB write is let through: the request no longer waits for that write past the close.
     */
    public function testARequestAfterACloseAndConnectDoesNotJoinASetUpLeftFromTheClosedConnection(): void
    {
        $server = $this->echoServer();
        $connection = $this->connect($server);
        $server->stallNextWriteContaining('SUB _INBOX.', 30.0, completesAfterClose: true);
        $first = $connection->request('svc.echo', 'one', 10_000);
        delay(0.01);
        $connection->disconnect()->await();

        try {
            $first->await(new TimeoutCancellation(2));
            self::fail('expected the first request to fail with the closed connection');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }

        $connection->connect()->await();

        $start = hrtime(true);
        self::assertSame('echo:two', $connection->request('svc.echo', 'two', 10_000)->await()->payload);
        self::assertLessThan(2.0, self::secondsSince($start), 'it did not wait for the set-up of the closed connection');
        $server->releaseStalledWrites();
        delay(0.01);

        self::assertCount(1, $server->controlLines('PUB svc.echo', 1), 'only the second request was sent');
        self::assertSame([], $server->controlLines('UNSUB', 1), 'the abandoned set-up wrote nothing on the new connection');
    }

    /** A terminal close while a request waits for the server to confirm the mux: it reports the closed connection. */
    public function testACloseWhileARequestWaitsForTheMuxToBeConfirmedReportsTheClosedConnection(): void
    {
        $server = $this->echoServer(limit: 1);
        $connection = $this->connect($server);
        $appSid = $this->subscribe($connection, 'app.updates');
        try {
            $connection->request('svc.echo', 'one', 1_000)->await();
            self::fail('expected the first request to fail');
        } catch (ConnectionException) {
            // The server rejected its reply inbox.
        }
        $connection->unsubscribe($appSid)->await();
        // The PING behind the next mux SUB goes unanswered.
        $server->answerPings = false;

        $request = $connection->request('svc.echo', 'two', 2_000);
        delay(0.05);
        $connection->disconnect()->await();

        try {
            $request->await();
            self::fail('expected the request to fail with the closed connection');
        } catch (ConnectionException $e) {
            self::assertMatchesRegularExpression(self::CLOSED, $e->getMessage());
        }
        self::assertCount(1, $server->controlLines('PUB svc.echo'), 'the second request was not sent');
    }

    /**
     * A drop, a terminal close and a new connect(): the first request on the new connection subscribes the mux the
     * way any first request does, with nothing to unsubscribe from the closed connection, and is sent before the
     * server answers instead of waiting for the new mux to be confirmed.
     */
    public function testAConnectAfterADropSubscribesTheMuxAfresh(): void
    {
        $server = $this->echoServer(limit: 1);
        $connection = $this->connect($server);
        $this->subscribe($connection, 'app.updates');
        try {
            $connection->request('svc.echo', 'one', 1_000)->await();
            self::fail('expected the first request to fail');
        } catch (ConnectionException) {
            // The server rejected its reply inbox.
        }
        $connection->disconnect()->await();
        $connection->connect()->await();

        $server->holdAnswers();
        $request = $connection->request('svc.echo', 'two', 1_000);
        delay(0.05);
        $sentBeforeTheAnswers = count($server->controlLines('PUB svc.echo', 1));
        $server->releaseAnswers();

        self::assertSame('echo:two', $request->await()->payload);
        self::assertSame(1, $sentBeforeTheAnswers, 'it was sent before the server answered');
        $installs = self::muxInstallWrites($server, epoch: 1);
        self::assertCount(1, $installs);
        self::assertMatchesRegularExpression('/^SUB _INBOX\.[0-9a-f]{24}\.\* \d+\r\nPING\r\n$/', $installs[0]);
    }

    /**
     * A mux SUB whose write finds the socket dead is rolled back when the connection does not come back within the
     * request - here at once, since the request may not wait for the reconnect. Once the reconnect is done, the
     * next request subscribes the mux on the new connection instead of being sent on one nothing subscribed.
     */
    public function testAMuxSubWhoseWriteFailsIsRolledBackWhenTheConnectionDoesNotComeBackInTime(): void
    {
        $server = $this->echoServer();
        $connection = $this->connect($server, reconnect: true, waitForReconnect: false);
        $server->dropConnection();

        try {
            $connection->request('svc.echo', 'one', 1_000)->await();
            self::fail('expected the request to fail: it may not wait for the reconnect');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }

        $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Open);
        self::assertSame([], self::muxInstallWrites($server, epoch: 1), 'the reconnect did not subscribe the abandoned mux');
        self::assertSame('echo:two', $connection->request('svc.echo', 'two', 1_000)->await()->payload);
        self::assertCount(1, self::muxInstallWrites($server, epoch: 1));
    }

    /**
     * The mux was in place and has answered; the limit is lowered and the connection drops, and the reconnect's
     * replay of the mux is rejected. The reconnect completes, and once a slot is free the next request subscribes
     * the mux again. It used to be sent on the rejected mux and time out.
     */
    public function testAReconnectWhoseReplayedMuxIsRejectedLeavesTheNextRequestWorkingOnceASlotIsFree(): void
    {
        $server = $this->echoServer(limit: 2);
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($server, reconnect: true, recorder: $recorder);
        $appSid = $this->subscribe($connection, 'app.updates');
        self::assertSame('echo:one', $connection->request('svc.echo', 'one', 1_000)->await()->payload);

        $server->limit = 1;
        $server->dropConnection();
        try {
            $connection->processIncoming()->await();
        } catch (\Throwable) {
            // The read that met the EOF; the reconnect is what matters.
        }
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $server->epoch());
        self::assertNotSame([], $recorder->errorsContaining('maximum subscriptions exceeded'), 'the replayed mux SUB was rejected');
        self::assertSame(['app.updates'], array_values($server->heldSubscriptions()));

        $connection->unsubscribe($appSid)->await();
        self::assertSame('echo:two', $connection->request('svc.echo', 'two', 1_000)->await()->payload);
    }

    /**
     * Over the limit after a reconnect, the server rejects the subscriptions replayed after the mux, which keeps
     * its slot: the PING replayed right behind it confirms it before their -ERRs arrive. A request in flight across
     * the reconnect gets its reply, and the mux is not unsubscribed.
     */
    public function testAReplayOverTheLimitKeepsAMuxThatKeptItsSlot(): void
    {
        [$server, $pending] = $this->slowServer(limit: 3);
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($server, reconnect: true, recorder: $recorder);
        self::assertSame('echo:a', $connection->request('svc.echo', 'a', 1_000)->await()->payload);
        $this->subscribe($connection, 'app.one');
        $this->subscribe($connection, 'app.two');

        $inFlight = $connection->request('svc.slow', 'b', 2_000);
        delay(0.02);
        $server->limit = 2;
        $server->dropConnection();
        $this->waitUntil(static fn(): bool => $server->epoch() === 1 && $connection->state() === ConnectionState::Open);
        $this->answerPending($server, $pending);

        self::assertSame('late', $inFlight->await()->payload);
        self::assertNotSame([], $recorder->errorsContaining('maximum subscriptions exceeded'), 'app.two was rejected');
        self::assertSame([1, 2], array_keys($server->heldSubscriptions()), 'the mux and app.one kept their slots');
        self::assertSame([], $server->controlLines('UNSUB'), 'the mux was not unsubscribed');
        self::assertSame('echo:c', $connection->request('svc.echo', 'c', 1_000)->await()->payload);
        self::assertCount(1, self::muxInstallWrites($server, epoch: 1), 'only the replay subscribed it');
    }

    /**
     * Once the mux subscribed after a drop is confirmed, requests stop waiting for a confirmation: after a later
     * reconnect a request is sent before the server answers the PING replayed behind the mux SUB (it never does
     * here), as it is when nothing was ever dropped, and its reply confirms the mux.
     */
    public function testOnceTheNewMuxIsConfirmedRequestsNoLongerWaitForAConfirmation(): void
    {
        $server = $this->echoServer(limit: 1);
        $connection = $this->connect($server, reconnect: true);
        $appSid = $this->subscribe($connection, 'app.updates');
        try {
            $connection->request('svc.echo', 'one', 1_000)->await();
            self::fail('expected the first request to fail');
        } catch (ConnectionException) {
            // The server rejected its reply inbox.
        }
        $connection->unsubscribe($appSid)->await();
        self::assertSame('echo:two', $connection->request('svc.echo', 'two', 1_000)->await()->payload);

        $server->answerPings = false;
        $server->dropConnection();
        try {
            $connection->processIncoming()->await();
        } catch (\Throwable) {
            // The read that met the EOF; the reconnect is what matters.
        }
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $server->epoch());

        self::assertSame('echo:three', $connection->request('svc.echo', 'three', 1_000)->await()->payload);
    }

    /**
     * The socket died without the client noticing: the first request's mux SUB write fails, the reconnect it starts
     * replays the SUB, and the new connection rejects it for the limit. The request reports that, rather than a
     * closed connection or a timeout, and once a slot is free the next request subscribes the mux again.
     */
    public function testAMuxSubWhoseReplayAfterAFailedWriteIsRejectedForTheLimit(): void
    {
        $server = $this->echoServer(limit: 1);
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($server, reconnect: true, recorder: $recorder);
        $appSid = $this->subscribe($connection, 'app.one');
        $server->dropConnection();

        $start = hrtime(true);
        try {
            $connection->request('svc.echo', 'one', 10_000)->await();
            self::fail('expected the request to fail');
        } catch (ConnectionException $e) {
            self::assertStringContainsString(self::MUX_DROPPED, $e->getMessage());
        }
        self::assertLessThan(2.0, self::secondsSince($start), 'it did not wait out its ten-second timeout');
        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $server->epoch());
        self::assertNotSame([], $recorder->errorsContaining('maximum subscriptions exceeded'));

        $connection->unsubscribe($appSid)->await();
        self::assertSame('echo:two', $connection->request('svc.echo', 'two', 1_000)->await()->payload);
    }

    /**
     * The reply-inbox wildcard is denied and the socket died without the client noticing: the first request's mux SUB
     * write fails, the reconnect it starts replays the SUB, and the new connection rejects it. The replay's -ERR is
     * read before the request resumes from its failed write, and the #167 latch still fires, so both requests fail
     * fast with the permissions error. Both used to wait out their timeouts.
     */
    public function testAMuxSubWhoseReplayAfterAFailedWriteIsNotPermittedLatchesTheRejection(): void
    {
        $server = $this->echoServer();
        $connection = $this->connect($server, reconnect: true);
        $server->denied = ['_INBOX.'];
        $server->dropConnection();

        foreach (['one', 'two'] as $payload) {
            $start = hrtime(true);
            try {
                $connection->request('svc.echo', $payload, 10_000)->await();
                self::fail('expected the request to fail');
            } catch (ConnectionException $e) {
                self::assertStringContainsString('was rejected by the server (permissions violation)', $e->getMessage());
            }
            self::assertLessThan(2.0, self::secondsSince($start), 'it did not wait out its ten-second timeout');
        }

        self::assertCount(1, self::muxInstallWrites($server), 'the latch kept the second request from subscribing again');
        self::assertSame([], $server->controlLines('PUB svc.echo'));
    }

    /**
     * The server's permissions -ERR for the mux SUB is read by an application loop before the request resumes from
     * writing it, as when a transport reports the write done late: the #167 latch fires all the same, and the request
     * fails fast with the permissions error without being sent. The latch used to miss it, the mux not being recorded
     * yet, and the request was sent and timed out.
     */
    public function testAPermissionsRejectionReadBeforeTheRequestResumesFailsItFast(): void
    {
        $server = $this->echoServer();
        $server->denied = ['_INBOX.'];
        $connection = $this->connect($server);
        [$stop, $loop] = $this->startReadLoop($connection);
        $server->completeNextWriteContainingLate('SUB _INBOX.', 0.05);

        try {
            $start = hrtime(true);
            try {
                $connection->request('svc.echo', 'one', 10_000)->await();
                self::fail('expected the request to fail');
            } catch (ConnectionException $e) {
                self::assertStringContainsString('was rejected by the server (permissions violation)', $e->getMessage());
            }
            self::assertLessThan(2.0, self::secondsSince($start), 'it did not wait out its ten-second timeout');
        } finally {
            $stop->cancel();
            $loop->await();
        }
        self::assertSame([], $server->controlLines('PUB svc.echo'));
    }

    /**
     * A requestMany that has collected a reply when a reconnect's replay of the mux is rejected returns what it
     * has, at once, rather than waiting for replies that cannot come.
     */
    public function testRequestManyReturnsWhatItCollectedWhenAReconnectDropsTheMux(): void
    {
        $server = new SubscriptionLimitServer();
        $server->limit = 2;
        $server->responder = static fn(string $subject, ?string $replyTo): array => $replyTo === null
            ? []
            : $server->replyFrame($replyTo, 'first');
        $connection = $this->connect($server, reconnect: true);
        // Replayed ahead of the mux, so it keeps the one slot left after the reconnect.
        $this->subscribe($connection, 'app.one');

        $many = $connection->requestMany('svc.scan', 'who', null, 5, 2_000);
        delay(0.05);
        $server->limit = 1;
        $server->dropConnection();

        $start = hrtime(true);
        self::assertSame(['first'], self::payloads($many->await()));
        self::assertLessThan(1.0, self::secondsSince($start), 'it returned when the mux was dropped, not at its timeout');
    }

    /** A server that echoes every request on its reply subject - when the server holds a subscription for it. */
    private function echoServer(int $limit = PHP_INT_MAX): SubscriptionLimitServer
    {
        $server = new SubscriptionLimitServer();
        $server->limit = $limit;
        $server->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $replyTo === null
            ? []
            : $server->replyFrame($replyTo, 'echo:' . $payload);

        return $server;
    }

    /**
     * A server whose "svc.slow" answers only when the test calls answerPending(), and which echoes anything else.
     *
     * @return array{SubscriptionLimitServer, \ArrayObject<int, string>}
     */
    private function slowServer(int $limit): array
    {
        $server = new SubscriptionLimitServer();
        $server->limit = $limit;
        /** @var \ArrayObject<int, string> $pending The reply subjects of the "svc.slow" requests. */
        $pending = new \ArrayObject();
        $server->responder = static function (string $subject, ?string $replyTo, string $payload) use ($server, $pending): array {
            if ($replyTo === null) {
                return [];
            }

            if ($subject === 'svc.slow') {
                $pending[] = $replyTo;

                return [];
            }

            return $server->replyFrame($replyTo, 'echo:' . $payload);
        };

        return [$server, $pending];
    }

    /**
     * Answers the "svc.slow" requests received so far, in one read, on whatever the live session holds for them.
     *
     * @param \ArrayObject<int, string> $pending
     */
    private function answerPending(SubscriptionLimitServer $server, \ArrayObject $pending): void
    {
        $frames = '';
        foreach ($pending as $replyTo) {
            $frames .= implode('', $server->replyFrame($replyTo, 'late'));
        }
        $pending->exchangeArray([]);

        if ($frames !== '') {
            $server->pushFrame($frames);
        }
    }

    private function connect(
        SubscriptionLimitServer $server,
        bool $reconnect = false,
        bool $waitForReconnect = true,
        int|float $pingIntervalSeconds = 0,
        ?LifecycleRecorder $recorder = null,
        int $requestTimeoutMs = 10_000,
    ): NatsConnection {
        $connection = new NatsConnection(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: $requestTimeoutMs,
            reconnectEnabled: $reconnect,
            maxReconnectAttempts: 3,
            reconnectDelayMs: 1,
            reconnectMaxDelayMs: 1,
            reconnectJitterMs: 0,
            pingIntervalSeconds: $pingIntervalSeconds,
            errorListener: $recorder?->errorListener(),
            waitForReconnect: $waitForReconnect,
        ), $server);
        $this->opened[] = $connection;
        $connection->connect()->await();

        return $connection;
    }

    private function subscribe(NatsConnection $connection, string $subject): int
    {
        return $connection->subscribe($subject, static function (): void {})->await();
    }

    /**
     * Has the server reject the mux at its limit of one subscription, held by another one, then frees that slot and
     * stops answering PINGs: the next request subscribes a new mux and waits for the server to confirm it.
     *
     * @return int How many requests were sent: the one the rejection failed.
     */
    private function dropTheMuxAndFreeASlot(NatsConnection $connection, SubscriptionLimitServer $server): int
    {
        $appSid = $this->subscribe($connection, 'app.updates');
        try {
            $connection->request('svc.echo', 'one', 1_000)->await();
            self::fail('expected the request to fail: the server rejected its reply inbox');
        } catch (ConnectionException $e) {
            self::assertSame(self::LIMIT_ERROR, $e->getMessage());
        }
        $connection->unsubscribe($appSid)->await();
        $server->answerPings = false;

        return 1;
    }

    /** The sid of the mux a write from {@see muxInstallWrites()} subscribed. */
    private static function muxSidOf(string $install): int
    {
        if (preg_match('/^SUB _INBOX\.\S+\.\* (\d+)\r$/m', $install, $match) !== 1) {
            self::fail('not a write that subscribed a mux: ' . $install);
        }

        return (int) $match[1];
    }

    /**
     * Flushes until a flush gets through: the -ERRs of the SUBs written before it come ahead of its PONG, so once one
     * does, every one of them has been read.
     */
    private function flushPastTheRejections(NatsClient $client): void
    {
        for ($attempt = 1; ; $attempt++) {
            try {
                $client->flush()->await();

                return;
            } catch (ConnectionException $e) {
                // Its own read met one of the -ERRs.
                if ($attempt === 50) {
                    throw $e;
                }
            }
        }
    }

    /** Makes $call after $hops event-loop hops: a callback queued that many times in a row. */
    private static function afterHops(int $hops, \Closure $call): void
    {
        if ($hops === 0) {
            $call();

            return;
        }

        EventLoop::queue(static function () use ($hops, $call): void {
            self::afterHops($hops - 1, $call);
        });
    }

    /**
     * Whether, of the writes after the first $from, the one that subscribed the mux came after the flush's PING, which
     * is written on its own.
     */
    private static function requestCameAfterTheFlushsPing(SubscriptionLimitServer $server, int $from): bool
    {
        $ping = null;
        $sub = null;
        foreach (array_slice($server->writes, $from) as $index => $write) {
            if ($write['bytes'] === "PING\r\n") {
                $ping ??= $index;
            } elseif (str_contains($write['bytes'], 'SUB _INBOX.')) {
                $sub ??= $index;
            }
        }

        return $ping !== null && $sub !== null && $ping < $sub;
    }

    /**
     * An application loop that keeps a read on the socket, as a consumer's does, and reads on past whatever a read
     * throws. Stop it with the cancellation, then await the future.
     *
     * @return array{DeferredCancellation, Future<void>}
     */
    private function startReadLoop(NatsConnection|NatsClient $connection): array
    {
        $stop = new DeferredCancellation();
        $loop = async(static function () use ($connection, $stop): void {
            while (!$stop->getCancellation()->isRequested()) {
                try {
                    $connection->processIncoming($stop->getCancellation())->await();
                } catch (CancelledException) {
                    return;
                } catch (\Throwable) {
                    // An -ERR the server keeps the connection open for fails a read; the loop reads on.
                    delay(0.001);
                }
            }
        });
        // Let the loop take the read first.
        delay(0.01);

        return [$stop, $loop];
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

    /**
     * The writes that subscribed a mux: each carries one wildcard SUB under the inbox prefix.
     *
     * @return list<string>
     */
    private static function muxInstallWrites(SubscriptionLimitServer $server, ?int $epoch = null): array
    {
        $installs = [];
        foreach ($server->writes as $write) {
            if (($epoch === null || $write['epoch'] === $epoch) && preg_match('/^SUB _INBOX\.\S+\.\* \d+\r$/m', $write['bytes']) === 1) {
                $installs[] = $write['bytes'];
            }
        }

        return $installs;
    }

    /** @return array<int, string> The mux subscriptions the server holds. */
    private static function heldMuxes(SubscriptionLimitServer $server): array
    {
        return array_filter($server->heldSubscriptions(), static fn(string $subject): bool => str_starts_with($subject, '_INBOX.'));
    }

    /**
     * @param array<array-key, NatsMessage> $messages
     * @return list<string>
     */
    private static function payloads(array $messages): array
    {
        return array_values(array_map(static fn(NatsMessage $message): string => $message->payload, $messages));
    }

    private static function secondsSince(int $start): float
    {
        return (hrtime(true) - $start) / 1e9;
    }
}
