<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\HeldDrainParticipant;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\SubscriptionLimitServer;
use IDCT\NATS\Tests\Support\UncancellableDialTransport;
use IDCT\NATS\Transport\TransportClosedException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\delay;

/**
 * Requests while a drain() delivers (#213). The drain used to unsubscribe the shared reply inbox before it delivered
 * anything, and the connection refused every request while Draining: a handler the drain ran - a core subscription's,
 * with a message the drain's flush brought, or a pull consumer run's in the hand-over - could not make a request, and a
 * pull consumer handler that acked with ackSync() processed every message the drain handed over twice. The drain now
 * keeps the reply inbox through its delivery phase, sets it up for a connection's first request, and takes requests
 * through the bounded request writers while it delivers; the end of that phase - once everything is handed over, or at
 * its deadline - ends every request still waiting, wherever it waits, with "Connection is not open", and closes request
 * admission before the drain releases the inbox and closes. PullConsumerClientDrainTest covers the pull consumer
 * hand-over and MuxInboxRejectionTest a set-up the drain overtakes.
 *
 * Over the scripted servers in tests/Support/ReconnectingTransport.php and tests/Support/SubscriptionLimitServer.php,
 * and FakeTransport for writes that suspend inline. A participant ({@see HeldDrainParticipant}) holds the delivery
 * phase open where a test needs it open, with no handler holding up the reads. Each test fails on 2.24.3 unless it is
 * declared a guard.
 */
final class RequestsDuringDrainTest extends TestCase
{
    use ReconnectScenarios;

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{bool}> */
    public static function replyInboxes(): iterable
    {
        yield 'the reply inbox made before the drain' => [true];
        yield 'the first request of the connection' => [false];
    }

    /**
     * A core subscription's handler that the drain's flush runs gets its request's reply: the server sends c-1 right
     * behind the drain's UNSUB of the subscription, as a message it delivered before the UNSUB, and the handler makes a
     * request for it while the connection is Draining. The drain kept the reply inbox, or subscribes it for the
     * connection's first request, the request is answered, and the drain releases the inbox behind it before it
     * closes. On 2.24.3 the request failed at once with "Connection is not open".
     */
    #[DataProvider('replyInboxes')]
    public function testACoreHandlerTheDrainsFlushRunsGetsItsRequestsReply(bool $inboxBeforeTheDrain): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, errorListener: $recorder->errorListener());
        $transport->responder = self::echo($transport);
        if ($inboxBeforeTheDrain) {
            $connection->request('svc', 'warm')->await();
        }
        $log = self::log();
        $sid = $connection->subscribe('updates', self::requestingHandler($connection, $log, 'svc', 2_000))->await();
        self::deliverRightBehindTheDrainsUnsubscribe($transport, $sid, ['c-1']);

        $connection->drain()->await(new TimeoutCancellation(5));

        self::assertSame(['c-1 (Draining)', 'reply: echo:c-1'], $log->getArrayCopy());
        self::assertSame([], $recorder->errors);
        $lines = $transport->controlLines();
        $request = array_search('PUB svc ' . self::replySubjectOf($transport, 'c-1') . ' 3', $lines, true);
        $release = array_search('UNSUB ' . self::muxSid($transport), $lines, true);
        self::assertIsInt($request);
        self::assertIsInt($release, 'the drain released the reply inbox');
        self::assertGreaterThan($request, $release, 'behind the request');
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /** @return iterable<string, array{float|null}> */
    public static function repliesTooLateForTheDrain(): iterable
    {
        yield 'a reply that never comes' => [null];
        yield 'a reply 600 ms later' => [0.6];
    }

    /**
     * A core handler's request whose reply comes later than the drain's budget allows ends with the delivery phase: a
     * budget of 300 ms, c-1 and c-2 coming in the drain's flush, and the handler of c-1 waiting on a request with a
     * timeout of five seconds. At the deadline the drain seals its delivery phase, counting c-2, and the request fails
     * with "Connection is not open", not its own timeout; c-2 is not handed over, the drain reports it once, and closes
     * within its budget. A drain that let the request run kept the drain open for as long as the request lasted (a
     * drain of 250 ms took about 600 ms with a reply 600 ms late in the design review's model); on 2.24.3 the request
     * failed at once and c-2 was handed over, to fail the same way.
     */
    #[DataProvider('repliesTooLateForTheDrain')]
    public function testACoreHandlersRequestSlowerThanTheDrainsBudgetEndsWithTheDeliveryPhase(?float $replyDelay): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, requestTimeoutMs: 300, errorListener: $recorder->errorListener());
        $transport->responder = $replyDelay === null ? static fn(): array => [] : self::echo($transport);
        $transport->responseDelay = $replyDelay ?? 0.0;
        $log = self::log();
        $sid = $connection->subscribe('updates', self::requestingHandler($connection, $log, 'svc', 5_000))->await();
        self::deliverRightBehindTheDrainsUnsubscribe($transport, $sid, ['c-1', 'c-2']);

        $start = hrtime(true);
        $connection->drain()->await(new TimeoutCancellation(5));
        $drainedIn = $this->secondsSince($start);

        self::assertSame(['c-1 (Draining)', 'request failed: ' . ConnectionException::class . ': Connection is not open'], $log->getArrayCopy());
        self::assertSame(['drain deadline exceeded: 1 buffered message(s) were not delivered before close'], $recorder->errorsContaining('drain deadline exceeded'));
        self::assertLessThan(0.55, $drainedIn, sprintf('the drain took %.3f s of its 300 ms budget', $drainedIn));
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * A request already waiting when the drain begins does not outlive the delivery phase, which it does not extend: an
     * unanswered request with a timeout of five seconds, from another fiber, its read parked on the socket while a
     * participant holds the delivery phase open, and a transport whose close takes a second, as a TLS or WebSocket close
     * may. Once the participant's hand-over is done the drain seals its delivery phase, and the request fails with
     * "Connection is not open" right then, not once the close fails its read a second later. On 2.24.3 the request
     * waited on the socket until the close was over.
     */
    public function testARequestWaitingWhenTheDrainBeginsEndsWithTheDeliveryPhaseNotTheClose(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $transport->responder = self::echo($transport);
        $connection->request('svc', 'warm')->await();
        $transport->responder = static fn(): array => [];
        $participant = new HeldDrainParticipant();
        $connection->addDrainParticipant($participant);
        $request = $connection->request('svc.never', 'x', 5_000);
        delay(0.02);

        $drain = $connection->drain();
        $this->waitUntil(static fn(): bool => $participant->asked === 1);
        // The request's read takes the socket again, now that the drain's flush is over.
        delay(0.05);
        $transport->closeDelay = 1.0;
        $participant->release();
        try {
            $request->await(new TimeoutCancellation(0.5));
            self::fail('expected the request to fail with the delivery phase');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }
        self::assertFalse($drain->isComplete(), 'the close was still under way');
        $drain->await(new TimeoutCancellation(5));
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * Guard, passes on both: the listener that gets the drain's "drain deadline exceeded" report cannot start a request
     * - the delivery phase is sealed before the report is made. A participant holding two messages holds the drain to
     * its deadline (300 ms); the listener's request fails with "Connection is not open" and is not written.
     */
    public function testTheDeadlineReportsListenerCannotStartARequest(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = self::echo($transport);
        $connection = null;
        $outcome = self::log();
        $connection = $this->connect($transport, requestTimeoutMs: 300, errorListener: static function (\Throwable $error) use (&$connection, $outcome): void {
            if (!str_contains($error->getMessage(), 'drain deadline exceeded') || !$connection instanceof NatsConnection) {
                return;
            }

            $outcome[] = $error->getMessage();
            try {
                $connection->request('svc', 'from the listener', 2_000)->await();
                $outcome[] = 'the request was answered';
            } catch (\Throwable $failure) {
                $outcome[] = 'the request failed: ' . $failure->getMessage();
            }
        });
        $connection->request('svc', 'warm')->await();
        $participant = new HeldDrainParticipant();
        $participant->held = 2;
        $connection->addDrainParticipant($participant);

        $connection->drain()->await(new TimeoutCancellation(5));

        self::assertSame([
            'drain deadline exceeded: 2 buffered message(s) were not delivered before close',
            'the request failed: Connection is not open',
        ], $outcome->getArrayCopy());
        self::assertSame([], $transport->controlLinesStartingWith('PUB svc _INBOX.', 0) === [] ? [] : array_values(array_filter(
            $transport->controlLinesStartingWith('PUB svc '),
            static fn(string $line): bool => str_ends_with($line, ' 17'),
        )), 'the listener\'s request was not written');
    }

    /**
     * A disconnect() that interrupts the delivery phase ends the requests it took at once, and takes no more: a
     * participant holds the phase open, a request made then is written and waits for a reply that never comes, and the
     * application disconnects with a transport whose close takes a second. The request fails with "Connection is not
     * open" as the disconnect() begins, a request made while the close is under way fails the same way without being
     * written, and the connection announces one Closed event. On 2.24.3 the first request was refused, unwritten.
     */
    public function testADisconnectDuringTheDeliveryPhaseEndsItsRequestsAtOnceAndTakesNoMore(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, connectionListener: $recorder->connectionListener());
        $transport->responder = self::echo($transport);
        $connection->request('svc', 'warm')->await();
        $transport->responder = static fn(): array => [];
        $participant = new HeldDrainParticipant();
        $connection->addDrainParticipant($participant);
        $drain = $connection->drain();
        $this->waitUntil(static fn(): bool => $participant->asked === 1);
        $request = $connection->request('svc.never', 'x', 5_000);
        $this->waitUntil(static fn(): bool => $transport->controlLinesStartingWith('PUB svc.never ') !== []);

        $transport->closeDelay = 1.0;
        $disconnect = $connection->disconnect();
        try {
            $request->await(new TimeoutCancellation(0.5));
            self::fail('expected the request to end as the disconnect() began');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }
        try {
            $connection->request('svc.later', 'x', 5_000)->await(new TimeoutCancellation(0.5));
            self::fail('expected a request made during the disconnect() to fail');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }
        self::assertFalse($disconnect->isComplete(), 'the close was still under way');
        $disconnect->await(new TimeoutCancellation(5));
        $participant->release();
        $drain->await(new TimeoutCancellation(5));

        self::assertSame([], $transport->controlLinesStartingWith('PUB svc.later '), 'not written');
        self::assertSame(1, $recorder->closedEvents());
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * Ordinary subscriptions, the guarded inboxes of the batch reads and new pulls stay refused while the drain delivers:
     * a participant holds the phase open; a request is answered, while subscribe() and fetchBatch() fail with
     * "Connection is not open", no SUB written for either. On 2.24.3 the request failed as well.
     */
    public function testOnlyRequestsAreTakenWhileTheDrainDelivers(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->own(new NatsClient($this->options(true, 2_000, 1_000, 0, 2, null, 5, 20, null), $transport));
        $this->opened[] = $client;
        $client->connect()->await();
        $transport->responder = self::echo($transport);
        $participant = new HeldDrainParticipant();
        $client->addDrainParticipant($participant);
        $drain = $client->drain();
        $this->waitUntil(static fn(): bool => $participant->asked === 1);
        $subscribes = count($transport->controlLinesStartingWith('SUB '));

        self::assertSame('echo:x', $client->request('svc', 'x', 2_000)->await()->payload);
        $subscribes = count($transport->controlLinesStartingWith('SUB ')) - $subscribes;
        foreach ([
            'subscribe()' => static fn(): Future => $client->subscribe('late', static function (): void {}),
            'fetchBatch()' => static fn(): Future => $client->jetStream()->fetchBatch('S', 'C', 1, 1_000),
        ] as $operation => $start) {
            try {
                $start()->await(new TimeoutCancellation(1));
                self::fail($operation . ' was taken during the drain');
            } catch (ConnectionException $e) {
                self::assertSame('Connection is not open', $e->getMessage(), $operation);
            }
        }
        $participant->release();
        $drain->await(new TimeoutCancellation(5));

        self::assertSame(1, $subscribes, 'only the reply inbox was subscribed, for the request');
        self::assertSame([], $transport->controlLinesStartingWith('SUB late '));
        self::assertSame([], $transport->controlLinesStartingWith('SUB _INBOX.JS.'));
        self::assertSame(ConnectionState::Closed, $client->state());
    }

    /**
     * A request whose write fails while the drain delivers is neither retried nor followed by a reconnect: close-intent
     * forbids the reconnect, and the drain closes the dead connection. The request fails with "Connection is not open",
     * the socket's error its cause, and no dial follows. On 2.24.3 the request was refused before it was written.
     */
    public function testARequestWhoseWriteFailsWhileTheDrainDeliversIsNotRetried(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $transport->responder = self::echo($transport);
        $connection->request('svc', 'warm')->await();
        $participant = new HeldDrainParticipant();
        $connection->addDrainParticipant($participant);
        $drain = $connection->drain();
        $this->waitUntil(static fn(): bool => $participant->asked === 1);
        $transport->failNextWriteContaining('PUB svc.fails ');

        try {
            $connection->request('svc.fails', 'x', 2_000)->await(new TimeoutCancellation(1));
            self::fail('expected the failed write to fail the request');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
            self::assertSame(0, $e->getCode());
            self::assertInstanceOf(TransportClosedException::class, $e->getPrevious());
        }
        $participant->release();
        $drain->await(new TimeoutCancellation(5));

        self::assertCount(1, $transport->connectCalls, 'no reconnect');
        self::assertSame([], $transport->controlLinesStartingWith('PUB svc.fails '));
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * A request whose write failed while the connection was Open goes on when a drain takes over the connection its
     * recovery opens, while that drain delivers: the request's PUB fails, the reconnect's dial is held, and the
     * application drains, which waits for that reconnect. Once the dial is let through, the reconnect replays the
     * reply inbox, the drain takes the new connection over, and the request is sent there and answered. On 2.24.3 the
     * drain resumed first, and the request found the connection Draining and failed with "Connection is not open".
     */
    public function testARequestWhoseFailedWriteARecoveringDrainOvertakesIsSentOnTheNewConnection(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $transport->responder = self::echo($transport);
        $connection->request('svc', 'warm')->await();
        $participant = new HeldDrainParticipant();
        $connection->addDrainParticipant($participant);
        $transport->holdNextDial();
        $transport->failNextWriteContaining('PUB svc.retried ');
        $request = $connection->request('svc.retried', 'x', 3_000);
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);

        $drain = $connection->drain();
        $transport->releaseDial();

        self::assertSame('echo:x', $request->await(new TimeoutCancellation(2))->payload);
        self::assertCount(1, $transport->controlLinesStartingWith('PUB svc.retried ', 1), 'sent once, on the new connection');
        $this->waitUntil(static fn(): bool => $participant->asked === 1);
        $participant->release();
        $drain->await(new TimeoutCancellation(5));
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * A reply-inbox SUB held up by backpressure on the socket does not hold the drain past its budget: the first request
     * of the connection comes from a core handler the drain's flush runs, its SUB wedges, and at the drain's deadline
     * (300 ms) the request ends with "Connection is not open" and the drain closes, failing the wedged write. The same
     * for a request's PUB, with the reply inbox made before the drain. On 2.24.3 the request was refused before
     * anything was written.
     */
    #[DataProvider('replyInboxes')]
    public function testARequestsWriteWedgedDuringTheDrainDoesNotHoldItPastItsBudget(bool $inboxBeforeTheDrain): void
    {
        $transport = new FakeTransport([ReconnectingTransport::INFO, "PONG\r\n"]);
        $connection = $this->own(new NatsConnection(new NatsOptions(requestTimeoutMs: 300, pingIntervalSeconds: 0), $transport));
        $this->opened[] = $connection;
        $connection->connect()->await();
        $backlog = [];
        $transport->onWrite = self::fakeServer($transport, $backlog);
        if ($inboxBeforeTheDrain) {
            self::assertSame('echo:warm', $connection->request('svc', 'warm', 1_000)->await()->payload);
        }
        $log = self::log();
        $sid = $connection->subscribe('updates', self::requestingHandler($connection, $log, 'svc', 5_000))->await();
        $backlog = ['UNSUB ' . $sid . "\r\n" => ReconnectingTransport::msgFrame('updates', $sid, 'c-1')];
        $transport->wedgeOnWriteContaining = $inboxBeforeTheDrain ? 'PUB svc ' : 'SUB _INBOX.';

        $start = hrtime(true);
        $connection->drain()->await(new TimeoutCancellation(5));
        $drainedIn = $this->secondsSince($start);

        self::assertSame(['c-1 (Draining)', 'request failed: ' . ConnectionException::class . ': Connection is not open'], $log->getArrayCopy());
        $wedged = $inboxBeforeTheDrain ? 'PUB svc ' : 'SUB _INBOX.';
        self::assertNotSame([], array_filter($transport->writes, static fn(string $bytes): bool => str_contains($bytes, $wedged)), 'the write was attempted');
        self::assertLessThan(1.5, $drainedIn, sprintf('the drain took %.3f s of its 300 ms budget', $drainedIn));
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * Guard, passes on both: the drain's release of the reply inbox, an UNSUB held up by backpressure on the socket, does
     * not hold the drain past its budget. On 2.24.3 the same UNSUB was written as the drain began.
     */
    public function testTheReplyInboxsReleaseWedgedAtTheEndOfTheDrainDoesNotHoldItPastItsBudget(): void
    {
        $transport = new FakeTransport([ReconnectingTransport::INFO, "PONG\r\n"]);
        $connection = $this->own(new NatsConnection(new NatsOptions(requestTimeoutMs: 300, pingIntervalSeconds: 0), $transport));
        $this->opened[] = $connection;
        $connection->connect()->await();
        $backlog = [];
        $transport->onWrite = self::fakeServer($transport, $backlog);
        self::assertSame('echo:warm', $connection->request('svc', 'warm', 1_000)->await()->payload);
        $muxSid = self::fakeMuxSid($transport);
        $transport->wedgeOnWriteContaining = 'UNSUB ' . $muxSid;

        $start = hrtime(true);
        $connection->drain()->await(new TimeoutCancellation(5));
        $drainedIn = $this->secondsSince($start);

        self::assertNotSame([], array_filter($transport->writes, static fn(string $bytes): bool => str_contains($bytes, 'UNSUB ' . $muxSid)), 'the release was attempted');
        self::assertLessThan(1.5, $drainedIn, sprintf('the drain took %.3f s of its 300 ms budget', $drainedIn));
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * Guard, passes on both: a release of the reply inbox whose write fails at the end of the drain is reported through
     * the error listener, and the drain still closes. On 2.24.3 the same UNSUB failed as the drain began, reported the
     * same way.
     */
    public function testAFailedReleaseOfTheReplyInboxIsReportedAndTheDrainStillCloses(): void
    {
        $transport = new FakeTransport([ReconnectingTransport::INFO, "PONG\r\n"]);
        $recorder = new LifecycleRecorder();
        $connection = $this->own(new NatsConnection(new NatsOptions(requestTimeoutMs: 300, pingIntervalSeconds: 0, errorListener: $recorder->errorListener()), $transport));
        $this->opened[] = $connection;
        $connection->connect()->await();
        $backlog = [];
        $transport->onWrite = self::fakeServer($transport, $backlog);
        self::assertSame('echo:warm', $connection->request('svc', 'warm', 1_000)->await()->payload);
        $transport->throwOnWriteContaining = 'UNSUB ' . self::fakeMuxSid($transport);

        $connection->drain()->await(new TimeoutCancellation(5));

        self::assertSame(['Simulated write failure'], $recorder->errors);
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * A reply-inbox SUB the drain's request admission let out, still being written when the drain ends, writes nothing
     * on the connection a Closed listener opens next: a budget of 300 ms, a server whose writes reach it in order, and
     * the first request's SUB held up, so that the drain's own writes wait behind it and its deadline ends the delivery
     * phase. The request fails with "Connection is not open", the drain closes, and its Closed listener connects again.
     * Once the held SUB is let through, as if its bytes had gone out before the close, its writer finds the connection
     * replaced and writes no UNSUB on the new one, and nothing is sent there but what the new connection writes itself.
     */
    public function testASetUpTheDrainOutlivedWritesNothingOnTheConnectionAClosedListenerOpens(): void
    {
        $server = new SubscriptionLimitServer();
        $server->responder = static fn(string $subject, ?string $replyTo, string $payload): array => [];
        $recorder = new LifecycleRecorder();
        $events = $recorder->connectionListener();
        $reconnect = null;
        $connection = null;
        $connection = $this->own(new NatsConnection(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: 300,
            reconnectEnabled: false,
            pingIntervalSeconds: 0,
            connectionListener: static function (ConnectionEvent $event, ?\Throwable $error = null) use ($events, &$connection, &$reconnect): void {
                $events($event, $error);
                if ($event === ConnectionEvent::Closed && $reconnect === null && $connection instanceof NatsConnection) {
                    $reconnect = $connection->connect();
                }
            },
        ), $server));
        $this->opened[] = $connection;
        $connection->connect()->await();
        $server->stallNextWriteContaining('SUB _INBOX.', 30.0, completesAfterClose: true);
        $request = $connection->request('svc', 'x', 5_000);
        $this->waitUntil(static fn(): bool => $server->writesStalled() === 1);

        $connection->drain()->await(new TimeoutCancellation(5));
        try {
            $request->await(new TimeoutCancellation(1));
            self::fail('expected the request to end with the delivery phase');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }
        $this->waitUntil(static fn(): bool => $reconnect instanceof Future && $reconnect->isComplete());
        self::assertSame(ConnectionState::Open, $connection->state());
        $server->releaseStalledWrites();
        delay(0.05);

        self::assertSame(['CONNECT', 'PING'], array_map(static fn(string $line): string => explode(' ', $line)[0], $server->controlLines('', 1)), 'nothing stale on the new connection');
        self::assertSame(1, $recorder->closedEvents());
    }

    /** @return iterable<string, array{bool}> */
    public static function handlersHoldingTheEventLoop(): iterable
    {
        yield 'then making a request' => [true];
        yield 'and returning' => [false];
    }

    /**
     * A handler that holds the event loop past the drain's deadline - work that never yields - gets no request taken,
     * and no handler another message, after that deadline, although the drain's timer has had no chance to seal the
     * delivery phase: the checks of the deadline seal it themselves (#213). A budget of 300 ms; c-1 for subscription A
     * and b-1 for B come in one chunk of the drain's flush, A's sid first; A's handler works for 500 ms without
     * yielding, then makes a request, or returns. The request fails with "Connection is not open" without being
     * written, b-1 is not handed over, and the report counts it. On 2.24.3 a new pass handed b-1 over past the
     * deadline.
     */
    #[DataProvider('handlersHoldingTheEventLoop')]
    public function testAHandlerHoldingTheEventLoopPastTheDeadlineGetsNoRequestTakenAndNoHandlerAnotherMessage(bool $request): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, requestTimeoutMs: 300, errorListener: $recorder->errorListener());
        $transport->responder = self::echo($transport);
        $connection->request('svc', 'warm')->await();
        $log = self::log();
        $sidA = $connection->subscribe('updates', static function (NatsMessage $message) use ($connection, $log, $request): void {
            $log[] = 'A ' . $message->payload;
            $until = hrtime(true) + 500_000_000;
            while (hrtime(true) < $until) {
                // Work that never yields to the event loop.
            }

            if (!$request) {
                return;
            }

            try {
                $connection->request('svc', 'too-late', 2_000)->await();
                $log[] = 'the request was answered';
            } catch (\Throwable $failure) {
                $log[] = 'the request failed: ' . $failure->getMessage();
            }
        })->await();
        $sidB = $connection->subscribe('other', static function (NatsMessage $message) use ($log): void {
            $log[] = 'B ' . $message->payload;
        })->await();
        $transport->afterWrite = static function (string $bytes) use ($transport, $sidA, $sidB): void {
            if ($bytes === 'UNSUB ' . $sidB . "\r\n") {
                $transport->pushFrame(ReconnectingTransport::msgFrame('updates', $sidA, 'c-1') . ReconnectingTransport::msgFrame('other', $sidB, 'b-1'));
            }
        };

        $connection->drain()->await(new TimeoutCancellation(5));

        self::assertSame($request ? ['A c-1', 'the request failed: Connection is not open'] : ['A c-1'], $log->getArrayCopy());
        self::assertSame([], array_values(array_filter($transport->controlLinesStartingWith('PUB svc '), static fn(string $line): bool => str_ends_with($line, ' 8'))), 'the request was not written');
        self::assertSame(['drain deadline exceeded: 1 buffered message(s) were not delivered before close'], $recorder->errorsContaining('drain deadline exceeded'));
    }

    /**
     * Guard, passes on both: a drain() that a disconnect() interrupts while a participant still holds two messages
     * reports them once its deadline (300 ms) comes, the participant not having ended.
     */
    public function testADrainADisconnectInterruptedReportsWhatAParticipantStillHeldAtItsDeadline(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, requestTimeoutMs: 300, errorListener: $recorder->errorListener());
        $participant = new HeldDrainParticipant();
        $participant->held = 2;
        $connection->addDrainParticipant($participant);
        $drain = $connection->drain();
        $this->waitUntil(static fn(): bool => $participant->asked === 1);

        $connection->disconnect()->await(new TimeoutCancellation(5));
        $drain->await(new TimeoutCancellation(5));

        self::assertSame(['drain deadline exceeded: 2 buffered message(s) were not delivered before close'], $recorder->errorsContaining('drain deadline exceeded'));
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * A disconnect() that interrupts the drain leaves the reply inbox to its own close: a participant holds the delivery
     * phase open, the application disconnects with a transport whose close takes 300 ms, and the participant's hand-over
     * ends while that close is under way. The drain, its delivery phase interrupted rather than over, writes no UNSUB of
     * the inbox on the connection being closed. On 2.24.3 the drain unsubscribed the inbox as it began.
     */
    public function testADisconnectThatInterruptsTheDrainLeavesTheReplyInboxToItsClose(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $transport->responder = self::echo($transport);
        $connection->request('svc', 'warm')->await();
        $muxSid = self::muxSid($transport);
        $participant = new HeldDrainParticipant();
        $connection->addDrainParticipant($participant);
        $drain = $connection->drain();
        $this->waitUntil(static fn(): bool => $participant->asked === 1);

        $transport->closeDelay = 0.3;
        $disconnect = $connection->disconnect();
        delay(0.05);
        $participant->release();
        $disconnect->await(new TimeoutCancellation(5));
        $drain->await(new TimeoutCancellation(5));

        self::assertSame([], $transport->controlLinesStartingWith('UNSUB ' . $muxSid), 'the close released the inbox');
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * A drain() without a connection to drain ends the requests waiting for the reconnect at once, rather than when the
     * reconnect stops (#213): a transport that cannot stop its dial, the reconnect's dial held for a second, a request
     * with a five-second timeout waiting for that reconnect, and a drain whose budget (100 ms) runs out while it waits
     * for it too. The request fails with "Connection is not open" well before the dial ends. On 2.24.3 it waited for
     * the dial.
     */
    public function testADrainWithoutAConnectionEndsTheRequestsWaitingForTheReconnectAtOnce(): void
    {
        $inner = new ReconnectingTransport();
        $connection = $this->connect(new UncancellableDialTransport($inner), requestTimeoutMs: 100);
        $reader = $this->startRecoveryHeldMidDial($connection, $inner);
        $request = $connection->request('svc', 'x', 5_000);
        delay(0.01);
        EventLoop::delay(1.0, static function () use ($inner): void {
            $inner->releaseDial();
        });

        $drain = $connection->drain();
        try {
            $request->await(new TimeoutCancellation(0.6));
            self::fail('expected the request to end with the drain');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }

        try {
            $drain->await(new TimeoutCancellation(3));
        } catch (ConnectionException) {
            // However the drain reports its budget running out, it closes the connection.
        }
        self::assertSame(ConnectionState::Closed, $connection->state());
        try {
            $reader->await(new TimeoutCancellation(2));
        } catch (\Throwable) {
            // The reader's read ends with the closed connection.
        }
    }

    /**
     * A request waiting for a reconnect that a drain() takes over is sent while the drain delivers: the connection
     * drops, the reconnect's dial is held, the application drains, and a request is made after that, both waiting for
     * the reconnect. Once the dial is let through the drain takes the new connection over first, a participant holding
     * its delivery phase open, and the request is sent there and answered. On 2.24.3 it found the connection Draining
     * and failed with "Connection is not open".
     */
    public function testARequestWaitingForAReconnectADrainTakesOverIsSentWhileTheDrainDelivers(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $transport->responder = self::echo($transport);
        $connection->request('svc', 'warm')->await();
        $participant = new HeldDrainParticipant();
        $connection->addDrainParticipant($participant);
        $reader = $this->startRecoveryHeldMidDial($connection, $transport);
        $drain = $connection->drain();
        delay(0.01);
        $request = $connection->request('svc', 'x', 3_000);
        delay(0.01);

        $transport->releaseDial();

        self::assertSame('echo:x', $request->await(new TimeoutCancellation(2))->payload);
        self::assertSame(ConnectionState::Draining, $connection->state(), 'the drain had taken the connection over');
        $this->waitUntil(static fn(): bool => $participant->asked === 1);
        $participant->release();
        $drain->await(new TimeoutCancellation(5));
        $reader->await(new TimeoutCancellation(2));
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * Guard, passes on both: subscribe() stays refused while the drain delivers, also when its SUB failed on a dead
     * socket while the connection was Open and a drain took over the connection the reconnect opened: the SUB's write
     * fails, the reconnect's dial is held, the application drains, a participant holding the delivery phase open, and
     * once the dial is let through, the subscribe fails with "Connection is not open" rather than resolve with a sid the
     * drain has unsubscribed.
     */
    public function testASubscribeWhoseFailedWriteADrainOvertakesStaysRefused(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $participant = new HeldDrainParticipant();
        $connection->addDrainParticipant($participant);
        $transport->holdNextDial();
        $transport->failNextWriteContaining('SUB late ');
        $subscribe = $connection->subscribe('late', static function (): void {});
        $this->waitUntil(static fn(): bool => count($transport->connectCalls) === 2);
        $drain = $connection->drain();
        delay(0.01);

        $transport->releaseDial();

        try {
            $subscribe->await(new TimeoutCancellation(2));
            self::fail('expected the subscribe to be refused');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }
        $this->waitUntil(static fn(): bool => $participant->asked === 1);
        $participant->release();
        $drain->await(new TimeoutCancellation(5));
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * A handler that logs the message with the connection's state, then makes a request on $subject for it, logging
     * the reply or why the request failed.
     *
     * @param \ArrayObject<int, string> $log
     * @return \Closure(NatsMessage): void
     */
    private static function requestingHandler(NatsConnection $connection, \ArrayObject $log, string $subject, int $timeoutMs): \Closure
    {
        return static function (NatsMessage $message) use ($connection, $log, $subject, $timeoutMs): void {
            $log[] = $message->payload . ' (' . $connection->state()->name . ')';
            try {
                $log[] = 'reply: ' . $connection->request($subject, $message->payload, $timeoutMs)->await()->payload;
            } catch (\Throwable $failure) {
                $log[] = 'request failed: ' . $failure::class . ': ' . $failure->getMessage();
            }
        };
    }

    /**
     * Has the scripted server send $payloads for subscription $sid right behind the drain's UNSUB of it, each in a read
     * of its own, as messages it delivered before the UNSUB.
     *
     * @param list<string> $payloads
     */
    private static function deliverRightBehindTheDrainsUnsubscribe(ReconnectingTransport $transport, int $sid, array $payloads): void
    {
        $transport->afterWrite = static function (string $bytes) use ($transport, $sid, $payloads): void {
            if ($bytes === 'UNSUB ' . $sid . "\r\n") {
                foreach ($payloads as $payload) {
                    $transport->pushFrame(ReconnectingTransport::msgFrame('updates', $sid, $payload));
                }
            }
        };
    }

    /** @return \Closure(string, ?string, string): list<string> A responder that echoes every request. */
    private static function echo(ReconnectingTransport $transport): \Closure
    {
        return static fn(string $subject, ?string $replyTo, string $payload): array => $replyTo === null ? [] : $transport->replyFrame($replyTo, 'echo:' . $payload);
    }

    /** The reply subject of the request whose payload was $payload. */
    private static function replySubjectOf(ReconnectingTransport $transport, string $payload): string
    {
        foreach ($transport->writes as $write) {
            if (preg_match('/^PUB \S+ (\S+) \d+\r\n' . preg_quote($payload, '/') . '\r\n/', $write['bytes'], $match) === 1) {
                return $match[1];
            }
        }

        self::fail('no request carried ' . $payload);
    }

    /** The sid the client subscribed its reply inbox with. */
    private static function muxSid(ReconnectingTransport $transport): int
    {
        foreach ($transport->controlLinesStartingWith('SUB _INBOX.') as $line) {
            if (preg_match('/^SUB _INBOX\.\S+\.\* (\d+)$/', $line, $match) === 1) {
                return (int) $match[1];
            }
        }

        self::fail('the client subscribed no reply inbox');
    }

    /** The sid of the reply inbox subscribed over a FakeTransport. */
    private static function fakeMuxSid(FakeTransport $transport): int
    {
        foreach ($transport->writes as $bytes) {
            if (preg_match('/^SUB _INBOX\.\S+\.\* (\d+)\r$/m', $bytes, $match) === 1) {
                return (int) $match[1];
            }
        }

        self::fail('the client subscribed no reply inbox');
    }

    /**
     * A FakeTransport server: a PONG for each PING, an echo for each request on "svc", and the frame $backlog holds for
     * a write it names, once.
     *
     * @param array<string, string> $backlog Frames to send behind a write, by the bytes of that write.
     * @return \Closure(string): list<string>
     */
    private static function fakeServer(FakeTransport $transport, array &$backlog): \Closure
    {
        return static function (string $bytes) use ($transport, &$backlog): array {
            $frames = [];
            if (isset($backlog[$bytes])) {
                $frames[] = $backlog[$bytes];
                unset($backlog[$bytes]);
            }

            if (preg_match('/^PUB svc (_INBOX\.\S+) \d+\r\n(.*)\r\n$/s', $bytes, $match) === 1) {
                $frames[] = ReconnectingTransport::msgFrame($match[1], self::fakeMuxSid($transport), 'echo:' . $match[2]);
            }

            if (str_contains($bytes, "PING\r\n")) {
                $frames[] = "PONG\r\n";
            }

            return $frames;
        };
    }

    /** @return \ArrayObject<int, string> */
    private static function log(): \ArrayObject
    {
        return new \ArrayObject();
    }
}
