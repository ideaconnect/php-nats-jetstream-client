<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Core\SubscriptionQueue;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\SlowConsumerException;
use IDCT\NATS\Tests\Support\HeldUpDelivery;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\OperationsThatRead;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Tests\Support\ThrowingLogger;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\AbstractLogger;

use function Amp\async;
use function Amp\delay;

/**
 * SlowConsumerPolicy::Error where a subscriber that cannot keep up - its queue full - meets the client's
 * own reads. The overflow drops a message and is reported, as the policy says, instead of failing what
 * that read was for:
 *   - a reconnect used to fail every attempt the same way (nothing delivers while a reconnect runs) until
 *     it gave up and closed the connection;
 *   - the flushes of drain() and drainSubscription() used to end early, before the messages still in
 *     flight;
 *   - an operation that reads while it waits for a result of its own - a request, a flush, a fetch, a
 *     polling queue - used to fail with another subscription's overflow, its result lost. With
 *     NatsOptions::$slowConsumerErrorsFailOperations it still does, as before that option existed.
 * The same holds for a SubscriptionQueue's own polling buffer: its overflow, thrown while a read delivers
 * to it, used to cut that delivery short and fail whichever read it was - or, on the heartbeat's, leave the
 * messages behind it for the next read.
 */
final class SlowConsumerErrorPolicyTest extends TestCase
{
    use OperationsThatRead;
    use ReconnectScenarios;

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /** @return iterable<string, array{bool}> */
    public static function eitherWay(): iterable
    {
        yield 'operations report overflows (default)' => [false];
        yield 'overflows fail operations' => [true];
    }

    /** Either way: a reconnect has nobody to throw the overflow to. */
    #[DataProvider('eitherWay')]
    public function testReconnectReportsAFullQueueInsteadOfFailing(bool $failOperations): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->errorPolicyConnection($transport, $recorder, $failOperations);
        $subscriber = $this->subscriberFallingBehind($connection, $transport);
        // Traffic goes on: the server sends on the subscription as soon as the reconnect re-subscribes it.
        $transport->afterWrite = static function (string $bytes) use ($transport, $subscriber): void {
            if (str_contains($bytes, 'SUB updates ')) {
                $transport->pushFrame(ReconnectingTransport::msgFrame('updates', $subscriber->sid, 'live'));
            }
        };

        $transport->dropConnection();
        // This read finds the connection gone and runs the reconnect.
        $connection->processIncoming()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $connection->statistics()->reconnects);
        self::assertCount(2, $transport->connectCalls, 'the first attempt succeeded');
        self::assertSame(['SUB updates ' . $subscriber->sid], $transport->controlLinesStartingWith('SUB', 1));
        self::assertSame(
            ['Subscription queue overflow for sid ' . $subscriber->sid],
            $recorder->errorsContaining('overflow'),
        );
        self::assertTrue($connection->isSubscriptionActive($subscriber->sid));

        $subscriber->busy->complete();
        $subscriber->dispatcher?->await();
        self::assertSame(['m1', 'm2', 'm3', 'm4'], $subscriber->payloads, 'the message that overflowed was dropped');
    }

    /**
     * A fatal -ERR in the same chunk as the overflow still fails the attempt: the replayed subscriptions
     * are not taken for accepted by a server that refused them.
     */
    public function testFatalErrorBehindAnOverflowStillFailsTheAttempt(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->errorPolicyConnection($transport, $recorder);
        $subscriber = $this->subscriberFallingBehind($connection, $transport);
        $this->answerTheFirstReplayWith(
            $transport,
            ReconnectingTransport::msgFrame('updates', $subscriber->sid, 'live') . "-ERR 'Unknown Protocol Operation'\r\n",
        );

        $transport->dropConnection();
        $connection->processIncoming()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(3, $transport->connectCalls, 'the first attempt failed on the -ERR, the second succeeded');
        self::assertSame(['SUB updates ' . $subscriber->sid], $transport->controlLinesStartingWith('SUB', 2));
        self::assertSame(
            ['Subscription queue overflow for sid ' . $subscriber->sid],
            $recorder->errorsContaining('overflow'),
        );
        $subscriber->busy->complete();
        $subscriber->dispatcher?->await();
    }

    /**
     * An overflow among the frames parsed before a corrupt one is reported too, while the corrupt frame
     * fails the attempt as before. That overflow used to vanish without a trace.
     */
    public function testOverflowBeforeACorruptFrameIsReported(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->errorPolicyConnection($transport, $recorder);
        $subscriber = $this->subscriberFallingBehind($connection, $transport);
        $this->answerTheFirstReplayWith(
            $transport,
            ReconnectingTransport::msgFrame('updates', $subscriber->sid, 'live') . "BOGUS\r\n",
        );

        $transport->dropConnection();
        $connection->processIncoming()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(3, $transport->connectCalls, 'the first attempt failed on the corrupt frame');
        self::assertSame(
            ['Subscription queue overflow for sid ' . $subscriber->sid],
            $recorder->errorsContaining('overflow'),
        );
        $subscriber->busy->complete();
        $subscriber->dispatcher?->await();
    }

    /**
     * drainSubscription()'s flush runs into the full queue of another subscription: that overflow is
     * reported, and the flush reads on to its PONG, so a message still in flight for the drained
     * subscription is delivered instead of arriving after the subscription was removed. Either way: a
     * drain never fails for what its flush runs into.
     */
    #[DataProvider('eitherWay')]
    public function testDrainSubscriptionFlushReadsOnPastAnotherSubscriptionsOverflow(bool $failOperations): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->errorPolicyConnection($transport, $recorder, $failOperations);
        $subscriber = $this->subscriberFallingBehind($connection, $transport);
        $drained = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $sid = $connection->subscribe('orders', static function (NatsMessage $message) use ($drained): void {
            $drained->payloads[] = $message->payload;
        })->await();
        // Before the PONG: a message that overflows the other subscription's queue, then one the server
        // sent the drained subscription before it processed the UNSUB.
        $this->answerTheNextPingWith(
            $transport,
            ReconnectingTransport::msgFrame('updates', $subscriber->sid, 'overflow'),
            ReconnectingTransport::msgFrame('orders', $sid, 'in-flight') . "PONG\r\n",
        );

        $connection->drainSubscription($sid)->await();

        self::assertSame(['in-flight'], $drained->payloads);
        self::assertFalse($connection->isSubscriptionActive($sid));
        self::assertSame(['Subscription queue overflow for sid ' . $subscriber->sid], $recorder->errors);
        $subscriber->busy->complete();
        $subscriber->dispatcher?->await();
        self::assertSame(['m1', 'm2', 'm3', 'm4'], $subscriber->payloads, 'the message that overflowed was dropped');
    }

    /**
     * drain()'s flush runs into the full queue of one subscription: that overflow is reported, and the
     * flush reads on to its PONG, so a message still in flight for another subscription is delivered
     * instead of being lost when the drain closes the socket. Either way, as for drainSubscription().
     */
    #[DataProvider('eitherWay')]
    public function testDrainFlushReadsOnPastAnOverflow(bool $failOperations): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->errorPolicyConnection($transport, $recorder, $failOperations);
        $subscriber = $this->subscriberFallingBehind($connection, $transport);
        $other = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $sid = $connection->subscribe('orders', static function (NatsMessage $message) use ($other, $subscriber): void {
            $other->payloads[] = $message->payload;
            // The flush got past the overflow: let the busy subscriber catch up, so the drain can end.
            $subscriber->busy->complete();
        })->await();
        // Before the PONG: a message that overflows the busy subscription's queue, then one the server sent
        // the other subscription before it processed the drain's UNSUB.
        $this->answerTheNextPingWith(
            $transport,
            ReconnectingTransport::msgFrame('updates', $subscriber->sid, 'overflow'),
            ReconnectingTransport::msgFrame('orders', $sid, 'in-flight') . "PONG\r\n",
        );

        $connection->drain()->await();

        self::assertSame(['in-flight'], $other->payloads);
        self::assertSame(['Subscription queue overflow for sid ' . $subscriber->sid], $recorder->errors);
        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::Closed], $recorder->events);
        $subscriber->dispatcher?->await();
        self::assertSame(['m1', 'm2', 'm3', 'm4'], $subscriber->payloads, 'the message that overflowed was dropped');
    }

    /**
     * Every operation, with each kind of overflow: a subscription's own queue, and a SubscriptionQueue's
     * polling buffer.
     *
     * @return iterable<string, array{string, string}>
     */
    public static function operationsAndOverflows(): iterable
    {
        foreach (self::operationsThatRead() as $name => [$operation]) {
            yield $name . ' / subscription queue' => [$operation, 'subscription queue'];
            yield $name . ' / polling buffer' => [$operation, 'polling buffer'];
        }
    }

    /**
     * An operation that reads while it waits for a result of its own runs into another subscription's
     * overflow, in the very read that brings its result: it completes with that result, and the overflow
     * is reported once. It used to fail with the overflow, the result lost.
     */
    #[DataProvider('operationsAndOverflows')]
    public function testOperationCompletesDespiteAnotherSubscriptionsOverflow(string $operation, string $kind): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder);
        [$overflow, $sid] = $this->overflow($kind, $client, $transport);
        [$run, $expected] = $this->prepareOperation($operation, $client, $transport, $overflow);

        self::assertSame($expected, $run());
        self::assertSame(['Subscription queue overflow for sid ' . $sid], $recorder->errorsContaining('overflow'));
    }

    /**
     * With slowConsumerErrorsFailOperations the overflow fails the operation, as before that option existed -
     * and a polling buffer's overflow is also reported, as it was then, so that code swallowing the
     * operation's failure cannot make it vanish.
     */
    #[DataProvider('operationsAndOverflows')]
    public function testOperationFailsOnAnotherSubscriptionsOverflowWhenConfiguredTo(string $operation, string $kind): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder, failOperations: true);
        [$overflow, $sid] = $this->overflow($kind, $client, $transport);
        [$run] = $this->prepareOperation($operation, $client, $transport, $overflow);

        try {
            $run();
            self::fail('expected the overflow');
        } catch (SlowConsumerException $e) {
            self::assertSame($sid, $e->sid);
        }

        self::assertCount($kind === 'polling buffer' ? 1 : 0, $recorder->errorsContaining('overflow'));
    }

    /** @return iterable<string, array{string}> */
    public static function collections(): iterable
    {
        yield 'requestMany(max 3)' => ['requestMany'];
        yield 'JetStream fetchBatch(3)' => ['fetchBatch'];
    }

    /**
     * Guard: with slowConsumerErrorsFailOperations, a requestMany() for three replies or a fetchBatch() of three that
     * has received one, in a read of its own, still fails with another subscription's overflow that its next read runs
     * into, the connection open. What such a collection has received is returned instead of its read's failure only
     * when the connection is going: lost, failed over, or closed ({@see
     * OperationReadReconnectTest::testACollectionWhoseReadFailsWithTheConnectionGoingReturnsWhatItReceived()}).
     */
    #[DataProvider('collections')]
    public function testACollectionThatHasReceivedPartOfItsResultStillFailsOnAnotherSubscriptionsOverflowWhenConfiguredTo(string $operation): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder, failOperations: true);
        [$overflow, $sid] = $this->overflow('subscription queue', $client, $transport);
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport, $overflow): array {
            $inboxSid = $replyTo === null ? null : $transport->sidFor($replyTo);
            if ($replyTo === null || $inboxSid === null) {
                return [];
            }

            // A read for the first of the three, then one for the overflow.
            return [
                $subject === 'svc'
                    ? ReconnectingTransport::msgFrame($replyTo, $inboxSid, 'r-1')
                    : ReconnectingTransport::msgFrame('evt.s', $inboxSid, 'm-1', '$JS.ACK.ORDERS.worker.1.1.1.0.0'),
                $overflow,
            ];
        };

        try {
            if ($operation === 'requestMany') {
                $client->requestMany('svc', 'ping', maxResponses: 3, totalTimeoutMs: 30_000)->await(new TimeoutCancellation(5));
            } else {
                $client->jetStream()->fetchBatch('ORDERS', 'worker', 3, 30_000)->await(new TimeoutCancellation(5));
            }
            self::fail('expected the overflow');
        } catch (SlowConsumerException $e) {
            self::assertSame($sid, $e->sid);
        }

        self::assertSame(ConnectionState::Open, $client->state(), 'the overflow left the connection open');
    }

    /** @return iterable<string, array{string}> */
    public static function failuresTakingPrecedence(): iterable
    {
        yield 'a fatal -ERR' => ["-ERR 'Unknown Protocol Operation'\r\n"];
        yield 'a corrupt frame the connection cannot recover from' => ["BOGUS\r\n"];
    }

    /**
     * With slowConsumerErrorsFailOperations, a polling buffer's overflow in a request's read is reported once
     * even when something worse from the same read fails the request in its place. It used to be reported
     * twice: once as the overflow the request was to fail with, and again when the other failure replaced it.
     */
    #[DataProvider('failuresTakingPrecedence')]
    public function testOverflowIsReportedOnceWhenAnotherFailureFailsTheOperationInstead(string $failure): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder, failOperations: true, reconnect: false);
        $queue = $this->fullQueue($client, $transport);
        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $subject === 'svc' && $replyTo !== null
            ? [ReconnectingTransport::msgFrame('backlog', $queue->sid, 'b3') . $failure]
            : [];

        try {
            $client->request('svc', 'ping')->await();
            self::fail('expected the request to fail');
        } catch (ConnectionException $e) {
            self::assertNotInstanceOf(SlowConsumerException::class, $e);
        }

        self::assertSame(['Subscription queue overflow for sid ' . $queue->sid], $recorder->errorsContaining('overflow'));
    }

    /**
     * Your own read throws a polling buffer's overflow only once it has delivered the rest of what it read -
     * a later subscription's message, and the reply another fiber's request waits for. It used to stop at
     * the overflow, and that request timed out.
     */
    public function testOwnReadDeliversTheRestBeforeThrowingAPollingBufferOverflow(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder);
        $queue = $this->fullQueue($client, $transport);
        $later = $this->recordingSubscription($client, 'updates');
        // The reply inbox is in place first, so that the request below writes only its PUB: setting the inbox up
        // also writes a PING, whose PONG would come ahead of the reply and be all your loop's one read gets.
        $transport->responder = static fn(string $subject, ?string $replyTo): array => $replyTo === null ? [] : $transport->replyFrame($replyTo, 'ready');
        self::assertSame('ready', $client->request('warm.up', '', 1_000)->await()->payload);
        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $subject === 'svc' && $replyTo !== null
            ? [
                ReconnectingTransport::msgFrame('backlog', $queue->sid, 'b3')
                . ReconnectingTransport::msgFrame('backlog', $queue->sid, 'b4')
                . ReconnectingTransport::msgFrame('updates', $later->sid, 'u1')
                . implode('', $transport->replyFrame($replyTo, 'pong')),
            ]
            : [];
        // Your read loop holds the socket when the request's reply arrives.
        $reader = async(static fn(): int => $client->processIncoming()->await());
        delay(0.01);
        $request = $client->request('svc', 'ping', 1_000);

        try {
            $reader->await();
            self::fail('expected the overflow');
        } catch (SlowConsumerException $e) {
            self::assertSame($queue->sid, $e->sid);
        }

        self::assertSame(['u1'], $later->payloads, 'delivered by the same read');
        self::assertSame('pong', $request->await()->payload);
        // b3 was thrown; b4, a second overflow of the same read, is reported - each once.
        self::assertSame(['Subscription queue overflow for sid ' . $queue->sid], $recorder->errorsContaining('overflow'));
        self::assertSame(2, $queue->droppedCount());
    }

    /** @return iterable<string, array{string}> */
    public static function readsCallingAHandler(): iterable
    {
        yield "a request's read" => ['request'];
        yield "a request's read, with handlerErrorsFailOperations" => ['request failing'];
        yield "the heartbeat's read" => ['heartbeat'];
    }

    /**
     * An overflow a handler lets escape - here the one its own poll of a SubscriptionQueue threw - is that
     * handler's failure, not an overflow of the subscription being delivered, and is treated like any other
     * exception from a handler: the request whose read called the handler reports it once and gets its reply,
     * as the heartbeat reports it, and with handlerErrorsFailOperations it fails that request without being
     * reported on top. A request used to take it for a new overflow and report it, after the handler had already
     * been given it.
     */
    #[DataProvider('readsCallingAHandler')]
    public function testAnOverflowEscapingAHandlerIsThatHandlersFailure(string $read): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder, handlerFailuresFailOperations: $read === 'request failing');
        $queue = $client->subscribeQueue('jobs')->await();
        $trigger = $client->subscribe('trigger', static function () use ($queue, $transport): void {
            // Three jobs in the poll's read; the queue's subscription holds two. Nothing here catches the
            // overflow that the poll then throws.
            $transport->pushFrame(
                ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j1')
                . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j2')
                . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j3'),
            );
            $queue->setTimeout(1.0)->next();
        })->await();

        if ($read === 'heartbeat') {
            $transport->pushFrame(ReconnectingTransport::msgFrame('trigger', $trigger, 'go'));
            (new \ReflectionMethod(NatsConnection::class, 'consumeHeartbeatResponse'))->invoke($this->connectionOf($client));

            self::assertSame(['Subscription queue overflow for sid ' . $queue->sid], $recorder->errorsContaining('overflow'));

            return;
        }

        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $subject === 'svc' && $replyTo !== null
            ? [ReconnectingTransport::msgFrame('trigger', $trigger, 'go') . implode('', $transport->replyFrame($replyTo, 'pong'))]
            : [];

        if ($read === 'request') {
            self::assertSame('pong', $client->request('svc', 'ping')->await()->payload);
            self::assertSame(['Subscription queue overflow for sid ' . $queue->sid], $recorder->errorsContaining('overflow'));

            return;
        }

        try {
            $client->request('svc', 'ping')->await();
            self::fail('expected the handler failure');
        } catch (SlowConsumerException $e) {
            self::assertSame($queue->sid, $e->sid);
        }

        self::assertSame([], $recorder->errorsContaining('overflow'));
    }

    /**
     * Service::run() has no caller to fail: it reports an overflow even with slowConsumerErrorsFailOperations,
     * and answers the request that arrived behind it. Following the option, it swallowed the overflow.
     */
    #[DataProvider('overflowKinds')]
    public function testServiceRunReportsOverflowsEvenWhenOperationsAreConfiguredToFail(string $kind): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder, failOperations: true);
        [$overflow, $sid] = $this->overflow($kind, $client, $transport);
        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => 'echo:' . $message->payload);
        $service->start()->await();
        $endpoint = $transport->sidFor('svc.echo');
        self::assertNotNull($endpoint);
        $transport->pushFrame($overflow . ReconnectingTransport::msgFrame('svc.echo', $endpoint, 'hi', '_INBOX.caller.1'));

        $service->run(timeoutSeconds: 0.2)->await();

        self::assertCount(1, $transport->controlLinesStartingWith('PUB _INBOX.caller.1 '), 'the request was answered');
        self::assertSame(['Subscription queue overflow for sid ' . $sid], $recorder->errorsContaining('overflow'));
    }

    /** @return iterable<string, array{string}> */
    public static function overflowKinds(): iterable
    {
        yield 'subscription queue' => ['subscription queue'];
        yield 'polling buffer' => ['polling buffer'];
    }

    /**
     * fetchAll() failing on its own subscription's overflow puts back what it had already taken from the
     * buffer, in order, for the next call. Those messages used to be lost with the failed call.
     */
    public function testSubscriptionQueueFetchAllKeepsWhatItCollectedWhenItFails(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->errorPolicyClient($transport, new LifecycleRecorder());
        $queue = $client->subscribeQueue('jobs')->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j1'));
        $client->processIncoming()->await();
        // fetchAll() takes j1 from the buffer, then reads three more in one read: the third overflows.
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j2')
            . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j3')
            . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j4'),
        );

        try {
            $queue->setTimeout(1.0)->fetchAll(10);
            self::fail('expected the overflow');
        } catch (SlowConsumerException $e) {
            self::assertSame($queue->sid, $e->sid);
        }

        self::assertSame(['j1', 'j2', 'j3'], self::payloads($queue->fetchAll(3)));
    }

    /**
     * subscribeQueue() replays the messages that arrived before its queue existed; one that does not fit is
     * counted and reported, and the queue is returned. It used to throw out of subscribeQueue(), leaving the
     * subscription feeding a queue nobody held.
     */
    public function testSubscribeQueueReportsAnEarlyOverflowAndReturnsTheQueue(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder);
        $sid = (new \ReflectionProperty(NatsConnection::class, 'nextSid'))->getValue($this->connectionOf($client));
        self::assertIsInt($sid);
        // The SUB write is held up while reads deliver three messages for it, in two reads.
        $transport->stallNextWriteContaining('SUB jobs', 0.2);
        $subscribing = async(static fn(): SubscriptionQueue => $client->subscribeQueue('jobs')->await());
        delay(0.02);
        $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $sid, 'j1') . ReconnectingTransport::msgFrame('jobs', $sid, 'j2'));
        $client->processIncoming()->await();
        $transport->pushFrame(ReconnectingTransport::msgFrame('jobs', $sid, 'j3'));
        $client->processIncoming()->await();

        $queue = $subscribing->await();

        self::assertSame($sid, $queue->sid);
        self::assertSame(1, $queue->droppedCount());
        self::assertSame(['Subscription queue overflow for sid ' . $sid], $recorder->errorsContaining('overflow'));
        self::assertSame(['j1', 'j2'], self::payloads($queue->fetchAll(2)));
    }

    /**
     * Your own read ending in a corrupt frame, with an overflow of a subscription's queue and of a polling
     * buffer in it: one is thrown and the other reported. The second used to hide the first, which was
     * neither thrown nor reported.
     */
    public function testOwnReadEndingInACorruptFrameSurfacesEachOverflowOnce(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder);
        $queue = $this->fullQueue($client, $transport);
        $other = $client->subscribe('other', static function (): void {})->await();
        $transport->pushFrame(self::overflowOf($other) . ReconnectingTransport::msgFrame('backlog', $queue->sid, 'b3') . "BOGUS\r\n");

        $thrown = null;
        try {
            $client->processIncoming()->await();
        } catch (SlowConsumerException $e) {
            $thrown = $e->sid;
        }

        self::assertSame($other, $thrown, 'the intake overflow is thrown');
        self::assertSame(['Subscription queue overflow for sid ' . $queue->sid], $recorder->errorsContaining('overflow'));
    }

    /** @return iterable<string, array{bool}> */
    public static function overflowAndFatalErrorOrders(): iterable
    {
        yield 'overflow, then -ERR' => [true];
        yield '-ERR, then overflow' => [false];
    }

    /**
     * Your own read that meets a subscription's overflow and a fatal -ERR throws the -ERR, whichever came
     * first, and reports the overflow: the -ERR says that the connection failed, the overflow only that a
     * subscriber fell behind. An overflow ahead of the -ERR used to be thrown in its place, the -ERR only
     * reported.
     */
    #[DataProvider('overflowAndFatalErrorOrders')]
    public function testOwnReadThrowsAFatalErrorAndReportsTheOverflowWhicheverCameFirst(bool $overflowFirst): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder);
        $other = $client->subscribe('other', static function (): void {})->await();
        $fatal = "-ERR 'Unknown Protocol Operation'\r\n";
        $transport->pushFrame($overflowFirst ? self::overflowOf($other) . $fatal : $fatal . self::overflowOf($other));

        try {
            $client->processIncoming()->await();
            self::fail('expected the -ERR');
        } catch (ConnectionException $e) {
            self::assertNotInstanceOf(SlowConsumerException::class, $e);
            self::assertStringContainsString('Unknown Protocol Operation', $e->getMessage());
        }

        self::assertSame(['Subscription queue overflow for sid ' . $other], $recorder->errorsContaining('overflow'));
        self::assertSame([], $recorder->errorsContaining('Unknown Protocol Operation'), 'thrown, not reported as well');
    }

    /**
     * Your own read ending in a corrupt frame that the connection cannot recover from - reconnecting is off -
     * fails with that, and the overflow it was to throw is reported instead of vanishing.
     */
    public function testOwnReadEndingInACorruptFrameItCannotRecoverFromReportsTheOverflow(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder, reconnect: false);
        $other = $client->subscribe('other', static function (): void {})->await();
        $transport->pushFrame(self::overflowOf($other) . "BOGUS\r\n");

        try {
            $client->processIncoming()->await();
            self::fail('expected the connection to fail');
        } catch (ConnectionException $e) {
            self::assertNotInstanceOf(SlowConsumerException::class, $e);
        }

        self::assertSame(['Subscription queue overflow for sid ' . $other], $recorder->errorsContaining('overflow'));
    }

    /**
     * An error listener that reacts to an overflow by draining that subscription reads on the connection from
     * inside the report, and still finds the rest of the read that overflowed queued ahead of that next read:
     * another subscription's messages keep their order. Reports used to run in the middle of the read, before
     * the rest of it was queued, and the next read's message overtook the one behind the overflow.
     */
    public function testListenerThatDrainsTheSlowSubscriptionKeepsOtherMessagesInOrder(): void
    {
        $transport = new ReconnectingTransport();
        $holder = new class {
            public ?NatsClient $client = null;
        };
        $client = $this->own(new NatsClient(
            new NatsOptions(
                connectTimeoutMs: 500,
                pingIntervalSeconds: 0,
                errorListener: static function (\Throwable $error) use ($holder): void {
                    if ($error instanceof SlowConsumerException) {
                        $holder->client?->drainSubscription($error->sid)->await();
                    }
                },
                maxPendingMessagesPerSubscription: 2,
                slowConsumerPolicy: SlowConsumerPolicy::Error,
            ),
            $transport,
        ));
        $this->opened[] = $client;
        $client->connect()->await();
        $holder->client = $client;
        $slow = $client->subscribe('slow', static function (): void {})->await();
        $orders = $this->recordingSubscription($client, 'orders');
        // The read the drain makes brings the next order.
        $transport->afterWrite = static function (string $bytes) use ($transport, $slow, $orders): void {
            if ($bytes === 'UNSUB ' . $slow . "\r\n") {
                $transport->afterWrite = null;
                $transport->pushFrame(ReconnectingTransport::msgFrame('orders', $orders->sid, 'x2'));
            }
        };
        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $subject === 'svc' && $replyTo !== null
            ? [
                ReconnectingTransport::msgFrame('slow', $slow, 's1')
                . ReconnectingTransport::msgFrame('slow', $slow, 's2')
                . ReconnectingTransport::msgFrame('slow', $slow, 's3')
                . ReconnectingTransport::msgFrame('orders', $orders->sid, 'x1')
                . implode('', $transport->replyFrame($replyTo, 'pong')),
            ]
            : [];

        self::assertSame('pong', $client->request('svc', 'ping')->await()->payload);
        self::assertSame(['x1', 'x2'], $orders->payloads);
        self::assertFalse($client->isSubscriptionActive($slow), 'the listener drained it');
    }

    /**
     * The heartbeat's read ending in a corrupt frame, with an overflow of a subscription's queue and a
     * handler that throws: both are reported. The handler's failure used to hide the overflow.
     */
    public function testHeartbeatReadEndingInACorruptFrameReportsBothAnOverflowAndAHandlerFailure(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder);
        $other = $client->subscribe('other', static function (): void {})->await();
        $failing = $client->subscribe('failing', static function (): void {
            throw new \RuntimeException('handler failed');
        })->await();
        $transport->pushFrame(self::overflowOf($other) . ReconnectingTransport::msgFrame('failing', $failing, 'x') . "BOGUS\r\n");

        (new \ReflectionMethod(NatsConnection::class, 'consumeHeartbeatResponse'))->invoke($this->connectionOf($client));

        self::assertSame(['Subscription queue overflow for sid ' . $other], $recorder->errorsContaining('overflow'));
        self::assertSame(['handler failed'], $recorder->errorsContaining('handler failed'));
    }

    /**
     * A message the server sent right behind a reconnect's handshake PONG overflows a busy subscriber's
     * queue: reported, and the attempt goes on. It used to fail the attempt - with a single attempt
     * allowed, closing the connection.
     */
    public function testReconnectHandshakeReportsAnOverflowOfTheFramesBehindItsPong(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->errorPolicyConnection($transport, $recorder);
        $subscriber = $this->subscriberFallingBehind($connection, $transport);
        $transport->handshakeTrailer = ReconnectingTransport::msgFrame('updates', $subscriber->sid, 'early');

        $transport->dropConnection();
        $connection->processIncoming()->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertCount(2, $transport->connectCalls, 'the first attempt succeeded');
        self::assertSame(['Subscription queue overflow for sid ' . $subscriber->sid], $recorder->errorsContaining('overflow'));
        $subscriber->busy->complete();
        $subscriber->dispatcher?->await();
    }

    /**
     * A handler that throws while a reconnect's handshake delivers - a server sent a frame right behind the
     * PONG, and messages were still queued - is reported, and the attempt goes on. It used to fail the
     * attempt, as if the server had refused it.
     */
    public function testReconnectHandshakeReportsAHandlerFailureInsteadOfFailingTheAttempt(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder);
        $failing = $client->subscribe('failing', static function (NatsMessage $message): void {
            throw new \RuntimeException('handler failed on ' . $message->payload);
        })->await();
        // Your read stops at the first failure: the second message stays queued.
        $transport->pushFrame(ReconnectingTransport::msgFrame('failing', $failing, 'f1') . ReconnectingTransport::msgFrame('failing', $failing, 'f2'));
        try {
            $client->processIncoming()->await();
            self::fail('expected the handler failure');
        } catch (\RuntimeException $e) {
            self::assertSame('handler failed on f1', $e->getMessage());
        }
        $transport->handshakeTrailer = ReconnectingTransport::INFO;

        // An operation's read notices the drop and leaves f2 queued for the handshake: your own read would continue
        // f2 before it read (#186).
        $transport->dropConnection();
        $client->readIncomingForOperation()->await();

        self::assertSame(1, $client->statistics()->reconnects);
        self::assertCount(2, $transport->connectCalls, 'the first attempt succeeded');
        self::assertSame(['handler failed on f2'], $recorder->errorsContaining('handler failed'));
    }

    /**
     * A fatal -ERR behind a reconnect's handshake PONG still fails the attempt when a handler throws while
     * the queued messages are delivered: the handler's failure is reported instead of standing in for it.
     */
    public function testReconnectHandshakeFailsOnAFatalErrorAndReportsAHandlerFailure(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder);
        $failing = $client->subscribe('failing', static function (NatsMessage $message): void {
            throw new \RuntimeException('handler failed on ' . $message->payload);
        })->await();
        // Your read stops at the first failure: the second message stays queued.
        $transport->pushFrame(ReconnectingTransport::msgFrame('failing', $failing, 'f1') . ReconnectingTransport::msgFrame('failing', $failing, 'f2'));
        try {
            $client->processIncoming()->await();
            self::fail('expected the handler failure');
        } catch (\RuntimeException $e) {
            self::assertSame('handler failed on f1', $e->getMessage());
        }
        $transport->handshakeTrailer = "-ERR 'Unknown Protocol Operation'\r\n";

        // An operation's read notices the drop and leaves f2 queued for the handshakes, as above (#186).
        $transport->dropConnection();
        $client->readIncomingForOperation()->await();

        self::assertCount(3, $transport->connectCalls, 'the first attempt failed on the -ERR, the second succeeded');
        self::assertSame(['handler failed on f2'], $recorder->errorsContaining('handler failed'));
    }

    /**
     * A drain that winds down without a connection delivers the backlog past a polling buffer's overflow.
     * The overflow used to end each delivery pass, and past the drain's deadline the rest was discarded.
     */
    public function testDrainWindingDownDeliversTheBacklogPastAPollingBufferOverflow(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder, requestTimeoutMs: 100);
        $hold = new HeldUpDelivery(fallbackSeconds: 2.0);
        $slow = $client->subscribe('slow', $hold->handler())->await();
        $queue = $this->fullQueue($client, $transport);
        $later = new class {
            public int $sid = 0;
            /** @var list<string> */
            public array $payloads = [];
        };
        $later->sid = $client->subscribe('updates', static function (NatsMessage $message) use ($later, $hold): void {
            $later->payloads[] = $message->payload;
            // Only the drain's backlog pass can get here while slow's handler holds the application's read up: let
            // that handler go, so that the read ends and the drain can tear down.
            $hold->end("the drain's delivery reaching updates");
        })->await();
        // Your read is held up in slow's handler, which awaits: the rest stays queued for the drain.
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('slow', $slow, 's')
            . ReconnectingTransport::msgFrame('backlog', $queue->sid, 'b3')
            . ReconnectingTransport::msgFrame('updates', $later->sid, 'u1'),
        );
        $heldUp = async(static fn(): int => $client->processIncoming()->await());
        $hold->began->getFuture()->await(new TimeoutCancellation(2));
        $transport->refuseDials();
        $transport->dropConnection();
        $reader = async(static fn(): int => $client->processIncoming()->await());
        $reader->ignore();
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Connecting);

        $client->drain()->await();

        self::assertSame("the drain's delivery reaching updates", $hold->endedBy);
        self::assertSame(['u1'], $later->payloads);
        self::assertSame(['Subscription queue overflow for sid ' . $queue->sid], $recorder->errorsContaining('overflow'));
        self::assertSame([], $recorder->errorsContaining('drain deadline exceeded'));
        self::assertSame(3, $heldUp->await(), 'the held-up read ends once its handler returns');
    }

    /**
     * drain() keeps to its time budget while a SubscriptionQueue's buffer overflows: once the budget is spent,
     * a slow handler gets no more messages, and what is left is reported as discarded. Each overflow used to
     * restart the delivery, and every restart gave the slow handler one more message past the deadline.
     */
    public function testDrainKeepsToItsBudgetWhileAPollingBufferOverflows(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        // A 0.2 s drain budget, and room for twelve messages per subscription.
        $client = $this->errorPolicyClient($transport, $recorder, requestTimeoutMs: 200, maxPending: 12);
        $slow = new class {
            public int $handled = 0;
        };
        $slowSid = $client->subscribe('slow', static function () use ($slow): void {
            // A handler that blocks for 50 ms per message.
            usleep(50_000);
            $slow->handled++;
        })->await();
        $queue = $client->subscribeQueue('backlog')->await();
        $backlog = '';
        for ($i = 1; $i <= 12; $i++) {
            $backlog .= ReconnectingTransport::msgFrame('backlog', $queue->sid, 'b' . $i);
        }
        $transport->pushFrame($backlog);
        $client->processIncoming()->await();
        // The drain's flush brings twelve messages for the slow handler, and twelve that the full queue
        // cannot take.
        $inFlight = '';
        for ($i = 1; $i <= 12; $i++) {
            $inFlight .= ReconnectingTransport::msgFrame('slow', $slowSid, 's' . $i);
        }
        for ($i = 1; $i <= 12; $i++) {
            $inFlight .= ReconnectingTransport::msgFrame('backlog', $queue->sid, 'x' . $i);
        }
        $this->answerTheNextPingWith($transport, $inFlight . "PONG\r\n");

        $client->drain()->await();

        self::assertLessThan(12, $slow->handled, 'the budget ran out before the slow handler had everything');
        self::assertCount(1, $recorder->errorsContaining('drain deadline exceeded'));
    }

    /**
     * A reported overflow reaches the error listener even when the logger throws on it, and it is logged
     * at error level: under SlowConsumerPolicy::Error a dropped message is an error.
     */
    public function testReportedOverflowReachesTheListenerDespiteAThrowingLoggerAndLogsAtErrorLevel(): void
    {
        foreach (['throwing', 'recording'] as $loggerKind) {
            $logger = $loggerKind === 'throwing'
                ? new ThrowingLogger('Subscription queue overflow')
                : new class extends AbstractLogger {
                    /** @var list<string> */
                    public array $levels = [];

                    /** @param array<mixed> $context */
                    public function log($level, string|\Stringable $message, array $context = []): void
                    {
                        if (str_contains((string) $message, 'Subscription queue overflow')) {
                            $this->levels[] = (string) $level;
                        }
                    }
                };
            $transport = new ReconnectingTransport();
            $recorder = new LifecycleRecorder();
            $client = $this->own(new NatsClient(
                new NatsOptions(
                    connectTimeoutMs: 500,
                    pingIntervalSeconds: 0,
                    errorListener: $recorder->errorListener(),
                    maxPendingMessagesPerSubscription: 2,
                    slowConsumerPolicy: SlowConsumerPolicy::Error,
                    logger: $logger,
                ),
                $transport,
            ));
            $this->opened[] = $client;
            $client->connect()->await();
            [$overflow, $sid] = $this->overflow('polling buffer', $client, $transport);
            [$run, $expected] = $this->prepareOperation('request', $client, $transport, $overflow);

            self::assertSame($expected, $run(), $loggerKind);
            self::assertSame(['Subscription queue overflow for sid ' . $sid], $recorder->errorsContaining('overflow'), $loggerKind);
            if ($logger instanceof ThrowingLogger) {
                continue;
            }

            self::assertSame(['error'], $logger->levels);
        }
    }

    /** @return iterable<string, array{string, SlowConsumerPolicy}> */
    public static function listingsAndPolicies(): iterable
    {
        foreach (['keys', 'history'] as $listing) {
            yield $listing . ' / Error' => [$listing, SlowConsumerPolicy::Error];
            yield $listing . ' / DropOldest' => [$listing, SlowConsumerPolicy::DropOldest];
        }
    }

    /**
     * A Key/Value listing is not cut short by the pending bound: one read can carry more records than it,
     * and the listing is bounded by the bucket anyway. It used to come back short under DropOldest, and fail
     * under Error.
     */
    #[DataProvider('listingsAndPolicies')]
    public function testKeyValueListingIsNotCutShortByThePendingBound(string $listing, SlowConsumerPolicy $policy): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->own(new NatsClient(
            new NatsOptions(connectTimeoutMs: 500, pingIntervalSeconds: 0, maxPendingMessagesPerSubscription: 2, slowConsumerPolicy: $policy),
            $transport,
        ));
        $this->opened[] = $client;
        $client->connect()->await();
        $consumer = $listing === 'keys' ? 'KEYS' : 'HIST';
        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => str_starts_with($subject, '$JS.API.CONSUMER.CREATE.') && $replyTo !== null
            ? $transport->replyFrame($replyTo, sprintf('{"stream_name":"KV_cfg","name":"%s","num_pending":3,"config":{"ack_policy":"none"}}', $consumer))
            : [];
        // All three records in one read, with a bound of two.
        $transport->afterWrite = static function (string $bytes) use ($transport, $listing, $consumer): void {
            if (preg_match('/^SUB _INBOX\.KV\.\S+ (\d+)\r\n$/', $bytes, $sub) !== 1) {
                return;
            }

            $transport->afterWrite = null;
            $records = '';
            foreach (['a', 'b', 'c'] as $i => $name) {
                $ack = sprintf('$JS.ACK.KV_cfg.%s.1.%d.%d.0.%d', $consumer, $i + 1, $i + 1, 2 - $i);
                $records .= $listing === 'keys'
                    ? ReconnectingTransport::hmsgFrame('$KV.cfg.' . $name, (int) $sub[1], "NATS/1.0\r\nNats-Sequence: " . ($i + 1) . "\r\n\r\n", '', $ack)
                    : ReconnectingTransport::msgFrame('$KV.cfg.theme', (int) $sub[1], $name, $ack);
            }
            $transport->pushFrame($records);
        };

        $result = $listing === 'keys'
            ? $client->jetStream()->keyValue('cfg')->keys()->await()
            : array_map(static fn($entry): ?string => $entry->value, $client->jetStream()->keyValue('cfg')->history('theme')->await());

        self::assertSame(['a', 'b', 'c'], $result);
    }

    /** Your own read throws a SubscriptionQueue's buffer overflow, as it throws a subscription's - and only throws it. */
    public function testProcessIncomingThrowsAPollingBufferOverflowWithoutReportingIt(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder);
        $queue = $this->fullQueue($client, $transport);
        $transport->pushFrame(ReconnectingTransport::msgFrame('backlog', $queue->sid, 'b3'));

        try {
            $client->processIncoming()->await();
            self::fail('expected the overflow');
        } catch (SlowConsumerException $e) {
            self::assertSame($queue->sid, $e->sid);
        }

        self::assertSame([], $recorder->errorsContaining('overflow'), 'thrown, not reported as well');
        self::assertSame(1, $queue->droppedCount());
    }

    /** @return iterable<string, array{string}> */
    public static function heartbeatReads(): iterable
    {
        yield 'a clean read' => [''];
        yield 'a read ending in a corrupt frame' => ["BOGUS\r\n"];
    }

    /**
     * The heartbeat's own read reports a SubscriptionQueue's buffer overflow and delivers the messages
     * behind it at once. The overflow used to cut the delivery short, leaving them for the next read.
     */
    #[DataProvider('heartbeatReads')]
    public function testHeartbeatReportsAPollingBufferOverflowAndDeliversTheRest(string $tail): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder);
        $queue = $this->fullQueue($client, $transport);
        $later = $this->recordingSubscription($client, 'updates');
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('backlog', $queue->sid, 'b3')
            . ReconnectingTransport::msgFrame('updates', $later->sid, 'u1')
            . $tail,
        );

        // The heartbeat timer's read, called directly.
        (new \ReflectionMethod(NatsConnection::class, 'consumeHeartbeatResponse'))->invoke($this->connectionOf($client));

        self::assertSame(['u1'], $later->payloads);
        self::assertSame(['Subscription queue overflow for sid ' . $queue->sid], $recorder->errorsContaining('overflow'));
    }

    /**
     * The heartbeat's own read reports a handler that throws, and delivers the messages behind it at once.
     * On a clean read the failure used to vanish without a trace, and either way the rest waited for the
     * next read.
     */
    #[DataProvider('heartbeatReads')]
    public function testHeartbeatReportsAHandlerFailureAndDeliversTheRest(string $tail): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder);
        $failing = $client->subscribe('failing', static function (): void {
            throw new \RuntimeException('handler failed');
        })->await();
        $later = $this->recordingSubscription($client, 'updates');
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('failing', $failing, 'x')
            . ReconnectingTransport::msgFrame('updates', $later->sid, 'u1')
            . $tail,
        );

        (new \ReflectionMethod(NatsConnection::class, 'consumeHeartbeatResponse'))->invoke($this->connectionOf($client));

        self::assertSame(['u1'], $later->payloads);
        self::assertSame(['handler failed'], $recorder->errorsContaining('handler failed'));
    }

    /**
     * A reconnect delivers what arrived while it re-subscribed once it is done: a SubscriptionQueue whose
     * buffer overflows there is reported, and the messages behind it are delivered in the same pass
     * instead of waiting for the next read.
     */
    public function testReconnectReportsAPollingBufferOverflowAndDeliversTheRestAtOnce(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder);
        $queue = $this->fullQueue($client, $transport);
        $later = $this->recordingSubscription($client, 'updates');
        // Right after the re-subscription: a message the full queue cannot take, and one behind it.
        $transport->afterWrite = static function (string $bytes) use ($transport, $queue, $later): void {
            if (str_contains($bytes, 'SUB updates ')) {
                $transport->afterWrite = null;
                $transport->pushFrame(
                    ReconnectingTransport::msgFrame('backlog', $queue->sid, 'b3')
                    . ReconnectingTransport::msgFrame('updates', $later->sid, 'u1'),
                );
            }
        };

        $transport->dropConnection();
        // This read finds the connection gone and runs the reconnect.
        $client->processIncoming()->await();

        self::assertSame(1, $client->statistics()->reconnects);
        self::assertSame(['u1'], $later->payloads);
        self::assertSame(['Subscription queue overflow for sid ' . $queue->sid], $recorder->errorsContaining('overflow'));
    }

    /**
     * A server can send frames right behind a handshake's PONG (an async INFO); the connection then
     * delivers what is queued. A SubscriptionQueue's buffer that overflows there is reported instead of
     * failing that reconnect attempt.
     */
    public function testReconnectHandshakeReportsAPollingBufferOverflowInsteadOfFailingTheAttempt(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder);
        $hold = new HeldUpDelivery(fallbackSeconds: 2.0);
        $slow = $client->subscribe('slow', $hold->handler())->await();
        $queue = $this->fullQueue($client, $transport);
        // Your read is held up in slow's handler, which awaits: the message for the full queue stays queued behind it.
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('slow', $slow, 's')
            . ReconnectingTransport::msgFrame('backlog', $queue->sid, 'b3'),
        );
        $heldUp = async(static fn(): int => $client->processIncoming()->await());
        $hold->began->getFuture()->await(new TimeoutCancellation(2));
        self::assertSame(0, $queue->droppedCount(), 'b3 is still queued');
        $transport->handshakeTrailer = ReconnectingTransport::INFO;

        $transport->dropConnection();
        $client->processIncoming()->await();

        self::assertSame(1, $client->statistics()->reconnects);
        self::assertCount(2, $transport->connectCalls, 'the first attempt succeeded');
        self::assertSame(['Subscription queue overflow for sid ' . $queue->sid], $recorder->errorsContaining('overflow'));
        self::assertSame(1, $queue->droppedCount());
        $hold->end('the end of the test');
        self::assertSame(2, $heldUp->await(), 'the held-up read ends once its handler returns');
    }

    /** @return iterable<string, array{string}> */
    public static function drains(): iterable
    {
        yield 'drain()' => ['drain'];
        yield 'drainSubscription()' => ['drainSubscription'];
    }

    /**
     * The flush of drain() or drainSubscription() runs into a SubscriptionQueue's full buffer: reported,
     * and the flush reads on, so a message still in flight for another subscription is delivered.
     */
    #[DataProvider('drains')]
    public function testDrainFlushReadsOnPastAPollingBufferOverflow(string $drain): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder);
        $queue = $this->fullQueue($client, $transport);
        $later = $this->recordingSubscription($client, 'orders');
        $this->answerTheNextPingWith(
            $transport,
            ReconnectingTransport::msgFrame('backlog', $queue->sid, 'b3'),
            ReconnectingTransport::msgFrame('orders', $later->sid, 'in-flight') . "PONG\r\n",
        );

        if ($drain === 'drain') {
            $client->drain()->await();
        } else {
            $client->drainSubscription($later->sid)->await();
        }

        self::assertSame(['in-flight'], $later->payloads);
        self::assertSame(['Subscription queue overflow for sid ' . $queue->sid], $recorder->errorsContaining('overflow'));
    }

    /**
     * An operation's read that ends in a corrupt frame still delivers past another subscription's overflow:
     * the request gets the reply that read brought, and the corrupt stream is recovered as usual.
     */
    #[DataProvider('overflowKinds')]
    public function testRequestReadEndingInACorruptFrameStillGetsItsReplyPastAnOverflow(string $kind): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder);
        [$overflow, $sid] = $this->overflow($kind, $client, $transport);
        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $subject === 'svc' && $replyTo !== null
            ? [$overflow . implode('', $transport->replyFrame($replyTo, 'pong')) . "BOGUS\r\n"]
            : [];

        self::assertSame('pong', $client->request('svc', 'ping')->await()->payload);
        self::assertSame(['Subscription queue overflow for sid ' . $sid], $recorder->errorsContaining('overflow'));
        self::assertSame(1, $client->statistics()->reconnects, 'the corrupt stream was recovered');
    }

    /** @return iterable<string, array{string}> */
    public static function polls(): iterable
    {
        yield 'fetch()' => ['fetch'];
        yield 'next()' => ['next'];
        yield 'next() with a timeout' => ['next with a timeout'];
        yield 'fetchAll()' => ['fetchAll'];
    }

    /** A polling queue still fails on an overflow of its own subscription: that loss is its caller's to see. */
    #[DataProvider('polls')]
    public function testSubscriptionQueueStillFailsOnItsOwnOverflow(string $poll): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->errorPolicyClient($transport, new LifecycleRecorder());
        $queue = $client->subscribeQueue('jobs')->await();
        // Three messages in one read: the subscription's queue holds two.
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j1')
            . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j2')
            . ReconnectingTransport::msgFrame('jobs', $queue->sid, 'j3'),
        );

        try {
            match ($poll) {
                'fetch' => $queue->fetch(),
                'next' => $queue->next(),
                'next with a timeout' => $queue->setTimeout(1.0)->next(),
                default => $queue->fetchAll(10),
            };
            self::fail('expected the overflow');
        } catch (SlowConsumerException $e) {
            self::assertSame($queue->sid, $e->sid);
        }

        self::assertSame(['j1', 'j2'], self::payloads($queue->fetchAll(2)), 'what fit is still delivered');
    }

    /**
     * A SubscriptionQueue polling its own subscription still fails when its own polling buffer overflows -
     * here a queue built with a smaller buffer than the connection's, so one read can bring it more than it
     * holds - once the rest of that read is delivered.
     */
    public function testSubscriptionQueueStillFailsWhenItsOwnPollingBufferOverflows(): void
    {
        $transport = new ReconnectingTransport();
        $client = $this->own(new NatsClient(
            new NatsOptions(
                connectTimeoutMs: 500,
                pingIntervalSeconds: 0,
                maxPendingMessagesPerSubscription: 4,
                slowConsumerPolicy: SlowConsumerPolicy::Error,
            ),
            $transport,
        ));
        $this->opened[] = $client;
        $client->connect()->await();
        $holder = new class {
            public ?SubscriptionQueue $queue = null;
        };
        $sid = $client->subscribe('jobs', static function (NatsMessage $message) use ($holder): void {
            $holder->queue?->enqueue($message);
        })->await();
        $queue = new SubscriptionQueue($client, $sid, 2, SlowConsumerPolicy::Error);
        $holder->queue = $queue;
        $later = $this->recordingSubscription($client, 'updates');
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('jobs', $sid, 'j1')
            . ReconnectingTransport::msgFrame('jobs', $sid, 'j2')
            . ReconnectingTransport::msgFrame('jobs', $sid, 'j3')
            . ReconnectingTransport::msgFrame('updates', $later->sid, 'u1'),
        );

        try {
            $queue->setTimeout(1.0)->next();
            self::fail('expected the overflow');
        } catch (SlowConsumerException $e) {
            self::assertSame($sid, $e->sid);
        }

        self::assertSame(['u1'], $later->payloads, 'delivered by the same read');
        self::assertSame('j1', $queue->next()?->payload, 'what fit is still delivered');
    }

    /**
     * Service::run() reads for the whole connection: another subscription's overflow is reported and the
     * service goes on answering requests. The overflow used to be swallowed without a trace.
     */
    public function testServiceReportsAnotherSubscriptionsOverflowAndKeepsServing(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $client = $this->errorPolicyClient($transport, $recorder);
        $other = $client->subscribe('other', static function (): void {})->await();
        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => 'echo:' . $message->payload);
        $service->start()->await();
        $endpoint = $transport->sidFor('svc.echo');
        self::assertNotNull($endpoint);
        $transport->pushFrame(self::overflowOf($other) . ReconnectingTransport::msgFrame('svc.echo', $endpoint, 'hi', '_INBOX.caller.1'));

        $service->run(timeoutSeconds: 0.2)->await();

        self::assertCount(1, $transport->controlLinesStartingWith('PUB _INBOX.caller.1 '), 'the request was answered');
        self::assertSame(['Subscription queue overflow for sid ' . $other], $recorder->errorsContaining('overflow'));
    }

    /** A client whose subscriptions hold at most two messages (or $maxPending), under SlowConsumerPolicy::Error. */
    private function errorPolicyClient(
        ReconnectingTransport $transport,
        LifecycleRecorder $recorder,
        bool $failOperations = false,
        int $requestTimeoutMs = 2_000,
        int $maxPending = 2,
        bool $reconnect = true,
        bool $handlerFailuresFailOperations = false,
    ): NatsClient {
        $client = $this->own(new NatsClient(
            new NatsOptions(
                connectTimeoutMs: 500,
                requestTimeoutMs: $requestTimeoutMs,
                reconnectEnabled: $reconnect,
                pingIntervalSeconds: 0,
                errorListener: $recorder->errorListener(),
                maxPendingMessagesPerSubscription: $maxPending,
                slowConsumerPolicy: SlowConsumerPolicy::Error,
                slowConsumerErrorsFailOperations: $failOperations,
                handlerErrorsFailOperations: $handlerFailuresFailOperations,
            ),
            $transport,
        ));
        $this->opened[] = $client;
        $client->connect()->await();

        return $client;
    }

    /**
     * Frames that overflow a subscription other than the operation's, and that subscription's sid: three
     * messages for a subscription whose queue holds two, or one for a SubscriptionQueue whose polling buffer
     * is full.
     *
     * @return array{string, int}
     */
    private function overflow(string $kind, NatsClient $client, ReconnectingTransport $transport): array
    {
        if ($kind === 'polling buffer') {
            $queue = $this->fullQueue($client, $transport);

            return [ReconnectingTransport::msgFrame('backlog', $queue->sid, 'b3'), $queue->sid];
        }

        $sid = $client->subscribe('other', static function (): void {})->await();

        return [self::overflowOf($sid), $sid];
    }

    /** A SubscriptionQueue holding two messages nobody has polled: its polling buffer is full. */
    private function fullQueue(NatsClient $client, ReconnectingTransport $transport): SubscriptionQueue
    {
        $queue = $client->subscribeQueue('backlog')->await();
        $transport->pushFrame(
            ReconnectingTransport::msgFrame('backlog', $queue->sid, 'b1')
            . ReconnectingTransport::msgFrame('backlog', $queue->sid, 'b2'),
        );
        $client->processIncoming()->await();

        return $queue;
    }

    /**
     * A subscription that records the payloads delivered to it.
     *
     * @return object{sid: int, payloads: list<string>}
     */
    private function recordingSubscription(NatsClient $client, string $subject): object
    {
        $subscription = new class {
            public int $sid = 0;
            /** @var list<string> */
            public array $payloads = [];
        };
        $subscription->sid = $client->subscribe($subject, static function (NatsMessage $message) use ($subscription): void {
            $subscription->payloads[] = $message->payload;
        })->await();

        return $subscription;
    }

    private function connectionOf(NatsClient $client): NatsConnection
    {
        $connection = (new \ReflectionProperty(NatsClient::class, 'connection'))->getValue($client);
        self::assertInstanceOf(NatsConnection::class, $connection);

        return $connection;
    }

    /** Three messages for the "other" subscription in one read: the third overflows its two-message queue. */
    private static function overflowOf(int $sid): string
    {
        return ReconnectingTransport::msgFrame('other', $sid, 'o1')
            . ReconnectingTransport::msgFrame('other', $sid, 'o2')
            . ReconnectingTransport::msgFrame('other', $sid, 'o3');
    }

    /** A connection whose subscriptions hold at most three messages, under SlowConsumerPolicy::Error. */
    private function errorPolicyConnection(ReconnectingTransport $transport, LifecycleRecorder $recorder, bool $failOperations = false): NatsConnection
    {
        $connection = $this->own(new NatsConnection(
            new NatsOptions(
                connectTimeoutMs: 500,
                requestTimeoutMs: 2_000,
                reconnectEnabled: true,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 5,
                reconnectMaxDelayMs: 20,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
                connectionListener: $recorder->connectionListener(),
                errorListener: $recorder->errorListener(),
                maxPendingMessagesPerSubscription: 3,
                slowConsumerPolicy: SlowConsumerPolicy::Error,
                slowConsumerErrorsFailOperations: $failOperations,
            ),
            $transport,
        ));
        $this->opened[] = $connection;
        $connection->connect()->await();

        return $connection;
    }

    /**
     * Subscribes a handler that is still busy with its first message ("m1") while three more wait
     * behind it: the subscription's queue is full.
     *
     * @return object{sid: int, payloads: list<string>, busy: DeferredFuture<null>, dispatcher: ?Future<int>}
     */
    private function subscriberFallingBehind(NatsConnection $connection, ReconnectingTransport $transport): object
    {
        $subscriber = new class {
            public int $sid = 0;
            /** @var list<string> */
            public array $payloads = [];
            /** @var DeferredFuture<null> */
            public DeferredFuture $busy;
            /** @var ?Future<int> */
            public ?Future $dispatcher = null;

            public function __construct()
            {
                $this->busy = new DeferredFuture();
            }
        };
        $subscriber->sid = $connection->subscribe('updates', static function (NatsMessage $message) use ($subscriber): void {
            $subscriber->payloads[] = $message->payload;
            if ($message->payload === 'm1') {
                $subscriber->busy->getFuture()->await();
            }
        })->await();

        $transport->pushFrame(
            ReconnectingTransport::msgFrame('updates', $subscriber->sid, 'm1')
            . ReconnectingTransport::msgFrame('updates', $subscriber->sid, 'm2')
            . ReconnectingTransport::msgFrame('updates', $subscriber->sid, 'm3'),
        );
        $subscriber->dispatcher = async(static fn(): int => $connection->processIncoming()->await());
        $this->waitUntil(static fn(): bool => $subscriber->payloads === ['m1']);
        $transport->pushFrame(ReconnectingTransport::msgFrame('updates', $subscriber->sid, 'm4'));
        $connection->processIncoming()->await();

        return $subscriber;
    }

    /**
     * Makes the server answer the next PING with $chunks, one read each, instead of a plain PONG - the
     * last chunk carries the PONG.
     */
    private function answerTheNextPingWith(ReconnectingTransport $transport, string ...$chunks): void
    {
        $transport->answerPings = false;
        $transport->afterWrite = static function (string $bytes) use ($transport, $chunks): void {
            if ($bytes === "PING\r\n") {
                $transport->afterWrite = null;
                foreach ($chunks as $chunk) {
                    $transport->pushFrame($chunk);
                }
            }
        };
    }

    /** Makes the server answer the reconnect's first re-subscription with $frames, once. */
    private function answerTheFirstReplayWith(ReconnectingTransport $transport, string $frames): void
    {
        $transport->afterWrite = static function (string $bytes) use ($transport, $frames): void {
            if (str_contains($bytes, 'SUB updates ')) {
                $transport->afterWrite = null;
                $transport->pushFrame($frames);
            }
        };
    }
}
