<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Future;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use IDCT\NATS\Transport\TransportClosedException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\delay;

/**
 * A control frame - a SUB, an UNSUB, the PING of flush() or rtt() - whose write finds the socket dead is a
 * connection failure, as a failed publish write already was: the connection recovers from it, or closes for
 * good with reconnect off, instead of staying Open on a dead socket while the raw socket error reaches the
 * caller.
 *
 * Nothing noticed such a socket until the heartbeat's next tick, and every operation that wrote a control
 * frame meanwhile failed the same way. A JetStream fetch subscribes its inbox first, so after a server
 * restart the application had not seen yet, a fetch threw "Broken pipe" and a consumer built on it exited.
 */
final class FailedControlWriteTest extends TestCase
{
    use ReconnectScenarios;

    protected function tearDown(): void
    {
        $this->closeOpenedConnections();
    }

    /**
     * The subscription stays registered while the connection recovers, so the reconnect subscribes it on
     * the new connection, and subscribe() returns as if the write had succeeded.
     */
    public function testSubscribeWhoseSubWriteFindsTheSocketDeadRecoversAndStaysSubscribed(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $received = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $transport->failNextWriteContaining('SUB orders');

        $sid = $connection->subscribe('orders', static function (NatsMessage $message) use ($received): void {
            $received->payloads[] = $message->payload;
        })->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $transport->epoch(), 'the connection was re-established');
        self::assertSame($sid, $transport->sidFor('orders'), 'the reconnect subscribed it on the new connection');
        $transport->pushFrame(ReconnectingTransport::msgFrame('orders', $sid, 'o1'));
        $connection->processIncoming()->await();
        self::assertSame(['o1'], $received->payloads);
    }

    /**
     * With reconnect off the dead socket closes the connection for good, and subscribe() fails with that
     * close rather than with the socket's error, leaving no subscription behind (#116).
     */
    public function testSubscribeWhoseSubWriteFindsTheSocketDeadWithReconnectOffClosesTheConnection(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = new NatsConnection(
            new NatsOptions(
                connectTimeoutMs: 500,
                reconnectEnabled: false,
                pingIntervalSeconds: 0,
                connectionListener: $recorder->connectionListener(),
            ),
            $transport,
        );
        $this->opened[] = $connection;
        $connection->connect()->await();
        $transport->failNextWriteContaining('SUB orders');

        try {
            $connection->subscribe('orders', static function (): void {})->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Reconnect is disabled', $e->getMessage());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
        self::assertContains(ConnectionEvent::Closed, $recorder->events);
    }

    /**
     * subscribe() waits for the connection to come back within its own timeout, as it waits for a reconnect
     * that is already in flight; the reconnect carries on without it, and the subscription it gave up on is
     * not subscribed again.
     */
    public function testSubscribeGivesUpAtItsOwnTimeoutWhenTheReconnectOutlastsIt(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 200);
        $transport->refuseDials();
        $transport->failNextWriteContaining('SUB orders');

        $start = hrtime(true);
        try {
            $connection->subscribe('orders', static function (): void {})->await();
            self::fail('expected TimeoutException');
        } catch (TimeoutException $e) {
            self::assertSame('Subscribe to "orders" timed out waiting for the connection to be re-established', $e->getMessage());
        }

        $elapsed = $this->secondsSince($start);
        self::assertGreaterThanOrEqual(0.19, $elapsed);
        self::assertLessThan(1.0, $elapsed);

        $transport->acceptDials();
        $this->waitUntilOpen($connection);
        self::assertNull($transport->sidFor('orders'));

        // Rolled back for good (#116): the next subscription takes the next sid, and a frame for the one
        // given up on is discarded rather than handed to a subscription the caller never got.
        $received = new class {
            /** @var list<string> */
            public array $payloads = [];
        };
        $sid = $connection->subscribe('other', static function (NatsMessage $message) use ($received): void {
            $received->payloads[] = $message->payload;
        })->await();
        self::assertSame(2, $sid);
        $transport->pushFrame(ReconnectingTransport::msgFrame('orders', 1, 'o1') . ReconnectingTransport::msgFrame('other', 2, 'x1'));
        $connection->processIncoming()->await();
        self::assertSame(['x1'], $received->payloads);
    }

    /**
     * With waiting disabled subscribe() fails at once, as it does while a reconnect is in flight, and the
     * connection still recovers.
     */
    public function testSubscribeWithWaitingDisabledFailsAtOnceWhileTheConnectionRecovers(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, waitForReconnect: false);
        $transport->failNextWriteContaining('SUB orders');

        $start = hrtime(true);
        try {
            $connection->subscribe('orders', static function (): void {})->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
            self::assertSame(0, $e->getCode());
            self::assertInstanceOf(TransportClosedException::class, $e->getPrevious());
        }

        self::assertLessThan(0.1, $this->secondsSince($start));
        $this->waitUntil(static fn(): bool => $transport->epoch() === 1 && $connection->state() === ConnectionState::Open);
        self::assertNull($transport->sidFor('orders'));
    }

    /**
     * With waiting disabled nothing waits for the recovery the failed write started. With reconnect off it
     * closes the connection and fails with "Reconnect is disabled" - on its own fiber, where that failure
     * must not escape to the event loop as an unhandled error, which would stop the application's loop.
     */
    public function testRecoveryThatNothingWaitsForFailsWithoutAnUnhandledError(): void
    {
        $transport = new ReconnectingTransport();
        $connection = new NatsConnection(
            new NatsOptions(connectTimeoutMs: 500, reconnectEnabled: false, pingIntervalSeconds: 0, waitForReconnect: false),
            $transport,
        );
        $this->opened[] = $connection;
        $connection->connect()->await();
        $transport->failNextWriteContaining('SUB orders');

        try {
            $connection->subscribe('orders', static function (): void {})->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
        }

        $this->waitUntil(static fn(): bool => $connection->state() === ConnectionState::Closed);
        // The recovery's future is collected once its fiber is done: an unhandled error would surface here.
        gc_collect_cycles();
        delay(0.05);
        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * A subscribe whose connection the user closes while it waits for the recovery fails with the close,
     * leaving no subscription behind.
     */
    public function testSubscribeWaitingForTheRecoveryFailsWhenTheUserClosesTheConnection(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $transport->refuseDials();
        $transport->failNextWriteContaining('SUB orders');
        EventLoop::delay(0.05, static function () use ($connection): void {
            $connection->disconnect()->ignore();
        });

        try {
            $connection->subscribe('orders', static function (): void {})->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Connection is not open', $e->getMessage());
            self::assertSame(0, $e->getCode());
            self::assertInstanceOf(TransportClosedException::class, $e->getPrevious());
        }

        self::assertSame(ConnectionState::Closed, $connection->state());
    }

    /**
     * A SUB write that fails only after the application closed the connection and opened a new one - a
     * transport that fails a pending write late - does not recover the new connection, which never had the
     * subscription: subscribe() fails instead of returning a sid nothing is subscribed to.
     */
    public function testSubscribeWhoseWriteFailsAfterTheConnectionWasReplacedFails(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $transport->stallNextWriteContaining('SUB orders', 0.2, outlivesSession: true);
        EventLoop::delay(0.02, static function () use ($connection): void {
            $connection->disconnect()->await();
            $connection->connect()->await();
        });

        try {
            $connection->subscribe('orders', static function (): void {})->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Subscribe to "orders" failed: the connection was closed', $e->getMessage());
            self::assertSame(0, $e->getCode());
            self::assertInstanceOf(TransportClosedException::class, $e->getPrevious());
        }

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $transport->epoch(), 'the connection the application opened is left alone');
        self::assertNull($transport->sidFor('orders'));
    }

    /**
     * The messenger's case: a JetStream fetch after a drop nobody has noticed yet. Its inbox SUB finds the
     * socket dead, the connection recovers, and the fetch is served on the new connection.
     */
    public function testJetStreamFetchWhoseInboxSubscriptionFindsTheSocketDeadFetchesOnTheNewConnection(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = static function (string $subject, ?string $replyTo, string $payload) use ($transport): array {
            if ($subject !== '$JS.API.CONSUMER.MSG.NEXT.ORDERS.worker' || $replyTo === null) {
                return [];
            }

            $sid = $transport->sidFor($replyTo);

            return $sid === null ? [] : [ReconnectingTransport::msgFrame(
                'orders.created',
                $sid,
                'order-1',
                '$JS.ACK.ORDERS.worker.1.1.1.1700000000000000000.0',
            )];
        };
        $client = $this->connectClient($transport);
        $transport->failNextWriteContaining('SUB _INBOX.JS.FETCH');

        $messages = $client->jetStream()->fetchBatch('ORDERS', 'worker', 1, 2_000)->await();

        self::assertCount(1, $messages);
        self::assertSame('order-1', $messages[0]->payload);
        self::assertSame(1, $transport->epoch(), 'the fetch was served on the new connection');
    }

    /**
     * A request subscribes its reply inbox on first use. When that SUB finds the socket dead, the connection
     * recovers, the reconnect subscribes the inbox, and the request is answered on the new connection.
     */
    public function testRequestWhoseReplyInboxSubscriptionFindsTheSocketDeadIsAnsweredOnTheNewConnection(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $subject === 'svc.echo' && $replyTo !== null
            ? $transport->replyFrame($replyTo, 'hello')
            : [];
        $connection = $this->connect($transport);
        $transport->failNextWriteContaining('SUB _INBOX.');

        $reply = $connection->request('svc.echo', 'x', 1_000)->await();

        self::assertSame('hello', $reply->payload);
        self::assertSame(1, $transport->epoch(), 'the request was served on the new connection');
    }

    /**
     * A request that gives up on its reply inbox - the connection did not come back within its timeout -
     * leaves nothing half-established (#118): the reconnect does not subscribe the inbox given up on, and the
     * next request subscribes it once, and is answered.
     */
    public function testRequestThatGivesUpOnItsReplyInboxLetsTheNextRequestSubscribeItAgain(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $subject === 'svc.echo' && $replyTo !== null
            ? $transport->replyFrame($replyTo, 'hello')
            : [];
        $connection = $this->connect($transport, requestTimeoutMs: 200);
        $transport->refuseDials();
        $transport->failNextWriteContaining('SUB _INBOX.');

        try {
            $connection->request('svc.echo', 'x')->await();
            self::fail('expected TimeoutException');
        } catch (TimeoutException $e) {
            self::assertStringStartsWith('Subscribe to "_INBOX.', $e->getMessage());
        }

        $transport->acceptDials();
        $this->waitUntilOpen($connection);
        $reply = $connection->request('svc.echo', 'x')->await();

        self::assertSame('hello', $reply->payload);
        self::assertCount(1, $transport->controlLinesStartingWith('SUB _INBOX.', $transport->epoch()));
    }

    /** @return iterable<string, array{\Closure(NatsConnection): Future<mixed>}> */
    public static function pingingOperations(): iterable
    {
        yield 'flush()' => [static fn(NatsConnection $connection): Future => $connection->flush()];
        yield 'rtt()' => [static fn(NatsConnection $connection): Future => $connection->rtt()];
    }

    /**
     * A flush whose PING finds the socket dead cannot vouch for what was written before it, so it fails, but
     * with the connection's failure, and the connection has recovered by then.
     *
     * @param \Closure(NatsConnection): Future<mixed> $operation
     */
    #[DataProvider('pingingOperations')]
    public function testPingWhoseWriteFindsTheSocketDeadFailsTheOperationAndTheConnectionRecovers(\Closure $operation): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $transport->failNextWriteContaining('PING');

        try {
            $operation($connection)->await();
            self::fail('expected ConnectionException');
        } catch (ConnectionException $e) {
            self::assertSame('Connection lost before the server answered the PING', $e->getMessage());
            self::assertSame(0, $e->getCode());
            self::assertInstanceOf(TransportClosedException::class, $e->getPrevious());
        }

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $transport->epoch(), 'the connection was re-established');
        $operation($connection)->await();
    }

    /** A flush waits for the recovery within its own budget, and gives up at it. */
    public function testFlushWhosePingWriteFindsTheSocketDeadGivesUpAtItsBudget(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport, requestTimeoutMs: 200);
        $transport->refuseDials();
        $transport->failNextWriteContaining('PING');

        $start = hrtime(true);
        try {
            $connection->flush()->await();
            self::fail('expected TimeoutException');
        } catch (TimeoutException $e) {
            self::assertSame('Flush timed out waiting for the connection to be re-established', $e->getMessage());
        }

        self::assertLessThan(1.0, $this->secondsSince($start));
        self::assertSame(ConnectionState::Connecting, $connection->state());
    }

    /**
     * An UNSUB whose write finds the socket dead has what it asked for - the server dropped the
     * subscription with the connection - so unsubscribe() does not throw, exactly as on a connection that
     * is not open: it runs in finally-based clean-up, where an error would mask the caller's own (#116).
     * The next operation that needs the socket recovers the connection, without the subscription.
     */
    public function testUnsubscribeWhoseUnsubWriteFindsTheSocketDeadDoesNotThrow(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $sid = $connection->subscribe('orders', static function (): void {})->await();
        $transport->failNextWriteContaining('UNSUB');

        $connection->unsubscribe($sid)->await();
        $connection->publish('audit', 'a1')->await();

        self::assertSame(ConnectionState::Open, $connection->state());
        self::assertSame(1, $transport->epoch(), 'the publish recovered the connection');
        self::assertNull($transport->sidFor('orders'), 'the reconnect did not subscribe it again');
    }

    /**
     * An auto-unsubscribe whose UNSUB finds the socket dead does not throw either, and stays armed: the
     * reconnect arms it on the new connection.
     */
    public function testAutoUnsubscribeWhoseWriteFindsTheSocketDeadIsArmedOnTheNewConnection(): void
    {
        $transport = new ReconnectingTransport();
        $connection = $this->connect($transport);
        $sid = $connection->subscribe('orders', static function (): void {})->await();
        $transport->failNextWriteContaining('UNSUB');

        $connection->unsubscribe($sid, 3)->await();
        $connection->publish('audit', 'a1')->await();

        self::assertSame(1, $transport->epoch(), 'the publish recovered the connection');
        self::assertSame($sid, $transport->sidFor('orders'));
        self::assertContains(sprintf('UNSUB %d 3', $sid), $transport->controlLines(1));
    }
}
