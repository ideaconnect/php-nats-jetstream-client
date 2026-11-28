<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Tests\Support\KeepsEventLoopAlive;
use IDCT\NATS\Tests\Support\LifecycleRecorder;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\ReconnectScenarios;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\delay;

/**
 * A lame-duck notice (INFO `ldm`) keeps the connection (#265). The server sends it, keeps the connection for its grace
 * period (10 s by default) and only then closes its clients, so the replies already on their way still come on it.
 * The client used to fail over at the notice (#47): it closed the socket a reply was coming on, and the operation whose
 * read brought the notice lost its reply whenever the reply came in a later read than the INFO, a request or a
 * JetStream publish timing out at its deadline, a fetch ending with a 408 and its message redelivered after the ack
 * wait. The notice now emits LameDuck and moves nothing; the reconnect runs once the server closes the connection, and
 * dials the server the notice named.
 *
 * Over the scripted server in tests/Support/ReconnectingTransport.php, whose every dial opens a new session (epoch 0,
 * then 1 after a reconnect). The notice names a second server in connect_urls, so that the pool holds two and the old
 * failover ran. The server answers the operation with the notice alone, in a read of its own, and the reply 50 ms
 * later on the same session, pushed only while that session is still the live one, as a real server answers only on
 * the connection the request came on. A reply in the same chunk as the INFO got through before as well. Each test
 * fails on the code before the change, how in its docblock.
 */
final class LameDuckNoticeKeepsTheConnectionTest extends TestCase
{
    use KeepsEventLoopAlive;
    use ReconnectScenarios;

    /** The first server's lame-duck notice, naming the second server: the pool grows to two. */
    private const LAME_DUCK = 'INFO {"server_id":"S1","version":"2.12.0","max_payload":1048576,"ldm":true,"connect_urls":["10.0.0.2:4222"]}' . "\r\n";

    /** The second server, as the client dials the one the notice names. */
    private const SECOND_SERVER = 'tcp://10.0.0.2:4222';

    /** How long after the notice the reply comes: a later read than the INFO's. */
    private const REPLY_DELAY = 0.05;

    private const PULL_SUBJECT = '$JS.API.CONSUMER.MSG.NEXT.S.C';

    private const ACK_SUBJECT = '$JS.ACK.S.C.1.1.1.0.0';

    protected function setUp(): void
    {
        $this->keepEventLoopAlive();
    }

    protected function tearDown(): void
    {
        $this->letEventLoopEnd();
        $this->closeOpenedConnections();
    }

    /**
     * A request() whose read brings the notice alone gets its reply, which follows 50 ms later on the same session: no
     * second dial, the connection Open on the first session, and the events Connected, DiscoveredServers, LameDuck.
     * When the server then closes the connection, as it does at the end of its grace period, the ordinary reconnect
     * runs once, to the server the notice named, a request there is answered, and the events go on with Disconnected
     * and Reconnected. The code before the change failed over at the notice, so the reply never came: the request
     * threw a TimeoutException at its timeout.
     */
    public function testARequestWhoseReadBringsTheNoticeGetsTheReplyThatFollowsIt(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $transport->responder = self::answeringAfterTheNotice($transport, 'svc.echo', static fn(string $replyTo): array => $transport->replyFrame($replyTo, 'reply-1'));
        $connection = $this->connect($transport, connectionListener: $recorder->connectionListener());
        // Sets the reply inbox up: the request below writes no SUB of its own, and its read is the one on the socket.
        self::assertSame('ok', $connection->request('svc.warm', 'x', 1_000)->await()->payload);

        $reply = $connection->request('svc.echo', 'y', 1_000)->await();

        self::assertSame('reply-1', $reply->payload);
        self::assertStillOnTheFirstServer($connection, $transport);
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::DiscoveredServers, ConnectionEvent::LameDuck], $recorder->events);

        // The server closes the connection once its grace period is over.
        $this->loseConnection($connection, $transport);
        $this->waitUntilOpen($connection);

        self::assertSame(1, $transport->epoch(), 'one reconnect');
        self::assertCount(2, $transport->connectCalls, 'one more dial');
        self::assertStringStartsWith(self::SECOND_SERVER, $transport->connectCalls[1], 'to the server the notice named');
        self::assertSame('ok', $connection->request('svc.warm', 'x', 1_000)->await()->payload, 'a request on the new connection');
        self::assertSame([
            ConnectionEvent::Connected,
            ConnectionEvent::DiscoveredServers,
            ConnectionEvent::LameDuck,
            ConnectionEvent::Disconnected,
            ConnectionEvent::Reconnected,
        ], $recorder->events);
    }

    /**
     * A JetStream publish() whose PubAck follows the notice the same way resolves with the PubAck's sequence, with no
     * second dial. The code before the change failed over at the notice: the publish threw a TimeoutException, though
     * the server had stored the message.
     */
    public function testAJetStreamPublishWhoseReadBringsTheNoticeGetsThePubAckThatFollowsIt(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = self::answeringAfterTheNotice(
            $transport,
            'orders.created',
            static fn(string $replyTo): array => $transport->replyFrame($replyTo, (string) json_encode(['stream' => 'ORDERS', 'seq' => 7])),
        );
        $client = $this->clientOver($transport);
        $client->request('svc.warm', 'x', 1_000)->await();

        $ack = $client->jetStream()->publish('orders.created', 'payload')->await();

        self::assertSame(7, $ack->seq);
        self::assertSame('ORDERS', $ack->stream);
        self::assertStillOnTheFirstServer($client, $transport);
    }

    /**
     * A fetchBatch() of one whose message follows the notice the same way returns the message, with no second dial.
     * The code before the change failed over at the notice: the fetch threw a JetStreamException 408, and its message
     * would have come back only after the consumer's ack wait.
     */
    public function testAFetchWhoseReadBringsTheNoticeGetsTheMessageThatFollowsIt(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = self::answeringAfterTheNotice(
            $transport,
            self::PULL_SUBJECT,
            static function (string $replyTo) use ($transport): array {
                $sid = $transport->sidFor($replyTo);

                return $sid === null ? [] : [ReconnectingTransport::msgFrame('evt.s', $sid, 'm-1', self::ACK_SUBJECT)];
            },
        );
        $client = $this->clientOver($transport);

        $messages = $client->jetStream()->fetchBatch('S', 'C', 1, 1_000)->await();

        self::assertSame(['m-1'], array_map(static fn(NatsMessage $message): string => $message->payload, $messages));
        self::assertStillOnTheFirstServer($client, $transport);
    }

    /**
     * Your own processIncoming() that reads the notice returns with no dial, the connection Open on the first session,
     * and the events Connected, DiscoveredServers, LameDuck. The code before the change ran the failover inline in that
     * read: it returned on the second server, after one more dial.
     */
    public function testYourOwnReadOfTheNoticeLeavesTheConnectionWhereItIs(): void
    {
        $transport = new ReconnectingTransport();
        $recorder = new LifecycleRecorder();
        $connection = $this->connect($transport, connectionListener: $recorder->connectionListener());
        $transport->pushFrame(self::LAME_DUCK);

        $connection->processIncoming(new TimeoutCancellation(2))->await();

        self::assertStillOnTheFirstServer($connection, $transport);
        self::assertSame([ConnectionEvent::Connected, ConnectionEvent::DiscoveredServers, ConnectionEvent::LameDuck], $recorder->events);
        self::assertTrue($connection->serverInfo()?->lameDuckMode, 'the notice is still on record');
    }

    /**
     * After the request's reply, and a pause of 0.3 s, the connection is still on the first session, with one dial:
     * the notice moves the connection neither at once nor later. A failover held back for a while after the notice
     * lets the request's reply through and can pass the tests above; this one catches a hold-back shorter than its
     * pause. One held back for longer would pass it as well: the pause is a bound, not a proof.
     */
    public function testTheConnectionStaysPutAfterTheReply(): void
    {
        $transport = new ReconnectingTransport();
        $transport->responder = self::answeringAfterTheNotice($transport, 'svc.echo', static fn(string $replyTo): array => $transport->replyFrame($replyTo, 'reply-1'));
        $connection = $this->connect($transport);
        $connection->request('svc.warm', 'x', 1_000)->await();
        self::assertSame('reply-1', $connection->request('svc.echo', 'y', 1_000)->await()->payload);

        delay(0.3);

        self::assertStillOnTheFirstServer($connection, $transport);
    }

    /** One dial, the first session, and the connection Open. */
    private static function assertStillOnTheFirstServer(NatsConnection|NatsClient $connection, ReconnectingTransport $transport): void
    {
        self::assertCount(1, $transport->connectCalls, 'no second dial');
        self::assertSame(0, $transport->epoch(), 'the first session');
        self::assertSame(ConnectionState::Open, $connection->state());
    }

    /**
     * Answers svc.warm with "ok" on the reply inbox, so that a test sets the inbox up first, and a publish to $subject
     * with the lame-duck notice alone, and then, {@see REPLY_DELAY} later, with the frames $answer builds for its reply
     * subject on the session it went out on: the server's reply still on its way when the client reads the notice. The
     * frames are pushed only while that session is still the live one.
     *
     * @param \Closure(string): list<string> $answer
     * @return \Closure(string, ?string, string): list<string>
     */
    private static function answeringAfterTheNotice(ReconnectingTransport $transport, string $subject, \Closure $answer): \Closure
    {
        return static function (string $written, ?string $replyTo) use ($transport, $subject, $answer): array {
            if ($replyTo === null) {
                return [];
            }

            if ($written !== $subject) {
                return $written === 'svc.warm' ? $transport->replyFrame($replyTo, 'ok') : [];
            }

            $epoch = $transport->epoch();
            $frames = $answer($replyTo);
            EventLoop::delay(self::REPLY_DELAY, static function () use ($transport, $epoch, $frames): void {
                if ($transport->epoch() !== $epoch) {
                    return;
                }

                foreach ($frames as $frame) {
                    $transport->pushFrame($frame);
                }
            });

            return [self::LAME_DUCK];
        };
    }

    /** A client over $transport, reconnecting at once to any server. */
    private function clientOver(ReconnectingTransport $transport): NatsClient
    {
        $client = $this->own(new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: 1_000,
            reconnectEnabled: true,
            maxReconnectAttempts: 1_000,
            reconnectDelayMs: 5,
            reconnectMaxDelayMs: 20,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            waitForReconnect: true,
        ), $transport));
        $this->opened[] = $client;
        $client->connect()->await();

        return $client;
    }
}
