<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Tests\Support\SubscriptionLimitServer;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\delay;

/**
 * The request mux inbox's set-up. The client records the mux before its SUB is written, so whichever read meets the
 * server's answer knows it is the mux's: the #167 permissions latch fires even when the reconnect that a failed SUB
 * write starts reads the rejection during its replay. A terminal close during the set-up leaves nothing of the
 * closed connection's mux behind: no recorded mux, and no set-up for a request on the next connection to join.
 */
final class MuxInboxRejectionTest extends TestCase
{
    private const CLOSED = '/^Connection (is not open|was closed while the reply inbox was being set up)$/';

    /** @var list<NatsConnection> */
    private array $opened = [];

    protected function tearDown(): void
    {
        foreach ($this->opened as $connection) {
            try {
                $connection->disconnect()->await(new TimeoutCancellation(1));
            } catch (\Throwable) {
                // Already closed.
            }
        }

        $this->opened = [];
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
     * the closed connection and is not sent on the new one.
     */
    public function testARequestAfterACloseAndConnectDoesNotJoinASetUpLeftFromTheClosedConnection(): void
    {
        $server = $this->echoServer();
        $connection = $this->connect($server);
        $server->stallNextWriteContaining('SUB _INBOX.', 0.2, completesAfterClose: true);
        $first = $connection->request('svc.echo', 'one', 1_000);
        delay(0.01);
        $connection->disconnect()->await();
        $connection->connect()->await();

        $start = hrtime(true);
        self::assertSame('echo:two', $connection->request('svc.echo', 'two', 1_000)->await()->payload);
        self::assertLessThan(0.15, self::secondsSince($start), 'it did not wait for the set-up of the closed connection');

        try {
            $first->await();
            self::fail('expected the first request to fail with the closed connection');
        } catch (ConnectionException $e) {
            self::assertSame('Connection was closed while the reply inbox was being set up', $e->getMessage());
        }
        self::assertCount(1, $server->controlLines('PUB svc.echo', 1), 'only the second request was sent');
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
                $connection->request('svc.echo', $payload, 1_000)->await();
                self::fail('expected the request to fail');
            } catch (ConnectionException $e) {
                self::assertStringContainsString('was rejected by the server (permissions violation)', $e->getMessage());
            }
            self::assertLessThan(0.5, self::secondsSince($start));
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
                $connection->request('svc.echo', 'one', 1_000)->await();
                self::fail('expected the request to fail');
            } catch (ConnectionException $e) {
                self::assertStringContainsString('was rejected by the server (permissions violation)', $e->getMessage());
            }
            self::assertLessThan(0.5, self::secondsSince($start));
        } finally {
            $stop->cancel();
            $loop->await();
        }
        self::assertSame([], $server->controlLines('PUB svc.echo'));
    }

    /** A server that echoes every request on its reply subject - when the server holds a subscription for it. */
    private function echoServer(): SubscriptionLimitServer
    {
        $server = new SubscriptionLimitServer();
        $server->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $replyTo === null
            ? []
            : $server->replyFrame($replyTo, 'echo:' . $payload);

        return $server;
    }

    private function connect(SubscriptionLimitServer $server, bool $reconnect = false, bool $waitForReconnect = true): NatsConnection
    {
        $connection = new NatsConnection(new NatsOptions(
            connectTimeoutMs: 500,
            reconnectEnabled: $reconnect,
            maxReconnectAttempts: 3,
            reconnectDelayMs: 1,
            reconnectMaxDelayMs: 1,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            waitForReconnect: $waitForReconnect,
        ), $server);
        $this->opened[] = $connection;
        $connection->connect()->await();

        return $connection;
    }

    /**
     * An application loop that keeps a read on the socket, as a consumer's does, and reads on past whatever a read
     * throws. Stop it with the cancellation, then await the future.
     *
     * @return array{DeferredCancellation, Future<void>}
     */
    private function startReadLoop(NatsConnection $connection): array
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

    private static function secondsSince(int $start): float
    {
        return (hrtime(true) - $start) / 1e9;
    }
}
