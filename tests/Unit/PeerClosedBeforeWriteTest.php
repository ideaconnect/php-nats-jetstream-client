<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Future;
use Amp\Socket\BindContext;
use Amp\Socket\Certificate;
use Amp\Socket\ServerTlsContext;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Tests\Support\LoopbackNatsServer;
use IDCT\NATS\Tests\Support\OwnsTestResources;
use IDCT\NATS\Tests\Support\PeerClosingTransport;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Tests\Support\WedgedWriteSocket;
use IDCT\NATS\Transport\AmpSocketTransport;
use IDCT\NATS\Transport\TransportClosedException;
use IDCT\NATS\Transport\WebSocketTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\delay;
use function Amp\Socket\connect;
use function Amp\Socket\listen;

/**
 * A write into a socket the server has already closed, the close not read yet (ideaconnect/symfony-nats-messenger#98).
 *
 * The connection reads the socket only while an operation waits for a reply. A server that stops while nothing reads
 * leaves its close unread, and a frame that waits for no reply, a JetStream ACK or NAK say, used to be taken by the
 * socket and reach no server: the message came back after its ack_wait and was handled again. A request's frame was
 * lost the same way, and the request waited for its reply on the recovered connection until its timeout. The
 * connection now asks the transport before such a write ({@see \IDCT\NATS\Transport\PeerCloseAwareTransportInterface})
 * and handles a closed socket as a write that failed there: the frame goes out on the recovered connection.
 *
 * The scripted tests use `tests/Support/PeerClosingTransport.php`, whose server closes a session without the client
 * reading it; the others use real loopback sockets. A timeout given to an await only ends a broken test
 * ({@see self::WATCHDOG}).
 */
final class PeerClosedBeforeWriteTest extends TestCase
{
    use OwnsTestResources;

    /** Seconds after which a wait that should have ended fails the test. */
    private const WATCHDOG = 3;

    /** @var list<\Closure(): void> Run after each test, before the owned connections close. */
    private array $teardowns = [];

    protected function tearDown(): void
    {
        foreach ($this->teardowns as $teardown) {
            try {
                $teardown();
            } catch (\Throwable) {
                // Already closed.
            }
        }

        $this->teardowns = [];
        $this->releaseOwnedResources();
    }

    /** @return iterable<string, array{string, bool}> */
    public static function publishes(): iterable
    {
        foreach (['waiting for a reconnect' => true, 'not waiting for a reconnect' => false] as $policy => $wait) {
            yield 'publish(), ' . $policy => ['publish', $wait];
            yield 'publishWithHeaders(), ' . $policy => ['publishWithHeaders', $wait];
            yield 'a JetStream ack(), ' . $policy => ['ack', $wait];
            yield 'a JetStream nak(), ' . $policy => ['nak', $wait];
            yield 'a header block segment, ' . $policy => ['header block', $wait];
        }
    }

    /**
     * A publish whose socket the server has closed, unread, resolves while the recovery's dial is held, with nothing
     * written into the closed socket; once the dial is let through, its frame goes out on the new connection, after the
     * CONNECT and the subscription replay, once. Without the check the publish resolved as well, but its frame went
     * into the closed socket, nothing recovered the connection, and nothing sent the frame again.
     */
    #[DataProvider('publishes')]
    public function testAPublishIntoASocketTheServerClosedGoesOutOnTheNewConnection(string $kind, bool $wait): void
    {
        $transport = new PeerClosingTransport(new ReconnectingTransport());
        $client = $this->client($transport, wait: $wait);
        $client->subscribe('updates', static function (): void {})->await();
        [$publish, $expected] = self::publish($kind);
        $transport->inner->holdNextDial();
        $this->resources()->onRelease($transport->inner->releaseDial(...), 'the held reconnect dial');
        $transport->closeUnread();

        $publish($client)->await(new TimeoutCancellation(self::WATCHDOG));
        self::assertSame([], $transport->lost, 'nothing went into the closed socket');
        self::assertSame(ConnectionState::Connecting, $client->state());
        $this->waitUntil(static fn(): bool => count($transport->inner->connectCalls) === 2);

        $transport->inner->releaseDial();
        $this->waitUntilOpen($client);
        $client->flush()->await(new TimeoutCancellation(self::WATCHDOG));

        $lines = $transport->inner->controlLines(1);
        self::assertStringStartsWith('CONNECT ', $lines[0]);
        self::assertSame($expected, self::publishLines($transport, 1));
        self::assertSame([], self::publishLines($transport, 0));
        self::assertLessThan(array_search($expected[0], $lines, true), array_search('SUB updates 1', $lines, true), 'the replay comes first');
        self::assertSame(count($expected), $client->statistics()->outMsgs);
    }

    /**
     * A request whose socket the server has closed, unread, is written on the recovered connection and gets its reply
     * at once. It used to write into the closed socket, recover the connection from its read, and wait there for a
     * reply to a frame no server received, until its timeout.
     */
    public function testARequestIntoASocketTheServerClosedIsSentOnTheNewConnection(): void
    {
        $transport = new PeerClosingTransport(new ReconnectingTransport());
        $inner = $transport->inner;
        $inner->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $replyTo !== null
            ? $inner->replyFrame($replyTo, 'reply:' . $payload)
            : [];
        $client = $this->client($transport);
        // The request inbox is set up on the first connection, so the request after the close only writes its frame.
        self::assertSame('reply:a', $client->request('svc', 'a', 2_000)->await(new TimeoutCancellation(self::WATCHDOG))->payload);
        $transport->closeUnread();

        $start = hrtime(true);
        $reply = $client->request('svc', 'b', 2_000)->await(new TimeoutCancellation(self::WATCHDOG));
        $elapsed = (hrtime(true) - $start) / 1e9;

        self::assertSame('reply:b', $reply->payload);
        self::assertLessThan(1.0, $elapsed, sprintf('the reply came after %.3f s, the request timeout being 2 s', $elapsed));
        self::assertSame([], $transport->lost, 'nothing went into the closed socket');
        self::assertCount(1, array_filter(self::publishLines($transport, 1), static fn(string $line): bool => str_starts_with($line, 'PUB svc ')));
        self::assertCount(2, $inner->connectCalls);
    }

    /**
     * With reconnect off, a publish into the closed socket fails, the connection closed, instead of resolving with its
     * frame lost: nothing will send it, and the caller hears so, with "Reconnect is disabled" as for a write that fails
     * (#172). A request fails the same way, at once, instead of waiting for its timeout.
     */
    public function testWithReconnectOffAWriteIntoTheClosedSocketFailsInsteadOfVanishing(): void
    {
        $transport = new PeerClosingTransport(new ReconnectingTransport());
        $client = $this->client($transport, reconnect: false);
        $transport->closeUnread();

        try {
            $client->publish('orders', 'x')->await(new TimeoutCancellation(self::WATCHDOG));
            self::fail('the publish into the closed socket resolved');
        } catch (ConnectionException $failed) {
            // As for a write that fails with reconnect off (#172), the found close its previous.
            self::assertSame('Reconnect is disabled', $failed->getMessage());
            $closed = $failed->getPrevious();
            self::assertInstanceOf(TransportClosedException::class, $closed);
            self::assertSame('Socket closed by peer (EOF), found before a write', $closed->getMessage());
        }

        self::assertSame([], $transport->lost);
        self::assertSame(ConnectionState::Closed, $client->state());
        self::assertCount(1, $transport->inner->connectCalls, 'no reconnect');

        $requestTransport = new PeerClosingTransport(new ReconnectingTransport());
        $inner = $requestTransport->inner;
        $inner->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $replyTo !== null
            ? $inner->replyFrame($replyTo, 'ok')
            : [];
        $requester = $this->client($requestTransport, reconnect: false);
        // The request inbox is set up on the open connection, so the request after the close only writes its frame.
        $requester->request('svc', 'warm-up', 2_000)->await(new TimeoutCancellation(self::WATCHDOG));
        $requestTransport->closeUnread();
        $start = hrtime(true);
        try {
            $requester->request('svc', 'x', 2_000)->await(new TimeoutCancellation(self::WATCHDOG));
            self::fail('the request into the closed socket returned');
        } catch (TimeoutException $timeout) {
            self::fail('the request waited for its timeout: ' . $timeout->getMessage());
        } catch (\Throwable) {
            // Failed at once: nothing will send the frame.
        }

        self::assertLessThan(1.0, (hrtime(true) - $start) / 1e9);
        self::assertSame([], $requestTransport->lost);
    }

    /**
     * On an open socket the transport is asked once before each publish and each request write, and the frames go out
     * as before; a transport without the capability is not asked at all, which the rest of the suite covers.
     */
    public function testAnOpenSocketIsAskedOncePerWriteAndWrittenAsBefore(): void
    {
        $transport = new PeerClosingTransport(new ReconnectingTransport());
        $inner = $transport->inner;
        $inner->responder = static fn(string $subject, ?string $replyTo, string $payload): array => $replyTo !== null
            ? $inner->replyFrame($replyTo, 'ok')
            : [];
        $client = $this->client($transport);
        $client->subscribe('updates', static function (): void {})->await();
        $before = $transport->peerChecks;

        $client->publish('orders.a', 'a')->await();
        $client->jetStream()->ack(new NatsMessage('orders', 1, '$JS.ACK.ORDERS.worker.1.1.1.0.0', 'x'))->await();
        $client->request('svc', 'q', 2_000)->await(new TimeoutCancellation(self::WATCHDOG));

        self::assertSame(3, $transport->peerChecks - $before);
        self::assertSame(['PUB orders.a 1', 'PUB $JS.ACK.ORDERS.worker.1.1.1.0.0 4'], array_slice(self::publishLines($transport, 0), 0, 2));
        self::assertSame([], $transport->lost);
        self::assertCount(1, $inner->connectCalls);
    }

    /**
     * The socket transport's check on a real loopback socket: false with no socket, on an idle one and while the peer's
     * bytes wait unread, which it leaves for the next read; true once the peer has closed with nothing left unread;
     * false again after close().
     */
    public function testTheSocketTransportSeesAPeerCloseWithNothingLeftUnread(): void
    {
        $transport = new AmpSocketTransport(new NatsOptions());
        self::assertFalse($transport->peerClosed(), 'no socket yet');
        [$transport, $peer] = $this->connectedSocketTransport();
        self::assertFalse($transport->peerClosed(), 'an idle socket');

        $peer->write("PING\r\n");
        $this->waitUntilReadable($transport);
        self::assertFalse($transport->peerClosed(), 'bytes wait unread');
        self::assertSame("PING\r\n", $transport->readLine(new TimeoutCancellation(self::WATCHDOG))->await(), 'the check read nothing');

        $peer->close();
        $this->waitUntil(static fn(): bool => $transport->peerClosed());
        self::assertTrue($transport->peerClosed(), 'asking again changes nothing');

        $transport->close()->await();
        self::assertFalse($transport->peerClosed(), 'closed on our side: no socket');
    }

    /**
     * A close behind bytes still unread is not seen by the check: the next read gets the bytes, and the one after it
     * the close, as before. Over TLS the server's close_notify is such a byte.
     */
    #[DataProvider('socketKinds')]
    public function testACloseBehindUnreadBytesIsLeftForTheNextRead(bool $tls): void
    {
        [$transport, $peer] = $this->connectedSocketTransport($tls);
        if (!$tls) {
            $peer->write("INFO {}\r\n");
        }

        $peer->close();
        delay(0.05);

        self::assertFalse($transport->peerClosed());
        $this->expectException(TransportClosedException::class);
        for ($reads = 0; $reads < 10; $reads++) {
            $transport->readLine(new TimeoutCancellation(self::WATCHDOG))->await();
        }

        self::fail('ten reads and no close');
    }

    /** @return iterable<string, array{bool}> */
    public static function socketKinds(): iterable
    {
        yield 'plain TCP, a frame ahead of the close' => [false];
        yield 'TLS, the close_notify ahead of the close' => [true];
    }

    /**
     * The WebSocket transport's check on a real loopback socket, the HTTP upgrade skipped: false on an idle socket and
     * with a socket that has no stream resource, true once the peer has closed it.
     */
    public function testTheWebSocketTransportSeesAPeerCloseOfItsSocket(): void
    {
        $server = listen('tcp://127.0.0.1:0');
        $accept = async(static fn(): ?Socket => $server->accept());
        $clientSocket = connect('tcp://' . $server->getAddress()->toString());
        $peer = $accept->await(new TimeoutCancellation(self::WATCHDOG));
        self::assertNotNull($peer);
        $transport = new WebSocketTransport(new NatsOptions());
        $socket = new \ReflectionProperty(WebSocketTransport::class, 'socket');
        $this->teardowns[] = static function () use ($transport, $server): void {
            $transport->close()->await();
            $server->close();
        };

        self::assertFalse($transport->peerClosed(), 'no socket yet');
        $socket->setValue($transport, $clientSocket);
        self::assertFalse($transport->peerClosed(), 'an idle socket');
        $peer->close();
        $this->waitUntil(static fn(): bool => $transport->peerClosed());

        $socket->setValue($transport, new WedgedWriteSocket());
        self::assertFalse($transport->peerClosed(), 'a socket without a stream resource cannot tell');
    }

    /**
     * End to end on a real socket: the loopback server closes the client's connection while nothing reads it, and an
     * ACK published then reaches the server on the client's next connection. Without the check it was taken by the
     * closed socket and the server never saw it.
     */
    public function testOnARealSocketAnAckAfterTheServerClosedReachesTheServerOnTheNextConnection(): void
    {
        $server = LoopbackNatsServer::start();
        $this->teardowns[] = $server->close(...);
        $options = new NatsOptions(
            servers: [$server->url()],
            connectTimeoutMs: 2_000,
            reconnectDelayMs: 1,
            reconnectMaxDelayMs: 2,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
        );
        $client = $this->own(new NatsClient($options, new AmpSocketTransport($options)));
        $client->connect()->await();
        $client->flush()->await(new TimeoutCancellation(self::WATCHDOG));

        $server->closeClient();
        delay(0.05);
        self::assertSame(ConnectionState::Open, $client->state(), 'the client has not read the close');
        $client->jetStream()->ack(new NatsMessage('orders', 1, '$JS.ACK.ORDERS.worker.1.1.1.0.0', 'x'))->await(new TimeoutCancellation(self::WATCHDOG));
        $this->waitUntil(static fn(): bool => $server->published() !== []);

        self::assertSame([[1, 'PUB $JS.ACK.ORDERS.worker.1.1.1.0.0 4']], $server->published());
        self::assertSame(2, $server->connections());
    }

    /**
     * @return array{\Closure(NatsClient): Future<void>, list<string>}
     */
    private static function publish(string $kind): array
    {
        $message = new NatsMessage('orders', 1, '$JS.ACK.ORDERS.worker.1.1.1.0.0', 'x');

        return match ($kind) {
            'publish' => [static fn(NatsClient $client): Future => $client->publish('orders', 'x'), ['PUB orders 1']],
            'publishWithHeaders' => [static fn(NatsClient $client): Future => $client->publishWithHeaders('orders', 'x', ['K' => 'v']), ['HPUB orders 17 18']],
            'ack' => [static fn(NatsClient $client): Future => $client->jetStream()->ack($message), ['PUB $JS.ACK.ORDERS.worker.1.1.1.0.0 4']],
            'nak' => [static fn(NatsClient $client): Future => $client->jetStream()->nak($message), ['PUB $JS.ACK.ORDERS.worker.1.1.1.0.0 4']],
            default => [
                static fn(NatsClient $client): Future => $client->publishHeaderBlock([
                    ['subject' => 'orders.1', 'payload' => 'a', 'headers' => ['K' => 'v']],
                    ['subject' => 'orders.2', 'payload' => 'b', 'headers' => ['K' => 'v']],
                ]),
                ['HPUB orders.1 17 18', 'HPUB orders.2 17 18'],
            ],
        };
    }

    /** @return list<string> */
    private static function publishLines(PeerClosingTransport $transport, int $epoch): array
    {
        return array_values(array_filter(
            $transport->inner->controlLines($epoch),
            static fn(string $line): bool => str_starts_with($line, 'PUB ') || str_starts_with($line, 'HPUB '),
        ));
    }

    private function client(PeerClosingTransport $transport, bool $wait = true, bool $reconnect = true): NatsClient
    {
        $client = $this->own(new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            requestTimeoutMs: 2_000,
            reconnectEnabled: $reconnect,
            maxReconnectAttempts: 1_000,
            reconnectDelayMs: 1,
            reconnectMaxDelayMs: 2,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            waitForReconnect: $wait,
        ), $transport));
        $client->connect()->await();

        return $client;
    }

    /**
     * An AmpSocketTransport connected to a loopback socket, with no NATS handshake, and the server's end of it.
     *
     * @return array{AmpSocketTransport, Socket}
     */
    private function connectedSocketTransport(bool $tls = false): array
    {
        $bind = new BindContext();
        if ($tls) {
            $certFile = self::selfSignedCertificateFile();
            $this->teardowns[] = static function () use ($certFile): void {
                @unlink($certFile);
            };
            $bind = $bind->withTlsContext((new ServerTlsContext())->withDefaultCertificate(new Certificate($certFile)));
        }

        $server = listen('tcp://127.0.0.1:0', $bind);
        $accepted = async(static function () use ($server, $tls): ?Socket {
            $peer = $server->accept();
            if ($peer !== null && $tls) {
                $peer->setupTls();
            }

            return $peer;
        });
        $transport = new AmpSocketTransport(new NatsOptions(tlsRequired: $tls, tlsVerifyPeer: false));
        $transport->connect(($tls ? 'tls://' : 'nats://') . $server->getAddress()->toString(), 2_000)->await();
        if ($tls) {
            $transport->upgradeTls()->await();
        }

        $peer = $accepted->await(new TimeoutCancellation(self::WATCHDOG));
        self::assertNotNull($peer);
        $this->teardowns[] = static function () use ($transport, $peer, $server): void {
            $transport->close()->await();
            $peer->close();
            $server->close();
        };

        return [$transport, $peer];
    }

    /** Waits until the transport's socket has bytes to read, looking with a peek that reads nothing. */
    private function waitUntilReadable(AmpSocketTransport $transport): void
    {
        $socket = (new \ReflectionProperty(AmpSocketTransport::class, 'socket'))->getValue($transport);
        self::assertInstanceOf(\Amp\Socket\ResourceSocket::class, $socket);
        $resource = $socket->getResource();
        self::assertIsResource($resource);
        $this->waitUntil(static fn(): bool => \is_string($peek = @stream_socket_recvfrom($resource, 1, STREAM_PEEK)) && $peek !== '');
    }

    private function waitUntilOpen(NatsClient $client): void
    {
        $this->waitUntil(static fn(): bool => $client->state() === ConnectionState::Open);
    }

    private function waitUntil(\Closure $condition): void
    {
        $deadline = hrtime(true) + self::WATCHDOG * 1_000_000_000;
        while (!$condition()) {
            if (hrtime(true) > $deadline) {
                self::fail('condition not reached within ' . self::WATCHDOG . ' s');
            }

            delay(0.001);
        }
    }

    private static function selfSignedCertificateFile(): string
    {
        if (!\extension_loaded('openssl')) {
            self::markTestSkipped('ext-openssl is required to generate the self-signed test certificate');
        }

        $key = openssl_pkey_new(['private_key_bits' => 2048, 'private_key_type' => OPENSSL_KEYTYPE_RSA]);
        if ($key === false) {
            throw new \RuntimeException('openssl_pkey_new() failed');
        }

        $csr = openssl_csr_new(['commonName' => '127.0.0.1'], $key, ['digest_alg' => 'sha256']);
        if (!$csr instanceof \OpenSSLCertificateSigningRequest) {
            throw new \RuntimeException('openssl_csr_new() failed');
        }

        $x509 = openssl_csr_sign($csr, null, $key, 2, ['digest_alg' => 'sha256']);
        if ($x509 === false) {
            throw new \RuntimeException('openssl_csr_sign() failed');
        }

        $certPem = '';
        $keyPem = '';
        if (!openssl_x509_export($x509, $certPem) || !openssl_pkey_export($key, $keyPem)) {
            throw new \RuntimeException('exporting the self-signed certificate failed');
        }

        $file = tempnam(sys_get_temp_dir(), 'nats-peer-close-cert-');
        if ($file === false) {
            throw new \RuntimeException('tempnam() failed');
        }

        file_put_contents($file, $certPem . $keyPem);

        return $file;
    }
}
