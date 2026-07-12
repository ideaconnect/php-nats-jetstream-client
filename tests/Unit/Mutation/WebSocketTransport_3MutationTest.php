<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\ByteStream\ReadableResourceStream;
use Amp\Socket\ResourceSocket;
use Amp\Socket\Socket;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Transport\WebSocketTransport;

use function Amp\async;
use function Amp\Socket\connect;
use function Amp\Socket\listen;

/**
 * Mutation-killing pins for {@see WebSocketTransport} chunk-size handling (#119), chunk 3.
 *
 * The read-path chunk-size raise (applyReadChunkSize) is observable only through the underlying
 * ResourceSocket's reader chunk size, so these tests reach into the Amp ResourceSocket's private
 * ReadableResourceStream to read its effective `defaultChunkSize`. Amp's default is 8192 bytes; a
 * successful setChunkSize(N) overwrites it with N, while setChunkSize(0) throws a ValueError - which
 * is exactly the lever that pins the two branch mutants on line 182.
 */
final class WebSocketTransport_3MutationTest extends \PHPUnit\Framework\TestCase
{
    /** Amp ResourceSocket's built-in default reader chunk size when setChunkSize was never applied. */
    private const AMP_DEFAULT_CHUNK_SIZE = 8192;

    /**
     * connect() must apply the configured read chunk size to the freshly connected socket (line 160).
     *
     * A loopback server accepts then immediately drops the connection, so the WebSocket handshake fails
     * fast - but connect() already ran applyReadChunkSize() (line 160, before the handshake at line 167)
     * and stored the socket, so the effect is observable on the transport's socket afterwards.
     */
    public function testConnectAppliesReadChunkSizeToSocket(): void
    {
        $server = listen('tcp://127.0.0.1:0');
        $address = (string) $server->getAddress();

        // Accept and close without a 101 response so performHandshake() fails as soon as it reads EOF.
        async(static function () use ($server): void {
            $accepted = $server->accept();
            $accepted?->close();
        });

        $transport = new WebSocketTransport(new NatsOptions(readChunkSizeBytes: 4096));

        try {
            $transport->connect('ws://' . $address . '/', 1000)->await();
            self::fail('expected the WebSocket handshake to fail against a server that drops the connection');
        } catch (\Throwable) {
            // Expected: the handshake could not complete. applyReadChunkSize() already ran before it.
        } finally {
            $server->close();
        }

        $socket = (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->getValue($transport);
        self::assertInstanceOf(Socket::class, $socket);

        // kills MethodCallRemoval @ 160: with the call removed, the socket keeps Amp's 8192 default.
        self::assertSame(4096, $this->readerChunkSize($socket));
    }

    /**
     * applyReadChunkSize() must call setChunkSize on a ResourceSocket when the configured size is > 0
     * (line 182): the socket's reader chunk size becomes exactly the configured value.
     */
    public function testApplyReadChunkSizeSetsConfiguredSizeOnResourceSocket(): void
    {
        [$client, $serverSocket, $server] = $this->connectedResourceSocket();

        try {
            $transport = new WebSocketTransport(new NatsOptions(readChunkSizeBytes: 4));
            (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $client);
            (new \ReflectionMethod(WebSocketTransport::class, 'applyReadChunkSize'))->invoke($transport);

            // kills InstanceOf_ @ 182: the socket IS a ResourceSocket, so the real (non-negated)
            // condition applies the size; the `!instanceof` mutant would skip it (leaving 8192).
            self::assertSame(4, $this->readerChunkSize($client));
        } finally {
            $client->close();
            $serverSocket->close();
            $server->close();
        }
    }

    /**
     * applyReadChunkSize() must NOT touch the socket when the configured chunk size is 0 (line 182):
     * `> 0` is false at the boundary, so setChunkSize is never called and nothing throws. A degenerate
     * options object supplies the boundary value that the NatsOptions constructor otherwise forbids.
     */
    public function testApplyReadChunkSizeLeavesSocketUntouchedForZeroChunkSize(): void
    {
        [$client, $serverSocket, $server] = $this->connectedResourceSocket();

        try {
            // NatsOptions rejects readChunkSizeBytes < 1, so build a bare object carrying exactly 0 to
            // exercise the `> 0` boundary the mutant flips to `>= 0`.
            $options = (new \ReflectionClass(NatsOptions::class))->newInstanceWithoutConstructor();
            (new \ReflectionProperty(NatsOptions::class, 'readChunkSizeBytes'))->setValue($options, 0);

            $transport = new WebSocketTransport($options);
            (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $client);

            // Real code (`> 0`): 0 > 0 is false, so setChunkSize is never called - this invoke is a
            // no-op. The `>= 0` mutant would call setChunkSize(0), which throws ValueError, so this very
            // invoke would blow up.
            (new \ReflectionMethod(WebSocketTransport::class, 'applyReadChunkSize'))->invoke($transport);

            // kills GreaterThan @ 182: the reader chunk size is left at Amp's untouched 8192 default
            // (and, decisively, the invoke above did not throw as the mutant's setChunkSize(0) would).
            self::assertSame(self::AMP_DEFAULT_CHUNK_SIZE, $this->readerChunkSize($client));
        } finally {
            $client->close();
            $serverSocket->close();
            $server->close();
        }
    }

    /**
     * Opens a loopback TCP pair and returns the connected client socket (a ResourceSocket) plus the
     * server side and listener so the caller can close them.
     *
     * @return array{0: Socket, 1: Socket, 2: \Amp\Socket\ServerSocket}
     */
    private function connectedResourceSocket(): array
    {
        $server = listen('tcp://127.0.0.1:0');
        $address = (string) $server->getAddress();

        $accept = async(static fn (): ?Socket => $server->accept());
        $client = connect('tcp://' . $address);
        $serverSocket = $accept->await();
        self::assertInstanceOf(Socket::class, $serverSocket);
        self::assertInstanceOf(ResourceSocket::class, $client);

        return [$client, $serverSocket, $server];
    }

    /**
     * Reads the effective reader chunk size out of an Amp ResourceSocket's private ReadableResourceStream
     * so a setChunkSize() call (or its absence) is directly observable.
     */
    private function readerChunkSize(Socket $socket): int
    {
        self::assertInstanceOf(ResourceSocket::class, $socket);

        $reader = (new \ReflectionProperty(ResourceSocket::class, 'reader'))->getValue($socket);
        self::assertInstanceOf(ReadableResourceStream::class, $reader);

        $value = (new \ReflectionProperty(ReadableResourceStream::class, 'defaultChunkSize'))->getValue($reader);
        self::assertIsInt($value);

        return $value;
    }
}
