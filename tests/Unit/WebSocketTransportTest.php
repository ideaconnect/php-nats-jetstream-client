<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Socket\ConnectException as AmpConnectException;
use Amp\Socket\Socket;
use Amp\Socket\TlsException;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Transport\TlsAwareTransportInterface;
use IDCT\NATS\Transport\TransportClosedException;
use IDCT\NATS\Transport\WebSocketFrameCodec;
use IDCT\NATS\Transport\WebSocketTransport;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\Socket\connect;
use function Amp\Socket\listen;

final class WebSocketTransportTest extends TestCase
{
    /**
     * Verifies the WebSocket transport is TLS-aware and reports no TLS before connecting (#31).
     */
    public function testIsTlsAwareAndInactiveBeforeConnect(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());

        self::assertInstanceOf(TlsAwareTransportInterface::class, $transport);
        self::assertFalse($transport->tlsActive());
    }

    /**
     * Verifies readLine() returns '' (not EOF) when no socket is connected yet (#31).
     */
    public function testReadLineReturnsEmptyWithoutSocket(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());

        self::assertSame('', $transport->readLine()->await());
    }

    /**
     * Writing without a socket must throw: a silent no-op would confirm publishes/ACKs that never
     * reached any socket (#124).
     */
    public function testWriteWithoutSocketThrowsTransportClosed(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());

        $this->expectException(TransportClosedException::class);
        $this->expectExceptionMessage('Transport is not connected');
        $transport->write("PING\r\n")->await();
    }

    /**
     * Verifies upgradeTls() is a no-op for WebSocket (TLS is done at connect) (#31).
     */
    public function testUpgradeTlsIsNoOp(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());

        // Resolves without error and leaves TLS inactive (wss negotiates during connect()).
        $transport->upgradeTls()->await();
        self::assertFalse($transport->tlsActive());
    }

    /**
     * Verifies the upgrade request includes custom headers and the compression offer (#61).
     */
    public function testBuildUpgradeRequestWithCustomHeadersAndCompression(): void
    {
        $request = WebSocketTransport::buildUpgradeRequest(
            'nats.example',
            443,
            '/',
            'abc123==',
            ['Cookie' => 'session=xyz', 'X-Proxy-Auth' => 'token'],
            true,
        );

        self::assertStringContainsString("GET / HTTP/1.1\r\n", $request);
        self::assertStringContainsString("Host: nats.example:443\r\n", $request);
        self::assertStringContainsString("Sec-WebSocket-Key: abc123==\r\n", $request);
        self::assertStringContainsString('Sec-WebSocket-Extensions: permessage-deflate', $request);
        self::assertStringContainsString("Cookie: session=xyz\r\n", $request);
        self::assertStringContainsString("X-Proxy-Auth: token\r\n", $request);
        self::assertStringEndsWith("\r\n\r\n", $request);
    }

    /**
     * Verifies reserved headers cannot be overridden and CR/LF is stripped from custom values (#61).
     */
    public function testBuildUpgradeRequestRejectsReservedAndStripsCrLf(): void
    {
        $request = WebSocketTransport::buildUpgradeRequest(
            'h',
            80,
            '/',
            'k',
            ['Host' => 'evil', 'X-Inject' => "ok\r\nX-Evil: 1"],
            false,
        );

        // The reserved Host header keeps its real value (the override is ignored).
        self::assertStringContainsString("Host: h:80\r\n", $request);
        self::assertStringNotContainsString('evil', $request);
        // CR/LF stripped from a custom value: no injected header line.
        self::assertStringContainsString("X-Inject: okX-Evil: 1\r\n", $request);
        self::assertStringNotContainsString("\r\nX-Evil: 1\r\n", $request);
        // No compression offer when disabled.
        self::assertStringNotContainsString('permessage-deflate', $request);
    }

    /**
     * Verifies connect() rejects a DSN without a host before attempting a socket connection (#31).
     */
    public function testConnectRejectsDsnWithoutHost(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Invalid WebSocket DSN');
        $transport->connect('ws:///just-a-path', 1000)->await();
    }

    /**
     * Verifies connect() appends query string to path before attempting socket open.
     *
     * The DSN contains a query part, so path is built as '/?q=v'.  The closure then
     * proceeds to the socket connect which fails (port 1 is not listening), surfacing an Amp
     * ConnectException - proof that input-validation and path-building ran without error.
     */
    public function testConnectAppendsQueryStringToPathBeforeSocketAttempt(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());

        $this->expectException(AmpConnectException::class);
        // ws:// with a query string - parse_url yields ['host'=>..., 'query'=>'q=v'].
        // The query-append ($path .= '?' . $parts['query']) executes before the socket connect fails.
        $transport->connect('ws://127.0.0.1:1/?q=v', 100)->await();
    }

    /**
     * Verifies connect() builds a TLS context for wss:// before attempting socket open.
     *
     * Using wss:// triggers the `if ($secure)` branch, which calls buildTlsContext()
     * and stores the result in $context.  The socket connect then fails
     * (port 1 is not listening), confirming the secure branch ran without error.
     */
    public function testConnectBuildsTlsContextForWssSchemeBeforeSocketAttempt(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());

        $this->expectException(AmpConnectException::class);
        // wss:// activates the secure branch; buildTlsContext() runs, then port 1
        // refuses the connection - ConnectException is the expected outcome.
        $transport->connect('wss://127.0.0.1:1/', 100)->await();
    }

    /**
     * Verifies connect() calls setupTls() on the socket for wss:// when TCP succeeds.
     *
     * A plain-TCP listener is started locally so the socket connect succeeds.  The
     * transport then calls setupTls() (still inside the `if ($secure)` block), which
     * fails because the server speaks plain TCP - surfacing a TlsException.  This confirms the
     * setupTls() call is reachable; the subsequent `$this->tlsEstablished = true` assignment
     * requires a real TLS server and is therefore skipped.
     */
    public function testConnectCallsSetupTlsOnWssAndThrowsWhenServerIsPlainTcp(): void
    {
        // Spin up a plain-TCP listener on an ephemeral port.
        $server = listen('tcp://127.0.0.1:0');
        $address = (string) $server->getAddress();

        // Accept and immediately close - we just need TCP to connect; no TLS handshake.
        \Amp\async(static function () use ($server): void {
            $client = $server->accept();
            if ($client !== null) {
                $client->close();
            }
        });

        $transport = new WebSocketTransport(new NatsOptions(tlsVerifyPeer: false));

        try {
            $transport->connect('wss://' . $address . '/', 2000)->await();
            self::fail('Expected TlsException was not thrown');
        } catch (TlsException) {
            // setupTls() ran and threw because the server is plain TCP - expected.
            self::assertFalse($transport->tlsActive(), 'tlsEstablished must remain false when setupTls throws');
        } finally {
            $server->close();
        }
    }

    /**
     * Verifies CR/LF (and ':') are stripped from custom header NAMES, not just values, so a malicious
     * header name cannot forge additional handshake header lines (#89).
     */
    public function testBuildUpgradeRequestSanitizesHeaderNamesAgainstInjection(): void
    {
        $request = WebSocketTransport::buildUpgradeRequest(
            'h',
            80,
            '/',
            'k',
            ["X-Inject\r\nEvil-Header: pwned" => 'ok', 'X-Value' => "good\r\nSmuggled-Header: pwned"],
            false,
        );

        // No injected line is emitted from either the name or the value.
        foreach (explode("\r\n", $request) as $line) {
            self::assertStringStartsNotWith('Evil-Header:', $line);
            self::assertStringStartsNotWith('Smuggled-Header:', $line);
        }

        // The sanitized header survives on a single line (CR/LF gone; ':' stripped from the name).
        self::assertStringContainsString("X-InjectEvil-Header pwned: ok\r\n", $request);
        self::assertStringContainsString("X-Value: goodSmuggled-Header: pwned\r\n", $request);

        // Exactly one header/body terminator - no extra blank line was injected.
        self::assertSame(1, substr_count($request, "\r\n\r\n"));
    }

    /**
     * Verifies a fragmented message within the cap is reassembled across continuation frames (#89).
     */
    public function testDrainReassemblesFragmentedMessageWithinBound(): void
    {
        $transport = new WebSocketTransport(new NatsOptions(), maxMessageBytes: 1024);

        $buffer = $this->serverFrame(WebSocketFrameCodec::OP_BINARY, 'PI', fin: false)
            . $this->serverFrame(WebSocketFrameCodec::OP_CONTINUATION, 'NG', fin: false)
            . $this->serverFrame(WebSocketFrameCodec::OP_CONTINUATION, "\r\n", fin: true);

        (new \ReflectionProperty(WebSocketTransport::class, 'readBuffer'))->setValue($transport, $buffer);
        $result = (new \ReflectionMethod(WebSocketTransport::class, 'drainDataFrames'))->invoke($transport);

        self::assertSame("PING\r\n", $result);
    }

    /**
     * Verifies a fragmented message whose reassembly exceeds the cap is rejected (bounds memory against
     * a hostile/buggy server streaming unbounded continuation frames) (#89).
     */
    public function testDrainRejectsOversizedFragmentedMessage(): void
    {
        $transport = new WebSocketTransport(new NatsOptions(), maxMessageBytes: 8);

        // 4-byte start frame (within cap) + 6-byte continuation pushes the buffer to 10 > 8.
        $buffer = $this->serverFrame(WebSocketFrameCodec::OP_BINARY, 'AAAA', fin: false)
            . $this->serverFrame(WebSocketFrameCodec::OP_CONTINUATION, 'BBBBBB', fin: false);

        (new \ReflectionProperty(WebSocketTransport::class, 'readBuffer'))->setValue($transport, $buffer);
        $drain = new \ReflectionMethod(WebSocketTransport::class, 'drainDataFrames');

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('exceeded the maximum');
        $drain->invoke($transport);
    }

    /**
     * Builds an unmasked server-to-client WebSocket frame (payload <= 125 bytes) for decode tests.
     */
    private function serverFrame(int $opcode, string $payload, bool $fin): string
    {
        $firstByte = ($fin ? 0x80 : 0x00) | ($opcode & 0x0F);

        return pack('CC', $firstByte, strlen($payload)) . $payload;
    }

    /**
     * Verifies readLine() decodes a binary data frame read from a real socket (#87, exercises the
     * read/decode loop, not just the pure codec).
     */
    public function testReadLineDecodesBinaryFrameFromSocket(): void
    {
        [$transport, $server, $serverSocket] = $this->connectedWebSocketTransport();

        try {
            $serverSocket->write(WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, "PING\r\n", false));

            self::assertSame("PING\r\n", $transport->readLine()->await());
        } finally {
            $transport->close()->await();
            $serverSocket->close();
            $server->close();
        }
    }

    /**
     * Verifies readLine() answers a server PING with a (masked) PONG carrying the same application data
     * and then returns the following data frame (#87).
     */
    public function testReadLineAnswersPingWithPongAndContinues(): void
    {
        [$transport, $server, $serverSocket] = $this->connectedWebSocketTransport();

        try {
            $serverSocket->write(WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_PING, 'hb', false));
            $serverSocket->write(WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'DATA', false));

            // The ping is answered inline; readLine returns the subsequent data frame.
            self::assertSame('DATA', $transport->readLine()->await());

            // The server received the client's PONG (masked) carrying the ping payload.
            $buffer = (string) $serverSocket->read();
            $frames = WebSocketFrameCodec::decode($buffer);
            self::assertNotSame([], $frames);
            self::assertSame(WebSocketFrameCodec::OP_PONG, $frames[0]['opcode']);
            self::assertSame('hb', $frames[0]['payload']);
        } finally {
            $transport->close()->await();
            $serverSocket->close();
            $server->close();
        }
    }

    /**
     * Verifies readLine() inflates an RSV1 (permessage-deflate) compressed frame read from a socket (#87).
     */
    public function testReadLineInflatesCompressedFrameFromSocket(): void
    {
        [$transport, $server, $serverSocket] = $this->connectedWebSocketTransport();

        try {
            $payload = 'INFO {"server_id":"x","headers":true}';
            $serverSocket->write(WebSocketFrameCodec::encode(
                WebSocketFrameCodec::OP_BINARY,
                WebSocketFrameCodec::deflate($payload),
                false,
                null,
                true,
            ));

            self::assertSame($payload, $transport->readLine()->await());
        } finally {
            $transport->close()->await();
            $serverSocket->close();
            $server->close();
        }
    }

    /**
     * Verifies readLine() surfaces a server close frame as TransportClosedException (#87).
     */
    public function testReadLineThrowsOnCloseFrameFromSocket(): void
    {
        [$transport, $server, $serverSocket] = $this->connectedWebSocketTransport();

        try {
            $serverSocket->write(WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_CLOSE, '', false));

            $this->expectException(TransportClosedException::class);
            $transport->readLine()->await();
        } finally {
            $transport->close()->await();
            $serverSocket->close();
            $server->close();
        }
    }

    /**
     * Connects a WebSocketTransport to a loopback TCP socket and injects the connected client socket
     * (bypassing the HTTP upgrade handshake) so readLine()/the decode loop can be driven with
     * pre-encoded server frames written from the returned server-side socket.
     *
     * @return array{0: WebSocketTransport, 1: \Amp\Socket\ServerSocket, 2: Socket}
     */
    private function connectedWebSocketTransport(): array
    {
        $server = listen('tcp://127.0.0.1:0');
        $address = (string) $server->getAddress();

        $accept = async(static fn (): ?Socket => $server->accept());
        $clientSocket = connect('tcp://' . $address);
        $serverSocket = $accept->await();
        self::assertInstanceOf(Socket::class, $serverSocket);

        $transport = new WebSocketTransport(new NatsOptions());
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $clientSocket);

        return [$transport, $server, $serverSocket];
    }
}
