<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\Socket\ConnectException as AmpConnectException;
use Amp\Socket\Socket;
use Amp\Socket\TlsException;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Tests\Support\ScriptedChunkSocket;
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
     * Verifies readLine() reassembles a large frame delivered in many small socket writes into the
     * byte-identical payload (#164 pins the chunk-accumulation join on the inbound path).
     */
    public function testReadLineReassemblesLargeFrameDeliveredInSmallChunks(): void
    {
        [$transport, $server, $serverSocket] = $this->connectedWebSocketTransport();

        try {
            // 256 KiB of distinct 32-bit counters so any join/ordering defect changes the bytes.
            $payload = pack('N*', ...range(0, 65535));
            $frame = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $payload, false);

            $writer = async(static function () use ($serverSocket, $frame): void {
                foreach (str_split($frame, 8192) as $piece) {
                    $serverSocket->write($piece);
                }
            });

            $received = '';
            while (strlen($received) < strlen($payload)) {
                $received .= $transport->readLine(new \Amp\TimeoutCancellation(5.0))->await();
            }
            $writer->await();

            self::assertSame($payload, $received);
        } finally {
            $transport->close()->await();
            $serverSocket->close();
            $server->close();
        }
    }

    /**
     * Verifies readLine() returns a complete frame while retaining a partially received next frame,
     * then returns that frame once its remaining bytes arrive (#164 pins head-frame completion
     * tracking across reads).
     */
    public function testReadLineKeepsPartialNextFrameAcrossReads(): void
    {
        [$transport, $server, $serverSocket] = $this->connectedWebSocketTransport();

        try {
            $frameA = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'FIRST', false);
            $frameB = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'SECOND', false);
            $half = intdiv(strlen($frameB), 2);

            $serverSocket->write($frameA . substr($frameB, 0, $half));
            self::assertSame('FIRST', $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await());

            $serverSocket->write(substr($frameB, $half));
            self::assertSame('SECOND', $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await());
        } finally {
            $transport->close()->await();
            $serverSocket->close();
            $server->close();
        }
    }

    /**
     * Verifies a PING interleaved between fragments of a fragmented message is still answered with a
     * PONG while the fragments reassemble intact (#164 pins control handling under fragment
     * accumulation).
     */
    public function testReadLineAnswersPingBetweenFragmentsAndReassembles(): void
    {
        [$transport, $server, $serverSocket] = $this->connectedWebSocketTransport();

        try {
            $serverSocket->write(
                $this->serverFrame(WebSocketFrameCodec::OP_BINARY, 'HEL', fin: false)
                . WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_PING, 'hb', false)
                . $this->serverFrame(WebSocketFrameCodec::OP_CONTINUATION, 'LO', fin: true),
            );

            self::assertSame('HELLO', $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await());

            // The server received the client's masked PONG carrying the ping payload.
            $buffer = (string) $serverSocket->read(new \Amp\TimeoutCancellation(2.0));
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
     * White-box pin for the #164 spill gate (both paths deliver identical bytes, so only internal
     * state proves which engaged): an incomplete frame carrying a 64-bit length must be sized for
     * chunk-list spill (`readFrameRequired` = full wire size), while an incomplete 16-bit-length
     * frame (<= 65535 bytes) must never be sized - it stays on the batch-decode `.=` path.
     */
    public function testDrainSizesLarge64BitFrameForSpillButLeaves16BitFrameOnBuffer(): void
    {
        $required = new \ReflectionProperty(WebSocketTransport::class, 'readFrameRequired');
        $buffer = new \ReflectionProperty(WebSocketTransport::class, 'readBuffer');
        $drain = new \ReflectionMethod(WebSocketTransport::class, 'drainDataFrames');

        // 64-bit-length frame (100000-byte payload), only its 10-byte header + 10 payload bytes buffered.
        $large = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, str_repeat('L', 100000), false);
        self::assertSame(127, ord($large[1]) & 0x7F);
        $big = new WebSocketTransport(new NatsOptions());
        $buffer->setValue($big, substr($large, 0, 20));
        self::assertSame('', $drain->invoke($big));
        self::assertSame(strlen($large), $required->getValue($big)); // sized -> spill engaged

        // 16-bit-length frame (1000-byte payload), partially buffered: must stay unsized.
        $medium = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, str_repeat('s', 1000), false);
        self::assertSame(126, ord($medium[1]) & 0x7F);
        $small = new WebSocketTransport(new NatsOptions());
        $buffer->setValue($small, substr($medium, 0, 20));
        self::assertSame('', $drain->invoke($small));
        self::assertNull($required->getValue($small)); // never sized -> batch-decode path
    }

    /**
     * Torture pin for frame-header assembly across read boundaries (#164): a 64-bit-extended-length
     * frame (65536-byte payload, 10-byte header) delivered in one-byte reads must be returned
     * byte-identical. A ScriptedChunkSocket guarantees the transport really observes 65546 one-byte
     * reads (loopback TCP would coalesce them).
     */
    public function testReadLineAssembles64BitLengthFrameDeliveredInOneByteReads(): void
    {
        // Distinct 32-bit counters so any join/ordering defect changes the bytes.
        $payload = pack('N*', ...range(0, 16383));
        self::assertSame(65536, strlen($payload));
        $frame = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $payload, false);
        self::assertSame(0x7F, ord($frame[1]) & 0x7F); // 64-bit extended length form

        $transport = $this->transportReadingChunks(str_split($frame, 1));

        $received = '';
        while (strlen($received) < strlen($payload)) {
            $received .= $transport->readLine()->await();
        }

        self::assertSame($payload, $received);
    }

    /**
     * Torture pin for continuation-frame headers split across read boundaries (#164): a fragmented
     * message whose middle (masked, 16-bit-length) continuation header is cut inside the extended
     * length bytes and inside the mask key, whose payload spans enough further reads to exercise the
     * chunk-list consume path, and whose final continuation header is cut between its two bytes,
     * must still reassemble byte-identically.
     */
    public function testReadLineReassemblesFragmentsWithHeadersSplitAcrossReads(): void
    {
        $mid = pack('N*', ...range(1000, 1074)); // 300 bytes, distinct
        $f1 = $this->serverFrame(WebSocketFrameCodec::OP_BINARY, 'HEAD-', fin: false);
        // Masked continuation: 2 base + 2 extended-length + 4 mask-key header bytes before the payload.
        $f2 = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_CONTINUATION, $mid, true, 'MKEY');
        $f2 = (chr(ord($f2[0]) & 0x7F)) . substr($f2, 1); // clear FIN: mid fragment
        $f3 = $this->serverFrame(WebSocketFrameCodec::OP_CONTINUATION, 'TAIL', fin: true);

        $transport = $this->transportReadingChunks([
            $f1 . substr($f2, 0, 1),                  // f2 header cut after its first byte
            substr($f2, 1, 2),                        // ...inside the 16-bit extended length
            substr($f2, 3, 3),                        // ...inside the mask key
            substr($f2, 6, 2) . substr($f2, 8, 100),  // rest of header + payload bytes 0-99
            substr($f2, 108, 100),                    // payload bytes 100-199
            substr($f2, 208) . substr($f3, 0, 1),     // payload tail + f3 header cut between its 2 bytes
            substr($f3, 1),
        ]);

        self::assertSame('HEAD-' . $mid . 'TAIL', $transport->readLine()->await());
    }

    /**
     * Torture pin for a large masked frame delivered in one-byte reads (#164): the payload needs a
     * 64-bit length, so the frame spills to chunk-list accumulation and is sized from its length
     * bytes before the mask key has arrived - the mask key itself therefore spans the queued reads
     * and must be assembled by the spanning-consume path before the payload is unmasked.
     * Byte-identical delivery required. (Server frames are unmasked in production; this pins the
     * codec's masked-decode symmetry on the spill path.)
     */
    public function testReadLineAssemblesLargeMaskedFrameDeliveredInOneByteReads(): void
    {
        // 70 000 distinct bytes: a 64-bit-length masked frame (> 65 535) that spills to the chunk path.
        $payload = pack('N*', ...range(0, 17499));
        self::assertSame(70000, strlen($payload));
        $frame = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $payload, true, "\xA5\x01\xFE\x42");
        self::assertSame(127, ord($frame[1]) & 0x7F); // 64-bit extended length form
        self::assertSame(0x80, ord($frame[1]) & 0x80); // mask bit set

        $transport = $this->transportReadingChunks(str_split($frame, 1));

        $received = '';
        while (strlen($received) < strlen($payload)) {
            $received .= $transport->readLine()->await();
        }

        self::assertSame($payload, $received);
    }

    /**
     * Builds a transport whose injected socket hands out exactly the given chunks, one per read.
     *
     * @param list<string> $chunks
     */
    private function transportReadingChunks(array $chunks): WebSocketTransport
    {
        $transport = new WebSocketTransport(new NatsOptions());
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))
            ->setValue($transport, new ScriptedChunkSocket($chunks));

        return $transport;
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
     * On a server-initiated Close frame the client must echo a Close frame back (RFC 6455 5.5.1,
     * mirroring the received status code) BEFORE surfacing the closure, so strict intermediaries do
     * not treat the connection as an abnormal 1006 closure. The TransportClosedException the
     * connection layer relies on to reconnect must still propagate (#161).
     */
    public function testReadLineEchoesCloseFrameOnServerInitiatedClose(): void
    {
        // Unmasked server Close frame carrying status 1001 ("going away"); the echo must mirror it.
        $status = pack('n', 1001);
        $closeFrame = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_CLOSE, $status, false);
        $socket = new ScriptedChunkSocket([$closeFrame]);

        $transport = new WebSocketTransport(new NatsOptions());
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $socket);

        try {
            $transport->readLine()->await();
            self::fail('expected TransportClosedException on the server close frame');
        } catch (TransportClosedException) {
            // The closure is still surfaced so the connection layer can reconnect.
        }

        // The client wrote back a (masked) Close frame mirroring the received status code.
        $written = $socket->writtenBytes();
        $frames = WebSocketFrameCodec::decode($written);
        self::assertNotSame([], $frames, 'no Close echo was written on the server close frame');
        self::assertSame(WebSocketFrameCodec::OP_CLOSE, $frames[0]['opcode']);
        self::assertSame($status, $frames[0]['payload']);
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
