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
use IDCT\NATS\Tests\Support\WedgedWriteSocket;
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
            $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);
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
        // An RSV1 frame is only legal once permessage-deflate was NEGOTIATED (RFC 6455 5.2); this
        // socket was injected without a handshake, so mark the negotiation explicitly.
        (new \ReflectionProperty(WebSocketTransport::class, 'compressionActive'))->setValue($transport, true);

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
            $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);
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
        // Unmasked server continuation (RFC 6455 5.1): 2 base + 2 extended-length header bytes.
        $f2 = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_CONTINUATION, $mid, false);
        $f2 = (chr(ord($f2[0]) & 0x7F)) . substr($f2, 1); // clear FIN: mid fragment
        $f3 = $this->serverFrame(WebSocketFrameCodec::OP_CONTINUATION, 'TAIL', fin: true);

        $transport = $this->transportReadingChunks([
            $f1 . substr($f2, 0, 1),                  // f2 header cut after its first byte
            substr($f2, 1, 1),                        // ...before the 16-bit extended length
            substr($f2, 2, 1),                        // ...inside the extended length
            substr($f2, 3, 1) . substr($f2, 4, 100),  // rest of header + payload bytes 0-99
            substr($f2, 104, 100),                    // payload bytes 100-199
            substr($f2, 204) . substr($f3, 0, 1),     // payload tail + f3 header cut between its 2 bytes
            substr($f3, 1),
        ]);

        self::assertSame('HEAD-' . $mid . 'TAIL', $transport->readLine()->await());
    }

    /**
     * Torture pin for a large masked frame delivered in one-byte reads (#164): the payload needs a
     * 64-bit length, so the frame spills to chunk-list accumulation and is sized from its length
     * bytes before the mask key has arrived - the mask key itself therefore spans the queued reads
     * and must be assembled by the spanning-consume path. Byte-identical delivery required.
     * (Unmasked, per RFC 6455 5.1 - masked server frames are now a terminal violation.)
     */
    public function testReadLineAssemblesLargeFrameDeliveredInOneByteReads(): void
    {
        // 70 000 distinct bytes: a 64-bit-length frame (> 65 535) that spills to the chunk path.
        $payload = pack('N*', ...range(0, 17499));
        self::assertSame(70000, strlen($payload));
        $frame = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $payload, false);
        self::assertSame(127, ord($frame[1]) & 0x7F); // 64-bit extended length form

        $transport = $this->transportReadingChunks(str_split($frame, 1));

        $received = '';
        while (strlen($received) < strlen($payload)) {
            $received .= $transport->readLine()->await();
        }

        self::assertSame($payload, $received);
    }

    /**
     * RFC 6455 5.1: a MASKED server frame on the large-frame spill path is a terminal protocol
     * violation - previously it was silently unmasked and accepted.
     */
    public function testReadLineRejectsMaskedServerFrameOnSpillPath(): void
    {
        $payload = str_repeat('Z', 70000); // > 65535 -> 64-bit length -> spill path
        $frame = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $payload, true, "\xA5\x01\xFE\x42");
        self::assertSame(0x80, ord($frame[1]) & 0x80); // mask bit set

        $transport = $this->transportReadingChunks(str_split($frame, 4096));

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('masked');
        // Loop until the spilled frame completes and the violation surfaces (bounded by chunks/EOF).
        for ($i = 0; $i < 64; $i++) {
            $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await();
        }
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

        // The client wrote back a Close frame mirroring the received status code.
        $written = $socket->writtenBytes();

        // RFC 6455 5.3: a client-sent frame MUST be masked (mask bit = high bit of byte 2). Check the
        // raw wire byte BEFORE decode() (which unmasks) so an unmasked-echo regression is caught.
        self::assertSame(0x80, ord($written[1]) & 0x80, 'the client Close echo must be masked (RFC 6455 5.3)');

        // decode() consumes its buffer by reference, so run it on a copy after the raw mask check.
        $buffer = $written;
        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);
        self::assertNotSame([], $frames, 'no Close echo was written on the server close frame');
        self::assertSame(WebSocketFrameCodec::OP_CLOSE, $frames[0]['opcode']);
        self::assertSame($status, $frames[0]['payload']);
    }

    /**
     * A single read carrying [data frame][OP_CLOSE] must NOT lose the data: readLine() returns the
     * decoded data bytes, the deferred close surfaces on the FOLLOWING readLine(), and the RFC 6455
     * Close echo (#161) is still written. Regression for #115 (a close frame discarded the data
     * decoded from the same read chunk, since the by-ref decode had already consumed those bytes).
     */
    public function testReadLineReturnsSameChunkDataThenDefersClose(): void
    {
        $payload = "MSG orders 1 5\r\nhello\r\n";
        $data = $this->serverFrame(WebSocketFrameCodec::OP_BINARY, $payload, fin: true);
        // Server Close mirroring status 1001 ("going away"), in the SAME read as the data frame.
        $close = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_CLOSE, pack('n', 1001), false);
        $socket = new ScriptedChunkSocket([$data . $close]);

        $transport = new WebSocketTransport(new NatsOptions());
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $socket);

        // The data decoded from the shared chunk is returned, not discarded by the trailing close.
        self::assertSame($payload, $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await());

        // The Close echo (#161) was still written when the close was seen (mirroring the 1001 status).
        $written = $socket->writtenBytes();
        self::assertNotSame('', $written, 'the RFC 6455 Close echo (#161) must still be sent');
        $buffer = $written;
        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);
        self::assertNotSame([], $frames, 'no Close echo was written');
        self::assertSame(WebSocketFrameCodec::OP_CLOSE, $frames[0]['opcode']);
        self::assertSame(pack('n', 1001), $frames[0]['payload']);

        // The close is surfaced on the NEXT readLine (deferred), distinct from a peer EOF.
        try {
            $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await();
            self::fail('expected the deferred close to surface on the following readLine');
        } catch (TransportClosedException $e) {
            self::assertStringContainsString('close frame', $e->getMessage());
        }
    }

    /**
     * close() must ALWAYS reach the socket close, even when the courtesy Close-frame write wedges on
     * backpressure. Amp's socket write() SUSPENDS (it does not throw) while the write buffer is full,
     * so the pre-fix inline write parked close() forever on a stalled peer - and because the socket
     * close is the only thing that errors pending writes out, recovery (whose every path awaits
     * transport close()) deadlocked permanently. The WedgedWriteSocket mirrors those exact semantics:
     * write() suspends until close() fails it with a ClosedException.
     */
    public function testCloseCompletesWhenCloseFrameWriteWedgesOnBackpressure(): void
    {
        $socket = new WedgedWriteSocket();
        $transport = new WebSocketTransport(new NatsOptions());
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $socket);

        // The outer cancellation turns a regression (close() suspended forever on the wedged write)
        // into a clean test failure instead of a hung PHPUnit run. It is far above the transport's
        // internal Close-frame write bound, so it never fires on the fixed path.
        $transport->close()->await(new \Amp\TimeoutCancellation(5.0));

        self::assertTrue($socket->isClosed(), 'close() must close the socket despite the wedged Close-frame write');
        self::assertSame(1, $socket->writeAttempts(), 'the Close frame write must still have been attempted');
        // The transport released the socket: a follow-up close is a no-op and write() reports closed.
        self::assertSame('', $transport->readLine()->await());
    }

    /**
     * A server that coalesces [BINARY "MSG …"][PING] into one read and then dies before the pong
     * answer must NOT cost the already-decoded MSG bytes: the pong write throwing inline in the read
     * fiber used to propagate out of readLine() and discard them - consumed from the buffer, never
     * delivered, and core NATS does not resend. The answer now goes out on its own fiber; the data
     * is returned, and the peer death surfaces on the NEXT read as a clean EOF.
     */
    public function testReadLinePreservesSameReadDataWhenPongWriteFails(): void
    {
        $payload = "MSG orders 1 5\r\nhello\r\n";
        $data = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $payload, false);
        $ping = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_PING, 'hb', false);
        $socket = new ScriptedChunkSocket([$data . $ping], failWrites: true);

        $transport = new WebSocketTransport(new NatsOptions());
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $socket);

        self::assertSame($payload, $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await());

        // The peer's death surfaces on the following read, after the data was delivered.
        $this->expectException(TransportClosedException::class);
        $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await();
    }

    /**
     * A backpressure-stalled peer must not park the READ path behind an outbound pong: the inline
     * pong write used to SUSPEND the read fiber (the same suspension class as the close() wedge),
     * stalling inbound delivery indefinitely. The answer now goes out on its own fiber, so the data
     * frame from the same read is returned promptly and close() still completes.
     */
    public function testReadLineNotStalledByWedgedPongWrite(): void
    {
        $payload = "MSG orders 1 5\r\nhello\r\n";
        $data = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $payload, false);
        $ping = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_PING, 'hb', false);
        $socket = new WedgedWriteSocket([$data . $ping]);

        $transport = new WebSocketTransport(new NatsOptions());
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $socket);

        // Pre-fix this await never returned (read fiber suspended in the pong write); the outer
        // cancellation turns a regression into a bounded failure instead of a hung run.
        self::assertSame($payload, $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await());
        self::assertSame(1, $socket->writeAttempts(), 'the pong answer must still have been attempted');

        // close() releases the wedged pong fiber (socket close errors the pending write out).
        $transport->close()->await(new \Amp\TimeoutCancellation(5.0));
        self::assertTrue($socket->isClosed());
    }

    /**
     * close() on a never-connected transport (and a second close() after the first) must be a clean
     * no-op: connection cleanup paths close transports whose connect() failed before a socket was
     * ever assigned, and recovery/disconnect can race a second close onto an already-closed one.
     */
    public function testCloseWithoutSocketAndRepeatedCloseAreNoOps(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());
        $transport->close()->await(new \Amp\TimeoutCancellation(5.0));

        $socket = new ScriptedChunkSocket([]);
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $socket);
        $transport->close()->await(new \Amp\TimeoutCancellation(5.0));
        self::assertTrue($socket->isClosed());

        // Second close on the already-released socket: a no-op, not a second frame write or error.
        $written = $socket->writtenBytes();
        $transport->close()->await(new \Amp\TimeoutCancellation(5.0));
        self::assertSame($written, $socket->writtenBytes());
    }

    /**
     * On a responsive socket close() still sends the RFC 6455 Close frame (masked, opcode 0x8, empty
     * payload) BEFORE closing - the wedge fix must not silently drop the courtesy frame from the
     * healthy path.
     */
    public function testCloseStillWritesCloseFrameOnResponsiveSocket(): void
    {
        $socket = new ScriptedChunkSocket([]);
        $transport = new WebSocketTransport(new NatsOptions());
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $socket);

        $transport->close()->await(new \Amp\TimeoutCancellation(5.0));

        self::assertTrue($socket->isClosed());

        $written = $socket->writtenBytes();
        self::assertNotSame('', $written, 'the Close frame must still be written on a responsive socket');
        // RFC 6455 5.3: client frames are masked - check the raw mask bit before decode() unmasks.
        self::assertSame(0x80, ord($written[1]) & 0x80, 'the client Close frame must be masked');
        $buffer = $written;
        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);
        self::assertNotSame([], $frames);
        self::assertSame(WebSocketFrameCodec::OP_CLOSE, $frames[0]['opcode']);
        self::assertSame('', $frames[0]['payload']);
        self::assertTrue($frames[0]['fin']);
    }

    /**
     * The deferred-close path integrates with the #164 large-frame spill: a 64-bit-length frame big
     * enough to spill to chunk-list accumulation, whose final read also carries the server's Close
     * frame, returns the full spilled payload first and defers the close to the next readLine() -
     * the spilled bytes must not be lost either. Regression for #115.
     */
    public function testReadLineReturnsSpilledFrameThenDefersCloseSharedWithFinalRead(): void
    {
        $payload = str_repeat('Z', 70000); // > 65535 -> 64-bit length -> spill path (#164)
        $frame = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $payload, false);
        self::assertSame(127, ord($frame[1]) & 0x7F); // 64-bit extended length form
        $close = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_CLOSE, '', false);

        $transport = $this->transportReadingChunks([
            substr($frame, 0, 20),        // header + a few payload bytes: sizes the spill
            substr($frame, 20) . $close,  // rest of the frame AND the trailing Close in one read
        ]);

        self::assertSame($payload, $transport->readLine(new \Amp\TimeoutCancellation(5.0))->await());

        try {
            $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await();
            self::fail('expected the deferred close after the spilled frame');
        } catch (TransportClosedException $e) {
            self::assertStringContainsString('close frame', $e->getMessage());
        }
    }

    /**
     * A single read carrying [complete data frame][fragment start][new data frame mid-fragmentation]
     * must NOT lose the complete message: readLine() returns its bytes first, and the RFC 6455 5.4
     * fragmentation violation surfaces as a ProtocolException on the FOLLOWING readLine() - deferred
     * exactly like the graceful close. Regression for #115 (the violation threw inline, discarding the
     * data frame already decoded out of the same read chunk by the by-ref decode).
     */
    public function testReadLineReturnsSameChunkDataThenDefersFragmentationViolation(): void
    {
        $payload = "MSG orders 1 5\r\nhello\r\n";
        // A complete data message, then a fragment start, then a COMPLETE data frame while fragmenting
        // (fin: true, so removing the deferring `return $out` would wrongly deliver its payload here -
        // this pins the return, not just the branch). All coalesced into one read.
        $chunk = $this->serverFrame(WebSocketFrameCodec::OP_BINARY, $payload, fin: true)
            . $this->serverFrame(WebSocketFrameCodec::OP_BINARY, 'PA', fin: false)
            . $this->serverFrame(WebSocketFrameCodec::OP_BINARY, "MSG orders 1 4\r\nleak\r\n", fin: true);
        $socket = new ScriptedChunkSocket([$chunk]);

        $transport = new WebSocketTransport(new NatsOptions());
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $socket);

        // Only the complete message before the fragmentation is returned; the mid-fragmentation frame's
        // payload must NOT be appended (the violation defers before it is delivered).
        self::assertSame($payload, $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await());

        // The violation is surfaced on the NEXT readLine (deferred), and it is the protocol error - not
        // a peer EOF - so the connection layer fails loudly rather than losing the earlier message.
        try {
            $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await();
            self::fail('expected the deferred fragmentation violation to surface on the following readLine');
        } catch (ProtocolException $e) {
            self::assertMatchesRegularExpression('/fragment/i', $e->getMessage());
        }
    }

    /**
     * A single read carrying [complete data frame][orphan continuation frame] must NOT lose the
     * complete message: readLine() returns its bytes first, and the RFC 6455 5.4 orphan-continuation
     * violation surfaces as a ProtocolException on the FOLLOWING readLine() - deferred like the close.
     * Regression for #115 (the violation threw inline, discarding the already-decoded data frame).
     */
    public function testReadLineReturnsSameChunkDataThenDefersOrphanContinuation(): void
    {
        $payload = "MSG orders 1 2\r\nhi\r\n";
        // A complete data message, then a continuation frame with no fragmentation in progress, in the
        // same read.
        $chunk = $this->serverFrame(WebSocketFrameCodec::OP_BINARY, $payload, fin: true)
            . $this->serverFrame(WebSocketFrameCodec::OP_CONTINUATION, 'XX', fin: true);
        $socket = new ScriptedChunkSocket([$chunk]);

        $transport = new WebSocketTransport(new NatsOptions());
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $socket);

        self::assertSame($payload, $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await());

        try {
            $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await();
            self::fail('expected the deferred orphan-continuation violation on the following readLine');
        } catch (ProtocolException $e) {
            self::assertMatchesRegularExpression('/continuation/i', $e->getMessage());
        }
    }

    /**
     * RFC 6455 5.4: a new data frame arriving mid-fragmentation is a protocol violation. With no data
     * decoded earlier in the same batch to return first, it must fail loudly (ProtocolException) right
     * away instead of silently overwriting the partial message (#115).
     */
    public function testDrainFailsWhenDataFrameArrivesMidFragmentation(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());
        // Start a fragmented message (FIN=0), then a NEW data frame instead of a continuation.
        $buffer = $this->serverFrame(WebSocketFrameCodec::OP_BINARY, 'PI', fin: false)
            . $this->serverFrame(WebSocketFrameCodec::OP_BINARY, 'XX', fin: false);
        (new \ReflectionProperty(WebSocketTransport::class, 'readBuffer'))->setValue($transport, $buffer);
        $drain = new \ReflectionMethod(WebSocketTransport::class, 'drainDataFrames');

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessageMatches('/fragment/i');
        $drain->invoke($transport);
    }

    /**
     * RFC 6455 5.4: a continuation frame with no fragmented message in progress is a protocol
     * violation. With no data decoded earlier in the same batch to return first, it must fail loudly
     * (ProtocolException) right away instead of being silently dropped (#115).
     */
    public function testDrainFailsOnOrphanContinuationFrame(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());
        $buffer = $this->serverFrame(WebSocketFrameCodec::OP_CONTINUATION, 'XX', fin: true);
        (new \ReflectionProperty(WebSocketTransport::class, 'readBuffer'))->setValue($transport, $buffer);
        $drain = new \ReflectionMethod(WebSocketTransport::class, 'drainDataFrames');

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessageMatches('/continuation/i');
        $drain->invoke($transport);
    }

    /**
     * The #115 deferral now covers codec strictness violations too: a read carrying
     * [valid MSG][oversized-length declaration] must return the MSG first (its bytes are consumed
     * and unrecoverable) and surface the violation on the NEXT readLine - pre-fix the throw
     * discarded the already-decoded MSG.
     */
    public function testReadLineReturnsSameReadDataBeforeOversizedLengthViolation(): void
    {
        $payload = "MSG orders 1 5\r\nhello\r\n";
        $data = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $payload, false);
        $poison = pack('CC', 0x82, 127) . pack('J', 64 * 1024 * 1024 + 1);

        $transport = $this->transportReadingChunks([$data . $poison]);

        self::assertSame($payload, $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await(), 'same-read data must be delivered before the violation');

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('out of bounds');
        $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await();
    }

    /**
     * A fragmented PING arriving mid-fragmented-message is a terminal RFC 6455 5.5 violation - the
     * pre-fix codec answered the pong and spliced the ping's CONTINUATION bytes into the data
     * message: silently corrupted payload fed to the NATS parser.
     */
    public function testReadLineRejectsFragmentedControlFrameInsteadOfCorruptingReassembly(): void
    {
        $head = $this->serverFrame(WebSocketFrameCodec::OP_BINARY, 'HEAD-', fin: false);
        $fragmentedPing = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_PING, 'abc', false);
        $fragmentedPing[0] = chr(ord($fragmentedPing[0]) & 0x7F); // clear FIN on a control frame
        $pingTail = $this->serverFrame(WebSocketFrameCodec::OP_CONTINUATION, 'def', fin: true);
        $realTail = $this->serverFrame(WebSocketFrameCodec::OP_CONTINUATION, 'TAIL', fin: true);

        $transport = $this->transportReadingChunks([$head . $fragmentedPing . $pingTail . $realTail]);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('control frame');
        // No corrupted "HEAD-defTAIL"-style delivery may precede the failure.
        $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await();
    }

    /**
     * RFC 6455 5.2: RSV1 on a data frame without a negotiated extension must fail the connection
     * with a NAMED violation - pre-fix the payload was blindly inflated, producing a confusing
     * zlib error (or garbage delivered as NATS bytes).
     */
    public function testReadLineRejectsRsv1WithoutNegotiatedCompression(): void
    {
        $frame = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'plain', false, null, true);

        $transport = $this->transportReadingChunks([$frame]);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('permessage-deflate was not negotiated');
        $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await();
    }

    /**
     * Runs a real ws:// handshake against a scripted HTTP server whose response is built from the
     * request's Sec-WebSocket-Key. Returns the connect() outcome (null = success) plus the transport.
     *
     * @param callable(string):string $responseFor Maps the accept key to the full HTTP response.
     * @return array{0: ?\Throwable, 1: WebSocketTransport}
     */
    private function handshakeAgainst(callable $responseFor, bool $compression = false): array
    {
        $server = listen('tcp://127.0.0.1:0');
        $address = (string) $server->getAddress();

        async(static function () use ($server, $responseFor): void {
            $socket = $server->accept();
            if ($socket === null) {
                return;
            }

            $request = '';
            while (!str_contains($request, "\r\n\r\n")) {
                $chunk = $socket->read();
                if ($chunk === null) {
                    return;
                }
                $request .= $chunk;
            }

            preg_match('/Sec-WebSocket-Key:\s*(\S+)/i', $request, $m);
            $socket->write($responseFor(WebSocketFrameCodec::acceptKey($m[1] ?? '')));
        });

        $transport = new WebSocketTransport(new NatsOptions(webSocketCompression: $compression));
        $error = null;
        try {
            $transport->connect('ws://' . $address . '/', 2000)->await();
        } catch (\Throwable $e) {
            $error = $e;
        } finally {
            $server->close();
        }

        return [$error, $transport];
    }

    /**
     * RFC 6455 4.1: a 101 without "Upgrade: websocket" / "Connection: Upgrade" is NOT a completed
     * WebSocket handshake (e.g. a proxy answering 101 for something else) - the client must fail
     * instead of speaking WebSocket into a non-WebSocket stream. Pre-fix only the status line and
     * accept key were checked.
     */
    public function testHandshakeRejectsMissingUpgradeHeaders(): void
    {
        [$error] = $this->handshakeAgainst(static fn (string $accept): string => "HTTP/1.1 101 Switching Protocols\r\n"
            . "Sec-WebSocket-Accept: {$accept}\r\n\r\n");

        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertStringContainsString('RFC 6455 4.1', $error->getMessage());
    }

    /**
     * RFC 6455 4.1: a server negotiating an extension the client never offered must fail the
     * handshake - pre-fix an unsolicited "permessage-deflate" silently flipped compression ON, so
     * the client RSV1-deflated its CONNECT into a server that never negotiated it.
     */
    public function testHandshakeRejectsUnsolicitedCompression(): void
    {
        [$error] = $this->handshakeAgainst(static fn (string $accept): string => "HTTP/1.1 101 Switching Protocols\r\n"
            . "Upgrade: websocket\r\nConnection: Upgrade\r\n"
            . "Sec-WebSocket-Accept: {$accept}\r\n"
            . "Sec-WebSocket-Extensions: permessage-deflate\r\n\r\n", compression: false);

        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertStringContainsString('never offered', $error->getMessage());
    }

    /**
     * RFC 7692: this codec inflates per message (no context takeover), so a server negotiating
     * parameters beyond the offered client/server_no_context_takeover would garble every message
     * after the first - the handshake must reject them. A conformant response negotiates cleanly.
     */
    public function testHandshakeValidatesCompressionParameters(): void
    {
        [$error] = $this->handshakeAgainst(static fn (string $accept): string => "HTTP/1.1 101 Switching Protocols\r\n"
            . "Upgrade: websocket\r\nConnection: Upgrade\r\n"
            . "Sec-WebSocket-Accept: {$accept}\r\n"
            . "Sec-WebSocket-Extensions: permessage-deflate; server_max_window_bits=10\r\n\r\n", compression: true);

        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertStringContainsString('unacceptable permessage-deflate parameter', $error->getMessage());

        // RFC 7692 7.1.1.1: server_no_context_takeover was offered, so the server must ECHO it or
        // decline - accepting without it means the server may retain deflate context across
        // messages, which this per-message-inflate codec cannot decode.
        [$error] = $this->handshakeAgainst(static fn (string $accept): string => "HTTP/1.1 101 Switching Protocols\r\n"
            . "Upgrade: websocket\r\nConnection: Upgrade\r\n"
            . "Sec-WebSocket-Accept: {$accept}\r\n"
            . "Sec-WebSocket-Extensions: permessage-deflate\r\n\r\n", compression: true);

        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertStringContainsString('server_no_context_takeover', $error->getMessage());

        [$error, $transport] = $this->handshakeAgainst(static fn (string $accept): string => "HTTP/1.1 101 Switching Protocols\r\n"
            . "Upgrade: websocket\r\nConnection: keep-alive, Upgrade\r\n"
            . "Sec-WebSocket-Accept: {$accept}\r\n"
            . "Sec-WebSocket-Extensions: permessage-deflate; client_no_context_takeover; server_no_context_takeover\r\n\r\n", compression: true);

        self::assertNull($error, 'a conformant negotiation must succeed');
        self::assertTrue(
            (new \ReflectionProperty(WebSocketTransport::class, 'compressionActive'))->getValue($transport),
            'compression must be active after a clean negotiation',
        );
        $transport->close()->await();
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
