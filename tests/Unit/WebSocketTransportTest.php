<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\ByteStream\ClosedException;
use Amp\Socket\BindContext;
use Amp\Socket\Certificate;
use Amp\Socket\ConnectException as AmpConnectException;
use Amp\Socket\ServerTlsContext;
use Amp\Socket\Socket;
use Amp\Socket\TlsException;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Tests\Support\ScriptedChunkSocket;
use IDCT\NATS\Tests\Support\WedgedWriteSocket;
use IDCT\NATS\Transport\AmpSocketTransport;
use IDCT\NATS\Transport\TlsAwareTransportInterface;
use IDCT\NATS\Transport\TransportClosedException;
use IDCT\NATS\Transport\WebSocketFrameCodec;
use IDCT\NATS\Transport\WebSocketTransport;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\delay;
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
        self::assertSame(1, $socket->writeAttempts(), 'the first close writes the Close frame once');

        // Second close on the already-released socket: a no-op, not a second frame write or error.
        // The ATTEMPT count is what pins this - the closed double throws before recording bytes and
        // close() swallows frame-write failures, so writtenBytes() alone would stay equal even if
        // close() regressed into re-attempting the write on a retained socket.
        $written = $socket->writtenBytes();
        $transport->close()->await(new \Amp\TimeoutCancellation(5.0));
        self::assertSame($written, $socket->writtenBytes());
        self::assertSame(1, $socket->writeAttempts(), 'a repeated close must not re-attempt the Close-frame write');
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
     * A socket write() that throws (peer died between delivering its last chunk and the client's
     * write) must surface through the RETURNED future - never as a synchronous throw from write() -
     * per the #136 inline-write contract, so callers that queue the future and await later still
     * observe the failure.
     */
    public function testWriteSurfacesSocketFailureThroughReturnedFuture(): void
    {
        $socket = new ScriptedChunkSocket([], failWrites: true);
        $transport = new WebSocketTransport(new NatsOptions());
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $socket);

        // The call itself must not throw; the failure is deferred to the returned future.
        $future = $transport->write("PING\r\n");

        try {
            $future->await();
            self::fail('expected the socket write failure to surface through the future');
        } catch (ClosedException $e) {
            self::assertStringContainsString('rejects writes', $e->getMessage());
        }
    }

    /**
     * The TCP transport shares the exact same write error-path contract (#136): a throwing socket
     * write surfaces through the returned future, never synchronously. Placed here because the
     * scripted-socket harness these error-path pins rely on lives in this file.
     */
    public function testAmpSocketTransportWriteSurfacesSocketFailureThroughReturnedFuture(): void
    {
        $socket = new ScriptedChunkSocket([], failWrites: true);
        $transport = new AmpSocketTransport(new NatsOptions());
        (new \ReflectionProperty(AmpSocketTransport::class, 'socket'))->setValue($transport, $socket);

        $future = $transport->write("PING\r\n");

        try {
            $future->await();
            self::fail('expected the socket write failure to surface through the future');
        } catch (ClosedException $e) {
            self::assertStringContainsString('rejects writes', $e->getMessage());
        }
    }

    /**
     * An empty-string socket read (a transient no-data return, e.g. from a TLS record boundary) is
     * neither EOF nor data: readLine() must skip it and deliver the frame from the following read,
     * rather than misclassifying it as a peer close or appending it as a phantom chunk.
     */
    public function testReadLineSkipsEmptySocketReadsAndDeliversFollowingFrame(): void
    {
        $payload = "MSG orders 1 2\r\nhi\r\n";
        $frame = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $payload, false);

        $transport = $this->transportReadingChunks(['', $frame]);

        self::assertSame($payload, $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await());
    }

    /**
     * A peer coalescing a PING flood into a single read costs ONE answer, not one per ping: a
     * queued-but-unsent pong payload is REPLACED by each newer ping's (RFC 6455 5.5.3 permits
     * answering only the most recent ping - eliding the OLDER answers, never the newest). 18 pings
     * coalesced with a data frame into one read yield EXACTLY one pong, carrying the NEWEST ping's
     * payload, while the data frame is still delivered. Pre-fix a counted-fiber cap kept the 16
     * OLDEST answers and dropped the newest - the 5.5.3 inversion.
     */
    public function testReadLineCoalescesPingFloodIntoSinglePongForNewestPing(): void
    {
        $pings = '';
        for ($i = 0; $i < 18; $i++) {
            $pings .= WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_PING, sprintf('p%02d', $i), false);
        }
        $data = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'DATA', false);
        $socket = new ScriptedChunkSocket([$pings . $data]);

        $transport = new WebSocketTransport(new NatsOptions());
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $socket);

        self::assertSame('DATA', $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await());

        // The writer fiber was spawned before readLine() resolved and its write completes
        // synchronously on the scripted socket; wait state-driven (bounded by an hrtime deadline,
        // never a bare sleep) until the answer has hit the wire.
        $deadlineNs = hrtime(true) + 2_000_000_000;
        do {
            $buffer = $socket->writtenBytes();
            $pongs = WebSocketFrameCodec::decode($buffer, allowMasked: true);
            if ($pongs !== []) {
                break;
            }
            delay(0.001);
        } while (hrtime(true) < $deadlineNs);

        // Two extra event-loop ticks: any erroneously queued additional answer would have been
        // written by now, so the exact count below pins the coalescing, not a race.
        delay(0);
        delay(0);

        $buffer = $socket->writtenBytes();
        $pongs = WebSocketFrameCodec::decode($buffer, allowMasked: true);
        self::assertCount(1, $pongs, 'coalesced pings collapse to exactly ONE pong (RFC 6455 5.5.3)');
        self::assertSame(WebSocketFrameCodec::OP_PONG, $pongs[0]['opcode']);
        self::assertSame('p17', $pongs[0]['payload'], 'the single pong answers the NEWEST ping, never an older one');
    }

    /**
     * The RFC 6455 5.5.1 Close echo is a MUST and lives in its OWN answer slot that a ping flood
     * can never displace: 17 pings + a Close coalesced into ONE read on a healthy socket yield
     * exactly one pong - for the NEWEST ping - followed by exactly one Close echo mirroring the
     * received status code. Pre-fix the counted-fiber cap (16) never decremented within a batch
     * (fibers only start once the read fiber suspends), so it admitted the 16 OLDEST pongs and
     * dropped the Close echo entirely despite zero actual backpressure.
     */
    public function testReadLineFlushesCloseEchoAfterCoalescedPingFlood(): void
    {
        $pings = '';
        for ($i = 0; $i < 17; $i++) {
            $pings .= WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_PING, sprintf('p%02d', $i), false);
        }
        $status = pack('n', 1000);
        $close = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_CLOSE, $status, false);
        $socket = new ScriptedChunkSocket([$pings . $close]);

        $transport = new WebSocketTransport(new NatsOptions());
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $socket);

        try {
            $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await();
            self::fail('expected TransportClosedException on the server close frame');
        } catch (TransportClosedException) {
            // The closure still surfaces; the answers are asserted below.
        }

        // State-driven bounded wait until both answers hit the wire, then two extra ticks so any
        // erroneously admitted additional answer would also have been written - the exact frame
        // list below pins the slot semantics, not a race.
        $deadlineNs = hrtime(true) + 2_000_000_000;
        do {
            $buffer = $socket->writtenBytes();
            $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);
            if (count($frames) >= 2) {
                break;
            }
            delay(0.001);
        } while (hrtime(true) < $deadlineNs);
        delay(0);
        delay(0);

        $buffer = $socket->writtenBytes();
        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);
        self::assertCount(2, $frames, 'exactly one pong plus the mandatory Close echo (RFC 6455 5.5.1)');
        self::assertSame(WebSocketFrameCodec::OP_PONG, $frames[0]['opcode']);
        self::assertSame('p16', $frames[0]['payload'], 'the pong answers the NEWEST of the 17 coalesced pings');
        self::assertSame(WebSocketFrameCodec::OP_CLOSE, $frames[1]['opcode']);
        self::assertSame($status, $frames[1]['payload'], 'the Close echo mirrors the received status code');
    }

    /**
     * An answer queued while the transport still had its socket must not resurrect after the
     * socket is released: the writer fiber reads the CURRENT socket, so a release (close() nulls
     * the property first) between queueing and flushing drops the answer silently and clears its
     * slot - the answer path on a closing transport stays silent.
     */
    public function testQueuedControlAnswerIsDroppedSilentlyOnceSocketReleased(): void
    {
        $socket = new ScriptedChunkSocket([]);
        $transport = new WebSocketTransport(new NatsOptions());
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $socket);

        // Queue a pong answer (the spawned writer fiber cannot start until this fiber yields),
        // then release the socket the way close() does before the writer ever runs.
        (new \ReflectionMethod(WebSocketTransport::class, 'answerControlFrame'))
            ->invoke($transport, WebSocketFrameCodec::OP_PONG, 'late');
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, null);

        // Two event-loop ticks let the writer fiber drain the slot against the released socket.
        delay(0);
        delay(0);

        self::assertSame(0, $socket->writeAttempts(), 'no write may be attempted once the socket is released');
        self::assertNull(
            (new \ReflectionProperty(WebSocketTransport::class, 'pendingPongPayload'))->getValue($transport),
            'the dropped answer must not linger in its slot',
        );
    }

    /**
     * An unsolicited server PONG (RFC 6455 5.5.3 allows them as unidirectional heartbeats) is
     * consumed silently: no answer frame goes out, and the data frame sharing the read is delivered
     * unharmed.
     */
    public function testReadLineIgnoresUnsolicitedPongWithoutAnswering(): void
    {
        $pong = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_PONG, 'hb', false);
        $data = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'DATA', false);
        $socket = new ScriptedChunkSocket([$pong . $data]);

        $transport = new WebSocketTransport(new NatsOptions());
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $socket);

        self::assertSame('DATA', $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await());
        self::assertSame('', $socket->writtenBytes(), 'a pong is never answered (only pings and closes are)');
    }

    /**
     * A corrupt permessage-deflate stream on a FIN frame is a terminal condition deferred like every
     * other (#115): the valid data frame decoded from the SAME read is returned first, and the
     * inflate failure surfaces as a typed ProtocolException on the following readLine() - pre-#115
     * semantics would have discarded the already-consumed data bytes.
     */
    #[\PHPUnit\Framework\Attributes\WithoutErrorHandler]
    public function testReadLineDeliversSameReadDataBeforeCorruptCompressedFrameFailure(): void
    {
        $payload = "MSG orders 1 5\r\nhello\r\n";
        $plain = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $payload, false);
        // RSV1 frame whose payload is definitely not a raw DEFLATE stream.
        $corrupt = WebSocketFrameCodec::encode(
            WebSocketFrameCodec::OP_BINARY,
            'this is not compressed data at all !!!!!!',
            false,
            null,
            true,
        );

        $transport = $this->transportReadingChunks([$plain . $corrupt]);
        (new \ReflectionProperty(WebSocketTransport::class, 'compressionActive'))->setValue($transport, true);

        self::assertSame($payload, $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await());

        try {
            $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await();
            self::fail('expected the deferred inflate failure on the following readLine');
        } catch (ProtocolException $e) {
            self::assertStringContainsString('inflate compressed WebSocket frame', $e->getMessage());
        }
    }

    /**
     * RFC 7692 fragments the COMPRESSED byte stream: a permessage-deflate message split across a
     * first fragment (RSV1 set, FIN=0) and a final continuation must reassemble the compressed
     * bytes first and inflate once, returning the original payload byte-identically.
     */
    public function testReadLineInflatesCompressedFragmentedMessage(): void
    {
        $payload = str_repeat('NATS-over-websocket payload ', 64);
        $compressed = WebSocketFrameCodec::deflate($payload);
        $half = intdiv(strlen($compressed), 2);

        // First fragment: RSV1 marks the message compressed; clear FIN to open fragmentation.
        $first = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, substr($compressed, 0, $half), false, null, true);
        $first[0] = chr(ord($first[0]) & 0x7F); // FIN=0, RSV1 (0x40) kept
        $final = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_CONTINUATION, substr($compressed, $half), false);

        $transport = $this->transportReadingChunks([$first . $final]);
        (new \ReflectionProperty(WebSocketTransport::class, 'compressionActive'))->setValue($transport, true);

        self::assertSame($payload, $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await());
    }

    /**
     * A corrupt compressed FRAGMENTED message fails on its final continuation, and the failure is
     * deferred (#115): the valid data frame from the same read is returned first, the fragment
     * state is torn down, and the typed inflate ProtocolException surfaces on the next readLine().
     */
    #[\PHPUnit\Framework\Attributes\WithoutErrorHandler]
    public function testReadLineDeliversSameReadDataBeforeCorruptCompressedFragmentFailure(): void
    {
        $payload = "MSG orders 1 5\r\nhello\r\n";
        $plain = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $payload, false);

        $garbage = 'this is not compressed data at all !!!!!!';
        $first = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, substr($garbage, 0, 20), false, null, true);
        $first[0] = chr(ord($first[0]) & 0x7F); // FIN=0, RSV1 kept
        $final = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_CONTINUATION, substr($garbage, 20), false);

        $transport = $this->transportReadingChunks([$plain . $first . $final]);
        (new \ReflectionProperty(WebSocketTransport::class, 'compressionActive'))->setValue($transport, true);

        self::assertSame($payload, $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await());

        try {
            $transport->readLine(new \Amp\TimeoutCancellation(3.0))->await();
            self::fail('expected the deferred inflate failure on the following readLine');
        } catch (ProtocolException $e) {
            self::assertStringContainsString('inflate compressed WebSocket frame', $e->getMessage());
        }
    }

    /**
     * White-box pin for consumeSpanningFrame()'s two rescue paths (established #164 spill-machinery
     * style, like testDrainSizesLarge64BitFrameForSpillButLeaves16BitFrameOnBuffer): (a) a spilling
     * frame whose header bytes are split between the working buffer and the queued chunks is topped
     * up chunk-by-chunk until the header parses; (b) queued chunks reaching BEYOND the consumed
     * frame are folded back into the working buffer, not dropped. The public read loop drains after
     * every socket read, so today the sized header is always fully buffered and at most the final
     * chunk overshoots - these paths guard that invariant against read-batching changes, and the pin
     * is byte-exact delivery of BOTH the spanning frame and the frame riding in the surplus chunks.
     */
    public function testConsumeSpanningFrameTopsUpSplitHeaderAndFoldsSurplusChunks(): void
    {
        $payload = pack('N*', ...range(0, 17499)); // 70000 distinct bytes -> 64-bit length form
        $frame = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $payload, false);
        self::assertSame(127, ord($frame[1]) & 0x7F);
        $trailing = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'TAIL', false);

        $transport = new WebSocketTransport(new NatsOptions());
        // 5 of the 10 header bytes in the buffer (parseFrameHeader returns null on 5), the rest of
        // the frame plus the trailing frame's head in chunk 1, the trailing tail in chunk 2 - so
        // the header top-up AND the beyond-the-frame fold both have to engage.
        $chunk1 = substr($frame, 5) . substr($trailing, 0, 3);
        $chunk2 = substr($trailing, 3);
        (new \ReflectionProperty(WebSocketTransport::class, 'readBuffer'))->setValue($transport, substr($frame, 0, 5));
        (new \ReflectionProperty(WebSocketTransport::class, 'readChunks'))->setValue($transport, [$chunk1, $chunk2]);
        (new \ReflectionProperty(WebSocketTransport::class, 'readChunksLength'))->setValue($transport, strlen($chunk1) + strlen($chunk2));
        (new \ReflectionProperty(WebSocketTransport::class, 'readFrameRequired'))->setValue($transport, strlen($frame));

        $out = (new \ReflectionMethod(WebSocketTransport::class, 'drainDataFrames'))->invoke($transport);

        // Both messages delivered byte-identically: the spanning payload, then the folded trailer.
        self::assertSame($payload . 'TAIL', $out);

        // The spill state is fully retired - nothing pending, nothing leaked.
        self::assertNull((new \ReflectionProperty(WebSocketTransport::class, 'readFrameRequired'))->getValue($transport));
        self::assertSame('', (new \ReflectionProperty(WebSocketTransport::class, 'readBuffer'))->getValue($transport));
        self::assertSame([], (new \ReflectionProperty(WebSocketTransport::class, 'readChunks'))->getValue($transport));
        self::assertSame(0, (new \ReflectionProperty(WebSocketTransport::class, 'readChunksLength'))->getValue($transport));
    }

    /**
     * White-box pin for the spill path's OWN masked-frame guard (RFC 6455 5.1): a masked frame
     * consumed by consumeSpanningFrame() must fail the connection as a ProtocolException via the
     * deferral seam - never be silently unmasked and delivered. The batch decoder already rejects a
     * masked head the moment its header is buffered, so this state is constructed directly (the
     * file's established white-box style) to pin the spill path's independent defense in depth.
     */
    public function testDrainRejectsMaskedSpilledFrameAsProtocolViolation(): void
    {
        $payload = str_repeat('Z', 70000);
        $frame = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, $payload, true, "\xA5\x01\xFE\x42");
        self::assertSame(0x80, ord($frame[1]) & 0x80); // mask bit set -> 14-byte header

        $transport = new WebSocketTransport(new NatsOptions());
        $tail = substr($frame, 14);
        (new \ReflectionProperty(WebSocketTransport::class, 'readBuffer'))->setValue($transport, substr($frame, 0, 14));
        (new \ReflectionProperty(WebSocketTransport::class, 'readChunks'))->setValue($transport, [$tail]);
        (new \ReflectionProperty(WebSocketTransport::class, 'readChunksLength'))->setValue($transport, strlen($tail));
        (new \ReflectionProperty(WebSocketTransport::class, 'readFrameRequired'))->setValue($transport, strlen($frame));

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('masked (RFC 6455 5.1 violation)');
        (new \ReflectionMethod(WebSocketTransport::class, 'drainDataFrames'))->invoke($transport);
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
        // The full message is the operator's only signal WHY the 101 was not a WebSocket upgrade.
        self::assertSame(
            'WebSocket handshake failed: the 101 response is missing the required '
            . '"Upgrade: websocket" / "Connection: Upgrade" headers (RFC 6455 4.1)',
            $error->getMessage(),
        );
    }

    /**
     * RFC 6455 4.1 requires BOTH headers: a 101 carrying only "Connection: Upgrade" (no Upgrade
     * header) and one carrying only "Upgrade: websocket" (no Connection header) must EACH fail -
     * either flag alone is insufficient, and neither flag may default to accepted.
     */
    public function testHandshakeRejectsWhenEitherUpgradeOrConnectionHeaderIsMissingAlone(): void
    {
        // Upgrade header missing, Connection present: $upgradeOk must default to (and stay) false.
        [$error] = $this->handshakeAgainst(static fn (string $accept): string => "HTTP/1.1 101 Switching Protocols\r\n"
            . "Connection: Upgrade\r\n"
            . "Sec-WebSocket-Accept: {$accept}\r\n\r\n");

        self::assertInstanceOf(ConnectionException::class, $error, 'a 101 without Upgrade: websocket must fail');
        self::assertStringContainsString('RFC 6455 4.1', $error->getMessage());

        // Connection header missing, Upgrade present: $connectionOk must default to (and stay) false.
        [$error] = $this->handshakeAgainst(static fn (string $accept): string => "HTTP/1.1 101 Switching Protocols\r\n"
            . "Upgrade: websocket\r\n"
            . "Sec-WebSocket-Accept: {$accept}\r\n\r\n");

        self::assertInstanceOf(ConnectionException::class, $error, 'a 101 without Connection: Upgrade must fail');
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
        // Full-message pin: the named extension and the RFC citation are the operator's diagnostics.
        self::assertSame(
            'WebSocket handshake failed: the server negotiated extension "permessage-deflate"'
            . ' that this client never offered (RFC 6455 4.1)',
            $error->getMessage(),
        );
    }

    /**
     * RFC 7692: this codec inflates per message (no context takeover), so a server negotiating
     * unknown parameters beyond the acceptable set would garble decoding in ways the client cannot
     * detect up front - the handshake must reject them. A conformant response negotiates cleanly.
     */
    public function testHandshakeValidatesCompressionParameters(): void
    {
        [$error] = $this->handshakeAgainst(static fn (string $accept): string => "HTTP/1.1 101 Switching Protocols\r\n"
            . "Upgrade: websocket\r\nConnection: Upgrade\r\n"
            . "Sec-WebSocket-Accept: {$accept}\r\n"
            . "Sec-WebSocket-Extensions: permessage-deflate; mystery_param=1\r\n\r\n", compression: true);

        self::assertInstanceOf(ConnectionException::class, $error);
        // Full-message pin: the offending parameter AND the accepted set are the operator's signal.
        self::assertSame(
            'WebSocket handshake failed: unacceptable permessage-deflate parameter "mystery_param=1"'
            . ' (accepted: client_no_context_takeover, server_no_context_takeover, server_max_window_bits=8..15)',
            $error->getMessage(),
        );

        // RFC 7692 7.1.1.1: server_no_context_takeover was offered, so the server must ECHO it or
        // decline - accepting without it means the server may retain deflate context across
        // messages, which this per-message-inflate codec cannot decode.
        [$error] = $this->handshakeAgainst(static fn (string $accept): string => "HTTP/1.1 101 Switching Protocols\r\n"
            . "Upgrade: websocket\r\nConnection: Upgrade\r\n"
            . "Sec-WebSocket-Accept: {$accept}\r\n"
            . "Sec-WebSocket-Extensions: permessage-deflate\r\n\r\n", compression: true);

        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertSame(
            'WebSocket handshake failed: the server accepted permessage-deflate without echoing '
            . 'server_no_context_takeover (RFC 7692 7.1.1.1) - it may use context takeover, which '
            . 'this client cannot decode',
            $error->getMessage(),
        );

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
     * RFC 7692 7.1.2.1: the server may volunteer server_max_window_bits - even though it was not
     * offered - to limit its OWN LZ77 window; the raw 15-bit inflater decodes any smaller-window
     * stream, so 8..15 must be ACCEPTED, not fail the handshake. The quoted-string spelling ="12"
     * is a legal equivalent encoding per RFC 6455 9.1's extension-param ABNF (token |
     * quoted-string) and must be accepted too. Pre-fix the parameter loop rejected everything but
     * the two no_context_takeover tokens.
     */
    public function testHandshakeAcceptsServerMaxWindowBitsWithinRange(): void
    {
        // 8 and 15 are the INCLUSIVE range ends of RFC 7692's window-bits and must be accepted in
        // both spellings; the upper-cased parameter name pins the case-insensitive (/i) matching -
        // extension parameter names are tokens, and tokens compare case-insensitively.
        foreach ([
            'server_max_window_bits=12',
            'server_max_window_bits="12"',
            'server_max_window_bits=8',
            'server_max_window_bits=15',
            'server_max_window_bits="8"',
            'server_max_window_bits="15"',
            'SERVER_MAX_WINDOW_BITS=12',
        ] as $param) {
            [$error, $transport] = $this->handshakeAgainst(static fn (string $accept): string => "HTTP/1.1 101 Switching Protocols\r\n"
                . "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                . "Sec-WebSocket-Accept: {$accept}\r\n"
                . "Sec-WebSocket-Extensions: permessage-deflate; server_no_context_takeover; client_no_context_takeover; {$param}\r\n\r\n", compression: true);

            self::assertNull($error, "{$param} is RFC-legal and decodable - the handshake must succeed");
            self::assertTrue(
                (new \ReflectionProperty(WebSocketTransport::class, 'compressionActive'))->getValue($transport),
                'compression must be active after the negotiation',
            );
            $transport->close()->await();
        }
    }

    /**
     * Parameter order must not matter: an accepted server_max_window_bits parameter CONTINUES the
     * parameter loop, so a server_no_context_takeover echoed AFTER it is still seen and the
     * negotiation completes. A loop that stopped at the accepted parameter would miss the echo and
     * wrongly fail the handshake with the RFC 7692 7.1.1.1 error.
     */
    public function testHandshakeAcceptsWindowBitsListedBeforeContextTakeoverEcho(): void
    {
        [$error, $transport] = $this->handshakeAgainst(static fn (string $accept): string => "HTTP/1.1 101 Switching Protocols\r\n"
            . "Upgrade: websocket\r\nConnection: Upgrade\r\n"
            . "Sec-WebSocket-Accept: {$accept}\r\n"
            . "Sec-WebSocket-Extensions: permessage-deflate; server_max_window_bits=12; server_no_context_takeover\r\n\r\n", compression: true);

        self::assertNull($error, 'parameters after an accepted server_max_window_bits must still be processed');
        self::assertTrue(
            (new \ReflectionProperty(WebSocketTransport::class, 'compressionActive'))->getValue($transport),
            'compression must be active after the negotiation',
        );
        $transport->close()->await();
    }

    /**
     * The server_max_window_bits acceptance stays bounded to the exact RFC 7692 section 7 grammar:
     * the value is MANDATORY for this parameter (only client_max_window_bits has an optional-value
     * form), decimal without leading zeroes, 8..15 - so the bare value-less spelling, out-of-range
     * values, and the leading-zero spelling all fail the handshake as invalid response parameters
     * (RFC 7692 section 5). client_max_window_bits stays rejected outright - it was never offered
     * (RFC 7692 7.1.2.2 forbids the server volunteering it) and would change what this client must
     * deflate.
     */
    public function testHandshakeRejectsOutOfRangeWindowBitsAndClientMaxWindowBits(): void
    {
        // 'xserver_max_window_bits=12' pins the ^ anchor (a suffix match is not the parameter);
        // 'server_max_window_bits=12x' pins the $ anchor (trailing junk is not a valid value).
        foreach ([
            'server_max_window_bits',
            'server_max_window_bits=7',
            'server_max_window_bits=16',
            'server_max_window_bits=08',
            'client_max_window_bits=12',
            'xserver_max_window_bits=12',
            'server_max_window_bits=12x',
        ] as $param) {
            [$error] = $this->handshakeAgainst(static fn (string $accept): string => "HTTP/1.1 101 Switching Protocols\r\n"
                . "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                . "Sec-WebSocket-Accept: {$accept}\r\n"
                . "Sec-WebSocket-Extensions: permessage-deflate; server_no_context_takeover; {$param}\r\n\r\n", compression: true);

            self::assertInstanceOf(ConnectionException::class, $error, "{$param} must fail the handshake");
            self::assertStringContainsString('unacceptable permessage-deflate parameter', $error->getMessage());
            self::assertStringContainsString($param, $error->getMessage());
        }
    }

    /**
     * A non-101 status line (e.g. an auth proxy answering 403) must fail the handshake naming the
     * server's status line, so operators see WHY the upgrade was refused instead of a frame-decode
     * error on an HTML error page.
     */
    public function testHandshakeRejectsNon101StatusLine(): void
    {
        [$error] = $this->handshakeAgainst(static fn (string $accept): string => "HTTP/1.1 403 Forbidden\r\n"
            . "Content-Length: 0\r\n\r\n");

        self::assertInstanceOf(ConnectionException::class, $error);
        // Full-message pin: the verbatim status line is the operator's only clue WHY the upgrade failed.
        self::assertSame('WebSocket upgrade rejected by server: HTTP/1.1 403 Forbidden', $error->getMessage());
    }

    /**
     * The 101 status check is anchored at the START of the status line: a response whose first line
     * merely CONTAINS "HTTP/1.1 101" further in (e.g. a mangled proxy banner) is not a valid HTTP
     * status line and must be rejected, not treated as a completed upgrade.
     */
    public function testHandshakeRejectsStatusLineWithLeadingGarbage(): void
    {
        [$error] = $this->handshakeAgainst(static fn (string $accept): string => "banner HTTP/1.1 101 Switching Protocols\r\n"
            . "Upgrade: websocket\r\nConnection: Upgrade\r\n"
            . "Sec-WebSocket-Accept: {$accept}\r\n\r\n");

        self::assertInstanceOf(ConnectionException::class, $error, 'a status line not STARTING with HTTP/1.x 101 must fail');
        self::assertSame(
            'WebSocket upgrade rejected by server: banner HTTP/1.1 101 Switching Protocols',
            $error->getMessage(),
        );
    }

    /**
     * A handshake response whose headers never terminate must be bounded: past 16 KiB without the
     * header terminator the client fails the connection instead of buffering a hostile/broken
     * server's stream indefinitely.
     */
    public function testHandshakeRejectsOversizedResponseHeaders(): void
    {
        // 20 000 header-ish bytes with no "\r\n\r\n" terminator anywhere.
        [$error] = $this->handshakeAgainst(static fn (string $accept): string => str_repeat('X', 20000));

        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertStringContainsString('exceeded the maximum header size', $error->getMessage());
    }

    /**
     * The 16 KiB header bound is EXCLUSIVE: a terminator-less response of exactly 16384 bytes does
     * not trip the size guard - the failure that surfaces is the peer's close (EOF), not the size
     * bound. One byte of slack must not reclassify a legal-sized response as oversized.
     */
    public function testHandshakeHeaderSizeBoundIsExclusiveAtExactly16384Bytes(): void
    {
        [$error] = $this->handshakeAgainst(static fn (string $accept): string => str_repeat('X', 16384));

        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertSame(
            'WebSocket handshake failed: connection closed before response',
            $error->getMessage(),
            'exactly 16384 headers bytes must NOT trip the (exclusive) size bound',
        );
    }

    /**
     * Header parsing tolerates real-world whitespace sloppiness: a name padded before the colon
     * ("Upgrade : websocket") must still match via trimming, and a value glued to the colon
     * ("Connection:Upgrade") must be read from the byte right after the colon - the handshake
     * completes on such a response.
     */
    public function testHandshakeParsesPaddedHeaderNamesAndUnpaddedValues(): void
    {
        [$error, $transport] = $this->handshakeAgainst(static fn (string $accept): string => "HTTP/1.1 101 Switching Protocols\r\n"
            . "Upgrade : websocket\r\n"
            . "Connection:Upgrade\r\n"
            . "Sec-WebSocket-Accept: {$accept}\r\n\r\n");

        self::assertNull($error, 'padded names and unpadded values are valid header spellings');
        $transport->close()->await();
    }

    /**
     * Bytes after the header terminator (e.g. the NATS INFO frame the server pipelines behind the
     * 101) belong to the WEBSOCKET STREAM: they are retained verbatim as the initial read buffer and
     * are never parsed as handshake headers - a header-shaped surplus (here an extension header that
     * would FAIL the handshake if it were parsed) must not affect the negotiation. Driven through a
     * scripted socket that computes the accept key from the emitted request, so the whole response
     * (terminator and surplus included) deterministically arrives in one read.
     */
    public function testHandshakeRetainsSurplusBytesVerbatimWithoutParsingThemAsHeaders(): void
    {
        $surplus = "Sec-WebSocket-Extensions: permessage-deflate\r\nX-After: 1\r\n\r\n";
        $socket = new ScriptedChunkSocket([], responseFromWritten: static function (string $request) use ($surplus): string {
            preg_match('/Sec-WebSocket-Key:\s*(\S+)/i', $request, $m);

            return "HTTP/1.1 101 Switching Protocols\r\n"
                . "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                . 'Sec-WebSocket-Accept: ' . WebSocketFrameCodec::acceptKey($m[1] ?? '') . "\r\n\r\n"
                . $surplus;
        });

        // Compression NOT offered: if the surplus "Sec-WebSocket-Extensions" line leaked into header
        // parsing, the handshake would fail with the unsolicited-extension error.
        $transport = new WebSocketTransport(new NatsOptions());
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($transport, $socket);
        (new \ReflectionMethod(WebSocketTransport::class, 'performHandshake'))->invoke($transport, 'h', 80, '/');

        // The surplus is retained BYTE-IDENTICALLY (nothing dropped, no terminator bytes leaked in).
        self::assertSame(
            $surplus,
            (new \ReflectionProperty(WebSocketTransport::class, 'readBuffer'))->getValue($transport),
        );
        self::assertFalse(
            (new \ReflectionProperty(WebSocketTransport::class, 'compressionActive'))->getValue($transport),
            'the surplus extension header must not have been parsed as a handshake header',
        );
    }

    /**
     * write() deflates the payload EXACTLY when permessage-deflate was negotiated: with compression
     * active the emitted binary frame carries RSV1 and a deflated payload that inflates back to the
     * original bytes; without negotiation the frame carries the raw bytes and no RSV1. A swapped
     * condition would deflate for the peer that cannot inflate and vice versa (#61).
     */
    public function testWriteDeflatesPayloadOnlyWhenCompressionActive(): void
    {
        $bytes = "PUB orders 5\r\nhello\r\n";

        // Compression negotiated: deflated RSV1 frame that round-trips through inflate().
        $compressedSocket = new ScriptedChunkSocket([]);
        $compressed = new WebSocketTransport(new NatsOptions());
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($compressed, $compressedSocket);
        (new \ReflectionProperty(WebSocketTransport::class, 'compressionActive'))->setValue($compressed, true);
        $compressed->write($bytes)->await();

        $buffer = $compressedSocket->writtenBytes();
        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);
        self::assertCount(1, $frames);
        self::assertSame(WebSocketFrameCodec::OP_BINARY, $frames[0]['opcode']);
        self::assertTrue($frames[0]['rsv1'], 'a compressed frame must be RSV1-marked');
        self::assertNotSame($bytes, $frames[0]['payload'], 'the payload must actually be deflated');
        self::assertSame($bytes, WebSocketFrameCodec::inflate($frames[0]['payload']));

        // No negotiation: the raw bytes go out unmarked and untouched.
        $plainSocket = new ScriptedChunkSocket([]);
        $plain = new WebSocketTransport(new NatsOptions());
        (new \ReflectionProperty(WebSocketTransport::class, 'socket'))->setValue($plain, $plainSocket);
        $plain->write($bytes)->await();

        $buffer = $plainSocket->writtenBytes();
        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);
        self::assertCount(1, $frames);
        self::assertFalse($frames[0]['rsv1']);
        self::assertSame($bytes, $frames[0]['payload'], 'without negotiation the payload must pass through verbatim');
    }

    /**
     * A malformed response header line without a colon is skipped, not fatal: real-world proxies
     * inject such lines, and RFC-required validation must still run on the well-formed headers
     * around it - the handshake completes when those are conformant.
     */
    public function testHandshakeSkipsMalformedHeaderLineWithoutColon(): void
    {
        [$error, $transport] = $this->handshakeAgainst(static fn (string $accept): string => "HTTP/1.1 101 Switching Protocols\r\n"
            . "Upgrade: websocket\r\nConnection: Upgrade\r\n"
            . "ThisLineHasNoColonAndMustBeSkipped\r\n"
            . "Sec-WebSocket-Accept: {$accept}\r\n\r\n");

        self::assertNull($error, 'a colon-less junk header line must not fail an otherwise valid handshake');
        $transport->close()->await();
    }

    /**
     * RFC 6455 4.1 step 4: a Sec-WebSocket-Accept that does not match the SHA-1 of the client key +
     * GUID proves the peer never processed THIS handshake (a replaying/broken intermediary) - the
     * client must fail the connection.
     */
    public function testHandshakeRejectsInvalidAcceptKey(): void
    {
        [$error] = $this->handshakeAgainst(static fn (string $accept): string => "HTTP/1.1 101 Switching Protocols\r\n"
            . "Upgrade: websocket\r\nConnection: Upgrade\r\n"
            . "Sec-WebSocket-Accept: {$accept}CORRUPT\r\n\r\n");

        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertStringContainsString('invalid Sec-WebSocket-Accept', $error->getMessage());
    }

    /**
     * With compression offered, the server may still only answer with permessage-deflate: a
     * different extension name, or a comma-separated multi-extension list, is an unsupported
     * response this codec cannot honor - the handshake must fail naming the response.
     */
    public function testHandshakeRejectsUnsupportedExtensionResponses(): void
    {
        // A different extension than the one offered.
        [$error] = $this->handshakeAgainst(static fn (string $accept): string => "HTTP/1.1 101 Switching Protocols\r\n"
            . "Upgrade: websocket\r\nConnection: Upgrade\r\n"
            . "Sec-WebSocket-Accept: {$accept}\r\n"
            . "Sec-WebSocket-Extensions: x-webkit-deflate-frame\r\n\r\n", compression: true);

        self::assertInstanceOf(ConnectionException::class, $error);
        // Full-message pin: the quoted server response is what the operator needs to see.
        self::assertSame(
            'WebSocket handshake failed: unsupported extension response "x-webkit-deflate-frame"',
            $error->getMessage(),
        );

        // A comma-separated list (a second extension) is equally unsupported.
        [$error] = $this->handshakeAgainst(static fn (string $accept): string => "HTTP/1.1 101 Switching Protocols\r\n"
            . "Upgrade: websocket\r\nConnection: Upgrade\r\n"
            . "Sec-WebSocket-Accept: {$accept}\r\n"
            . "Sec-WebSocket-Extensions: permessage-deflate; server_no_context_takeover, x-foo\r\n\r\n", compression: true);

        self::assertInstanceOf(ConnectionException::class, $error);
        self::assertSame(
            'WebSocket handshake failed: unsupported extension response "permessage-deflate; server_no_context_takeover, x-foo"',
            $error->getMessage(),
        );
    }

    /**
     * Empty permessage-deflate parameters (";;" from sloppy server serialization) are skipped, not
     * treated as unacceptable: the negotiation still validates the real parameters around them and
     * completes with compression active.
     */
    public function testHandshakeToleratesEmptyCompressionParameter(): void
    {
        [$error, $transport] = $this->handshakeAgainst(static fn (string $accept): string => "HTTP/1.1 101 Switching Protocols\r\n"
            . "Upgrade: websocket\r\nConnection: Upgrade\r\n"
            . "Sec-WebSocket-Accept: {$accept}\r\n"
            . "Sec-WebSocket-Extensions: permessage-deflate; ; client_no_context_takeover; server_no_context_takeover\r\n\r\n", compression: true);

        self::assertNull($error, 'an empty parameter between semicolons must not fail the negotiation');
        self::assertTrue(
            (new \ReflectionProperty(WebSocketTransport::class, 'compressionActive'))->getValue($transport),
            'compression must be active after the negotiation',
        );
        $transport->close()->await();
    }

    /**
     * wss:// establishes TLS during connect() (there is no post-INFO upgrade for WebSocket): after a
     * real TLS handshake against a self-signed local server followed by the scripted HTTP upgrade,
     * tlsActive() must report true. Complements testConnectCallsSetupTlsOnWssAndThrowsWhenServerIsPlainTcp,
     * which pins the failure arm.
     */
    public function testConnectEstablishesTlsForWssAndReportsTlsActive(): void
    {
        if (!\extension_loaded('openssl')) {
            self::markTestSkipped('ext-openssl is required to generate the self-signed test certificate');
        }

        $certFile = $this->selfSignedCertificateFile();
        $server = listen('tcp://127.0.0.1:0', (new BindContext())->withTlsContext(
            (new ServerTlsContext())->withDefaultCertificate(new Certificate($certFile)),
        ));
        $address = (string) $server->getAddress();

        async(static function () use ($server): void {
            $socket = $server->accept();
            if ($socket === null) {
                return;
            }
            $socket->setupTls();

            $request = '';
            while (!str_contains($request, "\r\n\r\n")) {
                $chunk = $socket->read();
                if ($chunk === null) {
                    return;
                }
                $request .= $chunk;
            }

            preg_match('/Sec-WebSocket-Key:\s*(\S+)/i', $request, $m);
            $socket->write("HTTP/1.1 101 Switching Protocols\r\n"
                . "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                . 'Sec-WebSocket-Accept: ' . WebSocketFrameCodec::acceptKey($m[1] ?? '') . "\r\n\r\n");
        });

        $transport = new WebSocketTransport(new NatsOptions(tlsVerifyPeer: false));
        try {
            $transport->connect('wss://' . $address . '/', 5000)->await();

            self::assertTrue($transport->tlsActive(), 'tlsActive() must report the completed wss:// TLS handshake');
        } finally {
            $transport->close()->await();
            $server->close();
            @unlink($certFile);
        }
    }

    /**
     * Writes a throwaway self-signed certificate + key PEM for the wss:// TLS test server.
     */
    private function selfSignedCertificateFile(): string
    {
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

        $path = tempnam(sys_get_temp_dir(), 'nats-ws-tls-');
        if ($path === false) {
            throw new \RuntimeException('tempnam() failed');
        }
        file_put_contents($path, $certPem . $keyPem);

        return $path;
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
