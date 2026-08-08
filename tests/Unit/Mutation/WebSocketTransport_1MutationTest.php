<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\Socket\Socket;
use Amp\Socket\TlsException;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Transport\WebSocketFrameCodec;
use IDCT\NATS\Transport\WebSocketTransport;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\delay;
use function Amp\Socket\connect;
use function Amp\Socket\listen;

/**
 * Mutation-killing tests for {@see WebSocketTransport} (chunk WebSocketTransport_1).
 *
 * Each test pins the exact observable behavior a surviving mutant would change. State-only mutants are
 * driven through reflection (mirroring the existing WebSocketTransportTest), socket-dependent mutants
 * through loopback TCP sockets, and handshake-path mutants by reading the emitted upgrade request.
 */
final class WebSocketTransport_1MutationTest extends TestCase
{
    private static function prop(string $name): \ReflectionProperty
    {
        return new \ReflectionProperty(WebSocketTransport::class, $name);
    }

    private static function drain(WebSocketTransport $t): string
    {
        return (string) (new \ReflectionMethod(WebSocketTransport::class, 'drainDataFrames'))->invoke($t);
    }

    /** Unmasked server->client frame (supports payloads beyond 125 bytes for boundary tests). */
    private static function serverFrame(int $opcode, string $payload, bool $fin): string
    {
        $len = strlen($payload);
        $firstByte = ($fin ? 0x80 : 0x00) | ($opcode & 0x0F);

        if ($len <= 125) {
            return pack('CC', $firstByte, $len) . $payload;
        }
        if ($len <= 0xFFFF) {
            return pack('CC', $firstByte, 126) . pack('n', $len) . $payload;
        }

        return pack('CC', $firstByte, 127) . pack('J', $len) . $payload;
    }

    // -------------------------------------------------------------------------------------------
    // connect() - timeout clamp and per-attempt state resets (lines 67, 71, 72)
    // -------------------------------------------------------------------------------------------

    /**
     * connect() clamps the stored connect timeout to a floor of 1ms via max(1, $timeoutMs).
     * Line 67 runs before the socket attempt, so the value is observable even when connect fails.
     */
    public function testConnectClampsTimeoutToOneMillisecondFloor(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());

        try {
            // timeout=0 is where max(1,0)=1 differs from max(0,0)=0 and max(2,0)=2.
            $transport->connect('ws://127.0.0.1:1/', 0)->await();
            self::fail('Expected the refused connection to throw');
        } catch (\Throwable) {
            // The socket attempt is expected to fail; we only assert the clamped value below.
        }

        // kills DecrementInteger (max(0,..)) & IncrementInteger (max(2,..)) @ line 67
        self::assertSame(1, self::prop('lastConnectTimeoutMs')->getValue($transport));
    }

    /**
     * connect() resets the fragment-reassembly and compression flags at the very start of an attempt,
     * before any DSN failure. Driving an invalid-but-parseable DSN throws at line 76 after the resets.
     */
    public function testConnectResetsFragmentingAndCompressionState(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('fragmenting')->setValue($transport, true);
        self::prop('compressionActive')->setValue($transport, true);

        try {
            $transport->connect('ws:path-only', 1000)->await();
            self::fail('Expected ConnectionException for a host-less DSN');
        } catch (ConnectionException) {
            // The resets on lines 71/72 already ran before the line-76 throw.
        }

        // kills FalseValue (fragmenting=true) @ line 71
        self::assertFalse(self::prop('fragmenting')->getValue($transport));
        // kills FalseValue (compressionActive=true) @ line 72
        self::assertFalse(self::prop('compressionActive')->getValue($transport));
    }

    // -------------------------------------------------------------------------------------------
    // connect() - DSN validation and error message (lines 75, 76)
    // -------------------------------------------------------------------------------------------

    /**
     * A DSN that parses successfully but carries no host must be rejected. With `||` the second clause
     * (!isset host) fires; with `&&` the check would pass and the connect would proceed host-less.
     */
    public function testConnectRejectsParseableDsnWithoutHost(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());

        // 'ws:path-only' => parse_url succeeds ($parts !== false) but has no 'host' key, so only the
        // second predicate is true: the case where || and && diverge.
        $this->expectException(ConnectionException::class);
        // kills LogicalOr (|| -> &&) @ line 75
        $transport->connect('ws:path-only', 1000)->await();
    }

    /**
     * The invalid-DSN exception message is exactly 'Invalid WebSocket DSN: ' followed by the DSN. This
     * pins both the literal text and the concatenation order/operands.
     */
    public function testConnectInvalidDsnMessageIncludesDsnInOrder(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());

        try {
            $transport->connect('ws:path-only', 1000)->await();
            self::fail('Expected ConnectionException');
        } catch (ConnectionException $e) {
            // kills Concat (swapped order) & ConcatOperandRemoval (dropped $dsn) @ line 76
            self::assertSame('Invalid WebSocket DSN: ws:path-only', $e->getMessage());
        }
    }

    // -------------------------------------------------------------------------------------------
    // connect() - scheme lower-casing decides TLS (line 79)
    // -------------------------------------------------------------------------------------------

    /**
     * An upper-case `WSS://` scheme must still be treated as secure: strtolower() folds it to 'wss',
     * so connect() negotiates TLS and (against a plain-TCP server) surfaces a TlsException. Without the
     * fold the scheme stays 'WSS' !== 'wss', TLS is skipped and the failure would not be a TlsException.
     */
    public function testConnectTreatsUppercaseWssAsSecure(): void
    {
        $server = listen('tcp://127.0.0.1:0');
        $address = (string) $server->getAddress();
        async(static function () use ($server): void {
            $client = $server->accept();
            $client?->close();
        });

        $transport = new WebSocketTransport(new NatsOptions(tlsVerifyPeer: false));

        try {
            $transport->connect('WSS://' . $address . '/', 2000)->await();
            self::fail('Expected TlsException for uppercase WSS scheme');
        } catch (TlsException) {
            // kills UnwrapStrToLower @ line 79
            self::addToAssertionCount(1);
        } finally {
            $server->close();
        }
    }

    // -------------------------------------------------------------------------------------------
    // connect() - handshake request path building (lines 83, 85)
    // -------------------------------------------------------------------------------------------

    /**
     * The GET request line of the upgrade handshake reflects the DSN path verbatim (a non-root path is
     * preserved, and the query string is appended as '?<query>' in that order).
     *
     * @return string The request line (first CRLF-delimited line) the transport emitted.
     */
    private function captureUpgradeRequestLine(string $pathAndQuery): string
    {
        $server = listen('tcp://127.0.0.1:0');
        $address = (string) $server->getAddress();

        $requestFuture = async(static function () use ($server): string {
            $client = $server->accept();
            if ($client === null) {
                return '';
            }
            $buf = '';
            while (!str_contains($buf, "\r\n\r\n")) {
                $chunk = $client->read();
                if ($chunk === null) {
                    break;
                }
                $buf .= $chunk;
            }
            $client->close();

            return $buf;
        });

        $transport = new WebSocketTransport(new NatsOptions());
        $connectFuture = async(
            static function () use ($transport, $address, $pathAndQuery): void {
                $transport->connect('ws://' . $address . $pathAndQuery, 2000)->await();
            },
        );

        $request = $requestFuture->await();
        try {
            // The server closed without a valid 101, so connect() throws - irrelevant to the path assertion.
            $connectFuture->await();
        } catch (\Throwable) {
        }
        $server->close();

        return (string) strtok($request, "\r\n");
    }

    public function testHandshakePreservesNonRootPath(): void
    {
        // kills Coalesce ('' ?? path) & Ternary (swapped) @ line 83 - a non-root path would collapse to '/'.
        self::assertSame('GET /foo HTTP/1.1', $this->captureUpgradeRequestLine('/foo'));
    }

    public function testHandshakeAppendsQueryAfterPathWithQuestionMark(): void
    {
        // kills Concat (swapped), both ConcatOperandRemoval, and Assignment (=) @ line 85.
        // Real: '/foo' .= '?' . 'q=v' => '/foo?q=v'.
        self::assertSame('GET /foo?q=v HTTP/1.1', $this->captureUpgradeRequestLine('/foo?q=v'));
    }

    // -------------------------------------------------------------------------------------------
    // readLine() - raw read buffer accumulation (line 161)
    // -------------------------------------------------------------------------------------------

    /**
     * Bytes received across multiple reads must be appended (`.=`) so a frame split across two reads is
     * eventually decoded. A replacing assignment would discard the first half and never complete a frame.
     */
    public function testReadLineAccumulatesPartialFrameAcrossReads(): void
    {
        [$transport, $server, $serverSocket, $clientSocket] = $this->connectedTransport();

        try {
            $frame = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, "PING\r\n", false);
            $half = intdiv(strlen($frame), 2);

            $writer = async(static function () use ($serverSocket, $frame, $half): void {
                $serverSocket->write(substr($frame, 0, $half));
                delay(0.02);
                $serverSocket->write(substr($frame, $half));
            });

            // kills Assignment (.= -> =) @ line 161 - without accumulation the first half is discarded and
            // no frame ever completes; the bounded cancellation then surfaces as a clean failure (not a hang).
            self::assertSame("PING\r\n", $transport->readLine(new TimeoutCancellation(3.0))->await());
            $writer->await();
        } finally {
            $clientSocket->close();
            $serverSocket->close();
            $server->close();
        }
    }

    // -------------------------------------------------------------------------------------------
    // close() - close frame + socket teardown (lines 174, 179, 184)
    // -------------------------------------------------------------------------------------------

    /**
     * close() sends a best-effort CLOSE control frame to the peer before tearing down. The early-return
     * guard must only fire when there is no socket; with a live socket the CLOSE frame must be written.
     */
    public function testCloseSendsCloseFrameToPeer(): void
    {
        [$transport, $server, $serverSocket, $clientSocket] = $this->connectedTransport();

        try {
            $transport->close()->await();

            // Bounded read: on the real code the CLOSE frame arrives immediately; a mutant that skips the
            // write (or early-returns) leaves the socket idle, so the deadline turns a hang into a clean
            // empty read and the assertions below fail.
            $buffer = (string) $serverSocket->read(new TimeoutCancellation(2.0));
            $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);

            // kills Identical (=== -> !== early-return) @ line 174 and MethodCallRemoval (write) @ line 179.
            self::assertNotSame([], $frames, 'A CLOSE frame must reach the peer');
            self::assertSame(WebSocketFrameCodec::OP_CLOSE, $frames[0]['opcode']);
        } finally {
            $clientSocket->close();
            $serverSocket->close();
            $server->close();
        }
    }

    /**
     * close() closes the underlying socket. Removing $socket->close() would leave it open.
     */
    public function testCloseClosesUnderlyingSocket(): void
    {
        [$transport, $server, $serverSocket, $clientSocket] = $this->connectedTransport();

        try {
            self::assertFalse($clientSocket->isClosed());
            $transport->close()->await();

            // kills MethodCallRemoval ($socket->close()) @ line 184.
            self::assertTrue($clientSocket->isClosed());
        } finally {
            $serverSocket->close();
            $server->close();
        }
    }

    // -------------------------------------------------------------------------------------------
    // drainDataFrames() - opcode handling, accumulation, ping reply (lines 198, 201, 214, 220, 229)
    // -------------------------------------------------------------------------------------------

    /**
     * An OP_TEXT data frame is decoded as application data (it shares the OP_BINARY arm). Removing the
     * shared OP_TEXT case label would drop the payload.
     */
    public function testDrainDecodesTextFrameAsData(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('readBuffer')->setValue(
            $transport,
            self::serverFrame(WebSocketFrameCodec::OP_TEXT, 'HELLO', true),
        );

        // kills SharedCaseRemoval (OP_TEXT) @ line 198
        self::assertSame('HELLO', self::drain($transport));
    }

    /**
     * A PING is answered via the null-safe `$this->socket?->write(...)`. When no socket is set, the
     * null-safe call is a no-op and draining must complete without error (a hard `->write` would fatal).
     */
    public function testDrainPingWithoutSocketDoesNotError(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());
        // socket stays null
        self::prop('readBuffer')->setValue(
            $transport,
            self::serverFrame(WebSocketFrameCodec::OP_PING, 'p', true),
        );

        // kills NullSafeMethodCall (?-> -> ->) @ line 201 - the hard call would throw on null socket.
        self::assertSame('', self::drain($transport));
    }

    /**
     * Two complete data frames in one buffer are concatenated into the returned payload. A replacing
     * assignment ($out = ...) would return only the last frame.
     */
    public function testDrainConcatenatesMultipleCompleteFrames(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('readBuffer')->setValue(
            $transport,
            self::serverFrame(WebSocketFrameCodec::OP_BINARY, 'AA', true)
                . self::serverFrame(WebSocketFrameCodec::OP_BINARY, 'BB', true),
        );

        // kills Assignment (.= -> =) @ line 214
        self::assertSame('AABB', self::drain($transport));
    }

    /**
     * The fragment bound is enforced on the very first (non-final) fragment too: a single opening frame
     * already over the cap must be rejected. Removing the enforce call would let it pass silently.
     */
    public function testDrainRejectsOversizedFirstFragment(): void
    {
        $transport = new WebSocketTransport(new NatsOptions(), maxMessageBytes: 4);
        self::prop('readBuffer')->setValue(
            $transport,
            self::serverFrame(WebSocketFrameCodec::OP_BINARY, 'AAAAAA', false), // 6 > 4, non-final
        );

        // kills MethodCallRemoval (enforceFragmentBound) @ line 220
        $this->expectException(ProtocolException::class);
        self::drain($transport);
    }

    /**
     * A completed fragmented message is concatenated onto any data already accumulated in the same drain
     * pass. A replacing assignment would drop the earlier complete frame.
     */
    public function testDrainConcatenatesCompletedFragmentOntoPriorData(): void
    {
        $transport = new WebSocketTransport(new NatsOptions(), maxMessageBytes: 1024);
        $buffer = self::serverFrame(WebSocketFrameCodec::OP_BINARY, 'X', true)
            . self::serverFrame(WebSocketFrameCodec::OP_BINARY, 'A', false)
            . self::serverFrame(WebSocketFrameCodec::OP_CONTINUATION, 'B', true);
        self::prop('readBuffer')->setValue($transport, $buffer);

        // kills Assignment (.= -> =) @ line 229
        self::assertSame('XAB', self::drain($transport));
    }

    /**
     * After a fragmented message completes, the reassembly flags are cleared so the next message starts
     * clean (fragmenting=false, fragmentCompressed=false).
     */
    public function testDrainResetsFragmentStateAfterCompletion(): void
    {
        $transport = new WebSocketTransport(new NatsOptions(), maxMessageBytes: 1024);
        $buffer = self::serverFrame(WebSocketFrameCodec::OP_BINARY, 'PI', false)
            . self::serverFrame(WebSocketFrameCodec::OP_CONTINUATION, "NG\r\n", true);
        self::prop('readBuffer')->setValue($transport, $buffer);

        self::assertSame("PING\r\n", self::drain($transport));

        // kills FalseValue (fragmenting=true) @ line 233
        self::assertFalse(self::prop('fragmenting')->getValue($transport));
        // kills FalseValue (fragmentCompressed=true) @ line 234
        self::assertFalse(self::prop('fragmentCompressed')->getValue($transport));
    }

    // -------------------------------------------------------------------------------------------
    // enforceFragmentBound() - boundary + post-throw state reset (lines 250, 256, 257)
    // -------------------------------------------------------------------------------------------

    /**
     * A reassembled message of exactly maxMessageBytes is accepted (the bound is inclusive: `<=`).
     * With a strict `<` the exact-cap message would be wrongly rejected.
     */
    public function testFragmentExactlyAtCapIsAccepted(): void
    {
        $transport = new WebSocketTransport(new NatsOptions(), maxMessageBytes: 8);
        // 4-byte opener + 4-byte final continuation => reassembled length exactly 8 == cap.
        $buffer = self::serverFrame(WebSocketFrameCodec::OP_BINARY, 'AAAA', false)
            . self::serverFrame(WebSocketFrameCodec::OP_CONTINUATION, 'BBBB', true);
        self::prop('readBuffer')->setValue($transport, $buffer);

        // kills LessThanOrEqualTo (<= -> <) @ line 250 - strict '<' would throw at exactly the cap.
        self::assertSame('AAAABBBB', self::drain($transport));
    }

    /**
     * When the bound is exceeded, the reassembly state is reset (fragmenting=false, fragmentCompressed=
     * false) before the ProtocolException is thrown, so a subsequent message is not corrupted.
     */
    public function testEnforceResetsStateBeforeThrowing(): void
    {
        $transport = new WebSocketTransport(new NatsOptions(), maxMessageBytes: 4);
        self::prop('fragmentBuffer')->setValue($transport, 'AAAAAA'); // 6 > 4
        self::prop('fragmenting')->setValue($transport, true);
        self::prop('fragmentCompressed')->setValue($transport, true);

        $enforce = new \ReflectionMethod(WebSocketTransport::class, 'enforceFragmentBound');

        try {
            $enforce->invoke($transport);
            self::fail('Expected ProtocolException for an over-cap fragment buffer');
        } catch (ProtocolException) {
            // kills FalseValue (fragmenting=true) @ line 256
            self::assertFalse(self::prop('fragmenting')->getValue($transport));
            // kills FalseValue (fragmentCompressed=true) @ line 257
            self::assertFalse(self::prop('fragmentCompressed')->getValue($transport));
        }
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    /**
     * Wires a WebSocketTransport to a loopback TCP socket pair (bypassing the HTTP upgrade) so the
     * read/decode/close paths run against a real socket driven from the server side.
     *
     * @return array{0: WebSocketTransport, 1: \Amp\Socket\ServerSocket, 2: Socket, 3: Socket}
     */
    private function connectedTransport(): array
    {
        $server = listen('tcp://127.0.0.1:0');
        $address = (string) $server->getAddress();

        $accept = async(static fn(): ?Socket => $server->accept());
        $clientSocket = connect('tcp://' . $address);
        $serverSocket = $accept->await();
        self::assertInstanceOf(Socket::class, $serverSocket);

        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('socket')->setValue($transport, $clientSocket);

        return [$transport, $server, $serverSocket, $clientSocket];
    }
}
