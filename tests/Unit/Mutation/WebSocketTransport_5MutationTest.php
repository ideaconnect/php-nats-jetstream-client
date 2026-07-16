<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Tests\Support\ScriptedChunkSocket;
use IDCT\NATS\Transport\TransportClosedException;
use IDCT\NATS\Transport\WebSocketFrameCodec;
use IDCT\NATS\Transport\WebSocketTransport;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\delay;
use function Amp\Socket\listen;

/**
 * Mutation-killing tests for {@see WebSocketTransport} (chunk WebSocketTransport_5).
 *
 * Covers the #164 large-frame spill sizing/consume math (consumeSpanningFrame surplus, the spill gate),
 * the RFC 6455 Close echo (#161) width and best-effort null-safety, fragment-bound accounting across
 * continuation frames (#89), the fragmentation-violation reset (#115), and the handshake read loop.
 * State-only mutants are driven through reflection (mirroring WebSocketTransportTest); socket-dependent
 * ones through a {@see ScriptedChunkSocket} or a loopback server.
 */
final class WebSocketTransport_5MutationTest extends TestCase
{
    private static function prop(string $name): \ReflectionProperty
    {
        return new \ReflectionProperty(WebSocketTransport::class, $name);
    }

    private static function drain(WebSocketTransport $t): string
    {
        return (string) (new \ReflectionMethod(WebSocketTransport::class, 'drainDataFrames'))->invoke($t);
    }

    /** Unmasked server->client frame (supports payloads beyond 125 bytes). */
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
    // consumeSpanningFrame() - the $rest surplus excludes the consumed frame bytes (line 303)
    // -------------------------------------------------------------------------------------------

    /**
     * After a spilled 64-bit frame is consumed, the working buffer ($rest) must be exactly the bytes
     * *after* the frame, computed as substr($readBuffer, headerBytes + strlen($pieces[0])). The frame is
     * delivered so its payload ends exactly on a chunk boundary (no overshoot, so line 318 never rewrites
     * $rest) - the only condition under which line 303's value survives to become the next readBuffer.
     *
     * kills UnwrapSubstr @ line 303  ($rest = $this->readBuffer would re-inject the header + already
     * consumed payload bytes back into the buffer instead of the empty remainder).
     */
    public function testConsumedSpilledFrameLeavesNoStaleBufferRemainder(): void
    {
        $payload = str_repeat('Z', 65536); // > 65535 -> 64-bit length -> spill path
        $frame = self::serverFrame(WebSocketFrameCodec::OP_BINARY, $payload, true);
        self::assertSame(65546, strlen($frame));

        // chunk1 = 10-byte header + 20 payload bytes (arms the spill: tail 65516 > 32768).
        // chunk2 = the remaining 65516 payload bytes exactly (payload ends on the chunk boundary).
        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('socket')->setValue(
            $transport,
            new ScriptedChunkSocket([substr($frame, 0, 30), substr($frame, 30)]),
        );

        self::assertSame($payload, $transport->readLine(new TimeoutCancellation(5.0))->await());

        // Real code: the whole frame was consumed, so nothing is left over.
        self::assertSame('', self::prop('readBuffer')->getValue($transport));
        self::assertNull(self::prop('readFrameRequired')->getValue($transport));
    }

    // -------------------------------------------------------------------------------------------
    // drainDataFrames() - the #164 spill gate (line 433, line 435)
    // -------------------------------------------------------------------------------------------

    /**
     * A 16-bit-length frame (<= 65535 bytes) must NEVER be sized for spill, even when its outstanding
     * tail is large: only frames carrying the 64-bit length marker (byte[1] & 0x7F === 127) qualify. A
     * 60000-byte frame uses the 16-bit form (byte[1] & 0x7F === 126) and a 59984-byte tail (> 32768).
     *
     * kills BitwiseAnd @ line 433  (`& 0x7F` -> `| 0x7F`: 126 | 0x7F === 127 would wrongly size a
     * 16-bit frame, arming readFrameRequired where the real code leaves it null).
     */
    public function testLargeSixteenBitFrameIsNeverSizedForSpill(): void
    {
        $frame = self::serverFrame(WebSocketFrameCodec::OP_BINARY, str_repeat('m', 60000), true);
        self::assertSame(126, ord($frame[1]) & 0x7F); // 16-bit extended length form

        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('readBuffer')->setValue($transport, substr($frame, 0, 20));

        self::assertSame('', self::drain($transport));
        // Real: 126 & 0x7F = 126 !== 127 -> not a 64-bit frame -> never sized.
        self::assertNull(self::prop('readFrameRequired')->getValue($transport));
    }

    /**
     * A 64-bit frame whose outstanding tail is at/below the spill threshold must NOT spill: it stays on
     * the batch-decode `.=` path (readFrameRequired null). Here the tail is 6 bytes - far below 32768.
     *
     * kills Minus @ line 435  (`$required - strlen(buf)` -> `$required + strlen(buf)`: 65546 + 65540
     * = 131086 > 32768 would spill).
     * kills LogicalAnd @ line 435  (`&&` -> `||`: `$required !== null || ...` is always true when the
     * length is known, so it would spill regardless of the tail size).
     */
    public function testNearlyCompleteLargeFrameIsNotSpilled(): void
    {
        $frame = self::serverFrame(WebSocketFrameCodec::OP_BINARY, str_repeat('Z', 65536), true);
        self::assertSame(127, ord($frame[1]) & 0x7F);

        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('readBuffer')->setValue($transport, substr($frame, 0, 65540)); // tail = 6

        self::assertSame('', self::drain($transport));
        // Real: 65546 - 65540 = 6 is not > 32768 -> not sized.
        self::assertNull(self::prop('readFrameRequired')->getValue($transport));
    }

    /**
     * The spill gate is a STRICT greater-than: a 64-bit frame whose outstanding tail equals the
     * threshold exactly (32768) must NOT spill. Buffered = 32778 of 65546 -> tail = 32768.
     *
     * kills GreaterThan @ line 435  (`> self::LARGE_FRAME_SPILL_BYTES` -> `>=`: 32768 >= 32768 would
     * wrongly arm readFrameRequired at exactly the boundary).
     */
    public function testFrameWithTailExactlyAtThresholdIsNotSpilled(): void
    {
        $frame = self::serverFrame(WebSocketFrameCodec::OP_BINARY, str_repeat('Z', 65536), true);
        self::assertSame(127, ord($frame[1]) & 0x7F);

        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('readBuffer')->setValue($transport, substr($frame, 0, 32778)); // tail = 32768

        self::assertSame('', self::drain($transport));
        // Real: 32768 is not > 32768 -> not sized (exactly at the boundary).
        self::assertNull(self::prop('readFrameRequired')->getValue($transport));
    }

    // -------------------------------------------------------------------------------------------
    // processFrames() - RFC 6455 Close echo width and best-effort null-safety (lines 470, 473)
    // -------------------------------------------------------------------------------------------

    /**
     * The Close echo mirrors ONLY the received status code - the first two payload bytes - even when the
     * peer's Close carries a trailing reason phrase. A close payload of status(2) + 'REASON'(6) must echo
     * back exactly the 2 status bytes.
     *
     * kills IncrementInteger @ line 470  (`substr($p, 0, 2)` -> `substr($p, 0, 3)`: would echo 3 bytes).
     * kills UnwrapSubstr @ line 470  (`substr($p, 0, 2)` -> `$p`: would echo the whole 8-byte payload).
     */
    public function testCloseEchoMirrorsOnlyTheTwoStatusBytes(): void
    {
        $status = pack('n', 1001);
        $closeFrame = self::serverFrame(WebSocketFrameCodec::OP_CLOSE, $status . 'REASON', true);
        $socket = new ScriptedChunkSocket([$closeFrame]);

        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('socket')->setValue($transport, $socket);

        try {
            $transport->readLine(new TimeoutCancellation(3.0))->await();
            self::fail('expected TransportClosedException on the server close frame');
        } catch (TransportClosedException) {
            // The close is still surfaced; we assert the echo below.
        }

        $written = $socket->writtenBytes();
        self::assertNotSame('', $written, 'a Close echo must be written');
        $frames = WebSocketFrameCodec::decode($written);
        self::assertNotSame([], $frames);
        self::assertSame(WebSocketFrameCodec::OP_CLOSE, $frames[0]['opcode']);
        // Exactly the 2-byte status - not 3 bytes, not the whole reason-carrying payload.
        self::assertSame($status, $frames[0]['payload']);
    }

    // -------------------------------------------------------------------------------------------
    // processFrames() - fragmentation-violation reset (line 497)
    // -------------------------------------------------------------------------------------------

    /**
     * RFC 6455 5.4: a new data frame mid-fragmentation is a violation. Before flagging it, the transport
     * MUST reset the in-progress fragment state so the abandoned partial message cannot corrupt anything
     * that inspects it afterwards.
     *
     * kills MethodCallRemoval @ line 497  ($this->resetFragmentState() deleted -> fragmenting stays true
     * after the violation).
     */
    public function testDataFrameMidFragmentationResetsFragmentState(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('fragmenting')->setValue($transport, true);
        self::prop('fragmentBuffer')->setValue($transport, 'PI');
        // A complete data frame arriving while a fragmented message is in progress = the violation.
        self::prop('readBuffer')->setValue(
            $transport,
            self::serverFrame(WebSocketFrameCodec::OP_BINARY, 'XX', true),
        );

        try {
            self::drain($transport);
            self::fail('expected the mid-fragmentation data frame to raise a ProtocolException');
        } catch (ProtocolException) {
            // Expected: the violation surfaces immediately (no data preceded it in this batch).
        }

        // Real: resetFragmentState() cleared the flag before the violation was flagged.
        self::assertFalse(self::prop('fragmenting')->getValue($transport));
    }

    // -------------------------------------------------------------------------------------------
    // processFrames() - fragment-bound accounting across continuation frames (line 535)
    // -------------------------------------------------------------------------------------------

    /**
     * The #89 fragment bound is enforced on the CUMULATIVE reassembly size: fragmentChunksLength must
     * accumulate (`+=`) every continuation frame's length, not just track the latest one. Three 4-byte
     * continuations on a 2-byte opener total 14 bytes and must trip an 10-byte cap - even though no
     * single frame exceeds it.
     *
     * kills Assignment @ line 535  (`fragmentChunksLength += strlen(...)` -> `= strlen(...)`: would
     * reset the counter to the last frame's 4 bytes each time, so 2 + 4 = 6 <= 10 never trips the bound
     * and the oversized message is silently reassembled).
     */
    public function testFragmentBoundCountsAllContinuationFrames(): void
    {
        $transport = new WebSocketTransport(new NatsOptions(), maxMessageBytes: 10);
        $buffer = self::serverFrame(WebSocketFrameCodec::OP_BINARY, 'PI', false)
            . self::serverFrame(WebSocketFrameCodec::OP_CONTINUATION, 'AAAA', false)
            . self::serverFrame(WebSocketFrameCodec::OP_CONTINUATION, 'BBBB', false)
            . self::serverFrame(WebSocketFrameCodec::OP_CONTINUATION, 'CCCC', true);
        self::prop('readBuffer')->setValue($transport, $buffer);

        // Real: 2 + (4+4+4) = 14 > 10 -> the bound trips on the final continuation.
        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessageMatches('/exceeded the maximum/');
        self::drain($transport);
    }

    // -------------------------------------------------------------------------------------------
    // performHandshake() - the header-terminator read loop (line 606)
    // -------------------------------------------------------------------------------------------

    /**
     * The handshake reads from the socket UNTIL the CRLFCRLF header terminator is seen, then validates
     * the 101 + accept key. Driving connect() against a server that performs a correct RFC 6455 upgrade
     * must complete without error.
     *
     * kills LogicalNot @ line 606  (`while (!str_contains($response, "\r\n\r\n"))` -> `while
     * (str_contains(...))`: the empty initial $response makes str_contains false, so the loop body never
     * runs, $response stays '' and the 101 validation throws a ConnectionException).
     * kills While_ @ line 606  (`while (...)` -> `while (false)`: same - the loop never reads the
     * response, so the handshake cannot succeed).
     */
    public function testHandshakeReadsUntilHeaderTerminator(): void
    {
        $server = listen('tcp://127.0.0.1:0');
        $address = (string) $server->getAddress();

        async(static function () use ($server): void {
            $client = $server->accept();
            if ($client === null) {
                return;
            }
            $request = '';
            while (!str_contains($request, "\r\n\r\n")) {
                $chunk = $client->read();
                if ($chunk === null) {
                    return;
                }
                $request .= $chunk;
            }

            if (preg_match('#Sec-WebSocket-Key:\s*(\S+)\r\n#i', $request, $m) !== 1) {
                $client->close();

                return;
            }
            $accept = WebSocketFrameCodec::acceptKey($m[1]);
            $client->write(
                "HTTP/1.1 101 Switching Protocols\r\n"
                . "Upgrade: websocket\r\n"
                . "Connection: Upgrade\r\n"
                . "Sec-WebSocket-Accept: {$accept}\r\n\r\n",
            );
            delay(0.3);
            $client->close();
        });

        $transport = new WebSocketTransport(new NatsOptions());

        try {
            // Real code reads the 101 response and validates it -> connect resolves without throwing.
            $transport->connect('ws://' . $address . '/', 2000)->await();
            self::assertFalse($transport->tlsActive()); // ws:// -> no TLS, and we got here without error
        } finally {
            $server->close();
        }
    }
}
