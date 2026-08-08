<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit\Mutation;

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Tests\Support\ScriptedChunkSocket;
use IDCT\NATS\Tests\Support\WedgedWriteSocket;
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
        $frames = WebSocketFrameCodec::decode($written, allowMasked: true);
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

    // -------------------------------------------------------------------------------------------
    // consumeSpanningFrame() - header top-up accounting and the surplus-chunk fold (lines 338, 368)
    // -------------------------------------------------------------------------------------------

    /**
     * When the header top-up loop exhausts the queued chunks without completing the header, the
     * chunk-length accounting must have been DECREMENTED for every chunk folded into the buffer
     * before the "header incomplete" violation is deferred - a replacing (=) or incrementing (+=)
     * accounting would leave a phantom byte count behind the deferred failure.
     *
     * kills Assignment (-= -> =) and MinusEqual (-= -> +=) @ line 338.
     */
    public function testHeaderTopUpDecrementsChunkAccountingBeforeDeferringIncompleteHeader(): void
    {
        $transport = new WebSocketTransport(new NatsOptions());
        // 1 buffered byte + a 1-byte chunk announcing a 16-bit length (126): after the top-up the
        // buffer holds 2 of the 4 needed header bytes and no chunks remain -> the sized-frame
        // invariant is violated and consumeSpanningFrame() defers the ProtocolException.
        self::prop('readBuffer')->setValue($transport, "\x82");
        self::prop('readChunks')->setValue($transport, [chr(126)]);
        self::prop('readChunksLength')->setValue($transport, 1);
        self::prop('readFrameRequired')->setValue($transport, 2);

        try {
            self::drain($transport);
            self::fail('expected the incomplete-header violation to surface');
        } catch (ProtocolException $e) {
            self::assertSame('WebSocket frame header incomplete despite sized frame', $e->getMessage());
        }

        // The shifted chunk's length was subtracted (1 - 1 = 0); '=' would leave 1, '+=' would leave 2.
        self::assertSame(0, self::prop('readChunksLength')->getValue($transport));
        self::assertSame([], self::prop('readChunks')->getValue($transport));
    }

    /**
     * The beyond-the-frame fold must re-inject ONLY the chunks past the consumed payload: the frame's
     * payload arrives as one fully consumed chunk (so the fold starts at index 1), and the trailing
     * frame is split across TWO further chunks. Folding the full chunk list would duplicate the
     * 70000 consumed payload bytes into the buffer; folding only one surplus chunk would lose the
     * trailing frame's tail. Byte-exact delivery of both messages pins it.
     *
     * kills UnwrapArraySlice @ line 368 (fold restarts at chunk 0, duplicating the consumed payload)
     * and SpreadOneItem @ line 368 (only the first surplus chunk is folded, dropping the tail).
     */
    public function testSpanningFrameFoldsOnlyChunksBeyondTheConsumedPayload(): void
    {
        $payload = str_repeat('Z', 70000);
        $header = pack('CC', 0x82, 127) . pack('J', 70000); // FIN+binary, 64-bit length form
        $tail = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'TAIL', false);

        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('readBuffer')->setValue($transport, $header);
        self::prop('readChunks')->setValue($transport, [$payload, substr($tail, 0, 3), substr($tail, 3)]);
        self::prop('readChunksLength')->setValue($transport, 70000 + strlen($tail));
        self::prop('readFrameRequired')->setValue($transport, 10 + 70000);

        self::assertSame($payload . 'TAIL', self::drain($transport));

        // The spill state is fully retired: nothing duplicated, nothing left behind.
        self::assertSame('', self::prop('readBuffer')->getValue($transport));
        self::assertSame([], self::prop('readChunks')->getValue($transport));
        self::assertSame(0, self::prop('readChunksLength')->getValue($transport));
        self::assertNull(self::prop('readFrameRequired')->getValue($transport));
    }

    // -------------------------------------------------------------------------------------------
    // drainDataFrames() - terminal precedence (lines 481, 498)
    // -------------------------------------------------------------------------------------------

    /**
     * When one read carries a Close frame AND a trailing codec strictness violation (here a masked
     * frame), the FIRST terminal condition wins: the surfaced failure must be the graceful
     * TransportClosedException, not the later ProtocolException - the connection layer reconnects on
     * the former and fails loudly on the latter.
     *
     * kills AssignCoalesce (??= -> =) @ line 481 (the codec terminal would overwrite the Close).
     */
    public function testCloseTerminalIsNotOverwrittenByTrailingCodecViolation(): void
    {
        $close = self::serverFrame(WebSocketFrameCodec::OP_CLOSE, pack('n', 1000), true);
        $masked = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'x', true, 'MKEY');

        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('readBuffer')->setValue($transport, $close . $masked);

        $this->expectException(TransportClosedException::class);
        $this->expectExceptionMessage('WebSocket close frame received');
        self::drain($transport);
    }

    /**
     * A read carrying [data][Close][incomplete frame declaring an out-of-bounds 64-bit length] must
     * return the data, then surface the CLOSE - the deferred Close must not be overwritten by the
     * head-frame sizing pass rediscovering the hostile length (its plain `=` assignment is only safe
     * because the terminal branch returns before the sizing block runs).
     *
     * kills ReturnRemoval @ line 498 (falling through to the sizing block replaces the deferred
     * TransportClosedException with the oversize ProtocolException).
     */
    public function testDeferredCloseSurvivesTrailingOversizedLengthDeclaration(): void
    {
        $payload = "MSG orders 1 5\r\nhello\r\n";
        $data = self::serverFrame(WebSocketFrameCodec::OP_BINARY, $payload, true);
        $close = self::serverFrame(WebSocketFrameCodec::OP_CLOSE, '', true);
        $poison = pack('CC', 0x82, 127) . pack('J', 64 * 1024 * 1024 + 1); // incomplete oversized head

        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('readBuffer')->setValue($transport, $data . $close . $poison);

        self::assertSame($payload, self::drain($transport), 'same-read data is returned before the close');

        try {
            self::drain($transport);
            self::fail('expected the deferred close on the following drain');
        } catch (TransportClosedException $e) {
            self::assertSame('WebSocket close frame received', $e->getMessage());
        }
    }

    // -------------------------------------------------------------------------------------------
    // answerControlFrame() - slot and writer-fiber semantics (lines 540, 544, 550, 552, 555, 583)
    // -------------------------------------------------------------------------------------------

    private static function answer(WebSocketTransport $t, int $opcode, string $payload): void
    {
        (new \ReflectionMethod(WebSocketTransport::class, 'answerControlFrame'))
            ->invoke($t, $opcode, $payload);
    }

    /**
     * With no socket there is nothing to answer: the guard must return BEFORE touching the slots or
     * spawning the writer fiber - a queued answer would otherwise chase the next connection's socket.
     *
     * kills ReturnRemoval @ line 540 (the slots would fill and the writer flag would flip).
     */
    public function testAnswerControlFrameWithoutSocketTouchesNeitherSlotsNorWriter(): void
    {
        $transport = new WebSocketTransport(new NatsOptions()); // socket stays null

        self::answer($transport, WebSocketFrameCodec::OP_PONG, 'late-pong');
        self::answer($transport, WebSocketFrameCodec::OP_CLOSE, 'late-echo');

        // Asserted synchronously, before any event-loop tick: the real guard never sets these.
        self::assertNull(self::prop('pendingPongPayload')->getValue($transport));
        self::assertNull(self::prop('pendingCloseEcho')->getValue($transport));
        self::assertFalse(self::prop('controlAnswerWriterActive')->getValue($transport));
    }

    /**
     * At most ONE Close echo is ever sent per connection, and it answers the FIRST Close seen: a
     * later Close's status must not replace a queued-but-unsent echo (RFC 6455 5.5.1 - the echo
     * mirrors the Close that initiated the closing handshake).
     *
     * kills AssignCoalesce (??= -> =) @ line 544 (the second status would overwrite the first).
     */
    public function testFirstQueuedCloseEchoIsNeverReplacedByALaterClose(): void
    {
        $socket = new ScriptedChunkSocket([]);
        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('socket')->setValue($transport, $socket);

        // Two Close answers queued back-to-back before the writer fiber can run.
        self::answer($transport, WebSocketFrameCodec::OP_CLOSE, pack('n', 1001));
        self::answer($transport, WebSocketFrameCodec::OP_CLOSE, pack('n', 4000));

        delay(0);
        delay(0);

        $buffer = $socket->writtenBytes();
        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);
        self::assertCount(1, $frames, 'exactly one Close echo may ever be written');
        self::assertSame(WebSocketFrameCodec::OP_CLOSE, $frames[0]['opcode']);
        self::assertSame(pack('n', 1001), $frames[0]['payload'], 'the FIRST Close status wins the echo slot');
    }

    /**
     * The answer slots are drained by a SINGLE writer fiber: while one is wedged on a
     * backpressure-suspended write, a newer answer only refills its slot - it must not spawn a
     * second writer (which would issue concurrent writes on the same socket).
     *
     * kills ReturnRemoval @ line 550 (a second fiber starts despite the active writer) and
     * TrueValue @ line 552 (the active flag is never armed, so every answer spawns a fiber).
     */
    public function testWedgedAnswerWriterIsNeverDuplicatedByNewerAnswers(): void
    {
        $socket = new WedgedWriteSocket();
        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('socket')->setValue($transport, $socket);

        self::answer($transport, WebSocketFrameCodec::OP_PONG, 'first');
        delay(0);
        delay(0);
        self::assertSame(1, $socket->writeAttempts(), 'the single writer starts and wedges on its first write');

        self::answer($transport, WebSocketFrameCodec::OP_PONG, 'second');
        delay(0);
        delay(0);

        // Real: the newer answer waits in its slot for the wedged writer - no concurrent second write.
        self::assertSame(1, $socket->writeAttempts(), 'no second writer fiber may start while one is active');
        self::assertSame('second', self::prop('pendingPongPayload')->getValue($transport));

        // Release the wedged writer; it drains the remaining slot against the closed socket silently.
        $socket->close();
        delay(0);
        delay(0);
        self::assertNull(self::prop('pendingPongPayload')->getValue($transport));
        self::assertFalse(self::prop('controlAnswerWriterActive')->getValue($transport));
    }

    /**
     * The writer fiber RETIRES once both slots are empty (its finally resets the active flag), so a
     * ping arriving after a completed drain must be answered by a fresh writer: two pings delivered
     * in two separate reads yield two pongs, each carrying its own ping's payload.
     *
     * kills UnwrapFinally @ line 555 and FalseValue @ line 583 (either leaves the active flag stuck,
     * so the second ping's pong is queued but never flushed).
     */
    public function testWriterFiberRetiresSoLaterPingsAreStillAnswered(): void
    {
        $socket = new ScriptedChunkSocket([
            WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_PING, 'p1', false)
            . WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'D1', false),
            WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_PING, 'p2', false)
            . WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, 'D2', false),
        ]);
        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('socket')->setValue($transport, $socket);

        self::assertSame('D1', $transport->readLine(new TimeoutCancellation(3.0))->await());
        $this->waitForWrittenFrames($socket, 1);

        self::assertSame('D2', $transport->readLine(new TimeoutCancellation(3.0))->await());
        $this->waitForWrittenFrames($socket, 2);
        delay(0);
        delay(0);

        $buffer = $socket->writtenBytes();
        $frames = WebSocketFrameCodec::decode($buffer, allowMasked: true);
        self::assertCount(2, $frames, 'each ping must be answered - the writer must retire between reads');
        self::assertSame(WebSocketFrameCodec::OP_PONG, $frames[0]['opcode']);
        self::assertSame('p1', $frames[0]['payload']);
        self::assertSame(WebSocketFrameCodec::OP_PONG, $frames[1]['opcode']);
        self::assertSame('p2', $frames[1]['payload']);
    }

    /** Bounded state-driven wait (hrtime deadline, never a bare sleep) for N decoded answer frames. */
    private function waitForWrittenFrames(ScriptedChunkSocket $socket, int $count): void
    {
        $deadlineNs = hrtime(true) + 2_000_000_000;
        do {
            $buffer = $socket->writtenBytes();
            if (count(WebSocketFrameCodec::decode($buffer, allowMasked: true)) >= $count) {
                return;
            }
            delay(0.001);
        } while (hrtime(true) < $deadlineNs);
    }

    // -------------------------------------------------------------------------------------------
    // processFrames() - frames AFTER a deferred terminal are dropped (lines 664, 673, 677, 695,
    // 725, 733, 735, 738)
    // -------------------------------------------------------------------------------------------

    /**
     * RFC 6455 5.2: RSV1 without a negotiated extension fails the connection EVEN IF the payload
     * happens to be a well-formed DEFLATE stream - the violation must surface, never the inflated
     * bytes (delivering them would hand attacker-shaped data to the NATS parser).
     *
     * kills ReturnRemoval @ line 664 (falling through would inflate the valid stream and DELIVER it).
     */
    public function testRsv1WithoutNegotiationRejectsEvenAValidDeflatePayload(): void
    {
        $frame = WebSocketFrameCodec::encode(
            WebSocketFrameCodec::OP_BINARY,
            WebSocketFrameCodec::deflate('SNEAKED'),
            false,
            null,
            true, // RSV1 set, but compression was never negotiated
        );

        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('readBuffer')->setValue($transport, $frame);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('permessage-deflate was not negotiated');
        self::drain($transport);
    }

    /**
     * A corrupt compressed frame is a terminal condition: the data frames AFTER it in the same batch
     * are dropped (nothing meaningful follows a failed connection), so with no data BEFORE it the
     * drain must throw - not deliver the later frame's payload.
     *
     * kills ReturnRemoval @ line 677 (processing would continue past the deferred inflate error and
     * deliver the trailing frame first).
     */
    #[\PHPUnit\Framework\Attributes\WithoutErrorHandler]
    public function testFramesAfterACorruptCompressedFrameAreDropped(): void
    {
        $corrupt = WebSocketFrameCodec::encode(
            WebSocketFrameCodec::OP_BINARY,
            'this is not compressed data at all !!!!!!',
            false,
            null,
            true,
        );
        $trailing = self::serverFrame(WebSocketFrameCodec::OP_BINARY, 'LEAK', true);

        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('compressionActive')->setValue($transport, true);
        self::prop('readBuffer')->setValue($transport, $corrupt . $trailing);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('inflate compressed WebSocket frame');
        self::drain($transport);
    }

    /**
     * An over-cap FIRST fragment is a terminal condition: the frames after it in the same batch are
     * dropped, so the drain throws the bound violation - it must not deliver the trailing frame.
     *
     * kills ReturnRemoval @ line 695 (the loop would continue and deliver 'LEAK').
     */
    public function testFramesAfterAnOversizedFirstFragmentAreDropped(): void
    {
        $transport = new WebSocketTransport(new NatsOptions(), maxMessageBytes: 4);
        self::prop('readBuffer')->setValue(
            $transport,
            self::serverFrame(WebSocketFrameCodec::OP_BINARY, 'AAAAAA', false) // 6 > 4, opens fragmentation
            . self::serverFrame(WebSocketFrameCodec::OP_BINARY, 'LEAK', true),
        );

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessageMatches('/exceeded the maximum/');
        self::drain($transport);
    }

    /**
     * An over-cap CONTINUATION is a terminal condition: the frames after it in the same batch are
     * dropped, so the drain throws the bound violation - it must not deliver the trailing frame.
     *
     * kills ReturnRemoval @ line 725 (the loop would continue and deliver 'LEAK').
     */
    public function testFramesAfterAnOversizedContinuationAreDropped(): void
    {
        $transport = new WebSocketTransport(new NatsOptions(), maxMessageBytes: 6);
        self::prop('readBuffer')->setValue(
            $transport,
            self::serverFrame(WebSocketFrameCodec::OP_BINARY, 'AA', false)
            . self::serverFrame(WebSocketFrameCodec::OP_CONTINUATION, 'BBBBBB', false) // 2 + 6 > 6
            . self::serverFrame(WebSocketFrameCodec::OP_BINARY, 'LEAK', true),
        );

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessageMatches('/exceeded the maximum/');
        self::drain($transport);
    }

    /**
     * A fragmented compressed message whose reassembled stream fails to inflate tears the fragment
     * state down AND drops the frames after it in the same batch: the drain throws the typed inflate
     * error, and the transport is back at the not-fragmenting baseline.
     *
     * kills ReturnRemoval @ line 738 (the trailing 'LEAK' frame would be delivered instead) and
     * MethodCallRemoval @ line 735 (the stale fragment state would survive the failure).
     */
    #[\PHPUnit\Framework\Attributes\WithoutErrorHandler]
    public function testCorruptCompressedFragmentDropsLaterFramesAndResetsState(): void
    {
        $garbage = 'this is not compressed data at all !!!!!!';
        $first = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, substr($garbage, 0, 20), false, null, true);
        $first[0] = chr(ord($first[0]) & 0x7F); // FIN=0, RSV1 kept: compressed fragment start
        $final = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_CONTINUATION, substr($garbage, 20), false);

        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('compressionActive')->setValue($transport, true);
        self::prop('readBuffer')->setValue(
            $transport,
            $first . $final . self::serverFrame(WebSocketFrameCodec::OP_BINARY, 'LEAK', true),
        );

        try {
            self::drain($transport);
            self::fail('expected the inflate failure to surface, not the trailing frame to be delivered');
        } catch (ProtocolException $e) {
            self::assertStringContainsString('inflate compressed WebSocket frame', $e->getMessage());
        }

        // The fragment state was reset before the deferral - nothing stale survives the failure.
        self::assertFalse(self::prop('fragmenting')->getValue($transport));
        self::assertSame('', self::prop('fragmentBuffer')->getValue($transport));
        self::assertSame([], self::prop('fragmentChunks')->getValue($transport));
        self::assertSame(0, self::prop('fragmentChunksLength')->getValue($transport));
    }

    /**
     * An inflated (RSV1) frame's output is APPENDED to data already decoded from the same read: a
     * plain frame followed by a compressed frame yields the concatenation, in order.
     *
     * kills Assignment (.= -> =) @ line 673 (only the inflated payload would be returned).
     */
    public function testInflatedFrameAppendsToSameReadPlainData(): void
    {
        $compressedPayload = 'COMPRESSED-TAIL';
        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('compressionActive')->setValue($transport, true);
        self::prop('readBuffer')->setValue(
            $transport,
            self::serverFrame(WebSocketFrameCodec::OP_BINARY, 'PLAIN-', true)
            . WebSocketFrameCodec::encode(
                WebSocketFrameCodec::OP_BINARY,
                WebSocketFrameCodec::deflate($compressedPayload),
                false,
                null,
                true,
            ),
        );

        self::assertSame('PLAIN-' . $compressedPayload, self::drain($transport));
    }

    /**
     * A completed compressed FRAGMENTED message's output is APPENDED to data already decoded from
     * the same read, exactly like the unfragmented case.
     *
     * kills Assignment (.= -> =) @ line 733 (only the inflated message would be returned).
     */
    public function testInflatedFragmentedMessageAppendsToSameReadPlainData(): void
    {
        $message = 'COMPRESSED-FRAGMENTED-MESSAGE';
        $compressed = WebSocketFrameCodec::deflate($message);
        $half = intdiv(strlen($compressed), 2);

        $first = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_BINARY, substr($compressed, 0, $half), false, null, true);
        $first[0] = chr(ord($first[0]) & 0x7F); // FIN=0, RSV1 kept
        $final = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_CONTINUATION, substr($compressed, $half), false);

        $transport = new WebSocketTransport(new NatsOptions());
        self::prop('compressionActive')->setValue($transport, true);
        self::prop('readBuffer')->setValue(
            $transport,
            self::serverFrame(WebSocketFrameCodec::OP_BINARY, 'PLAIN-', true) . $first . $final,
        );

        self::assertSame('PLAIN-' . $message, self::drain($transport));
    }
}
