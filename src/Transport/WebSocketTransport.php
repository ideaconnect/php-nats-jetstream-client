<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\Future;
use Amp\Socket\ClientTlsContext;
use Amp\Socket\ConnectContext;
use Amp\Socket\ResourceSocket;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\ProtocolException;

use function Amp\async;
use function Amp\Socket\connect;

/**
 * NATS-over-WebSocket transport (`ws://` / `wss://`).
 *
 * After a TCP (and, for `wss://`, TLS) connect it performs the RFC 6455 HTTP upgrade handshake, then
 * carries raw NATS protocol bytes as WebSocket binary frames: outbound writes are masked client
 * frames; inbound reads are decoded (and reassembled) via {@see WebSocketFrameCodec}, with ping/close
 * control frames handled transparently. TLS for `wss://` is negotiated during {@see connect()}, so
 * {@see upgradeTls()} is a no-op (there is no separate post-INFO upgrade for WebSocket).
 */
final class WebSocketTransport implements TlsAwareTransportInterface
{
    use AssemblesClientTlsContext;

    /** Hard cap on a reassembled fragmented message, bounding memory against a hostile/buggy server (#89). */
    private const DEFAULT_MAX_MESSAGE_BYTES = 64 * 1024 * 1024;

    /**
     * Bound (seconds) on the best-effort Close-frame write in {@see close()}. Amp's socket write()
     * SUSPENDS the calling fiber (it does not throw) while the write buffer is full, so an unbounded
     * inline write on a backpressure-stalled peer would park close() forever before it reaches the
     * socket close - and since close() is what errors pending writes out, nothing else could ever
     * break that deadlock. Generous enough for a draining-but-slow buffer to flush the 6-byte frame;
     * on timeout the frame is abandoned and the socket close below fails the pending write (RFC 6455
     * permits closing without completing the Close handshake).
     */
    private const CLOSE_FRAME_WRITE_TIMEOUT = 0.25;

    /**
     * Outstanding-tail size above which a head frame that declares a 64-bit length switches from
     * cheap `.=` buffer appends to O(1) chunk-list accumulation joined once (#164). Only frames
     * carrying the 64-bit length marker are ever considered: a frame that fits a 7-bit or 16-bit
     * length is at most 65 535 bytes, so its `.=` accumulation is inherently bounded and stays on
     * the pre-#164 batch-decode path byte-for-byte - the small-frame and fragmented-message paths
     * are therefore untouched and never even size the pending frame. A frame larger than this,
     * whose repeated `.=` growth is superlinear, spills so each payload byte is copied a bounded
     * number of times. Below the threshold the two paths differ only in cost, never in bytes.
     */
    private const LARGE_FRAME_SPILL_BYTES = 32 * 1024;

    private ?Socket $socket = null;
    private int $lastConnectTimeoutMs = 5_000;
    private bool $tlsEstablished = false;

    /** Raw (post-handshake) bytes received but not yet decoded into complete frames. */
    private string $readBuffer = '';

    /**
     * Full wire size of the large head frame currently spilling to chunk-list accumulation, or null
     * when no frame is spilling (the common case). Set only once an incomplete head frame is sized
     * and its outstanding tail exceeds {@see LARGE_FRAME_SPILL_BYTES}; while set, further reads queue
     * in $readChunks (O(1) each) and the frame is consumed by {@see consumeSpanningFrame()} with a
     * single payload join, so each payload byte is copied a bounded number of times no matter how
     * many reads deliver it (#164, mirrors the #140 TCP fix). Bounded by the codec's per-frame cap.
     */
    private ?int $readFrameRequired = null;

    /**
     * Socket chunks received while a large head frame is spilling, in arrival order; joined once by
     * {@see consumeSpanningFrame()}. Empty whenever $readFrameRequired is null.
     *
     * @var list<string>
     */
    private array $readChunks = [];

    /** Total byte length of $readChunks, tracked so frame completion is detected without joining. */
    private int $readChunksLength = 0;

    /** First fragment of an in-progress fragmented data message and whether one is in progress. */
    private string $fragmentBuffer = '';

    /**
     * Continuation-frame payloads of the in-progress fragmented message, joined once on its final
     * frame instead of a message-sized `.=` copy per continuation frame (#164).
     *
     * @var list<string>
     */
    private array $fragmentChunks = [];

    /** Total byte length of $fragmentChunks, tracked so the #89 bound is enforced without joining. */
    private int $fragmentChunksLength = 0;

    private bool $fragmenting = false;
    /** Whether the in-progress fragmented message was flagged compressed (RSV1 on its first frame). */
    private bool $fragmentCompressed = false;

    /**
     * Holds a deferred terminal condition - a graceful Close, or an RFC 6455 5.4 fragmentation
     * violation (a data frame mid-fragmentation, or an orphan continuation) - that shared a read with
     * data frames whose already-decoded bytes were returned first. The stored exception is surfaced on
     * the next {@see readLine()} once the buffered data is drained, so neither a graceful server close
     * nor a fragmentation violation can discard same-read data (#115). The first terminal condition in
     * a read wins (later frames after it are dropped). Null when no condition is pending. For a Close,
     * the RFC 6455 echo (#161) is sent when the Close is first seen, not when it is finally surfaced.
     */
    private ?\Throwable $pendingTerminal = null;

    /** Whether permessage-deflate was negotiated with the server (#61). */
    private bool $compressionActive = false;

    /**
     * Latest queued-but-unsent pong payload, or null when none is pending. A newer ping's payload
     * REPLACES a queued pong - RFC 6455 5.5.3 permits eliding the answers to OLDER pings in favor
     * of the most recent one - so a coalesced ping flood costs one <=125-byte slot, constant
     * memory by construction (no counter, no cap that could drop the newest answer).
     */
    private ?string $pendingPongPayload = null;

    /**
     * The single RFC 6455 5.5.1 Close-echo payload awaiting write, or null. A dedicated slot a
     * ping flood can never displace: the echo is a MUST, and at most one is ever sent per
     * connection (the first Close seen wins; processFrames() drops frames after a Close anyway).
     */
    private ?string $pendingCloseEcho = null;

    /** Whether the single writer fiber draining the two answer slots is currently running. */
    private bool $controlAnswerWriterActive = false;

    /**
     * @param NatsOptions $options Client options controlling TLS (used for `wss://`) and socket behavior.
     * @param int $maxMessageBytes Hard cap on a single reassembled fragmented message; a server that
     *                             streams continuation frames past this is treated as hostile (#89).
     */
    public function __construct(
        private readonly NatsOptions $options = new NatsOptions(),
        private readonly int $maxMessageBytes = self::DEFAULT_MAX_MESSAGE_BYTES,
    ) {}

    /**
     * Connects to a `ws://` or `wss://` NATS endpoint and completes the WebSocket upgrade handshake.
     */
    public function connect(string $dsn, int $timeoutMs): Future
    {
        return async(function () use ($dsn, $timeoutMs): void {
            $this->lastConnectTimeoutMs = max(1, $timeoutMs);
            $this->tlsEstablished = false;
            $this->readBuffer = '';
            $this->readFrameRequired = null;
            $this->readChunks = [];
            $this->readChunksLength = 0;
            $this->fragmentBuffer = '';
            $this->fragmentChunks = [];
            $this->fragmentChunksLength = 0;
            $this->fragmenting = false;
            $this->pendingTerminal = null;
            $this->compressionActive = false;
            // Stale answers must not leak onto a new connection's socket - a leftover Close echo
            // in particular would tell the fresh server this side is closing.
            $this->pendingPongPayload = null;
            $this->pendingCloseEcho = null;

            $parts = parse_url($dsn);
            if ($parts === false || !isset($parts['host'])) {
                throw new ConnectionException('Invalid WebSocket DSN: ' . $dsn);
            }

            $scheme = strtolower($parts['scheme'] ?? 'ws');
            $secure = $scheme === 'wss';
            $host = $parts['host'];
            $port = $parts['port'] ?? ($secure ? 443 : 80);
            $path = ($parts['path'] ?? '') === '' ? '/' : $parts['path'];
            if (isset($parts['query'])) {
                $path .= '?' . $parts['query'];
            }

            $context = (new ConnectContext())
                ->withConnectTimeout($this->lastConnectTimeoutMs / 1000)
                ->withTcpNoDelay();
            if ($secure) {
                $context = $context->withTlsContext($this->buildTlsContext($host));
            }

            $socket = connect("tcp://{$host}:{$port}", $context);
            $this->socket = $socket;
            $this->applyReadChunkSize();

            if ($secure) {
                $socket->setupTls(new TimeoutCancellation($this->lastConnectTimeoutMs / 1000));
                $this->tlsEstablished = true;
            }

            $this->performHandshake($host, $port, $path);
        });
    }

    /**
     * Raises the underlying socket read chunk size to the configured value (default 128 KiB, up from
     * Amp's 8 KiB default) so large inbound WebSocket frames arrive in far fewer socket reads, dividing
     * the per-chunk syscall + parser-push overhead (#119). A no-op for a Socket implementation that is
     * not a ResourceSocket, since setChunkSize is not on the Socket interface. Only the MAXIMUM a read
     * may return is raised; the socket still returns just what is available, so small frames are
     * unaffected.
     */
    private function applyReadChunkSize(): void
    {
        $chunkSize = $this->options->readChunkSizeBytes;
        if ($this->socket instanceof ResourceSocket && $chunkSize > 0) {
            $this->socket->setChunkSize($chunkSize);
        }
    }

    /**
     * No-op: for WebSocket, TLS (`wss://`) is established during {@see connect()}, not as a separate
     * post-INFO upgrade.
     */
    public function upgradeTls(): Future
    {
        return async(static function (): void {});
    }

    /**
     * Reports whether the `wss://` TLS handshake has completed.
     */
    public function tlsActive(): bool
    {
        return $this->tlsEstablished;
    }

    /**
     * Writes NATS protocol bytes as a masked WebSocket binary frame.
     *
     * Runs inline in the caller's fiber and returns an already-resolved future (#136): every
     * caller awaits immediately, so an async() wrapper only added a Future allocation plus a fiber
     * suspend/resume per write. The underlying socket write may suspend this fiber on
     * backpressure, which the immediate-await contract makes safe. Failures surface through the
     * returned future - never a synchronous throw - so callers observe them exactly as before.
     */
    public function write(string $bytes): Future
    {
        $socket = $this->socket;
        if ($socket === null) {
            // A silent no-op here would confirm publishes/ACKs that never reached any socket
            // (#124); erroring lets callers buffer, join the in-flight recovery, or fail loudly.
            return Future::error(new TransportClosedException('Transport is not connected'));
        }

        try {
            // When permessage-deflate was negotiated, compress the payload and mark the frame (RSV1).
            $payload = $this->compressionActive ? WebSocketFrameCodec::deflate($bytes) : $bytes;
            $socket->write(WebSocketFrameCodec::encode(
                WebSocketFrameCodec::OP_BINARY,
                $payload,
                rsv1: $this->compressionActive,
            ));
        } catch (\Throwable $e) {
            return Future::error($e);
        }

        return Future::complete();
    }

    /**
     * Reads the next available decoded NATS bytes, transparently answering pings and reassembling
     * fragmented messages. Throws {@see TransportClosedException} on a close frame or peer EOF.
     */
    public function readLine(?Cancellation $cancellation = null): Future
    {
        return async(function () use ($cancellation): string {
            // Captured for the whole read loop: close() nulls the property BEFORE the socket is
            // actually closed (up to the Close-frame write bound), so a looping reader re-reading
            // $this->socket in that window would deref null. The local keeps this reader pinned to
            // its own socket; once close() reaches the socket close, the pending read returns
            // EOF/throws and the loop exits through the normal closed paths.
            $socket = $this->socket;
            if ($socket === null) {
                return '';
            }

            while (true) {
                $data = $this->drainDataFrames();
                if ($data !== '') {
                    return $data;
                }

                $chunk = $socket->read($cancellation);
                if ($chunk === null) {
                    throw new TransportClosedException('WebSocket closed by peer (EOF)');
                }

                if ($chunk === '') {
                    continue;
                }

                if ($this->readFrameRequired !== null) {
                    // A large head frame is spilling: queue reads in O(1) instead of re-copying the
                    // growing prefix per read; consumeSpanningFrame() joins its payload once (#164).
                    $this->readChunks[] = $chunk;
                    $this->readChunksLength += strlen($chunk);
                } else {
                    // No frame spilling: the buffer holds at most one sub-threshold frame's bytes, so
                    // this `.=` is bounded - byte-for-byte the pre-#164 append path.
                    $this->readBuffer = $this->readBuffer === '' ? $chunk : $this->readBuffer . $chunk;
                }
            }
        });
    }

    /**
     * Consumes the pending head frame - its header at the start of $readBuffer, its payload spanning
     * $readBuffer and $readChunks - joining the payload exactly once. The unconsumed tail of the
     * chunk the frame ends inside becomes the new $readBuffer. The caller must have verified the
     * frame is fully buffered.
     *
     * @return array{opcode: int, payload: string, fin: bool, rsv1: bool}
     *
     * @phpstan-impure Consumes readBuffer/readChunks state and throws on a masked frame; callers
     *                 must not assume remembered property values (pendingTerminal) persist across it.
     */
    private function consumeSpanningFrame(): array
    {
        // Pathologically small reads can leave part of the header itself in the chunks; topping the
        // buffer up costs at most one extra chunk copy per frame, keeping the per-byte bound.
        while (($header = WebSocketFrameCodec::parseFrameHeader($this->readBuffer)) === null) {
            $chunk = array_shift($this->readChunks);
            if ($chunk === null) {
                throw new ProtocolException('WebSocket frame header incomplete despite sized frame');
            }
            $this->readChunksLength -= strlen($chunk);
            $this->readBuffer .= $chunk;
        }

        // The header top-up above may have overshot the frame end, so bound the payload slice and
        // start $rest from any surplus already in the buffer.
        $pieces = [substr($this->readBuffer, $header['headerBytes'], $header['payloadLength'])];
        $need = $header['payloadLength'] - strlen($pieces[0]);
        $rest = substr($this->readBuffer, $header['headerBytes'] + strlen($pieces[0]));
        $index = 0;

        while ($need > 0) {
            $chunk = $this->readChunks[$index];
            $length = strlen($chunk);
            $index++;

            if ($need >= $length) {
                $pieces[] = $chunk;
                $need -= $length;
                continue;
            }

            $pieces[] = substr($chunk, 0, $need);
            $rest = substr($chunk, $need);
            $need = 0;
        }

        // Reads drain immediately, so at most the final chunk overshoots the frame; fold any
        // remainder back into the working buffer to restore the append-mode invariant.
        if ($index < count($this->readChunks)) {
            $rest = implode('', [$rest, ...array_slice($this->readChunks, $index)]);
        }
        $this->readBuffer = $rest;
        $this->readChunks = [];
        $this->readChunksLength = 0;

        $payload = implode('', $pieces);
        if ($header['masked']) {
            // RFC 6455 5.1: server-to-client frames MUST NOT be masked; fail the connection instead
            // of silently unmasking (the buffer state is already consumed - the caller defers this).
            throw new ProtocolException('WebSocket server-to-client frame is masked (RFC 6455 5.1 violation)');
        }

        return [
            'opcode' => $header['opcode'],
            'payload' => $payload,
            'fin' => $header['fin'],
            'rsv1' => $header['rsv1'],
        ];
    }

    /**
     * Sends a close frame (best effort, time-bounded) and closes the underlying socket.
     *
     * close() MUST always reach the socket close: recovery and drain await it, and the socket close
     * is what errors out writes suspended on backpressure. The Close-frame courtesy write therefore
     * runs in its own fiber with a bounded await - an inline write would suspend (not throw) on a
     * full write buffer, the try/catch would never engage, and the wedged close() would deadlock the
     * whole client (nothing else closes the socket once state has left Open).
     */
    public function close(): Future
    {
        return async(function (): void {
            $socket = $this->socket;
            $this->socket = null;
            if ($socket === null) {
                return;
            }

            $closeFrame = async(static function () use ($socket): void {
                try {
                    $socket->write(WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_CLOSE, ''));
                } catch (\Throwable) {
                    // The socket may already be gone, or the close below failed this write out
                    // mid-suspension; closing is what matters.
                }
            });

            try {
                $closeFrame->await(new TimeoutCancellation(self::CLOSE_FRAME_WRITE_TIMEOUT));
            } catch (CancelledException) {
                // The frame write is wedged on backpressure; abandon it - the socket close below
                // fails the pending write and wakes its fiber.
            }

            $socket->close();
        });
    }

    /**
     * Decodes whatever complete frames are buffered, handling control frames inline and returning the
     * concatenated payload of any completed data messages ('' when none are ready yet).
     *
     * Frames whose bytes fit the buffer take the tight batch {@see WebSocketFrameCodec::decode()}
     * path unchanged from before #164. Only a frame whose outstanding tail exceeds
     * {@see LARGE_FRAME_SPILL_BYTES} spills: its remaining reads queue in $readChunks (O(1) each) and
     * it is consumed with a single payload join, bounding the per-byte copy count no matter how many
     * reads it spans (#164).
     */
    private function drainDataFrames(): string
    {
        // A terminal condition (Close or fragmentation violation) shared a read with data we already
        // returned; surface it now that the buffered data is drained (#115). A Close's RFC 6455 echo
        // (#161) was already written when it was first seen, so the connection layer just needs this
        // exception to reconnect (or to fail loudly on a protocol violation).
        if ($this->pendingTerminal !== null) {
            $terminal = $this->pendingTerminal;
            $this->pendingTerminal = null;

            throw $terminal;
        }

        $out = '';

        // A large frame was spilling to $readChunks: once fully buffered, consume it with one join,
        // then fall through to decode whatever trailing bytes arrived with its final read.
        if ($this->readFrameRequired !== null) {
            if (strlen($this->readBuffer) + $this->readChunksLength < $this->readFrameRequired) {
                return '';
            }

            $this->readFrameRequired = null;
            try {
                $out .= $this->processFrames([$this->consumeSpanningFrame()]);
            } catch (ProtocolException $violation) {
                // A masked spilled frame (RFC 6455 5.1): defer like every other strictness
                // violation so previously returned data stands and the failure surfaces cleanly.
                $this->pendingTerminal ??= $violation;
            }
        }

        // Skip the batch decode if the spilled frame's trailing bytes already carried a terminal
        // condition (Close or fragmentation violation).
        if ($this->pendingTerminal === null) {
            $frames = WebSocketFrameCodec::decode($this->readBuffer, $codecTerminal);
            if ($frames !== []) {
                $out .= $this->processFrames($frames);
            }

            // A strictness violation (oversized declared length, masked server frame, fragmented/
            // oversized control frame) stopped the decode AFTER the valid frames above were
            // collected: deliver them first, defer the violation to the next readLine() (#115).
            if ($codecTerminal !== null) {
                $this->pendingTerminal ??= $codecTerminal;
            }
        }

        // processFrames() flags a terminal condition (Close, or an RFC 6455 5.4 fragmentation
        // violation) instead of throwing when data preceded it in the same batch. If that data was
        // returned this call, hand it back now and defer the condition to the next call (#115); with no
        // accompanying data, surface it immediately - so the connection still fails loudly, just after
        // the already-decoded messages are delivered.
        if ($this->pendingTerminal !== null) {
            if ($out === '') {
                $terminal = $this->pendingTerminal;
                $this->pendingTerminal = null;

                throw $terminal;
            }

            return $out;
        }

        // decode() consumed every complete frame, so the buffer now holds at most one incomplete head
        // frame - a hostile declared length on a COMPLETE frame was already deferred via the codec
        // terminal above. Only a frame that declares a 64-bit length can exceed 65 535 bytes; size
        // just those and, when the outstanding tail is large, switch that frame to O(1) chunk
        // accumulation. Frames with a 7-bit/16-bit length are never sized here, so the small-frame
        // and fragmented paths keep the pre-#164 `.=` + batch-decode cost exactly. An INCOMPLETE
        // head frame declaring an out-of-bounds length throws from the sizing itself - defer it the
        // same way so data decoded this call is returned first.
        // (No pendingTerminal guard needed: the block above already returned/threw when one was set.)
        if (strlen($this->readBuffer) >= 2 && (ord($this->readBuffer[1]) & 0x7F) === 127) {
            try {
                $required = WebSocketFrameCodec::frameRequiredBytes($this->readBuffer);
                if ($required !== null && $required - strlen($this->readBuffer) > self::LARGE_FRAME_SPILL_BYTES) {
                    $this->readFrameRequired = $required;
                }
            } catch (ProtocolException $oversize) {
                $this->pendingTerminal = $oversize;
            }
        }

        return $out;
    }

    /**
     * Queues a control-frame answer (pong / Close echo) in its slot and starts the single writer
     * fiber that drains the slots, swallowing any write failure. Inline control-frame writes in
     * the read fiber were a loss/stall hazard: a throw discarded the data frames decoded from the
     * same read, and a backpressure suspension parked the read path indefinitely (the same
     * suspension class as the close() wedge). Answers are held as DATA, not fibers: a newer ping's
     * payload replaces a queued-but-unsent pong (RFC 6455 5.5.3), while the mandatory Close echo
     * (RFC 6455 5.5.1) keeps its own slot so a ping flood can never displace it - constant memory
     * with no cap whose overflow would drop the NEWEST answers (a counted-fiber cap did exactly
     * that: within one read batch the fibers never start, so the count only ever grew). If the
     * socket dies (or close() releases it) before a slot is flushed, the answer is dropped
     * silently - the failure that matters surfaces via the read path.
     */
    private function answerControlFrame(int $opcode, string $payload): void
    {
        if ($this->socket === null) {
            return;
        }

        if ($opcode === WebSocketFrameCodec::OP_CLOSE) {
            $this->pendingCloseEcho ??= $payload;
        } else {
            $this->pendingPongPayload = $payload;
        }

        if ($this->controlAnswerWriterActive) {
            return;
        }
        $this->controlAnswerWriterActive = true;

        async(function (): void {
            try {
                // Drain until both slots are empty: a write may suspend on backpressure, during
                // which newer answers can (re)fill a slot - the loop picks them up on resume. The
                // pong goes first (its ping preceded the Close on the wire); the Close echo is
                // flushed even if the pong write failed.
                while (true) {
                    $pong = $this->pendingPongPayload;
                    $closeEcho = $this->pendingCloseEcho;

                    if ($pong !== null) {
                        $this->pendingPongPayload = null;
                        $frame = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_PONG, $pong);
                    } elseif ($closeEcho !== null) {
                        $this->pendingCloseEcho = null;
                        $frame = WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_CLOSE, $closeEcho);
                    } else {
                        break;
                    }

                    try {
                        // Read at write time, not queue time: close() nulls the property before
                        // closing, and an answer must never chase a socket the transport released.
                        $this->socket?->write($frame);
                    } catch (\Throwable) {
                        // Dead/closing socket: the read path surfaces the failure; the answer is moot.
                    }
                }
            } finally {
                $this->controlAnswerWriterActive = false;
            }
        });
    }

    /**
     * Applies decoded frames: answers pings, reacts to close, reassembles fragmented messages, and
     * returns the concatenated payload of any completed data messages.
     *
     * @param list<array{opcode: int, payload: string, fin: bool, rsv1: bool}> $frames
     *
     * @phpstan-impure Mutates fragment-reassembly state and sets $pendingTerminal on a Close frame or
     *                 an RFC 6455 5.4 fragmentation violation.
     */
    private function processFrames(array $frames): string
    {
        $out = '';

        foreach ($frames as $frame) {
            switch ($frame['opcode']) {
                case WebSocketFrameCodec::OP_PING:
                    // Answer with a pong carrying the same application data - via the answer
                    // slots, never inline: this runs inside the READ fiber, and an inline write
                    // could (a) THROW on a just-died socket, discarding the data frames already
                    // decoded into $out from the same read (their bytes are consumed - core NATS
                    // never resends), or (b) SUSPEND on a backpressure-stalled peer, parking the
                    // whole read path behind an outbound control frame. A failed/abandoned pong
                    // needs no surfacing of its own: the socket is dead or dying, and the next
                    // read (or the bounded close()) surfaces that. Coalesced pings collapse to ONE
                    // pong answering the newest (RFC 6455 5.5.3).
                    $this->answerControlFrame(WebSocketFrameCodec::OP_PONG, $frame['payload']);
                    break;

                case WebSocketFrameCodec::OP_PONG:
                    break;

                case WebSocketFrameCodec::OP_CLOSE:
                    // RFC 6455 5.5.1: an endpoint receiving a Close MUST send a Close in response.
                    // Echo one mirroring the received status code (the first two payload bytes; empty
                    // when the peer sent no status), best-effort - the socket may already be gone (#161).
                    // Queued in its own answer slot (which a preceding ping flood can never displace)
                    // and flushed by the writer fiber: an inline echo could suspend the read fiber on
                    // a stalled peer, delaying the close from surfacing.
                    $echo = strlen($frame['payload']) >= 2 ? substr($frame['payload'], 0, 2) : '';
                    $this->answerControlFrame(WebSocketFrameCodec::OP_CLOSE, $echo);

                    // Flag the close and stop instead of throwing here: any data frames earlier in this
                    // batch have already been decoded out of the read buffer, so throwing would silently
                    // discard them (#115). drainDataFrames() returns that data now and surfaces the close
                    // on the next readLine(); nothing meaningful follows a Close, so later frames drop.
                    // First terminal condition in the read wins (??=).
                    $this->pendingTerminal ??= new TransportClosedException('WebSocket close frame received');

                    return $out;

                case WebSocketFrameCodec::OP_BINARY:
                case WebSocketFrameCodec::OP_TEXT:
                    if ($this->fragmenting) {
                        // RFC 6455 5.4: a new data frame while a fragmented message is in progress is a
                        // protocol violation. Fail loudly instead of silently overwriting the partial
                        // message (#115) - but defer exactly like the Close: data frames earlier in this
                        // batch were already decoded out of the read buffer, so throwing here would
                        // discard them. Flag the violation and stop; drainDataFrames() returns that data
                        // now and surfaces this exception on the next readLine() (immediately when there
                        // is no accompanying data). First terminal condition in the read wins (??=).
                        $this->resetFragmentState();
                        $this->pendingTerminal ??= new ProtocolException(
                            'WebSocket data frame received while a fragmented message is in progress',
                        );

                        return $out;
                    }

                    if ($frame['rsv1'] && !$this->compressionActive) {
                        // RFC 6455 5.2: a non-zero RSV bit outside a negotiated extension MUST fail
                        // the connection. Inflating an uncompressed payload as before produced a
                        // confusing zlib error (or garbage) instead of naming the violation.
                        $this->pendingTerminal ??= new ProtocolException(
                            'WebSocket data frame sets RSV1 but permessage-deflate was not negotiated (RFC 6455 5.2 violation)',
                        );

                        return $out;
                    }

                    if ($frame['fin']) {
                        // A compressed (RSV1) message is inflated once fully received. An inflate
                        // failure (corrupt deflate stream) is deferred like every terminal condition
                        // so data already decoded from this read is returned first (#115).
                        if ($frame['rsv1']) {
                            try {
                                $out .= WebSocketFrameCodec::inflate($frame['payload']);
                            } catch (ProtocolException $inflateError) {
                                $this->pendingTerminal ??= $inflateError;

                                return $out;
                            }
                        } else {
                            $out .= $frame['payload'];
                        }
                    } else {
                        $this->fragmentBuffer = $frame['payload'];
                        $this->fragmentChunks = [];
                        $this->fragmentChunksLength = 0;
                        $this->fragmenting = true;
                        // permessage-deflate marks RSV1 only on the first frame of the message.
                        $this->fragmentCompressed = $frame['rsv1'];

                        try {
                            $this->enforceFragmentBound();
                        } catch (ProtocolException $boundError) {
                            $this->pendingTerminal ??= $boundError;

                            return $out;
                        }
                    }
                    break;

                case WebSocketFrameCodec::OP_CONTINUATION:
                    if (!$this->fragmenting) {
                        // RFC 6455 5.4: a continuation frame with no fragmented message in progress is a
                        // protocol violation. Fail loudly instead of silently dropping the orphan (#115),
                        // but defer exactly like the Close so data frames earlier in this batch (already
                        // decoded out of the read buffer) are returned first rather than discarded.
                        // drainDataFrames() surfaces this on the next readLine() (immediately when there
                        // is no accompanying data). First terminal condition in the read wins (??=).
                        $this->pendingTerminal ??= new ProtocolException(
                            'WebSocket continuation frame with no fragmented message in progress',
                        );

                        return $out;
                    }

                    $this->fragmentChunks[] = $frame['payload'];
                    $this->fragmentChunksLength += strlen($frame['payload']);

                    try {
                        $this->enforceFragmentBound();
                    } catch (ProtocolException $boundError) {
                        // enforceFragmentBound() already reset the fragment state; defer the
                        // violation so same-read data is returned first (#115).
                        $this->pendingTerminal ??= $boundError;

                        return $out;
                    }

                    if ($frame['fin']) {
                        // Single join of all fragments (#164) - one copy of every byte.
                        $message = implode('', [$this->fragmentBuffer, ...$this->fragmentChunks]);
                        if ($this->fragmentCompressed) {
                            try {
                                $out .= WebSocketFrameCodec::inflate($message);
                            } catch (ProtocolException $inflateError) {
                                $this->resetFragmentState();
                                $this->pendingTerminal ??= $inflateError;

                                return $out;
                            }
                        } else {
                            $out .= $message;
                        }
                        $this->resetFragmentState();
                    }
                    break;
            }
        }

        return $out;
    }

    /**
     * Bounds the in-progress fragment-reassembly buffer so a server streaming unbounded continuation
     * frames cannot OOM the client. Resets the fragment state and throws when the cap is exceeded (#89).
     */
    private function enforceFragmentBound(): void
    {
        if (strlen($this->fragmentBuffer) + $this->fragmentChunksLength <= $this->maxMessageBytes) {
            return;
        }

        $limit = $this->maxMessageBytes;
        $this->resetFragmentState();

        throw new ProtocolException(
            sprintf('WebSocket fragmented message exceeded the maximum of %d bytes', $limit),
        );
    }

    /**
     * Clears all in-progress fragment-reassembly state, returning the transport to the not-fragmenting
     * baseline (on completion, on a bound overflow, or on a protocol violation).
     */
    private function resetFragmentState(): void
    {
        $this->fragmentBuffer = '';
        $this->fragmentChunks = [];
        $this->fragmentChunksLength = 0;
        $this->fragmenting = false;
        $this->fragmentCompressed = false;
    }

    /**
     * Sends the HTTP upgrade request and validates the server's 101 response (status + accept key).
     * Any bytes the server sent after the header terminator (e.g. the NATS INFO frame) are retained.
     */
    private function performHandshake(string $host, int $port, string $path): void
    {
        $socket = $this->socket;
        if ($socket === null) {
            throw new ConnectionException('WebSocket socket not connected');
        }

        $clientKey = WebSocketFrameCodec::generateClientKey();
        $socket->write(self::buildUpgradeRequest(
            $host,
            $port,
            $path,
            $clientKey,
            $this->options->webSocketHeaders,
            $this->options->webSocketCompression,
        ));

        $cancellation = new TimeoutCancellation($this->lastConnectTimeoutMs / 1000);
        $response = '';
        while (!str_contains($response, "\r\n\r\n")) {
            $chunk = $socket->read($cancellation);
            if ($chunk === null) {
                throw new ConnectionException('WebSocket handshake failed: connection closed before response');
            }
            $response .= $chunk;
            if (strlen($response) > 16384) {
                throw new ConnectionException('WebSocket handshake response exceeded the maximum header size');
            }
        }

        $separator = (int) strpos($response, "\r\n\r\n");
        $header = substr($response, 0, $separator);
        // Surplus bytes after the header belong to the WebSocket stream (e.g. the NATS INFO frame).
        $this->readBuffer = substr($response, $separator + 4);

        $lines = explode("\r\n", $header);
        $statusLine = $lines[0];
        if (preg_match('#^HTTP/1\.[01]\s+101\b#', $statusLine) !== 1) {
            throw new ConnectionException('WebSocket upgrade rejected by server: ' . $statusLine);
        }

        $accept = null;
        $upgradeOk = false;
        $connectionOk = false;
        $negotiatedExtensions = null;
        foreach (array_slice($lines, 1) as $line) {
            $colon = strpos($line, ':');
            if ($colon === false) {
                continue;
            }
            $headerName = trim(substr($line, 0, $colon));
            $headerValue = trim(substr($line, $colon + 1));

            if (strcasecmp($headerName, 'Sec-WebSocket-Accept') === 0) {
                $accept = $headerValue;
            } elseif (strcasecmp($headerName, 'Upgrade') === 0) {
                $upgradeOk = strcasecmp($headerValue, 'websocket') === 0;
            } elseif (strcasecmp($headerName, 'Connection') === 0) {
                // A token list ("Upgrade", "keep-alive, Upgrade", ...) - require the Upgrade token.
                foreach (explode(',', $headerValue) as $token) {
                    if (strcasecmp(trim($token), 'upgrade') === 0) {
                        $connectionOk = true;
                    }
                }
            } elseif (strcasecmp($headerName, 'Sec-WebSocket-Extensions') === 0) {
                $negotiatedExtensions = $headerValue;
            }
        }

        // RFC 6455 4.1 steps 2-3: a 101 without Upgrade: websocket / Connection: Upgrade is NOT a
        // completed WebSocket handshake (e.g. a proxy answering 101 for something else); the client
        // MUST fail the connection rather than speak WebSocket into a non-WebSocket stream.
        if (!$upgradeOk || !$connectionOk) {
            throw new ConnectionException(
                'WebSocket handshake failed: the 101 response is missing the required '
                . '"Upgrade: websocket" / "Connection: Upgrade" headers (RFC 6455 4.1)',
            );
        }

        if ($accept === null || !hash_equals(WebSocketFrameCodec::acceptKey($clientKey), $accept)) {
            throw new ConnectionException('WebSocket handshake failed: invalid Sec-WebSocket-Accept');
        }

        if ($negotiatedExtensions !== null) {
            $this->compressionActive = $this->negotiateCompression($negotiatedExtensions);
        }
    }

    /**
     * Validates the server's Sec-WebSocket-Extensions response and reports whether permessage-deflate
     * is active. RFC 6455 4.1 requires FAILING the connection on an extension that was not offered
     * (pre-fix an unsolicited "permessage-deflate" flipped compression on, so the client started
     * RSV1-deflating its CONNECT into a server that never negotiated it), and RFC 7692 requires
     * rejecting unknown/unacceptable parameters - this codec inflates per message, so a server
     * negotiating window-bits/context-takeover semantics beyond the offered
     * client_no_context_takeover/server_no_context_takeover would garble every message after the
     * first.
     */
    private function negotiateCompression(string $extensions): bool
    {
        if (!$this->options->webSocketCompression) {
            throw new ConnectionException(
                'WebSocket handshake failed: the server negotiated extension "' . $extensions
                . '" that this client never offered (RFC 6455 4.1)',
            );
        }

        $parts = array_map(trim(...), explode(';', $extensions));
        $name = array_shift($parts);
        if (strcasecmp((string) $name, 'permessage-deflate') !== 0 || str_contains($extensions, ',')) {
            throw new ConnectionException(
                'WebSocket handshake failed: unsupported extension response "' . $extensions . '"',
            );
        }

        $serverNoContext = false;
        foreach ($parts as $param) {
            if ($param === '') {
                continue;
            }

            if (strcasecmp($param, 'server_no_context_takeover') === 0) {
                $serverNoContext = true;

                continue;
            }

            if (strcasecmp($param, 'client_no_context_takeover') === 0) {
                continue;
            }

            // RFC 7692 7.1.2.1: the server may volunteer server_max_window_bits - even when absent
            // from the offer - to limit its OWN LZ77 window; the raw 15-bit inflater decodes any
            // smaller-window stream, so 8..15 costs nothing to accept. The section 7 ABNF makes the
            // value MANDATORY for this parameter ("server_max_window_bits" "=" window-bits, decimal
            // without leading zeroes) - only client_max_window_bits has an optional-value form - so
            // the bare spelling is rejected as an invalid response parameter (RFC 7692 section 5).
            // The quoted-string spelling ="12" is a legal equivalent encoding per RFC 6455 9.1's
            // extension-param ABNF (token | quoted-string). client_max_window_bits stays rejected
            // below: it was not offered (7.1.2.2 forbids the server sending it unsolicited) and
            // would change what this client deflates.
            if (preg_match('/^server_max_window_bits=(?:([89]|1[0-5])|"([89]|1[0-5])")$/i', $param) === 1) {
                continue;
            }

            throw new ConnectionException(
                'WebSocket handshake failed: unacceptable permessage-deflate parameter "' . $param
                . '" (accepted: client_no_context_takeover, server_no_context_takeover, server_max_window_bits=8..15)',
            );
        }

        // RFC 7692 7.1.1.1: a server that received server_no_context_takeover in the offer MUST echo
        // it or decline the extension. A server that accepts WITHOUT echoing may retain deflate
        // context across messages - which this per-message-inflate codec cannot decode (every
        // back-referencing message would fail, flakily). nats-server always echoes both params.
        if (!$serverNoContext) {
            throw new ConnectionException(
                'WebSocket handshake failed: the server accepted permessage-deflate without echoing '
                . 'server_no_context_takeover (RFC 7692 7.1.1.1) - it may use context takeover, which '
                . 'this client cannot decode',
            );
        }

        return true;
    }

    /**
     * Builds the RFC 6455 HTTP upgrade request, including any caller-supplied headers (#61, e.g. cookies
     * / proxy auth) and the permessage-deflate extension offer when compression is requested. Pure and
     * static so it can be unit-tested without a socket.
     *
     * @param array<string,string> $extraHeaders
     */
    public static function buildUpgradeRequest(
        string $host,
        int $port,
        string $path,
        string $clientKey,
        array $extraHeaders = [],
        bool $compression = false,
    ): string {
        $lines = [
            "GET {$path} HTTP/1.1",
            "Host: {$host}:{$port}",
            'Upgrade: websocket',
            'Connection: Upgrade',
            "Sec-WebSocket-Key: {$clientKey}",
            'Sec-WebSocket-Version: 13',
        ];

        if ($compression) {
            $lines[] = 'Sec-WebSocket-Extensions: permessage-deflate; client_no_context_takeover; server_no_context_takeover';
        }

        // Reserved handshake headers cannot be overridden by caller headers (they would corrupt it).
        $reserved = ['host', 'upgrade', 'connection', 'sec-websocket-key', 'sec-websocket-version'];
        foreach ($extraHeaders as $name => $value) {
            if (in_array(strtolower($name), $reserved, true)) {
                continue;
            }
            // Strip CR/LF from BOTH name and value (and ':' from the name) to prevent header/request
            // injection - a CR/LF or colon in the name would otherwise forge additional header lines.
            $cleanName = str_replace(["\r", "\n", ':'], '', $name);
            $lines[] = $cleanName . ': ' . str_replace(["\r", "\n"], '', $value);
        }

        return implode("\r\n", $lines) . "\r\n\r\n";
    }

    /**
     * Builds the TLS context for a `wss://` connection from the client options.
     */
    private function buildTlsContext(string $host): ClientTlsContext
    {
        return $this->clientTlsContextFromOptions($this->options, $host);
    }
}
