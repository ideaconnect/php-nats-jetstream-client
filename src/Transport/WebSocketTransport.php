<?php

declare(strict_types=1);

namespace IDCT\NATS\Transport;

use Amp\Cancellation;
use Amp\Future;
use Amp\Socket\Certificate;
use Amp\Socket\ClientTlsContext;
use Amp\Socket\ConnectContext;
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
    /** Hard cap on a reassembled fragmented message, bounding memory against a hostile/buggy server (#89). */
    private const DEFAULT_MAX_MESSAGE_BYTES = 64 * 1024 * 1024;

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

    /** Whether permessage-deflate was negotiated with the server (#61). */
    private bool $compressionActive = false;

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
            $this->compressionActive = false;

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

            $this->socket = connect("tcp://{$host}:{$port}", $context);

            if ($secure) {
                $this->socket->setupTls(new TimeoutCancellation($this->lastConnectTimeoutMs / 1000));
                $this->tlsEstablished = true;
            }

            $this->performHandshake($host, $port, $path);
        });
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
            if ($this->socket === null) {
                return '';
            }

            while (true) {
                $data = $this->drainDataFrames();
                if ($data !== '') {
                    return $data;
                }

                $chunk = $this->socket->read($cancellation);
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
        if ($header['masked'] && $payload !== '') {
            $payload = WebSocketFrameCodec::unmask($payload, $header['maskKey']);
        }

        return [
            'opcode' => $header['opcode'],
            'payload' => $payload,
            'fin' => $header['fin'],
            'rsv1' => $header['rsv1'],
        ];
    }

    /**
     * Sends a close frame (best effort) and closes the underlying socket.
     */
    public function close(): Future
    {
        return async(function (): void {
            $socket = $this->socket;
            $this->socket = null;
            if ($socket === null) {
                return;
            }

            try {
                $socket->write(WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_CLOSE, ''));
            } catch (\Throwable) {
                // The socket may already be gone; closing below is what matters.
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
        $out = '';

        // A large frame was spilling to $readChunks: once fully buffered, consume it with one join,
        // then fall through to decode whatever trailing bytes arrived with its final read.
        if ($this->readFrameRequired !== null) {
            if (strlen($this->readBuffer) + $this->readChunksLength < $this->readFrameRequired) {
                return '';
            }

            $this->readFrameRequired = null;
            $out .= $this->processFrames([$this->consumeSpanningFrame()]);
        }

        $frames = WebSocketFrameCodec::decode($this->readBuffer);
        if ($frames !== []) {
            $out .= $this->processFrames($frames);
        }

        // decode() consumed every complete frame, so the buffer now holds at most one incomplete head
        // frame - and already threw on a hostile declared length (its own MAX_FRAME_PAYLOAD guard
        // fires the moment the length bytes are in). Only a frame that declares a 64-bit length can
        // exceed 65 535 bytes; size just those and, when the outstanding tail is large, switch that
        // frame to O(1) chunk accumulation. Frames with a 7-bit/16-bit length are never sized here,
        // so the small-frame and fragmented paths keep the pre-#164 `.=` + batch-decode cost exactly.
        if (strlen($this->readBuffer) >= 2 && (ord($this->readBuffer[1]) & 0x7F) === 127) {
            $required = WebSocketFrameCodec::frameRequiredBytes($this->readBuffer);
            if ($required !== null && $required - strlen($this->readBuffer) > self::LARGE_FRAME_SPILL_BYTES) {
                $this->readFrameRequired = $required;
            }
        }

        return $out;
    }

    /**
     * Applies decoded frames: answers pings, reacts to close, reassembles fragmented messages, and
     * returns the concatenated payload of any completed data messages.
     *
     * @param list<array{opcode: int, payload: string, fin: bool, rsv1: bool}> $frames
     */
    private function processFrames(array $frames): string
    {
        $out = '';

        foreach ($frames as $frame) {
            switch ($frame['opcode']) {
                case WebSocketFrameCodec::OP_PING:
                    // Answer with a pong carrying the same application data.
                    $this->socket?->write(WebSocketFrameCodec::encode(WebSocketFrameCodec::OP_PONG, $frame['payload']));
                    break;

                case WebSocketFrameCodec::OP_PONG:
                    break;

                case WebSocketFrameCodec::OP_CLOSE:
                    throw new TransportClosedException('WebSocket close frame received');

                case WebSocketFrameCodec::OP_BINARY:
                case WebSocketFrameCodec::OP_TEXT:
                    if ($frame['fin']) {
                        // A compressed (RSV1) message is inflated once fully received.
                        $out .= $frame['rsv1'] ? WebSocketFrameCodec::inflate($frame['payload']) : $frame['payload'];
                    } else {
                        $this->fragmentBuffer = $frame['payload'];
                        $this->fragmentChunks = [];
                        $this->fragmentChunksLength = 0;
                        $this->fragmenting = true;
                        // permessage-deflate marks RSV1 only on the first frame of the message.
                        $this->fragmentCompressed = $frame['rsv1'];
                        $this->enforceFragmentBound();
                    }
                    break;

                case WebSocketFrameCodec::OP_CONTINUATION:
                    if ($this->fragmenting) {
                        $this->fragmentChunks[] = $frame['payload'];
                        $this->fragmentChunksLength += strlen($frame['payload']);
                        $this->enforceFragmentBound();
                        if ($frame['fin']) {
                            // Single join of all fragments (#164) - one copy of every byte.
                            $message = implode('', [$this->fragmentBuffer, ...$this->fragmentChunks]);
                            $out .= $this->fragmentCompressed
                                ? WebSocketFrameCodec::inflate($message)
                                : $message;
                            $this->fragmentBuffer = '';
                            $this->fragmentChunks = [];
                            $this->fragmentChunksLength = 0;
                            $this->fragmenting = false;
                            $this->fragmentCompressed = false;
                        }
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
        $this->fragmentBuffer = '';
        $this->fragmentChunks = [];
        $this->fragmentChunksLength = 0;
        $this->fragmenting = false;
        $this->fragmentCompressed = false;

        throw new ProtocolException(
            sprintf('WebSocket fragmented message exceeded the maximum of %d bytes', $limit),
        );
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
        foreach (array_slice($lines, 1) as $line) {
            $colon = strpos($line, ':');
            if ($colon === false) {
                continue;
            }
            $headerName = trim(substr($line, 0, $colon));
            $headerValue = trim(substr($line, $colon + 1));

            if (strcasecmp($headerName, 'Sec-WebSocket-Accept') === 0) {
                $accept = $headerValue;
            } elseif (strcasecmp($headerName, 'Sec-WebSocket-Extensions') === 0
                && stripos($headerValue, 'permessage-deflate') !== false
            ) {
                // The server accepted compression; (de)compress data frames from here on (#61).
                $this->compressionActive = true;
            }
        }

        if ($accept === null || !hash_equals(WebSocketFrameCodec::acceptKey($clientKey), $accept)) {
            throw new ConnectionException('WebSocket handshake failed: invalid Sec-WebSocket-Accept');
        }
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
        // Honor a caller-supplied TLS context verbatim (in-memory PEM, ALPN, custom verification).
        if ($this->options->tlsContext !== null) {
            return $this->options->tlsContext;
        }

        $peerName = $this->options->tlsPeerName;
        if ($peerName === null || $peerName === '') {
            $peerName = $host;
        }

        $tlsContext = new ClientTlsContext($peerName);

        if (!$this->options->tlsVerifyPeer) {
            $tlsContext = $tlsContext->withoutPeerVerification();
        }

        if ($this->options->tlsCaFile !== null && $this->options->tlsCaFile !== '') {
            $tlsContext = $tlsContext->withCaFile($this->options->tlsCaFile);
        }

        if ($this->options->tlsCertFile !== null && $this->options->tlsCertFile !== '') {
            $tlsContext = $tlsContext->withCertificate(new Certificate(
                $this->options->tlsCertFile,
                $this->options->tlsKeyFile,
                $this->options->tlsKeyPassphrase,
            ));
        }

        return $tlsContext;
    }
}
