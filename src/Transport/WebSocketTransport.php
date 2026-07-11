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

    private ?Socket $socket = null;
    private int $lastConnectTimeoutMs = 5_000;
    private bool $tlsEstablished = false;

    /** Raw (post-handshake) bytes received but not yet decoded into complete frames. */
    private string $readBuffer = '';

    /** Reassembly buffer for a fragmented data message and whether one is in progress. */
    private string $fragmentBuffer = '';
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
            $this->fragmentBuffer = '';
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
     */
    public function write(string $bytes): Future
    {
        return async(function () use ($bytes): void {
            $socket = $this->socket;
            if ($socket === null) {
                // A silent no-op here would confirm publishes/ACKs that never reached any socket
                // (#124); throwing lets callers buffer, join the in-flight recovery, or fail loudly.
                throw new TransportClosedException('Transport is not connected');
            }

            // When permessage-deflate was negotiated, compress the payload and mark the frame (RSV1).
            $payload = $this->compressionActive ? WebSocketFrameCodec::deflate($bytes) : $bytes;
            $socket->write(WebSocketFrameCodec::encode(
                WebSocketFrameCodec::OP_BINARY,
                $payload,
                rsv1: $this->compressionActive,
            ));
        });
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

                $this->readBuffer .= $chunk;
            }
        });
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
     */
    private function drainDataFrames(): string
    {
        $frames = WebSocketFrameCodec::decode($this->readBuffer);
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
                        $this->fragmenting = true;
                        // permessage-deflate marks RSV1 only on the first frame of the message.
                        $this->fragmentCompressed = $frame['rsv1'];
                        $this->enforceFragmentBound();
                    }
                    break;

                case WebSocketFrameCodec::OP_CONTINUATION:
                    if ($this->fragmenting) {
                        $this->fragmentBuffer .= $frame['payload'];
                        $this->enforceFragmentBound();
                        if ($frame['fin']) {
                            $out .= $this->fragmentCompressed
                                ? WebSocketFrameCodec::inflate($this->fragmentBuffer)
                                : $this->fragmentBuffer;
                            $this->fragmentBuffer = '';
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
        if (strlen($this->fragmentBuffer) <= $this->maxMessageBytes) {
            return;
        }

        $limit = $this->maxMessageBytes;
        $this->fragmentBuffer = '';
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
