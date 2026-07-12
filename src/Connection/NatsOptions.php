<?php

declare(strict_types=1);

namespace IDCT\NATS\Connection;

use Amp\Socket\ClientTlsContext;
use IDCT\NATS\Auth\NonceSignerInterface;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use Psr\Log\LoggerInterface;

/**
 * Configuration container for NATS connection, auth, reconnect, and protocol options.
 */
final class NatsOptions
{
    /** Default server endpoint used when none is configured. */
    public const DEFAULT_SERVER = 'nats://127.0.0.1:4222';

    /**
     * Configures connection and runtime behavior for a NATS client instance.
     *
      * @param list<string> $servers Ordered list of NATS server URLs. The client dials the first server and rotates
      *                              through the list on reconnect attempts.
      * @param string $name Logical client name sent in CONNECT; visible in server monitoring and logs.
      * @param string $inboxPrefix Prefix used to generate request/reply inbox subjects (for example `_INBOX`).
      * @param int $connectTimeoutMs Socket dial timeout in milliseconds for initial and reconnect connection attempts.
      * @param int $requestTimeoutMs Default request/reply timeout in milliseconds when no per-request timeout is provided.
      * @param bool $reconnectEnabled Enables automatic reconnect and subscription replay after transport loss.
      * @param int $maxReconnectAttempts Maximum reconnect attempts before failing the connection lifecycle.
      *                                  Set `0` to disable reconnect retries.
      * @param int $reconnectDelayMs Base reconnect delay in milliseconds before exponential backoff and jitter.
      * @param int $reconnectMaxDelayMs Upper bound in milliseconds for reconnect backoff growth.
      * @param int $reconnectJitterMs Random jitter range in milliseconds added to reconnect delays to avoid thundering herd.
      * @param int|float $pingIntervalSeconds Interval in seconds for protocol PING probes while the
      *                                       connection is open. Fractional values are supported for
      *                                       sub-second intervals (e.g. `0.05`); `0` disables the heartbeat.
      * @param int $maxPingsOut Maximum outstanding PING frames allowed before treating the server as unresponsive.
      * @param bool $verbose Requests server `+OK` confirmations for protocol commands.
      * @param bool $pedantic Enables server-side strict subject and protocol validation checks.
      * @param bool $noEcho Requests that publishes from this connection are not echoed back to its own subscriptions.
      * @param bool $tlsRequired Forces TLS transport mode even when using `nats://` DSNs.
      * @param bool $tlsHandshakeFirst Enables immediate TLS handshake before normal protocol exchange when required.
      * @param string|null $tlsCaFile Optional CA bundle path used to verify server certificates.
      * @param string|null $tlsCertFile Optional client certificate path for mTLS authentication.
      * @param string|null $tlsKeyFile Optional private key path paired with `tlsCertFile`.
      * @param string|null $tlsKeyPassphrase Optional passphrase for encrypted client private keys.
      * @param string|null $tlsPeerName Optional TLS peer name override for certificate hostname validation.
      * @param bool $tlsVerifyPeer Whether TLS peer certificate validation is enforced.
      * @param string|null $token Optional token-based authentication credential (`auth_token` in CONNECT).
      * @param string|null $username Optional username for user/password CONNECT authentication.
      * @param string|null $password Optional password for user/password CONNECT authentication.
      * @param string|null $jwt Optional user JWT for NATS 2.0 decentralized auth mode.
      * @param string|null $nkey Optional public NKey used in JWT or standalone NKey challenge-response auth.
      * @param NonceSignerInterface|null $nonceSigner Signer used to produce Ed25519 signatures for server nonce challenges.
      * @param int $maxPendingMessagesPerSubscription In-memory per-subscription queue cap used by slow-consumer handling.
      *        This is a message COUNT bound only; there is no byte-based bound, so this many large payloads can pin
      *        proportional memory per slow subscription (nats.go's pending limits are both count- and byte-based) (#159).
      * @param SlowConsumerPolicy $slowConsumerPolicy Strategy applied when per-subscription pending queue reaches capacity.
      * @param (\Closure(\IDCT\NATS\Connection\Enum\ConnectionEvent,?\Throwable):void)|null $connectionListener
      *        Optional callback invoked on connection lifecycle transitions (connected, disconnected,
      *        reconnected, closed, discovered-servers, lame-duck). Exceptions thrown by the listener are
      *        swallowed so a faulty handler cannot break the connection runtime.
      * @param (\Closure(\Throwable):void)|null $errorListener Optional callback invoked on asynchronous
      *        errors that do not surface to a specific caller: slow-consumer drops, recoverable server
      *        `-ERR` frames, and transport read failures that trigger reconnect. Exceptions thrown by the
      *        listener are swallowed.
      * @param (\Closure():?string)|null $jwtProvider Optional callback returning the user JWT, invoked on
      *        every (re)connect. Takes precedence over the static {@see $jwt} when set, so short-lived or
      *        rotated JWTs are picked up on reconnect without rebuilding the client. Pair with a
      *        {@see $nonceSigner} that reads the current seed for full credential reload.
      * @param (\Closure():?string)|null $tokenProvider Optional callback returning the auth token, invoked
      *        on every (re)connect. Takes precedence over the static {@see $token} when set.
      * @param int $reconnectBufferSize Max bytes of outbound publishes to buffer while the connection is
      *        down and reconnecting; the buffer is flushed on a successful reconnect. `0` disables
      *        buffering (publishes while disconnected throw). Default 8 MiB, matching nats.go.
      * @param ClientTlsContext|null $tlsContext Escape hatch: a pre-built Amp TLS context used verbatim
      *        for the handshake (in-memory PEM material, ALPN, custom verification, ...), overriding the
      *        individual tls* options. When set, the connection is treated as TLS-required.
      * @param bool $randomizeServers Shuffle the configured server pool once at construction so a fleet
      *        of clients spreads its initial connections across the cluster (no-randomize is the default,
      *        preserving the configured order).
      * @param bool $retryOnFailedInitialConnect Retry the very first connection (up to
      *        maxReconnectAttempts, with backoff) when it fails, even if {@see $reconnectEnabled} is off.
      *        With reconnect enabled the initial connect already retries; this decouples "try hard to
      *        connect once" from "reconnect forever after".
      * @param array<string,string> $webSocketHeaders Extra HTTP headers added to the WebSocket upgrade
      *        request (e.g. cookies, proxy auth) - used by {@see \IDCT\NATS\Transport\WebSocketTransport}.
      * @param bool $webSocketCompression Request permessage-deflate compression on the WebSocket
      *        connection; payloads are (de)compressed only when the server agrees in the handshake.
      * @param LoggerInterface|null $logger Optional PSR-3 logger; lifecycle events (connect/disconnect/
      *        reconnect/close, discovery, lame-duck), reconnect attempts, and async errors are logged to
      *        it. Defaults to a NullLogger (no output).
      * @param int $readChunkSizeBytes Max bytes a single transport socket read may return, applied via
      *        the Amp socket's chunk size (default 128 KiB, up from Amp's 8 KiB). A larger cap divides
      *        the per-chunk syscall + fiber-spawn + parser-push overhead for large payloads (e.g. an
      *        ObjectStore download) without forcing larger reads: the socket still returns only what is
      *        available, so small messages are unaffected (#119). Must be at least 1.
     */
    public function __construct(
        public readonly array $servers = [self::DEFAULT_SERVER],
        public readonly string $name = 'idct-php-nats-client',
        public readonly string $inboxPrefix = '_INBOX',
        public readonly int $connectTimeoutMs = 5_000,
        public readonly int $requestTimeoutMs = 10_000,
        public readonly bool $reconnectEnabled = true,
        public readonly int $maxReconnectAttempts = 10,
        public readonly int $reconnectDelayMs = 100,
        public readonly int $reconnectMaxDelayMs = 10_000,
        public readonly int $reconnectJitterMs = 50,
        public readonly int|float $pingIntervalSeconds = 30,
        public readonly int $maxPingsOut = 2,
        public readonly bool $verbose = false,
        public readonly bool $pedantic = false,
        public readonly bool $noEcho = false,
        public readonly bool $tlsRequired = false,
        public readonly bool $tlsHandshakeFirst = false,
        public readonly ?string $tlsCaFile = null,
        public readonly ?string $tlsCertFile = null,
        public readonly ?string $tlsKeyFile = null,
        public readonly ?string $tlsKeyPassphrase = null,
        public readonly ?string $tlsPeerName = null,
        public readonly bool $tlsVerifyPeer = true,
        public readonly ?string $token = null,
        public readonly ?string $username = null,
        public readonly ?string $password = null,
        public readonly ?string $jwt = null,
        public readonly ?string $nkey = null,
        public readonly ?NonceSignerInterface $nonceSigner = null,
        public readonly int $maxPendingMessagesPerSubscription = 1_024,
        public readonly SlowConsumerPolicy $slowConsumerPolicy = SlowConsumerPolicy::DropOldest,
        public readonly ?\Closure $connectionListener = null,
        public readonly ?\Closure $errorListener = null,
        public readonly ?\Closure $jwtProvider = null,
        public readonly ?\Closure $tokenProvider = null,
        public readonly int $reconnectBufferSize = 8_388_608,
        public readonly ?ClientTlsContext $tlsContext = null,
        public readonly bool $randomizeServers = false,
        public readonly bool $retryOnFailedInitialConnect = false,
        public readonly array $webSocketHeaders = [],
        public readonly bool $webSocketCompression = false,
        public readonly ?LoggerInterface $logger = null,
        public readonly int $readChunkSizeBytes = 131_072,
    ) {
        // Fail fast on values that have no valid meaning, rather than misbehaving later. Note that
        // pingIntervalSeconds <= 0 (disables the heartbeat) and an empty servers list (falls back to
        // the default server) are intentionally allowed.
        if ($connectTimeoutMs <= 0) {
            throw new \InvalidArgumentException('connectTimeoutMs must be greater than zero');
        }

        if ($requestTimeoutMs <= 0) {
            throw new \InvalidArgumentException('requestTimeoutMs must be greater than zero');
        }

        if ($maxPendingMessagesPerSubscription < 1) {
            throw new \InvalidArgumentException('maxPendingMessagesPerSubscription must be at least 1');
        }

        if ($readChunkSizeBytes < 1) {
            throw new \InvalidArgumentException('readChunkSizeBytes must be at least 1');
        }

        foreach ([
            'maxReconnectAttempts' => $maxReconnectAttempts,
            'reconnectDelayMs' => $reconnectDelayMs,
            'reconnectMaxDelayMs' => $reconnectMaxDelayMs,
            'reconnectJitterMs' => $reconnectJitterMs,
            'maxPingsOut' => $maxPingsOut,
        ] as $field => $value) {
            if ($value < 0) {
                throw new \InvalidArgumentException(sprintf('%s must not be negative', $field));
            }
        }
    }

    /**
     * Returns the preferred server endpoint used for initial connection attempts.
     */
    public function firstServer(): string
    {
        return $this->servers[0] ?? self::DEFAULT_SERVER;
    }
}
