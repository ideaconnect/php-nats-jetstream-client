<?php

declare(strict_types=1);

namespace IDCT\NATS\Connection;

use IDCT\NATS\Auth\NonceSignerInterface;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;

/**
 * Configuration container for NATS connection, auth, reconnect, and protocol options.
 */
final class NatsOptions
{
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
      * @param int $pingIntervalSeconds Interval in seconds for protocol PING probes while the connection is open.
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
      * @param SlowConsumerPolicy $slowConsumerPolicy Strategy applied when per-subscription pending queue reaches capacity.
     */
    public function __construct(
        public readonly array $servers = ['nats://127.0.0.1:4222'],
        public readonly string $name = 'idct-php-nats-client',
        public readonly string $inboxPrefix = '_INBOX',
        public readonly int $connectTimeoutMs = 5_000,
        public readonly int $requestTimeoutMs = 10_000,
        public readonly bool $reconnectEnabled = true,
        public readonly int $maxReconnectAttempts = 10,
        public readonly int $reconnectDelayMs = 100,
        public readonly int $reconnectMaxDelayMs = 10_000,
        public readonly int $reconnectJitterMs = 50,
        public readonly int $pingIntervalSeconds = 30,
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
    ) {
    }

    /**
     * Returns the preferred server endpoint used for initial connection attempts.
     */
    public function firstServer(): string
    {
        return $this->servers[0] ?? 'nats://127.0.0.1:4222';
    }
}
