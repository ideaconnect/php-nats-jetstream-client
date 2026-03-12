<?php

declare(strict_types=1);

namespace IDCT\NATS\Protocol;

/**
 * Parsed server INFO payload describing capabilities and limits.
 */
final class ServerInfo
{
    /**
     * Captures selected server capabilities from the INFO handshake payload.
     *
     * @param string $serverId Unique server identifier from INFO `server_id`.
     * @param string $serverName Human-readable server name from INFO `server_name`.
     * @param string $version Server version string from INFO `version`.
     * @param bool $jetStreamEnabled Whether INFO advertises JetStream support.
     * @param int $maxPayload Maximum accepted payload bytes (`max_payload`) for PUB/HPUB commands.
     * @param bool $headersSupported Whether server supports NATS headers (`headers` capability).
     * @param string|null $nonce Optional nonce challenge from INFO used for JWT/NKey signature auth.
     */
    public function __construct(
        public readonly string $serverId,
        public readonly string $serverName,
        public readonly string $version,
        public readonly bool $jetStreamEnabled,
        public readonly int $maxPayload,
        public readonly bool $headersSupported,
        public readonly ?string $nonce = null,
    ) {
    }

    /**
     * Creates a typed ServerInfo object from raw INFO JSON data.
     *
     * @param array<string,mixed> $payload
     */
    public static function fromInfoPayload(array $payload): self
    {
        return new self(
            serverId: (string) ($payload['server_id'] ?? ''),
            serverName: (string) ($payload['server_name'] ?? ''),
            version: (string) ($payload['version'] ?? ''),
            jetStreamEnabled: (bool) ($payload['jetstream'] ?? false),
            maxPayload: (int) ($payload['max_payload'] ?? 0),
            headersSupported: (bool) ($payload['headers'] ?? false),
            nonce: isset($payload['nonce']) ? (string) $payload['nonce'] : null,
        );
    }
}
