<?php

declare(strict_types=1);

namespace Idct\Nats\Protocol;

final class ServerInfo
{
    /**
     * Captures selected server capabilities from the INFO handshake payload.
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
