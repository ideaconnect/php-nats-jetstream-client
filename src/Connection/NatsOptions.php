<?php

declare(strict_types=1);

namespace Idct\Nats\Connection;

use Idct\Nats\Auth\NonceSignerInterface;

enum SlowConsumerPolicy: string
{
    /** Drop the oldest queued message and keep newer arrivals. */
    case DropOldest = 'drop_oldest';
    /** Drop the newest incoming message when queue is full. */
    case DropNewest = 'drop_newest';
    /** Raise an error when queue capacity is exceeded. */
    case Error = 'error';
}

final class NatsOptions
{
    /**
     * Configures connection and runtime behavior for a NATS client instance.
     *
     * @param list<string> $servers
        * @param bool $reconnectEnabled Enables automatic reconnect attempts after transport failures.
        * @param int $maxReconnectAttempts Maximum reconnect attempts before closing; use 0 to disable retries.
        * @param int $reconnectDelayMs Base reconnect delay in milliseconds.
        * @param int $reconnectJitterMs Random jitter in milliseconds added to reconnect delay.
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
        public readonly int $reconnectJitterMs = 50,
        public readonly int $pingIntervalSeconds = 30,
        public readonly int $maxPingsOut = 2,
        public readonly bool $verbose = false,
        public readonly bool $pedantic = false,
        public readonly bool $tlsRequired = false,
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
