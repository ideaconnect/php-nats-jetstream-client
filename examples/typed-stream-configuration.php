<?php

/**
 * Typed Stream Configuration - configure with enums, not magic strings.
 *
 * Creates a stream and consumer using the typed configuration enums (RetentionPolicy,
 * StorageBackend, DiscardPolicy, DeliverPolicy, AckPolicy, ReplayPolicy) instead of
 * raw string values.
 *
 * Mirrors the README "Typed Stream Configuration" example. Run: php examples/typed-stream-configuration.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\JetStream\Enum\AckPolicy;
use IDCT\NATS\JetStream\Enum\DeliverPolicy;
use IDCT\NATS\JetStream\Enum\DiscardPolicy;
use IDCT\NATS\JetStream\Enum\ReplayPolicy;
use IDCT\NATS\JetStream\Enum\RetentionPolicy;
use IDCT\NATS\JetStream\Enum\StorageBackend;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-typed-stream-configuration'));
$client->connect()->await();

$stream = 'EX_TYPED_ORDERS';
$consumer = 'EX_TYPED_PROC';
$js = $client->jetStream();

try {
    // Create stream with typed configuration.
    $info = $js->createStream($stream, ['ex.typed.orders.>'], [
        'retention' => RetentionPolicy::Limits->value,
        'storage' => StorageBackend::Memory->value,
        'discard' => DiscardPolicy::Old->value,
        'max_msgs' => 100_000,
        'max_bytes' => 50 * 1024 * 1024,
        'max_age' => 86_400_000_000_000,  // 24h in nanoseconds
        'num_replicas' => 1,
        'duplicate_window' => 120_000_000_000,  // 2 min in nanoseconds
    ])->await();

    // Create consumer with typed configuration.
    $js->createConsumer($stream, $consumer, 'ex.typed.orders.created', [
        'deliver_policy' => DeliverPolicy::New->value,
        'ack_policy' => AckPolicy::Explicit->value,
        'replay_policy' => ReplayPolicy::Instant->value,
        'max_deliver' => 5,
        'max_ack_pending' => 1000,
        'ack_wait' => 30_000_000_000,  // 30s in nanoseconds
    ])->await();

    $retention = $info->raw['config']['retention'] ?? '?';
    $storage = $info->raw['config']['storage'] ?? '?';

    echo "OK typed-stream-configuration: stream {$info->name} retention={$retention} storage={$storage}, consumer {$consumer} created\n";
} finally {
    try {
        $js->deleteConsumer($stream, $consumer)->await();
    } catch (\Throwable) {
        // best-effort cleanup
    }

    try {
        $js->deleteStream($stream)->await();
    } catch (\Throwable) {
        // best-effort cleanup
    }

    $client->disconnect()->await();
}
