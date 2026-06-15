<?php

/**
 * Polling Subscribe - synchronous consumption with SubscriptionQueue.
 *
 * Uses subscribeQueue() to get a SubscriptionQueue, then pulls messages with the
 * blocking next() (single, returns null on timeout) and fetchAll() (batch within
 * the configured timeout window).
 *
 * Mirrors the README "Polling Subscribe (SubscriptionQueue)" example. Run: php examples/polling-subscribe.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-polling-subscribe'));
$client->connect()->await();

$queue = null;

try {
    // subscribeQueue() returns a SubscriptionQueue for polling-style consumption.
    $queue = $client->subscribeQueue('events.>', queue: 'workers')->await();
    $queue->setTimeout(5.0);

    // Ensure the SUB is registered server-side before we publish (avoid losing messages).
    $client->flush()->await();

    // Publish a few messages for the poller to pick up.
    $client->publish('events.created', 'one')->await();
    $client->publish('events.created', 'two')->await();
    $client->publish('events.created', 'three')->await();

    // Blocking fetch - waits up to the configured timeout, returns null on timeout.
    $first = $queue->next();

    // Batch fetch - collects up to 10 messages within the timeout window.
    $rest = $queue->fetchAll(limit: 10);

    $total = ($first !== null ? 1 : 0) + count($rest);

    if ($total === 0) {
        throw new RuntimeException('No messages polled from the SubscriptionQueue within the deadline');
    }

    $firstPayload = $first?->payload ?? '(none)';

    echo 'OK polling-subscribe: first="' . $firstPayload . '" total=' . $total . PHP_EOL;
} finally {
    if ($queue !== null) {
        try {
            $queue->unsubscribe()->await();
        } catch (Throwable) {
            // Best-effort cleanup.
        }
    }

    $client->disconnect()->await();
}
