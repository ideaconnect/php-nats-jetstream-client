<?php

/**
 * Graceful Drain - flush in-flight work before closing.
 *
 * Publishes a message that is in flight, then calls drain(): it unsubscribes all
 * subscriptions, delivers the pending messages to their handlers, and closes the
 * connection cleanly (the right shutdown for workers).
 *
 * Mirrors the README "Graceful Drain" example. Run: php examples/graceful-drain.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-graceful-drain'));
$client->connect()->await();

try {
    $delivered = [];

    $client->subscribe('events.>', static function (NatsMessage $message) use (&$delivered): void {
        $delivered[] = $message->payload;
    })->await();

    // Ensure the SUB is registered server-side before publishing so the buffered message exists.
    $client->flush()->await();

    // Publish a message that is in flight when we begin draining.
    $client->publish('events.order', '{"id":1}')->await();

    // Gracefully drain: unsubscribes all SIDs, delivers pending messages, then closes the connection.
    $client->drain()->await();

    echo 'OK graceful-drain: delivered ' . count($delivered) . ' buffered message(s) before close' . PHP_EOL;
} finally {
    // drain() already closes the connection; disconnect() is a best-effort no-op afterward.
    try {
        $client->disconnect()->await();
    } catch (Throwable) {
        // Connection already closed by drain().
    }
}
