<?php

/**
 * Publish / Subscribe - the most basic core messaging round-trip.
 *
 * Subscribes to a subject, publishes one JSON message to it, then drives
 * processIncoming() until the subscription callback receives the message.
 * Demonstrates subscribe()/publish() and the manual read pump on one connection.
 *
 * Mirrors the README "Connect and Publish/Subscribe" example. Run: php examples/publish-subscribe.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:14222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-publish-subscribe'));
$client->connect()->await();

try {
    $received = [];

    $sid = $client->subscribe('ex.pubsub.orders.created', static function (NatsMessage $message) use (&$received): void {
        $received[] = $message->payload;
    })->await();

    // Ensure the SUB has reached the server before we publish, so the message is not missed.
    $client->flush()->await();

    $client->publish('ex.pubsub.orders.created', json_encode(['id' => 123], JSON_THROW_ON_ERROR))->await();

    // Drive delivery with a bounded poll loop and a monotonic deadline.
    $deadline = hrtime(true) / 1e9 + 5.0;
    while ($received === [] && hrtime(true) / 1e9 < $deadline) {
        try {
            $client->processIncoming(new TimeoutCancellation(0.5))->await();
        } catch (\Amp\CancelledException) {
            // No frame this cycle; keep polling until the deadline.
        }
    }

    $client->unsubscribe($sid)->await();

    if ($received === []) {
        throw new \RuntimeException('No message delivered within the deadline');
    }

    echo 'OK publish-subscribe: received ' . count($received) . ' message(s), first=' . $received[0] . PHP_EOL;
} finally {
    $client->disconnect()->await();
}
