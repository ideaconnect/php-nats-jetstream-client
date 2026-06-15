<?php

/**
 * Queue Group Subscribe - load-balanced delivery across workers.
 *
 * Subscribes with a queue group so the server delivers each message to exactly one
 * member of the group, then publishes a message and drives delivery to a worker.
 *
 * Mirrors the README "Queue Group Subscribe" example. Run: php examples/queue-group-subscribe.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-queue-group-subscribe'));
$client->connect()->await();

$sid = null;

try {
    $received = null;

    // Subscribe with a queue group for load-balanced delivery across workers: only one member of
    // the "workers" group receives each message.
    $sid = $client->subscribe('tasks.process', static function (NatsMessage $message) use (&$received): void {
        $received = $message->payload;
    }, queue: 'workers')->await();

    // Ensure the SUB is registered server-side before publishing (avoid losing the message).
    $client->flush()->await();

    $client->publish('tasks.process', '{"job":"build"}')->await();

    // Drive delivery with a bounded poll loop and a monotonic deadline.
    $deadline = hrtime(true) / 1e9 + 5.0;
    while ($received === null && hrtime(true) / 1e9 < $deadline) {
        $client->processIncoming()->await();
    }

    if ($received === null) {
        throw new RuntimeException('No message delivered to the queue group within the deadline');
    }

    echo 'OK queue-group-subscribe: worker received "' . $received . '"' . PHP_EOL;
} finally {
    if ($sid !== null) {
        try {
            $client->unsubscribe($sid)->await();
        } catch (Throwable) {
            // Best-effort cleanup.
        }
    }

    $client->disconnect()->await();
}
