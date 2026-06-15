<?php

/**
 * JetStream Stream and Durable Consumer - the basic JetStream setup.
 *
 * Creates a stream and a durable (explicit-ack) consumer, publishes a message, and
 * reads the resulting PubAck (the stream it landed in and its stream sequence).
 *
 * Mirrors the README "JetStream Stream and Durable Consumer" example. Run: php examples/jetstream-stream-and-consumer.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-jetstream-stream-and-consumer'));
$client->connect()->await();

$stream = 'EX_JS_ORDERS';
$consumer = 'EX_JS_PROC';

$js = $client->jetStream();

try {
    $js->createStream($stream, ['ex.js.orders.>'])->await();

    // If you omit ack_policy, helper methods default it to explicit.
    // Pass ack_policy explicitly when you need none/all.
    $js->createConsumer($stream, $consumer, 'ex.js.orders.created')->await();

    $ack = $js->publish('ex.js.orders.created', '{"id":123}')->await();

    echo "OK jetstream-stream-and-consumer: stored in {$ack->stream} at seq {$ack->seq}\n";
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
