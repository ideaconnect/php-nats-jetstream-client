<?php

/**
 * JetStream Pull Consumer (Fetch + ACK) - pull one, acknowledge it.
 *
 * Uses fetchNext() to pull a single message and ack() it, then demonstrates ackSync()
 * (double-ack): it sends the ACK as a request and waits for the server to confirm
 * durable acknowledgement.
 *
 * Mirrors the README "JetStream Pull Consumer (Fetch + ACK)" example. Run: php examples/pull-consumer-fetch-ack.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-pull-consumer-fetch-ack'));
$client->connect()->await();

$stream = 'EX_JS_PULL_ORDERS';
$consumer = 'EX_JS_PULL';

$js = $client->jetStream();

try {
    $js->createStream($stream, ['ex.pull.orders.created'])->await();
    $js->createConsumer($stream, $consumer, 'ex.pull.orders.created')->await();
    $js->publish('ex.pull.orders.created', '{"id":123}')->await();

    // Fetch a single message with a 3s pull expiry, then acknowledge it.
    $message = $js->fetchNext($stream, $consumer, 3000)->await();
    $js->ack($message)->await();

    // Double-ack: block until the server confirms the ACK (250ms confirmation timeout).
    // ackSync sends +ACK as a request and waits for durable confirmation; shown here on a
    // second message so the first ack() and this ackSync() both have something to acknowledge.
    $js->publish('ex.pull.orders.created', '{"id":124}')->await();
    $second = $js->fetchNext($stream, $consumer, 3000)->await();
    $js->ackSync($second, 250)->await();

    echo "OK pull-consumer-fetch-ack: fetched and acked '{$message->payload}' then ackSync'd '{$second->payload}'\n";
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
