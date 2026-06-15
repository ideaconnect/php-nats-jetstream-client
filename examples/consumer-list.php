<?php

/**
 * Consumer List - enumerate a stream's consumers.
 *
 * Creates two durable consumers on a stream and lists them with listConsumers(),
 * reading the returned ConsumerInfo fields (name, push/pull, ...).
 *
 * Mirrors the README "Consumer List" example. Run: php examples/consumer-list.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-consumer-list'));
$client->connect()->await();

$stream = 'EX_CONSUMER_LIST_ORDERS';
$procA = 'EX_CL_PROC_A';
$procB = 'EX_CL_PROC_B';

$js = $client->jetStream();

try {
    $js->createStream($stream, ['ex.consumerlist.orders.>'])->await();
    $js->createConsumer($stream, $procA, 'ex.consumerlist.orders.created')->await();
    $js->createConsumer($stream, $procB, 'ex.consumerlist.orders.updated')->await();

    // listConsumers() returns ConsumerInfo objects (name, push, ...).
    $consumers = $js->listConsumers($stream)->await();
    $names = [];
    foreach ($consumers as $consumer) {
        $names[] = $consumer->name . ' (push=' . ($consumer->push ? 'yes' : 'no') . ')';
    }

    echo 'OK consumer-list: ' . count($consumers) . ' consumer(s): ' . implode(', ', $names) . "\n";
} finally {
    try {
        $js->deleteConsumer($stream, $procA)->await();
    } catch (\Throwable) {
        // best-effort cleanup
    }

    try {
        $js->deleteConsumer($stream, $procB)->await();
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
