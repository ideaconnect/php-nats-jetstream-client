<?php

/**
 * Fetch Batch - pull several messages in one request.
 *
 * Publishes several messages, then retrieves up to N of them in a single pull with
 * fetchBatch() (bounded by a pull expiry) and acks each one.
 *
 * Mirrors the README "Fetch Batch" example. Run: php examples/fetch-batch.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-fetch-batch'));
$client->connect()->await();

$stream = 'EX_FETCH_BATCH_LOGS';
$consumer = 'EX_FETCH_BATCH';

$js = $client->jetStream();

try {
    $js->createStream($stream, ['ex.fetchbatch.logs.>'])->await();
    $js->createConsumer($stream, $consumer, 'ex.fetchbatch.logs.>')->await();

    for ($i = 0; $i < 5; $i++) {
        $js->publish('ex.fetchbatch.logs.app', "log entry $i")->await();
    }

    // Fetch up to 5 messages in one batch (3s pull expiry).
    $messages = $js->fetchBatch($stream, $consumer, batch: 5, expiresMs: 3000)->await();
    foreach ($messages as $message) {
        $js->ack($message)->await();
    }

    echo 'OK fetch-batch: fetched and acked ' . count($messages) . " message(s) in one batch\n";
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
