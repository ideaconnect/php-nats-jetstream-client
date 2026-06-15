<?php

/**
 * Pull Consumer Batching / Iteration - the fluent pull iterator.
 *
 * Uses the fluent pullConsumer() builder (setBatching / setExpiresMs / setIterations)
 * with handle() to process messages in bounded batches across a bounded number of
 * pull iterations; the iterator wraps fetchBatch().
 *
 * Mirrors the README "Pull Consumer Batching/Iteration" example. Run: php examples/pull-consumer-batching-iteration.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\JetStream\JetStreamContext;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-pull-consumer-batching-iteration'));
$client->connect()->await();

$stream = 'EX_PULL_ITER_ORDERS';
$consumer = 'EX_PULL_ITER_PROC';

$js = $client->jetStream();

try {
    // Set up the stream/consumer and seed a few messages so the iterator has work to do.
    $js->createStream($stream, ['ex.pulliter.orders.>'])->await();
    $js->createConsumer($stream, $consumer, 'ex.pulliter.orders.>')->await();

    for ($i = 0; $i < 7; $i++) {
        $js->publish('ex.pulliter.orders.created', "order $i")->await();
    }

    // Process messages in batches of 10, up to 5 iterations.
    // The iterator wraps fetchBatch(); a low expiry keeps total runtime short once drained.
    $totalProcessed = $js->pullConsumer($stream, $consumer)
        ->setBatching(10)
        ->setExpiresMs(1000)
        ->setIterations(5)
        ->handle(function (NatsMessage $msg, JetStreamContext $js): void {
            $js->ack($msg)->await();
        })->await();

    echo "OK pull-consumer-batching-iteration: processed {$totalProcessed} message(s) across bounded iterations\n";
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
