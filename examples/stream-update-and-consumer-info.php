<?php

/**
 * Stream Update and Consumer Info - reconfigure + introspect.
 *
 * Widens a stream's accepted subjects with updateStream(), creates a consumer, and
 * reads it back with getConsumer() to inspect its ConsumerInfo.
 *
 * Mirrors the README "JetStream Stream Update and Consumer Info" example. Run: php examples/stream-update-and-consumer-info.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-stream-update-and-consumer-info'));
$client->connect()->await();

$stream = 'EX_JS_UPD_ORDERS';
$consumer = 'EX_JS_UPD_PROC';

$js = $client->jetStream();

try {
    $js->createStream($stream, ['ex.upd.orders.created'])->await();

    // Update the stream config to widen the set of accepted subjects.
    $js->updateStream($stream, [
        'subjects' => ['ex.upd.orders.created', 'ex.upd.orders.updated'],
    ])->await();

    $js->createConsumer($stream, $consumer, 'ex.upd.orders.created')->await();
    $consumerInfo = $js->getConsumer($stream, $consumer)->await();

    echo "OK stream-update-and-consumer-info: consumer {$consumerInfo->name} on stream {$consumerInfo->streamName}\n";
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
