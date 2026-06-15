<?php

/**
 * Object Store Streaming to Callback - download chunk-by-chunk.
 *
 * Uses getToCallback() to stream a stored object to a callback one chunk at a time
 * (the whole object is never buffered in memory) and verifies the SHA-256 digest
 * incrementally after the final chunk.
 *
 * Mirrors the README "Object Store Streaming to Callback" example. Run: php examples/object-store-streaming-to-callback.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-object-store-streaming-to-callback'));
$client->connect()->await();

$bucket = 'EX_OBJ_STREAM_CB';
$store = $client->jetStream()->objectStore($bucket);

try {
    $store->create()->await();
    $store->put('logo.txt', 'hello-object')->await();

    // getToCallback streams the object chunk-by-chunk: the callback is invoked once per stored
    // chunk as it is downloaded (the whole object is never buffered in memory), and the SHA-256
    // digest is verified incrementally after the final chunk. It returns ObjectInfo|null.
    $assembled = '';
    $info = $store->getToCallback('logo.txt', static function (string $chunk) use (&$assembled): void {
        $assembled .= $chunk;
    })->await();

    $name = $info?->name ?? '<none>';

    echo 'OK object-store-streaming-to-callback: streamed ' . $name . ', received "' . $assembled . "\"\n";
} finally {
    try {
        $store->deleteBucket()->await();
    } catch (\Throwable) {
        // best-effort cleanup
    }

    $client->disconnect()->await();
}
