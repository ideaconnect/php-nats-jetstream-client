<?php

/**
 * Object Store Streaming Upload - upload large objects without buffering.
 *
 * Uses putStream() to pull bytes from a producer callback, re-chunk them to the bucket
 * chunk size, publish them in bounded in-flight windows, and compute the SHA-256 digest
 * incrementally - the streaming counterpart to getToCallback().
 *
 * Mirrors the README "Object Store Streaming Upload" example. Run: php examples/object-store-streaming-upload.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-object-store-streaming-upload'));
$client->connect()->await();

$bucket = 'EX_OBJ_STREAM_UP';
$store = $client->jetStream()->objectStore($bucket);

// Create a temporary source file so the example is self-contained and runnable.
// 256 KiB exceeds the default chunk size, so putStream() re-chunks across many chunks.
$sourcePath = tempnam(sys_get_temp_dir(), 'nats-obj-') ?: throw new RuntimeException('cannot create temp file');
file_put_contents($sourcePath, str_repeat("nats-jetstream-object-store\n", 8192));
$expectedSize = (int) filesize($sourcePath);

try {
    $store->create()->await();

    // putStream() pulls the object's bytes from a producer callback (return the next block, or null at
    // end of stream), so the whole payload is never held in memory. Blocks of any size are re-chunked to
    // the bucket's chunk size, published in bounded in-flight windows, and the SHA-256 digest is computed
    // incrementally -- the streaming counterpart to getToCallback(). It returns ObjectInfo.
    $handle = fopen($sourcePath, 'rb') ?: throw new RuntimeException('cannot open source file');
    try {
        $info = $store->putStream('large.bin', static function () use ($handle): ?string {
            $block = fread($handle, 1 << 16);

            return ($block === '' || $block === false) ? null : $block;
        })->await();
    } finally {
        fclose($handle);
    }

    if ($info->size !== $expectedSize) {
        throw new RuntimeException("unexpected size: expected {$expectedSize}, got {$info->size}");
    }

    echo 'OK object-store-streaming-upload: ' . $info->size . ' bytes in ' . $info->chunks . " chunks\n";
} finally {
    @unlink($sourcePath);

    try {
        $store->deleteBucket()->await();
    } catch (\Throwable) {
        // best-effort cleanup
    }

    $client->disconnect()->await();
}
