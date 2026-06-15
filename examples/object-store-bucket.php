<?php

/**
 * Object Store Bucket - store and retrieve whole objects.
 *
 * Demonstrates put / info / get (digest-verified) / list / delete on an Object Store
 * bucket, reading the returned ObjectInfo (name, SHA-256 digest, ...).
 *
 * Mirrors the README "Object Store Bucket" example. Run: php examples/object-store-bucket.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-object-store-bucket'));
$client->connect()->await();

$bucket = 'EX_OBJ_ASSETS';
$store = $client->jetStream()->objectStore($bucket);

try {
    $store->create()->await();

    // put() returns the ObjectInfo for the stored object.
    $stored = $store->put('logo.txt', 'hello-object', ['content-type' => 'text/plain'])->await();
    $name = $stored->name;

    // info() returns ObjectInfo|null with the verified SHA-256 digest.
    $info = $store->info('logo.txt')->await();
    $digest = $info?->digest ?? '<none>';

    // get() returns ObjectData|null; ->data holds the reassembled, digest-verified bytes.
    $objectData = $store->get('logo.txt')->await();
    $payload = $objectData?->data ?? '';

    // list() returns ObjectInfo[] for the (non-deleted) objects in the bucket.
    $objects = $store->list()->await();
    $names = [];
    foreach ($objects as $object) {
        $names[] = $object->name;
    }

    $store->delete('logo.txt')->await();

    echo 'OK object-store-bucket: stored ' . $name . ', payload "' . $payload . '", digest ' . $digest
        . ', listed [' . implode(',', $names) . "]\n";
} finally {
    try {
        $store->deleteBucket()->await();
    } catch (\Throwable) {
        // best-effort cleanup
    }

    $client->disconnect()->await();
}
