<?php

/**
 * KeyValue Bucket - the JetStream KV store.
 *
 * Demonstrates put / get / optimistic-concurrency update / getAll / getStatus / delete /
 * purge on a KV bucket, plus a live watch() that observes updates and their revisions.
 *
 * Mirrors the README "KeyValue Bucket" example. Run: php examples/keyvalue-bucket.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\CancelledException;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\JetStream\KeyValue\KeyValueEntry;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-keyvalue-bucket'));
$client->connect()->await();

$bucket = 'EX_KV_CFG';
$js = $client->jetStream();
$kv = $js->keyValue($bucket);

try {
    $kv->create()->await();

    // Register the watcher BEFORE the writes it should observe: watch() delivers live updates only
    // (deliver_policy=new) and does not replay pre-existing values. Each entry carries its revision.
    $observed = [];
    $watchSid = $kv->watch(static function (KeyValueEntry $entry) use (&$observed): void {
        $observed[] = $entry->key . ':' . ($entry->value ?? '<deleted>') . ' (rev ' . ($entry->revision ?? 0) . ')';
    }, 'theme')->await();

    $kv->put('theme', 'dark')->await();

    $entry = $kv->get('theme')->await();
    $current = $entry?->value ?? '<none>';

    if ($entry !== null) {
        // Optimistic concurrency: update only succeeds when the expected revision matches.
        $kv->update('theme', 'light', $entry->revision ?? 1)->await();
    }

    // getAll() returns a map of key => latest value.
    $all = $kv->getAll()->await();
    $allTheme = $all['theme'] ?? '';

    // getStatus() exposes the backing stream name and message counts.
    $status = $kv->getStatus()->await();
    $streamName = (string) $status['stream'];

    $kv->delete('theme')->await();
    $kv->purge('theme')->await();

    // Drive delivery so the watcher receives the buffered updates, bounded so it cannot block forever.
    try {
        $cancellation = new TimeoutCancellation(2.0);
        while (true) {
            $client->processIncoming($cancellation)->await();
        }
    } catch (CancelledException) {
        // Watch window elapsed; we have drained whatever live updates arrived.
    }

    // A watch rides an ordered consumer, so stop it through the context rather than with
    // $client->unsubscribe($watchSid): a recreate rotates the internal sid, which makes the sid
    // returned by watch() stale after the first recreate. The context resolves the CURRENT sid and
    // also deletes the server-side consumer a plain unsubscribe would strand.
    $js->stopOrderedConsumer($watchSid)->await();

    echo 'OK keyvalue-bucket: stream ' . $streamName . ', updated ' . $current . '->' . $allTheme
        . ', watcher saw ' . count($observed) . " events\n";
} finally {
    try {
        $kv->deleteBucket()->await();
    } catch (\Throwable) {
        // best-effort cleanup
    }

    $client->disconnect()->await();
}
