<?php

/**
 * Atomic Batch Publish - commit many messages atomically (NATS 2.12+).
 *
 * Stages several messages with the fluent $js->batch()->add(...) API and commits them
 * atomically in one operation. Requires a stream created with allow_atomic; on a
 * pre-2.12 server it skips cleanly via UnsupportedFeatureException.
 *
 * Mirrors the README "Atomic Batch Publish" example. Run: php examples/atomic-batch-publish.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\UnsupportedFeatureException;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-atomic-batch-publish'));
$client->connect()->await();

$stream = 'EX_ATOMIC_BATCH';
$created = false;

try {
    $js = $client->jetStream();

    try {
        $js->deleteStream($stream)->await();
    } catch (\Throwable) {
        // Stream may not exist yet; ignore.
    }

    // Atomic batches require a NATS 2.12+ server and a stream created with allow_atomic.
    // On a pre-2.12 server this fails fast with UnsupportedFeatureException.
    try {
        $js->createStream($stream, ['ex_atomic_batch.>'], ['allow_atomic' => true])->await();
        $created = true;
    } catch (UnsupportedFeatureException $e) {
        echo 'OK atomic-batch-publish: skipped, server does not support atomic batches ('
            . $e->getMessage() . ')' . PHP_EOL;

        return;
    }

    // Stage messages with the fluent API, then commit them all atomically.
    $batch = $js->batch()
        ->add('ex_atomic_batch.created', '{"id":1}')
        ->add('ex_atomic_batch.created', '{"id":2}')
        ->add('ex_atomic_batch.created', '{"id":3}');

    $staged = $batch->count();
    $batchId = $batch->batchId();

    $ack = $batch->commit()->await();

    echo 'OK atomic-batch-publish: staged ' . $staged . ' in batch ' . $batchId
        . ', committed ' . $ack->batchCount . ' (batch ' . ($ack->batchId ?? 'n/a') . ')' . PHP_EOL;
} finally {
    if ($created) {
        try {
            $js->deleteStream($stream)->await();
        } catch (\Throwable) {
            // Best-effort cleanup.
        }
    }
    $client->disconnect()->await();
}
