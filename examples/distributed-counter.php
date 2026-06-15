<?php

/**
 * Distributed Counter - CRDT counters on a stream (NATS 2.12+).
 *
 * On a stream with allow_msg_counter + allow_direct, atomically increments a subject
 * with incrementCounter() and reads the running total with counterValue() (served by
 * Direct Get).
 *
 * Mirrors the README "Distributed Counter" example. Run: php examples/distributed-counter.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-distributed-counter'));
$client->connect()->await();

$stream = 'EX_COUNTERS';
$js = $client->jetStream();

try {
    // The backing stream must enable allow_msg_counter (NATS 2.12+); allow_direct is required because
    // counterValue() reads the current total via Direct Get.
    $js->createStream($stream, ['ex.counters.>'], [
        'allow_msg_counter' => true,
        'allow_direct' => true,
    ])->await();

    // Atomically increment; the new total is returned as a string.
    $total = $js->incrementCounter('ex.counters.visits', '+5')->await();
    // $total === "5"

    $js->incrementCounter('ex.counters.visits', '+3')->await();
    $js->incrementCounter('ex.counters.visits', '-1')->await();

    // Read the current value via Direct Get ("0" if nothing stored yet).
    $current = $js->counterValue($stream, 'ex.counters.visits')->await();

    if ($current !== '7') {
        throw new RuntimeException("unexpected counter value: expected 7, got {$current}");
    }

    echo "OK distributed-counter: first increment {$total}, current total {$current}\n";
} finally {
    try {
        $js->deleteStream($stream)->await();
    } catch (\Throwable) {
        // best-effort cleanup
    }

    $client->disconnect()->await();
}
