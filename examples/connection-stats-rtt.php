<?php

/**
 * Connection Statistics and RTT - client counters + latency.
 *
 * Reads the synchronous statistics() snapshot (in/out message + byte counts and
 * reconnects) and measures round-trip latency to the server with rtt().
 *
 * Mirrors the README "Connection Statistics and RTT" example. Run: php examples/connection-stats-rtt.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-connection-stats-rtt'));
$client->connect()->await();

try {
    $client->publish('ex.stats.events.orders', json_encode(['id' => 1], JSON_THROW_ON_ERROR))->await();

    // statistics() is a synchronous immutable snapshot (no await).
    $stats = $client->statistics();
    echo "out: {$stats->outMsgs} msgs / {$stats->outBytes} bytes" . PHP_EOL;
    echo "in:  {$stats->inMsgs} msgs / {$stats->inBytes} bytes" . PHP_EOL;
    echo "reconnects: {$stats->reconnects}" . PHP_EOL;

    // rtt() resolves to a float in seconds.
    $rttSeconds = $client->rtt()->await();
    $rttMs = round($rttSeconds * 1000, 2);
    echo 'rtt: ' . $rttMs . ' ms' . PHP_EOL;

    echo 'OK connection-stats-rtt: out=' . $stats->outMsgs . ' msgs, rtt=' . $rttMs . 'ms' . PHP_EOL;
} finally {
    $client->disconnect()->await();
}
