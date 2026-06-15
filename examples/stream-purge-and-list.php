<?php

/**
 * Stream Purge and List - purge messages, enumerate streams.
 *
 * Purges a stream by subject filter and then fully with purgeStream() (reading the
 * purged counts), and enumerates all streams on the server with listStreams().
 *
 * Mirrors the README "Stream Purge and List" example. Run: php examples/stream-purge-and-list.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-stream-purge-and-list'));
$client->connect()->await();

$stream = 'EX_PURGE_LIST';

try {
    $js = $client->jetStream();

    // Fresh stream for this example.
    try {
        $js->deleteStream($stream)->await();
    } catch (\Throwable) {
        // Stream may not exist yet; ignore.
    }

    $js->createStream($stream, ['ex_purge_list.>'])->await();
    $js->publish('ex_purge_list.app', 'entry 1')->await();
    $js->publish('ex_purge_list.app', 'entry 2')->await();
    $js->publish('ex_purge_list.sys', 'entry 3')->await();

    // Purge by subject filter: removes only the two ex_purge_list.app messages.
    $byFilter = $js->purgeStream($stream, ['filter' => 'ex_purge_list.app'])->await();

    // Purge everything that remains.
    $purgedAll = $js->purgeStream($stream)->await();

    // List all streams and confirm ours is present.
    $streams = $js->listStreams()->await();
    $names = [];
    foreach ($streams as $info) {
        $names[] = $info->name;
    }
    $found = in_array($stream, $names, true) ? 'yes' : 'no';

    echo 'OK stream-purge-and-list: purged ' . $byFilter['purged']
        . ' by filter, ' . $purgedAll['purged'] . ' remaining, '
        . count($streams) . ' streams listed (found ' . $stream . ': ' . $found . ')' . PHP_EOL;
} finally {
    try {
        $js->deleteStream($stream)->await();
    } catch (\Throwable) {
        // Best-effort cleanup.
    }
    $client->disconnect()->await();
}
