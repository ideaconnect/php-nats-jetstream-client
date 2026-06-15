<?php

/**
 * Stream Mirroring and Sourcing - mirror one stream, aggregate many.
 *
 * Builds StreamSource configs to create a subjectless mirror-only stream
 * (StreamSource::mirror) and a subjectless multi-source aggregate stream
 * (StreamSource::source, with subject filter / start sequence).
 *
 * Mirrors the README "Stream Mirroring and Sourcing" example. Run: php examples/stream-mirroring-and-sourcing.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\JetStream\Configuration\StreamSource;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-stream-mirroring-and-sourcing'));
$client->connect()->await();

$origin = 'EX_SRC_ORDERS';
$payments = 'EX_SRC_PAYMENTS';
$mirrorStream = 'EX_SRC_MIRROR';
$aggregate = 'EX_SRC_AGG';
$js = $client->jetStream();

try {
    // Origin streams that will be mirrored / sourced.
    $js->createStream($origin, ['ex.src.orders.>'])->await();
    $js->createStream($payments, ['ex.src.payments.>'])->await();

    // StreamSource builds the mirror/source configuration arrays.
    $mirror = StreamSource::mirror($origin)->toArray();

    $aggregateSources = [
        StreamSource::source($origin)->filterSubject('ex.src.orders.>')->toArray(),
        StreamSource::source($payments)->startSeq(1)->toArray(),
    ];

    // A mirror-only stream is created with an empty subjects list plus a mirror config.
    $mirrorInfo = $js->createStream($mirrorStream, [], [
        'mirror' => $mirror,
    ])->await();

    // An aggregate stream pulls from several sources (also subjectless).
    $aggInfo = $js->createStream($aggregate, [], [
        'sources' => $aggregateSources,
    ])->await();

    $mirrorName = $mirrorInfo->raw['config']['mirror']['name'] ?? '?';
    $sourceCount = is_array($aggInfo->raw['config']['sources'] ?? null)
        ? count($aggInfo->raw['config']['sources'])
        : 0;

    echo "OK stream-mirroring-and-sourcing: mirror of {$mirrorName}, aggregate with {$sourceCount} sources\n";
} finally {
    foreach ([$aggregate, $mirrorStream, $payments, $origin] as $name) {
        try {
            $js->deleteStream($name)->await();
        } catch (\Throwable) {
            // best-effort cleanup
        }
    }

    $client->disconnect()->await();
}
