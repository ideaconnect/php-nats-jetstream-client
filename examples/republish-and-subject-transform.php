<?php

/**
 * Republish and Subject Transform - stream-level message routing.
 *
 * Creates streams configured with Republish (including a headers-only variant) to mirror
 * messages onto another subject, and with SubjectTransform to remap subjects on ingest.
 *
 * Mirrors the README "Republish and Subject Transform" example. Run: php examples/republish-and-subject-transform.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\JetStream\Configuration\Republish;
use IDCT\NATS\JetStream\Configuration\SubjectTransform;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-republish-and-subject-transform'));
$client->connect()->await();

$ordersStream = 'EX_RP_ORDERS';
$eventsStream = 'EX_RP_EVENTS';
$mappedStream = 'EX_RP_MAPPED';
$js = $client->jetStream();

try {
    // Republish all order messages to a monitoring subject.
    $orders = $js->createStream($ordersStream, ['ex.rp.orders.>'], [
        'republish' => Republish::create('ex.rp.orders.>', 'ex.rp.monitor.orders.>')->toArray(),
    ])->await();

    // Republish headers only (strip payload) for lightweight notifications.
    $events = $js->createStream($eventsStream, ['ex.rp.events.>'], [
        'republish' => Republish::create('ex.rp.events.>', 'ex.rp.notify.events.>')->headersOnly()->toArray(),
    ])->await();

    // Apply a subject transform to remap subjects on ingest.
    $mapped = $js->createStream($mappedStream, ['ex.rp.raw.>'], [
        'subject_transform' => SubjectTransform::create('ex.rp.raw.>', 'ex.rp.processed.>')->toArray(),
    ])->await();

    $republishDest = $orders->raw['config']['republish']['dest'] ?? '?';
    $headersOnly = ($events->raw['config']['republish']['headers_only'] ?? false) === true ? 'yes' : 'no';
    $transformDest = $mapped->raw['config']['subject_transform']['dest'] ?? '?';

    echo "OK republish-and-subject-transform: republish->{$republishDest} headers_only={$headersOnly} transform->{$transformDest}\n";
} finally {
    foreach ([$ordersStream, $eventsStream, $mappedStream] as $name) {
        try {
            $js->deleteStream($name)->await();
        } catch (\Throwable) {
            // best-effort cleanup
        }
    }

    $client->disconnect()->await();
}
