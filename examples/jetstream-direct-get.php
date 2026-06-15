<?php

/**
 * JetStream Direct Get - low-latency reads served by any replica.
 *
 * On an allow_direct stream, fetches a stored message by sequence with
 * directGetStreamMessage() and the last message stored on a subject with
 * directGetLastMessageForSubject().
 *
 * Mirrors the README "JetStream Direct Get" example. Run: php examples/jetstream-direct-get.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-jetstream-direct-get'));
$client->connect()->await();

$stream = 'EX_DIRECT_GET';

try {
    $js = $client->jetStream();

    try {
        $js->deleteStream($stream)->await();
    } catch (\Throwable) {
        // Stream may not exist yet; ignore.
    }

    // Direct Get requires the stream to be created with allow_direct enabled.
    $js->createStream($stream, ['ex_direct_get.>'], ['allow_direct' => true])->await();

    $ack = $js->publish('ex_direct_get.order', '{"id":1}')->await();

    // Direct Get by stream sequence (served by any replica).
    $bySeq = $js->directGetStreamMessage($stream, $ack->seq)->await();

    // Direct Get the last message stored on a subject.
    $last = $js->directGetLastMessageForSubject($stream, 'ex_direct_get.order')->await();

    echo 'OK jetstream-direct-get: bySeq ' . $bySeq->subject . '=' . $bySeq->payload
        . ', lastBySubject=' . $last->payload . PHP_EOL;
} finally {
    try {
        $js->deleteStream($stream)->await();
    } catch (\Throwable) {
        // Best-effort cleanup.
    }
    $client->disconnect()->await();
}
