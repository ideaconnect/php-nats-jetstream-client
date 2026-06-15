<?php

/**
 * Consumer Pause / Resume - temporarily stop delivery.
 *
 * Pauses a consumer until a future RFC 3339 instant with pauseConsumer() (a dynamic
 * +1h time keeps the example re-runnable), then clears the pause with resumeConsumer().
 *
 * Mirrors the README "Consumer Pause/Resume" example. Run: php examples/consumer-pause-resume.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-consumer-pause-resume'));
$client->connect()->await();

$stream = 'EX_PAUSE';
$consumer = 'EX_PAUSE_PROC';

$js = $client->jetStream();
$js->createStream($stream, ['ex.pause.>'])->await();
$js->createConsumer($stream, $consumer, 'ex.pause.created')->await();

try {
    // Pause the consumer until a future instant (RFC 3339 / ISO 8601). Using a dynamic future
    // time keeps the example re-runnable instead of a hardcoded date that may already be past.
    $pauseUntil = (new \DateTimeImmutable('+1 hour', new \DateTimeZone('UTC')))->format('Y-m-d\TH:i:s\Z');
    $js->pauseConsumer($stream, $consumer, $pauseUntil)->await();

    // Resume the consumer immediately.
    $js->resumeConsumer($stream, $consumer)->await();

    echo 'OK consumer-pause-resume: paused-until=' . $pauseUntil . ' then resumed' . PHP_EOL;
} finally {
    try {
        $js->deleteConsumer($stream, $consumer)->await();
    } catch (Throwable) {
        // Best-effort cleanup.
    }

    try {
        $js->deleteStream($stream)->await();
    } catch (Throwable) {
        // Best-effort cleanup.
    }

    $client->disconnect()->await();
}
