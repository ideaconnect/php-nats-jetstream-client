<?php

/**
 * Ordered Consumer - gap-free, strictly in-order delivery.
 *
 * Uses subscribeOrderedConsumer() (an ephemeral push consumer with flow control, idle
 * heartbeat and ack_policy=none). The helper transparently recreates the consumer on a
 * sequence gap so delivery stays strictly in order.
 *
 * Mirrors the README "Ordered Consumer" example. Run: php examples/ordered-consumer.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-ordered-consumer'));
$client->connect()->await();

$stream = 'EX_ORDERED';

$js = $client->jetStream();
$js->createStream($stream, ['ex.ordered.>'])->await();

$received = [];

try {
    // Ordered consumer: an ephemeral push consumer with flow control, idle heartbeat, and
    // ack_policy=none. The helper transparently recreates the consumer on a sequence gap so
    // delivery stays strictly in order.
    $sid = $js->subscribeOrderedConsumer(
        stream: $stream,
        handler: static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        },
        filterSubject: 'ex.ordered.>',
    )->await();

    // Ensure the consumer/SUB are live before publishing.
    $client->flush()->await();

    for ($i = 1; $i <= 3; $i++) {
        $js->publish('ex.ordered.event', '{"id":' . $i . '}')->await();
    }

    // Bounded delivery loop until all three messages arrive or the deadline.
    $deadline = hrtime(true) / 1e9 + 5.0;
    while (count($received) < 3 && hrtime(true) / 1e9 < $deadline) {
        try {
            $client->processIncoming(new TimeoutCancellation(0.5))->await();
        } catch (\Amp\CancelledException) {
            // Idle window elapsed with no frame; keep polling until the deadline.
        }
    }

    if (count($received) < 3) {
        throw new RuntimeException('Ordered consumer delivered ' . count($received) . '/3 messages before the deadline');
    }

    // Stop through the context, not $client->unsubscribe($sid): a gap-driven recreate rotates the
    // internal sid, so the sid returned above goes stale after the first recreate. The context
    // resolves the CURRENT sid, cancels the heartbeat watchdog, and deletes the server-side
    // ephemeral instead of leaving it for the inactive threshold to reap.
    $js->stopOrderedConsumer($sid)->await();

    echo 'OK ordered-consumer: delivered=' . count($received)
        . ' order=' . implode(',', $received) . PHP_EOL;
} finally {
    // stopOrderedConsumer() already removed the ephemeral consumer on the success path, and the
    // server reaps it on the failure path; drop the stream we created either way.
    try {
        $js->deleteStream($stream)->await();
    } catch (Throwable) {
        // Best-effort cleanup.
    }

    $client->disconnect()->await();
}
