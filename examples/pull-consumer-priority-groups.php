<?php

/**
 * Pull Consumer Priority Groups - pinned-client steering (NATS 2.11+).
 *
 * Creates a pull consumer with a pinned-client priority group, pulls under the group
 * (the iterator captures and resends the Nats-Pin-Id and re-pins transparently on a
 * stale-pin 423), then releases the pin with unpinConsumer().
 *
 * Mirrors the README "Pull Consumer Priority Groups" example. Run: php examples/pull-consumer-priority-groups.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\JetStream\JetStreamContext;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-pull-consumer-priority-groups'));
$client->connect()->await();

$stream = 'EX_PRIORITY_ORDERS';
$consumer = 'EX_PRIORITY_PROC';
$group = 'g1';

$js = $client->jetStream();

// Priority-group fields require NATS server 2.11+. If the server is older it rejects the
// consumer config; surface that as a clear note rather than an opaque failure.
$pinId = null;

try {
    $js->createStream($stream, ['ex.priority.orders.>'])->await();

    // Create a pull consumer with a pinned-client priority group (NATS 2.11+).
    $js->createConsumer($stream, $consumer, 'ex.priority.orders.>', [
        'priority_groups' => [$group],
        'priority_policy' => 'pinned_client',
    ])->await();

    for ($i = 0; $i < 5; $i++) {
        $js->publish('ex.priority.orders.created', "order $i")->await();
    }

    // Pull under the group. The iterator captures and resends the Nats-Pin-Id
    // automatically, and re-pins transparently if the pin goes stale (423).
    $totalProcessed = $js->pullConsumer($stream, $consumer)
        ->setGroup($group)
        ->setBatching(10)
        ->setExpiresMs(1000)
        ->setIterations(5)
        ->setMaxBytes(1048576)
        ->handle(function (NatsMessage $msg, JetStreamContext $js) use (&$pinId): void {
            // Inspect the pin id carried by the first message of the pinned group.
            $pinId ??= $js->pinIdOf($msg); // string|null
            $js->ack($msg)->await();
        })->await();

    // Release the active pin so another client can take over the group.
    $js->unpinConsumer($stream, $consumer, $group)->await();

    echo "OK pull-consumer-priority-groups: processed {$totalProcessed} message(s) under group '{$group}', pinId="
        . ($pinId ?? 'none') . "\n";
} catch (\Throwable $e) {
    // Priority groups need NATS 2.11+; make an unsupported-server failure obvious.
    fwrite(STDERR, "NOTE pull-consumer-priority-groups requires NATS server 2.11+: " . $e->getMessage() . "\n");
    throw $e;
} finally {
    try {
        $js->deleteConsumer($stream, $consumer)->await();
    } catch (\Throwable) {
        // best-effort cleanup
    }

    try {
        $js->deleteStream($stream)->await();
    } catch (\Throwable) {
        // best-effort cleanup
    }

    $client->disconnect()->await();
}
