<?php

/**
 * JetStream Push Consumer (Durable) - server-pushed delivery.
 *
 * Uses subscribePushConsumer(), which creates the durable consumer and transparently
 * handles JetStream flow-control / idle-heartbeat control frames, invoking the handler
 * once per user message; the message is acked in the handler.
 *
 * Mirrors the README "JetStream Push Consumer (Durable)" example. Run: php examples/push-consumer-durable.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-push-consumer-durable'));
$client->connect()->await();

$stream = 'EX_PUSH_DUR';
$consumer = 'EX_PUSH_DUR_PROC';

$js = $client->jetStream();
$js->createStream($stream, ['ex.push.durable.>'])->await();

$received = [];

try {
    // Durable push consumer: the helper creates the consumer and handles JetStream
    // flow-control / idle-heartbeat control frames transparently before invoking the handler.
    $sid = $js->subscribePushConsumer(
        stream: $stream,
        consumer: $consumer,
        handler: static function (NatsMessage $message) use ($js, &$received): void {
            $received[] = $message->payload;
            $js->ack($message)->await();
        },
        filterSubject: 'ex.push.durable.created',
    )->await();

    // Ensure the SUB and consumer are registered server-side before publishing.
    $client->flush()->await();

    $js->publish('ex.push.durable.created', '{"id":123}')->await();

    // Bounded delivery loop: drive processIncoming() until the message arrives or the deadline.
    $deadline = hrtime(true) / 1e9 + 5.0;
    while ($received === [] && hrtime(true) / 1e9 < $deadline) {
        try {
            $client->processIncoming(new TimeoutCancellation(0.5))->await();
        } catch (\Amp\CancelledException) {
            // Idle window elapsed with no frame; keep polling until the deadline.
        }
    }

    if ($received === []) {
        throw new RuntimeException('Durable push consumer did not deliver the message within the deadline');
    }

    $client->unsubscribe($sid)->await();

    echo 'OK push-consumer-durable: delivered=' . count($received) . ' payload="' . $received[0] . '"' . PHP_EOL;
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
