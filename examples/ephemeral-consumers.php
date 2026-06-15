<?php

/**
 * JetStream Ephemeral Consumers - server-named, auto-cleaned consumers.
 *
 * Creates an ephemeral PULL consumer (server-assigned name) and fetches/acks from it,
 * then runs an ephemeral PUSH consumer via subscribeEphemeralPushConsumer(). Both are
 * auto-removed by the server when the client goes away.
 *
 * Mirrors the README "JetStream Ephemeral Consumers" example. Run: php examples/ephemeral-consumers.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-ephemeral-consumers'));
$client->connect()->await();

$stream = 'EX_EPHEMERAL';

$js = $client->jetStream();
$js->createStream($stream, ['ex.ephemeral.>'])->await();

$pushReceived = [];

try {
    // --- Ephemeral PULL consumer ---
    // createEphemeralConsumer() returns a ConsumerInfo whose ->name is the server-assigned
    // ephemeral consumer name we then pull from.
    $ephemeral = $js->createEphemeralConsumer($stream, 'ex.ephemeral.created')->await();
    $client->flush()->await();

    $js->publish('ex.ephemeral.created', '{"id":1}')->await();

    $pullMessage = $js->fetchNext($stream, $ephemeral->name, expiresMs: 3000)->await();
    $js->ack($pullMessage)->await();

    // --- Ephemeral PUSH consumer ---
    // The helper creates an ephemeral push consumer and handles control frames; the handler
    // is invoked for each user message.
    $sid = $js->subscribeEphemeralPushConsumer(
        stream: $stream,
        handler: static function (NatsMessage $message) use ($js, &$pushReceived): void {
            $pushReceived[] = $message->payload;
            $js->ack($message)->await();
        },
        filterSubject: 'ex.ephemeral.created',
    )->await();
    $client->flush()->await();

    $js->publish('ex.ephemeral.created', '{"id":2}')->await();

    // Bounded delivery loop for the push side.
    $deadline = hrtime(true) / 1e9 + 5.0;
    while ($pushReceived === [] && hrtime(true) / 1e9 < $deadline) {
        try {
            $client->processIncoming(new TimeoutCancellation(0.5))->await();
        } catch (\Amp\CancelledException) {
            // Idle window elapsed with no frame; keep polling until the deadline.
        }
    }

    if ($pushReceived === []) {
        throw new RuntimeException('Ephemeral push consumer did not deliver the message within the deadline');
    }

    $client->unsubscribe($sid)->await();

    echo 'OK ephemeral-consumers: pull="' . $pullMessage->payload
        . '" push="' . $pushReceived[0] . '"' . PHP_EOL;
} finally {
    // Ephemeral consumers are auto-cleaned by the server, but the stream is ours to remove.
    try {
        $js->deleteStream($stream)->await();
    } catch (Throwable) {
        // Best-effort cleanup.
    }

    $client->disconnect()->await();
}
