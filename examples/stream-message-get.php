<?php

/**
 * Stream Message Get - fetch a stored message by sequence.
 *
 * Publishes a message, reads the stream sequence from its PubAck, and fetches the
 * stored message back by that sequence with getStreamMessage().
 *
 * Mirrors the README "Stream Message Get" example. Run: php examples/stream-message-get.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-stream-message-get'));
$client->connect()->await();

$stream = 'EX_MSG_GET';

try {
    $js = $client->jetStream();

    try {
        $js->deleteStream($stream)->await();
    } catch (\Throwable) {
        // Stream may not exist yet; ignore.
    }

    $js->createStream($stream, ['ex_msg_get.>'])->await();

    // Publish a message; the PubAck tells us the stored stream sequence.
    $ack = $js->publish('ex_msg_get.order', '{"id":1}')->await();

    // Fetch the stored message back by its stream sequence number.
    $message = $js->getStreamMessage($stream, $ack->seq)->await();

    echo 'OK stream-message-get: seq ' . $ack->seq
        . ' subject=' . $message->subject
        . ' payload=' . $message->payload . PHP_EOL;
} finally {
    try {
        $js->deleteStream($stream)->await();
    } catch (\Throwable) {
        // Best-effort cleanup.
    }
    $client->disconnect()->await();
}
