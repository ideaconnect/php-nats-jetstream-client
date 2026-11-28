<?php

/**
 * Stream Message Get - fetch stored messages by sequence and by subject.
 *
 * Publishes messages on two interleaved subjects, reads the stream sequence from a PubAck, and fetches
 * the stored message back by that sequence with getStreamMessage(). Then reads by subject over the same
 * leader STREAM.MSG.GET API: the last message on a subject with getLastMessageForSubject(), and every
 * message on a subject with getNextMessageForSubject(), walking by `sequence + 1` until the 404.
 *
 * Mirrors the README "Stream Message Get" example. Run: php examples/stream-message-get.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;

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
    $js->publish('ex_msg_get.invoice', '{"id":2}')->await();
    $js->publish('ex_msg_get.order', '{"id":3}')->await();

    // Fetch the stored message back by its stream sequence number.
    $message = $js->getStreamMessage($stream, $ack->seq)->await();

    // The last message stored on a subject.
    $last = $js->getLastMessageForSubject($stream, 'ex_msg_get.order')->await();
    if ($last->payload !== '{"id":3}') {
        throw new RuntimeException('expected the last order to be {"id":3}, got ' . $last->payload);
    }

    // Walk a subject: every answer carries its stream sequence, and a 404 ends the walk.
    $walked = [];
    $next = 0;
    try {
        while (count($walked) < 10) {
            $stored = $js->getNextMessageForSubject($stream, 'ex_msg_get.order', $next)->await();
            $walked[] = $stored->sequence . '=' . $stored->payload;
            $next = $stored->sequence + 1;
        }
    } catch (JetStreamException $e) {
        if ($e->getCode() !== 404) {
            throw $e;
        }
    }

    $expected = [$ack->seq . '={"id":1}', ($ack->seq + 2) . '={"id":3}'];
    if ($walked !== $expected) {
        throw new RuntimeException('expected the walk ' . implode(', ', $expected) . ', got ' . implode(', ', $walked));
    }

    echo 'OK stream-message-get: seq ' . $ack->seq
        . ' subject=' . $message->subject
        . ' payload=' . $message->payload
        . ' last=' . $last->payload
        . ' walk=' . implode(',', $walked) . PHP_EOL;
} finally {
    try {
        $js->deleteStream($stream)->await();
    } catch (\Throwable) {
        // Best-effort cleanup.
    }
    $client->disconnect()->await();
}
