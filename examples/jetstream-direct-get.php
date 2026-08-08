<?php

/**
 * JetStream Direct Get - low-latency reads served by any replica.
 *
 * On an allow_direct stream, fetches a stored message by sequence with
 * directGetStreamMessage() and the last message stored on a subject with
 * directGetLastMessageForSubject(), then reads several subjects in one request with the
 * batched helpers directGetLastForSubjects() and directGetBatch() (NATS 2.11+).
 *
 * Mirrors the README "JetStream Direct Get" example. Run: php examples/jetstream-direct-get.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

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
    $js->publish('ex_direct_get.user', '{"id":2}')->await();

    // Direct Get by stream sequence (served by any replica).
    $bySeq = $js->directGetStreamMessage($stream, $ack->seq)->await();

    // Direct Get the last message stored on a subject.
    $last = $js->directGetLastMessageForSubject($stream, 'ex_direct_get.order')->await();

    // Batched Direct Get: the latest message per subject in one request instead of one request per
    // subject. Subjects must be exact (no wildcards). ex_direct_get.nothing_here matches the
    // stream's subject filter but was never written, so it is simply left out of the result rather
    // than failing the call.
    $latest = $js->directGetLastForSubjects($stream, [
        'ex_direct_get.order',
        'ex_direct_get.user',
        'ex_direct_get.nothing_here',
    ])->await();

    $latestSubjects = array_map(static fn(NatsMessage $message): string => $message->subject, $latest);
    sort($latestSubjects);

    if ($latestSubjects !== ['ex_direct_get.order', 'ex_direct_get.user']) {
        throw new RuntimeException(
            'expected the two stored subjects, got: ' . (implode(', ', $latestSubjects) ?: '(none)'),
        );
    }

    // When none of the requested subjects has a stored message the server answers with a single 404
    // status, which means "nothing stored here" rather than an error: the call returns an empty
    // list and any other subjects asked for in the same call keep their results.
    $noneStored = $js->directGetLastForSubjects($stream, [
        'ex_direct_get.nothing_here',
        'ex_direct_get.nothing_here_either',
    ])->await();

    if ($noneStored !== []) {
        throw new RuntimeException('expected no messages for subjects that were never written');
    }

    // Raw batched Direct Get: up to 10 messages starting at stream sequence 1.
    $range = $js->directGetBatch($stream, ['seq' => 1, 'batch' => 10])->await();

    if (count($range) !== 2) {
        throw new RuntimeException('expected 2 messages in the batched range, got ' . count($range));
    }

    echo 'OK jetstream-direct-get: bySeq ' . $bySeq->subject . '=' . $bySeq->payload
        . ', lastBySubject=' . $last->payload
        . ', lastForSubjects=' . implode('+', $latestSubjects) . ' (subject with no message omitted)'
        . ', allSubjectsMissing=' . count($noneStored) . ' messages'
        . ', batch=' . count($range) . ' messages' . PHP_EOL;
} finally {
    try {
        $js->deleteStream($stream)->await();
    } catch (\Throwable) {
        // Best-effort cleanup.
    }
    $client->disconnect()->await();
}
