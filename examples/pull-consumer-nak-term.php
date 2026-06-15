<?php

/**
 * Pull Consumer (NAK / Delayed NAK / TERM / In-Progress) - redelivery control.
 *
 * On a pulled message, demonstrates inProgress() (extend the ack deadline), nak()
 * (redeliver now), nakWithDelay() (redeliver after a delay), and term() (stop
 * redelivery entirely).
 *
 * Mirrors the README "JetStream Pull Consumer (NAK, Delayed NAK, TERM, In-Progress)" example. Run: php examples/pull-consumer-nak-term.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-pull-consumer-nak-term'));
$client->connect()->await();

$stream = 'EX_JS_JOBS';
$consumer = 'EX_JS_WORKER';

$js = $client->jetStream();

try {
    $js->createStream($stream, ['ex.jobs.>'])->await();
    $js->createConsumer($stream, $consumer, 'ex.jobs.>')->await();
    $js->publish('ex.jobs.process', '{"task":"rebuild"}')->await();

    $message = $js->fetchNext($stream, $consumer, 3000)->await();

    // Signal work-in-progress to extend the ack deadline.
    $js->inProgress($message)->await();

    // NAK: redeliver the message immediately.
    $js->nak($message)->await();

    // The message is redelivered after the immediate NAK; fetch it again.
    $redelivered = $js->fetchNext($stream, $consumer, 3000)->await();

    // NAK with delay: redeliver after a short delay (250ms here for a fast example).
    $js->nakWithDelay($redelivered, 250)->await();

    // Fetch the delayed redelivery, then TERM it so it is not redelivered again.
    $final = $js->fetchNext($stream, $consumer, 3000)->await();

    // TERM: terminate delivery, do not redeliver.
    $js->term($final)->await();

    echo "OK pull-consumer-nak-term: in-progress + nak + nakWithDelay + term on '{$final->payload}'\n";
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
