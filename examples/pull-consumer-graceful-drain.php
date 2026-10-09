<?php

/**
 * Pull Consumer Graceful Drain - shut a pull consumer worker down without losing what its pulls hold.
 *
 * Starts an infinite pullConsumer() run whose pull asks for more messages than the stream has and waits long for
 * them, so that the pull holds the stream's three messages without completing (the engine hands a pull over only
 * when its batch is full, a status comes, or it expires), as a worker's pull does on a stream that trickles. Once the
 * server shows the three as delivered and unacknowledged, the worker shuts down the way a SIGTERM handler would: with
 * the client's drain(). The drain hands what the pull holds to the handler while the connection is Draining, so that
 * the acks still go out, waits for that, and then closes; handle() resolves with the count. A fresh client then sees
 * nothing pending and an ack floor of 3.
 *
 * Mirrors the README "Graceful Drain" note on pull consumer runs and the "Pull Consumer Batching/Iteration" section.
 * Run: php examples/pull-consumer-graceful-drain.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\JetStream\JetStreamContext;

use function Amp\delay;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-pull-consumer-graceful-drain'));
$client->connect()->await();

$stream = 'EX_PULL_DRAIN_ORDERS';
$consumer = 'EX_PULL_DRAIN_PROC';

$js = $client->jetStream();

// Polls the consumer's state on $jetStream until $done accepts it, or $seconds have passed.
$waitForConsumer = static function (JetStreamContext $jetStream, \Closure $done, float $seconds) use ($stream, $consumer): array {
    $deadline = hrtime(true) / 1e9 + $seconds;
    do {
        $raw = $jetStream->getConsumer($stream, $consumer)->await()->raw;
        if ($done($raw)) {
            return $raw;
        }

        delay(0.05);
    } while (hrtime(true) / 1e9 < $deadline);

    return $raw;
};

$run = null;

try {
    $js->createStream($stream, ['ex.pulldrain.orders.>'])->await();
    $js->createConsumer($stream, $consumer, 'ex.pulldrain.orders.>', ['ack_policy' => 'explicit'])->await();

    for ($i = 1; $i <= 3; $i++) {
        $js->publish('ex.pulldrain.orders.created', "order $i")->await();
    }

    // An infinite run (no setIterations()) that asks for up to 10 messages per pull and lets a pull wait 30 s: its
    // first pull receives the three messages and keeps waiting for more, holding them.
    $handled = [];
    $run = $js->pullConsumer($stream, $consumer)
        ->setBatching(10)
        ->setExpiresMs(30_000)
        ->handle(function (NatsMessage $msg, JetStreamContext $js) use (&$handled): void {
            $handled[] = $msg->payload;
            $js->ack($msg)->await();
        });

    // The server has delivered the three to the run's pull, and nobody has acked them yet.
    $before = $waitForConsumer($js, static fn(array $raw): bool => ($raw['num_ack_pending'] ?? null) === 3, 10.0);
    if (($before['num_ack_pending'] ?? null) !== 3 || $handled !== []) {
        throw new RuntimeException(sprintf(
            'expected the run\'s pull to hold the 3 messages unhandled, got num_ack_pending %s and %d handled',
            json_encode($before['num_ack_pending'] ?? null),
            count($handled),
        ));
    }

    // The worker's shutdown: the client's drain() hands what the pull holds to the handler, waits for it, and closes.
    $client->drain()->await();

    try {
        $processed = $run->await(new TimeoutCancellation(10));
    } catch (Throwable $e) {
        throw new RuntimeException(sprintf(
            'handle() failed after the drain, the handler having got %d message(s): %s',
            count($handled),
            $e->getMessage(),
        ), 0, $e);
    }

    if ($processed !== 3 || $handled !== ['order 1', 'order 2', 'order 3']) {
        throw new RuntimeException(sprintf('expected handle() to return 3 with all three handled, got %d and [%s]', $processed, implode(', ', $handled)));
    }

    // A fresh client: the drained one is closed. The acks went out during the drain.
    $checker = new NatsClient(new NatsOptions(servers: [$url], name: 'example-pull-consumer-graceful-drain-check'));
    $checker->connect()->await();
    try {
        $after = $waitForConsumer($checker->jetStream(), static fn(array $raw): bool => ($raw['num_ack_pending'] ?? null) === 0, 5.0);
    } finally {
        $checker->disconnect()->await();
    }

    $ackFloor = $after['ack_floor']['consumer_seq'] ?? null;
    if (($after['num_ack_pending'] ?? null) !== 0 || $ackFloor !== 3) {
        throw new RuntimeException(sprintf(
            'expected nothing pending and an ack floor of 3, got num_ack_pending %s and ack floor %s',
            json_encode($after['num_ack_pending'] ?? null),
            json_encode($ackFloor),
        ));
    }

    echo "OK pull-consumer-graceful-drain: drain() handed the {$processed} message(s) the pull held to the handler and waited for their acks (ack floor {$ackFloor})\n";
} finally {
    // drain() closes the connection; on a failure before it, close it here, the run ending with it.
    $run?->ignore();
    try {
        $client->disconnect()->await();
    } catch (Throwable) {
        // Already closed.
    }

    // Clean up with a client of its own, since this one is closed by now.
    $cleanup = new NatsClient(new NatsOptions(servers: [$url], name: 'example-pull-consumer-graceful-drain-cleanup'));
    $cleanup->connect()->await();
    try {
        $cleanup->jetStream()->deleteConsumer($stream, $consumer)->await();
    } catch (Throwable) {
        // best-effort cleanup
    }

    try {
        $cleanup->jetStream()->deleteStream($stream)->await();
    } catch (Throwable) {
        // best-effort cleanup
    }

    $cleanup->disconnect()->await();
}
