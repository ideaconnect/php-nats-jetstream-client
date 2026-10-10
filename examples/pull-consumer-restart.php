<?php

/**
 * Pull Consumer Restart - reconfigure a running pull consumer with stop() and handle() in one tick.
 *
 * Starts an infinite pullConsumer() run on an empty stream, its pulls waiting on the server, and restarts it the way a
 * supervisor that changes the batch size on a signal does: stop() and then handle() again on the same iterator, without
 * awaiting the first run. stop() acts on the runs active when it is called, and each handle() run starts with stop and
 * drain flags of its own (#189), so the first run ends on its stop with nothing handled, and the new run, started with
 * the new batch size, handles everything published afterwards. Up to 2.24.6 the second handle() cleared the stop
 * before the first run saw it, and the two runs split the stream between their handlers.
 *
 * Mirrors the README "Pull Consumer Batching/Iteration" section. Run: php examples/pull-consumer-restart.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\CancelledException;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\JetStream\JetStreamContext;

use function Amp\delay;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-pull-consumer-restart'));
$client->connect()->await();

$stream = 'EX_PULL_RESTART_ORDERS';
$consumer = 'EX_PULL_RESTART_PROC';

$js = $client->jetStream();

// Polls $done until it holds, or $seconds have passed.
$waitUntil = static function (\Closure $done, float $seconds): bool {
    $deadline = hrtime(true) / 1e9 + $seconds;
    while (!$done()) {
        if (hrtime(true) / 1e9 >= $deadline) {
            return false;
        }

        delay(0.05);
    }

    return true;
};

$iterator = null;
$runs = [];

try {
    $js->createStream($stream, ['ex.pullrestart.orders.>'])->await();
    $js->createConsumer($stream, $consumer, 'ex.pullrestart.orders.>', ['ack_policy' => 'explicit'])->await();

    $handled = [];
    $handler = static function (string $run) use (&$handled): \Closure {
        return static function (NatsMessage $msg, JetStreamContext $js) use (&$handled, $run): void {
            $handled[] = "$run: {$msg->payload}";
            $js->ack($msg)->await();
        };
    };

    // An infinite run (no setIterations()) whose pulls wait on the server for the empty stream.
    $iterator = $js->pullConsumer($stream, $consumer)->setBatching(10)->setExpiresMs(30_000);
    $runs[] = $first = $iterator->handle($handler('first run'));
    $waiting = $waitUntil(
        static fn(): bool => ($js->getConsumer($stream, $consumer)->await()->raw['num_waiting'] ?? 0) > 0,
        10.0,
    );
    if (!$waiting) {
        throw new RuntimeException('expected the first run\'s pulls to wait on the server');
    }

    // The restart, in one tick: the first run ends on its stop, and the new run pulls with the new batch size, which
    // the three orders below fill, so that its pull hands them over at once.
    $iterator->stop();
    $runs[] = $second = $iterator->setBatching(3)->handle($handler('second run'));

    try {
        $stoppedRunProcessed = $first->await(new TimeoutCancellation(10));
    } catch (CancelledException $e) {
        throw new RuntimeException('expected the stopped run to end at its stop, it is still running', 0, $e);
    }
    if ($stoppedRunProcessed !== 0) {
        throw new RuntimeException(sprintf('expected the stopped run to have handled nothing, it handled %d', $stoppedRunProcessed));
    }

    for ($i = 1; $i <= 3; $i++) {
        $js->publish('ex.pullrestart.orders.created', "order $i")->await();
    }

    // A closure that reads $handled by reference: an arrow function would see only its value when it was made.
    if (!$waitUntil(static function () use (&$handled): bool {
        return count($handled) >= 3;
    }, 10.0)) {
        throw new RuntimeException(sprintf('expected 3 messages handled, got [%s]', implode(', ', $handled)));
    }

    // Shutdown: one stop() ends every run still active on the iterator.
    $iterator->stop();
    $processed = $second->await(new TimeoutCancellation(10));

    $expected = ['second run: order 1', 'second run: order 2', 'second run: order 3'];
    if ($processed !== 3 || $handled !== $expected) {
        throw new RuntimeException(sprintf('expected the new run alone to handle the 3 orders, got %d and [%s]', $processed, implode(', ', $handled)));
    }

    echo "OK pull-consumer-restart: the stopped run handled {$stoppedRunProcessed} message(s), the restarted run all {$processed}\n";
} finally {
    $iterator?->stop();
    foreach ($runs as $run) {
        $run->ignore();
    }

    try {
        $js->deleteConsumer($stream, $consumer)->await();
    } catch (Throwable) {
        // best-effort cleanup
    }

    try {
        $js->deleteStream($stream)->await();
    } catch (Throwable) {
        // best-effort cleanup
    }

    $client->disconnect()->await();
}
