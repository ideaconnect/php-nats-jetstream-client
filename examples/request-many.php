<?php

/**
 * Request Many (scatter-gather) - one request, many replies.
 *
 * Stands up several responders on a shared subject and uses requestMany() to
 * scatter one request and gather every reply, bounded by a total timeout and a
 * stall timeout (stop once no new reply arrives within the window).
 *
 * Mirrors the README "Request Many (Scatter-Gather)" example. Run: php examples/request-many.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

use function Amp\async;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:14222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-request-many'));
$client->connect()->await();

try {
    // Stand up three local responders on a shared subject. requestMany() scatters one request and
    // gathers the replies from every responder (scatter-gather).
    $sids = [];
    foreach (['alpha', 'beta', 'gamma'] as $node) {
        $sids[] = $client->subscribe('ex.reqmany.svc.scan', static function (NatsMessage $message) use ($node): void {
            if ($message->isReplyable()) {
                $message->respond('here:' . $node)->await();
            }
        })->await();
    }

    // Ensure all responder subscriptions are registered server-side before scattering.
    $client->flush()->await();

    // Pump incoming frames concurrently so the responders can answer while requestMany() collects.
    $pump = async(function () use ($client): void {
        $deadline = hrtime(true) / 1e9 + 5.0;
        while (hrtime(true) / 1e9 < $deadline) {
            try {
                $client->processIncoming(new TimeoutCancellation(0.25))->await();
            } catch (\Amp\CancelledException) {
                // No frame this cycle; keep pumping until the deadline.
            }
        }
    });

    // Time-bounded scatter-gather: collect until 250ms pass with no new reply, or 3000ms total.
    // Signature: requestMany(subject, payload, headers, maxResponses, totalTimeoutMs, stallMs).
    $replies = $client->requestMany('ex.reqmany.svc.scan', 'who-is-there', null, null, 3000, 250)->await();

    $pump->ignore();
    foreach ($sids as $sid) {
        $client->unsubscribe($sid)->await();
    }

    $payloads = array_map(static fn (NatsMessage $m): string => $m->payload, $replies);
    sort($payloads);

    echo 'OK request-many: ' . count($replies) . ' responder(s): ' . implode(',', $payloads) . PHP_EOL;
} finally {
    $client->disconnect()->await();
}
