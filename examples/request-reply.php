<?php

/**
 * Request / Reply - one request, the first reply.
 *
 * Stands up a local echo responder, then uses request() to send a message and
 * await the first reply, pumping incoming frames concurrently so the responder
 * handler can run on the same connection.
 *
 * Mirrors the README "Request/Reply" example. Run: php examples/request-reply.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

use function Amp\async;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-request-reply'));
$client->connect()->await();

try {
    // Stand up a local responder so this example is self-contained: it echoes each request back
    // on the request's reply subject.
    $sid = $client->subscribe('ex.reqrep.svc.echo', static function (NatsMessage $message): void {
        if ($message->isReplyable()) {
            $message->respond('reply:' . $message->payload)->await();
        }
    })->await();

    // Make sure the responder subscription is registered server-side before requesting.
    $client->flush()->await();

    // request() resolves with the FIRST reply. Pump incoming frames concurrently so the responder
    // handler runs and can answer while request() awaits the reply.
    $pump = async(function () use ($client): void {
        $deadline = hrtime(true) / 1e9 + 4.0;
        while (hrtime(true) / 1e9 < $deadline) {
            try {
                $client->processIncoming(new TimeoutCancellation(0.25))->await();
            } catch (\Amp\CancelledException) {
                // No frame this cycle; keep pumping until the deadline.
            }
        }
    });

    $reply = $client->request('ex.reqrep.svc.echo', json_encode(['hello' => 'world'], JSON_THROW_ON_ERROR), 2000)->await();

    $pump->ignore();
    $client->unsubscribe($sid)->await();

    echo 'OK request-reply: ' . $reply->payload . PHP_EOL;
} finally {
    $client->disconnect()->await();
}
