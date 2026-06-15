<?php

/**
 * Headers and Server Info - NATS headers + server metadata.
 *
 * Publishes a message carrying NATS headers, runs an echo responder that reflects a
 * request-id header back via requestWithHeaders(), and reads the negotiated
 * serverInfo().
 *
 * Mirrors the README "Headers and Server Info" example. Run: php examples/headers-and-server-info.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsHeaders;
use IDCT\NATS\Core\NatsMessage;

$url = getenv('NATS_URL') ?: 'nats://127.0.0.1:4222';

$client = new NatsClient(new NatsOptions(servers: [$url], name: 'example-headers-and-server-info'));
$client->connect()->await();

try {
    // Fire-and-forget publish carrying NATS headers (e.g. for dedup / content negotiation).
    $client->publishWithHeaders('events.orders', '{"id":123}', [
        'Nats-Msg-Id' => 'orders-123',
        'Content-Type' => 'application/json',
    ])->await();

    // Stand up a tiny echo responder on the same connection so requestWithHeaders has a service to
    // talk to. The handler echoes the request payload back and reflects the request id header.
    $client->subscribe('svc.echo', static function (NatsMessage $message): void {
        if ($message->replyTo === null) {
            return;
        }

        $requestHeaders = NatsHeaders::fromWireBlock($message->rawHeaders);
        $requestId = $requestHeaders['X-Request-Id'] ?? 'unknown';

        $message->respondWithHeaders($message->payload, ['X-Request-Id' => $requestId])->await();
    })->await();

    // Ensure the SUB is registered server-side before we publish the request (avoid a 503 race).
    $client->flush()->await();

    $reply = $client->requestWithHeaders('svc.echo', 'hello', [
        'X-Request-Id' => 'req-123',
    ], 2000)->await();

    $serverName = $client->serverInfo()?->serverName ?? '(unknown)';

    echo 'OK headers-and-server-info: reply="' . $reply->payload . '" server="' . $serverName . '"' . PHP_EOL;
} finally {
    $client->disconnect()->await();
}
