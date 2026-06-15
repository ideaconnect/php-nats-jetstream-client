<?php

/**
 * WebSocket Transport - NATS over a ws:// connection.
 *
 * Connects with WebSocketTransport (negotiating permessage-deflate compression and
 * sending custom upgrade headers) and round-trips a message to prove the WebSocket
 * transport carries real traffic.
 *
 * Mirrors the README "WebSocket Transport" example. Run: php examples/websocket-transport.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Transport\WebSocketTransport;

// ws:// endpoints are handled only by WebSocketTransport, never the default TCP transport.
$url = getenv('NATS_WS_URL') ?: 'ws://127.0.0.1:14229';

$options = new NatsOptions(
    servers: [$url],                 // 'wss://...' would negotiate TLS using the tls* options
    name: 'example-websocket-transport',
    webSocketCompression: true,      // negotiate permessage-deflate when the server agrees (needs ext-zlib)
    webSocketHeaders: ['X-Example' => 'websocket-transport'], // extra headers on the upgrade handshake
);

// Inject the WebSocket transport explicitly; the same options instance is shared.
$client = new NatsClient($options, new WebSocketTransport($options));
$client->connect()->await();

try {
    // Drive a pub/sub round-trip to prove the WebSocket transport carries real traffic.
    $received = null;
    $sid = $client->subscribe('ex.ws.demo', static function (NatsMessage $message) use (&$received): void {
        $received = $message->payload;
    })->await();

    $client->flush()->await();
    $client->publish('ex.ws.demo', 'ws-hello')->await();

    $deadline = hrtime(true) / 1e9 + 5.0;
    while ($received === null && hrtime(true) / 1e9 < $deadline) {
        $client->processIncoming()->await();
        Amp\delay(0.02);
    }

    $client->unsubscribe($sid)->await();

    if ($received !== 'ws-hello') {
        throw new RuntimeException('Did not receive the message over the WebSocket transport');
    }

    echo 'OK websocket-transport: connected to ' . $url . ' and round-tripped payload="' . $received . '"' . PHP_EOL;
} finally {
    $client->disconnect()->await();
}
