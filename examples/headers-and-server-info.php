<?php

/**
 * Headers and Server Info - NATS headers + server metadata.
 *
 * Publishes a message carrying NATS headers, runs an echo responder that reflects a
 * request-id header back via requestWithHeaders(), reads headers back with the
 * case-insensitive NatsHeaders::get() helper, and reads the negotiated serverInfo().
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
        // Header names travel on the wire exactly as the publisher wrote them, and publishers
        // disagree on spelling (a Go publisher canonicalizes to "X-Request-Id"). NatsHeaders::get()
        // matches the name case-insensitively, so a responder does not have to guess the casing;
        // here it finds the "X-Request-Id" the request carries while asking for "x-request-id".
        $requestId = NatsHeaders::get($requestHeaders, 'x-request-id') ?? 'unknown';

        // Reply with a different spelling of the same header name, the way a client with another
        // canonicalization would hand it to us.
        $message->respondWithHeaders($message->payload, ['X-REQUEST-ID' => $requestId])->await();
    })->await();

    // Ensure the SUB is registered server-side before we publish the request (avoid a 503 race).
    $client->flush()->await();

    $reply = $client->requestWithHeaders('svc.echo', 'hello', [
        'X-Request-Id' => 'req-123',
    ], 2000)->await();

    // The responder wrote the header as "X-REQUEST-ID", so an exact-case array lookup for the
    // spelling this side used finds nothing, while NatsHeaders::get() resolves the same header
    // under either spelling.
    $replyHeaders = NatsHeaders::fromWireBlock($reply->rawHeaders);
    $exactCase = $replyHeaders['X-Request-Id'] ?? '(not found)';
    $getWireCase = NatsHeaders::get($replyHeaders, 'X-REQUEST-ID') ?? '(not found)';
    $getOtherCase = NatsHeaders::get($replyHeaders, 'X-Request-Id') ?? '(not found)';

    if ($getWireCase !== 'req-123' || $getOtherCase !== 'req-123') {
        throw new RuntimeException(
            'expected both header lookups to return req-123, got "' . $getWireCase . '" and "' . $getOtherCase . '"',
        );
    }

    // Header values go out verbatim: the encoder writes the caller's bytes untouched, so a value
    // whose surrounding spaces are part of it (a signature, a checksum) is not silently changed.
    // The decoder still trims surrounding whitespace on the way in, as a tolerance for publishers
    // that pad their values.
    $signatureLine = explode("\r\n", NatsHeaders::toWireBlock(['X-Signature' => ' sig-1 ']))[1];

    if ($signatureLine !== 'X-Signature: sig-1 ') {
        throw new RuntimeException('expected the header value to be written verbatim, got "' . $signatureLine . '"');
    }

    $serverName = $client->serverInfo()?->serverName ?? '(unknown)';

    echo 'OK headers-and-server-info: reply="' . $reply->payload . '"'
        . ', reply carries the header as X-REQUEST-ID: array["X-Request-Id"]=' . $exactCase
        . ', get("X-REQUEST-ID")=' . $getWireCase
        . ', get("X-Request-Id")=' . $getOtherCase
        . ', verbatim header line="' . $signatureLine . '"'
        . ', server="' . $serverName . '"' . PHP_EOL;
} finally {
    $client->disconnect()->await();
}
