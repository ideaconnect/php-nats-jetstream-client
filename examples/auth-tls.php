<?php

/**
 * Mutual TLS - encrypted connection with a client certificate.
 *
 * Connects over tls:// presenting a client certificate (CA + cert + key fixtures) for
 * mutual TLS, then round-trips a message to prove the encrypted channel carries
 * traffic. Skips cleanly if the TLS fixtures are missing.
 *
 * Mirrors the README "Authentication Options" example. Run: php examples/auth-tls.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

// TLS endpoint with mutual TLS (the dev tls.conf requires verify + handshake_first).
$url = getenv('NATS_TLS_URL') ?: 'tls://127.0.0.1:14225';

$caFile = __DIR__ . '/../build/tls/ca.pem';
$certFile = __DIR__ . '/../build/tls/client-cert.pem';
$keyFile = __DIR__ . '/../build/tls/client-key.pem';

foreach (['CA' => $caFile, 'client cert' => $certFile, 'client key' => $keyFile] as $label => $path) {
    if (!is_file($path)) {
        echo 'SKIP auth-tls: missing ' . $label . ' at ' . $path
            . " (run the TLS fixture generator first)\n";
        exit(0);
    }
}

$client = new NatsClient(new NatsOptions(
    servers: [$url],
    name: 'example-auth-tls',
    tlsRequired: true,
    tlsHandshakeFirst: true,      // dev server runs with handshake_first: true
    tlsCaFile: $caFile,
    tlsCertFile: $certFile,       // mTLS: present a client certificate
    tlsKeyFile: $keyFile,
));
$client->connect()->await();

try {
    // Prove the encrypted connection actually carries traffic with a self round-trip.
    $received = null;
    $sid = $client->subscribe('ex.auth.tls', static function (NatsMessage $message) use (&$received): void {
        $received = $message->payload;
    })->await();

    $client->flush()->await();
    $client->publish('ex.auth.tls', 'secure-hello')->await();

    $deadline = hrtime(true) / 1e9 + 5.0;
    while ($received === null && hrtime(true) / 1e9 < $deadline) {
        $client->processIncoming()->await();
        Amp\delay(0.02);
    }

    $client->unsubscribe($sid)->await();

    if ($received !== 'secure-hello') {
        throw new RuntimeException('Did not receive the message over the TLS connection');
    }

    echo 'OK auth-tls: mTLS connection to ' . $url . ' carried payload="' . $received . '"' . PHP_EOL;
} finally {
    $client->disconnect()->await();
}
