<?php

/**
 * Username / Password Authentication - connect with user + pass.
 *
 * Sends a username and password in the CONNECT frame; connect() only resolves once the
 * server accepts them. Round-trips a message to prove the authenticated session works.
 *
 * Mirrors the README "Authentication Options" example. Run: php examples/auth-userpass.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

$url = getenv('NATS_USERPASS_URL') ?: 'nats://127.0.0.1:14224';

// Username/password auth: both fields are sent in the CONNECT frame.
$client = new NatsClient(new NatsOptions(
    servers: [$url],
    name: 'example-auth-userpass',
    username: getenv('NATS_USER') ?: 'local-user',
    password: getenv('NATS_PASS') ?: 'local-pass',
));

// connect() only resolves once the server has accepted our credentials.
$client->connect()->await();

try {
    // Prove the authenticated session can actually move data end-to-end.
    $subject = 'ex.auth.userpass';
    $received = null;

    $sid = $client->subscribe($subject, static function (NatsMessage $message) use (&$received): void {
        $received = $message->payload;
    })->await();

    $client->publish($subject, 'userpass-ok')->await();

    // Bounded poll loop until the message lands (or a monotonic deadline elapses).
    $deadline = hrtime(true) / 1e9 + 5.0;
    while ($received === null && hrtime(true) / 1e9 < $deadline) {
        $client->processIncoming()->await();
    }

    $client->unsubscribe($sid)->await();

    if ($received !== 'userpass-ok') {
        throw new RuntimeException('User/password-authenticated round-trip did not deliver the message');
    }

    echo 'OK auth-userpass: authenticated to ' . ($client->connectedUrl() ?? $url)
        . " and round-tripped a message\n";
} finally {
    $client->disconnect()->await();
}
