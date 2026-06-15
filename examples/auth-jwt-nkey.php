<?php

/**
 * JWT + NKey Authentication - operator/account credentials.
 *
 * Loads a user JWT and its NKey seed (from `composer fixture:jwt`), signs the server's
 * nonce challenge with NkeySeedSigner, connects, and round-trips a message. Skips
 * cleanly if the fixtures are not present.
 *
 * Mirrors the README "Authentication Options" example. Run: php examples/auth-jwt-nkey.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Auth\NkeySeedSigner;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

$url = getenv('NATS_JWT_URL') ?: 'nats://127.0.0.1:14227';

// The JWT and the matching NKey seed are produced by `composer fixture:jwt`.
$jwtFile = __DIR__ . '/../build/nats/jwt/user.jwt';
$seedFile = __DIR__ . '/../build/nats/jwt/user.seed';

if (!is_file($jwtFile) || !is_file($seedFile)) {
    echo "OK auth-jwt-nkey: SKIPPED - run 'composer fixture:jwt' to generate "
        . "build/nats/jwt/user.jwt and user.seed first\n";
    exit(0);
}

$jwt = trim((string) file_get_contents($jwtFile));
$seed = trim((string) file_get_contents($seedFile));

// The signer derives the public NKey from the seed and signs the server's nonce challenge.
$signer = new NkeySeedSigner($seed);

$client = new NatsClient(new NatsOptions(
    servers: [$url],
    name: 'example-auth-jwt-nkey',
    jwt: $jwt,
    nkey: $signer->publicKey(),
    nonceSigner: $signer,
));

// connect() only resolves once the JWT is accepted and the nonce signature verifies.
$client->connect()->await();

try {
    // Prove the authenticated session can actually move data end-to-end.
    $subject = 'ex.auth.jwt';
    $received = null;

    $sid = $client->subscribe($subject, static function (NatsMessage $message) use (&$received): void {
        $received = $message->payload;
    })->await();

    $client->publish($subject, 'jwt-ok')->await();

    // Bounded poll loop until the message lands (or a monotonic deadline elapses).
    $deadline = hrtime(true) / 1e9 + 5.0;
    while ($received === null && hrtime(true) / 1e9 < $deadline) {
        $client->processIncoming()->await();
    }

    $client->unsubscribe($sid)->await();

    if ($received !== 'jwt-ok') {
        throw new RuntimeException('JWT/NKey-authenticated round-trip did not deliver the message');
    }

    echo 'OK auth-jwt-nkey: authenticated as ' . $signer->publicKey()
        . ' to ' . ($client->connectedUrl() ?? $url) . " and round-tripped a message\n";
} finally {
    $client->disconnect()->await();
}
