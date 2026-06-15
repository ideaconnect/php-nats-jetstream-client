<?php

/**
 * Credentials File Authentication - auth from a .creds bundle.
 *
 * Parses a .creds file (bundled user JWT + NKey seed) with CredentialsParser, signs the
 * nonce with NkeySeedSigner, connects, and round-trips a message. Skips cleanly if the
 * credentials file (from `composer fixture:jwt`) is missing.
 *
 * Mirrors the README "Credentials File Authentication" example. Run: php examples/auth-credentials-file.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Auth\CredentialsParser;
use IDCT\NATS\Auth\NkeySeedSigner;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

// Credentials-file (JWT + NKey seed) auth runs against the operator/JWT-mode server.
$url = getenv('NATS_JWT_URL') ?: 'nats://127.0.0.1:14227';

// A .creds file bundles both the user JWT and the user NKey seed in two PEM-like blocks.
$credsFile = getenv('NATS_CREDS_FILE') ?: __DIR__ . '/../build/nats/jwt/user.creds';

if (!is_file($credsFile)) {
    echo 'SKIP auth-credentials-file: credentials file not found at ' . $credsFile
        . " (run 'composer fixture:jwt' first)\n";
    exit(0);
}

// Parse the .creds file to extract the JWT and the NKey seed.
$creds = CredentialsParser::fromFile($credsFile);
$signer = new NkeySeedSigner($creds['nkeySeed']);

$client = new NatsClient(new NatsOptions(
    servers: [$url],
    name: 'example-auth-credentials-file',
    jwt: $creds['jwt'],
    nkey: $signer->publicKey(),
    nonceSigner: $signer,
));
$client->connect()->await();

try {
    // Confirm the authenticated session can publish and receive on its own subject.
    $received = null;
    $sid = $client->subscribe('ex.auth.creds', static function (NatsMessage $message) use (&$received): void {
        $received = $message->payload;
    })->await();

    $client->flush()->await();
    $client->publish('ex.auth.creds', 'creds-hello')->await();

    $deadline = hrtime(true) / 1e9 + 5.0;
    while ($received === null && hrtime(true) / 1e9 < $deadline) {
        $client->processIncoming()->await();
        Amp\delay(0.02);
    }

    $client->unsubscribe($sid)->await();

    if ($received !== 'creds-hello') {
        throw new RuntimeException('Did not receive the message over the credentials-authenticated connection');
    }

    echo 'OK auth-credentials-file: authenticated via ' . basename($credsFile)
        . ' (nkey=' . $signer->publicKey() . '), payload="' . $received . '"' . PHP_EOL;
} finally {
    $client->disconnect()->await();
}
