<?php

/**
 * Standalone NKey Authentication - challenge signing without a JWT.
 *
 * Ed25519 nonce-challenge auth using only nkey + nonceSigner (no JWT): the server trusts
 * the account's public NKey and the client proves ownership by signing the nonce. Skips
 * unless NATS_NKEY_SEED is set to a seed the server trusts.
 *
 * Mirrors the README "Authentication Options" example. Run: php examples/auth-standalone-nkey.php
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use IDCT\NATS\Auth\NkeySeedSigner;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;

$url = getenv('NATS_NKEY_URL') ?: 'nats://127.0.0.1:14226';

// Standalone NKey auth uses Ed25519 challenge signing with NO JWT: the server is configured
// with the account's public NKey and we prove ownership by signing the nonce with its seed.
$seed = getenv('NATS_NKEY_SEED');
if ($seed === false || $seed === '') {
    echo "SKIP auth-standalone-nkey: set NATS_NKEY_SEED to the user NKey seed "
        . "(an 'SU...' encoded seed) that the standalone-nkey server trusts\n";
    exit(0);
}

// The signer derives the public NKey from the seed and signs the server's nonce challenge.
$signer = new NkeySeedSigner(trim($seed));

// Standalone NKey: set nkey + nonceSigner and omit jwt entirely.
$client = new NatsClient(new NatsOptions(
    servers: [$url],
    name: 'example-auth-standalone-nkey',
    nkey: $signer->publicKey(),
    nonceSigner: $signer,
));

// connect() only resolves once the nonce signature verifies against the trusted NKey.
$client->connect()->await();

try {
    // Prove the authenticated session can actually move data end-to-end.
    $subject = 'ex.auth.nkey';
    $received = null;

    $sid = $client->subscribe($subject, static function (NatsMessage $message) use (&$received): void {
        $received = $message->payload;
    })->await();

    $client->publish($subject, 'nkey-ok')->await();

    // Bounded poll loop until the message lands (or a monotonic deadline elapses).
    $deadline = hrtime(true) / 1e9 + 5.0;
    while ($received === null && hrtime(true) / 1e9 < $deadline) {
        $client->processIncoming()->await();
    }

    $client->unsubscribe($sid)->await();

    if ($received !== 'nkey-ok') {
        throw new RuntimeException('Standalone NKey-authenticated round-trip did not deliver the message');
    }

    echo 'OK auth-standalone-nkey: authenticated as ' . $signer->publicKey()
        . ' to ' . ($client->connectedUrl() ?? $url) . " and round-tripped a message\n";
} finally {
    $client->disconnect()->await();
}
