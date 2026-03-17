<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Integration;

use PHPUnit\Framework\TestCase;
use RuntimeException;

trait IntegrationTestBootstrap
{
    /**
     * Returns a fixture value from the environment or a fallback file.
     */
    protected function integrationFixtureValue(string $envVar, string $fallbackRelativePath, bool $stripQuotes = false): ?string
    {
        $value = getenv($envVar);
        if (is_string($value) && $value !== '') {
            return $stripQuotes ? trim($value, "\"'") : $value;
        }

        $fallback = $this->repoRoot() . '/' . $fallbackRelativePath;
        if (!is_file($fallback)) {
            return null;
        }

        $contents = trim((string) file_get_contents($fallback));
        if ($contents === '') {
            return null;
        }

        return $stripQuotes ? trim($contents, "\"'") : $contents;
    }

    /**
     * Returns the repository root path.
     */
    protected function repoRoot(): string
    {
        return dirname(__DIR__, 2);
    }

    /**
     * Skips the current test unless integration tests are explicitly enabled.
     */
    protected function requireIntegrationEnabled(): void
    {
        $flag = getenv('RUN_INTEGRATION');
        if ($flag !== '1') {
            if ($this instanceof TestCase) {
                $this->markTestSkipped('Set RUN_INTEGRATION=1 to run integration tests.');
            }

            throw new RuntimeException('Set RUN_INTEGRATION=1 to run integration tests.');
        }
    }

    /**
     * Returns the configured NATS server URL used for integration tests.
     */
    protected function integrationServerUrl(): string
    {
        $url = getenv('NATS_URL');

        return is_string($url) && $url !== '' ? $url : 'nats://127.0.0.1:14222';
    }

    /**
     * Returns the configured token-auth NATS server URL.
     */
    protected function integrationTokenServerUrl(): string
    {
        $url = getenv('NATS_TOKEN_URL');

        return is_string($url) && $url !== '' ? $url : 'nats://127.0.0.1:14223';
    }

    /**
     * Returns the configured valid token for token-auth integration tests.
     */
    protected function integrationToken(): string
    {
        $token = getenv('NATS_TOKEN');

        return is_string($token) && $token !== '' ? $token : 'local-test-token';
    }

    /**
     * Returns the configured invalid token for negative auth integration tests.
     */
    protected function integrationInvalidToken(): string
    {
        $token = getenv('NATS_TOKEN_INVALID');

        return is_string($token) && $token !== '' ? $token : 'invalid-local-test-token';
    }

    /**
     * Returns the configured username/password NATS server URL.
     */
    protected function integrationUserPassServerUrl(): string
    {
        $url = getenv('NATS_USERPASS_URL');

        return is_string($url) && $url !== '' ? $url : 'nats://127.0.0.1:14224';
    }

    /**
     * Returns the configured valid username for user/password integration tests.
     */
    protected function integrationUsername(): string
    {
        $username = getenv('NATS_USERNAME');

        return is_string($username) && $username !== '' ? $username : 'local-user';
    }

    /**
     * Returns the configured valid password for user/password integration tests.
     */
    protected function integrationPassword(): string
    {
        $password = getenv('NATS_PASSWORD');

        return is_string($password) && $password !== '' ? $password : 'local-pass';
    }

    /**
     * Returns the configured invalid password for negative auth integration tests.
     */
    protected function integrationBadPassword(): string
    {
        $password = getenv('NATS_BAD_PASSWORD');

        return is_string($password) && $password !== '' ? $password : 'wrong-local-pass';
    }

    /**
     * Returns the configured standalone NKey server URL.
     */
    protected function integrationNkeyServerUrl(): string
    {
        $url = getenv('NATS_NKEY_URL');

        return is_string($url) && $url !== '' ? $url : 'nats://127.0.0.1:14226';
    }

    /**
     * Returns the configured JWT-auth NATS server URL.
     */
    protected function integrationJwtServerUrl(): string
    {
        $url = getenv('NATS_JWT_URL');

        return is_string($url) && $url !== '' ? $url : 'nats://127.0.0.1:14227';
    }

    /**
     * Returns the configured JWT used for resolver-backed auth integration tests.
     */
    protected function integrationJwt(): ?string
    {
        return $this->integrationFixtureValue('NATS_JWT', 'build/nats/jwt/user.jwt');
    }

    /**
     * Returns the configured standalone or JWT user NKey seed.
     */
    protected function integrationNkeySeed(string $envVar = 'NATS_NKEY_SEED'): string
    {
        $seed = getenv($envVar);

        return is_string($seed) && $seed !== '' ? $seed : 'SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY';
    }

    /**
     * Returns the configured JWT user seed used to sign the server nonce.
     */
    protected function integrationJwtSeed(): ?string
    {
        return $this->integrationFixtureValue('NATS_JWT_NKEY_SEED', 'build/nats/jwt/user.seed');
    }

    /**
     * Returns the configured TLS-enabled NATS server URL.
     */
    protected function integrationTlsServerUrl(): string
    {
        $url = getenv('NATS_TLS_URL');

        return is_string($url) && $url !== '' ? $url : 'tls://127.0.0.1:14225';
    }

    /**
     * Returns the CA file path for the TLS integration fixture when present.
     */
    protected function integrationTlsCaFile(): ?string
    {
        $file = getenv('NATS_TLS_CA_FILE');
        if (is_string($file) && $file !== '') {
            return $file;
        }

        $fallback = $this->repoRoot() . '/build/tls/ca.pem';

        return is_file($fallback) ? $fallback : null;
    }

    /**
     * Returns the client certificate path for the TLS integration fixture when present.
     */
    protected function integrationTlsCertFile(): ?string
    {
        $file = getenv('NATS_TLS_CERT_FILE');
        if (is_string($file) && $file !== '') {
            return $file;
        }

        $fallback = $this->repoRoot() . '/build/tls/client-cert.pem';

        return is_file($fallback) ? $fallback : null;
    }

    /**
     * Returns the client key path for the TLS integration fixture when present.
     */
    protected function integrationTlsKeyFile(): ?string
    {
        $file = getenv('NATS_TLS_KEY_FILE');
        if (is_string($file) && $file !== '') {
            return $file;
        }

        $fallback = $this->repoRoot() . '/build/tls/client-key.pem';

        return is_file($fallback) ? $fallback : null;
    }
}
