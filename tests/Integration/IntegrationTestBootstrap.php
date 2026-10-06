<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Integration;

use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Tests\Support\GatedDialTransport;
use IDCT\NATS\Tests\Support\SeveringTransport;
use IDCT\NATS\Transport\AmpSocketTransport;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;
use RuntimeException;

use function Amp\delay;

trait IntegrationTestBootstrap
{
    /**
     * Connects a client whose "server" the test can take down: a live transport that can be severed and
     * whose dials can be refused, with a fast heartbeat so a dead socket is noticed - and recovered in
     * the heartbeat's own fiber - while the caller merely waits.
     *
     * The heartbeat notices the severed socket because its next PING write fails (or its read meets the
     * EOF), never because PONGs are missing, so it is not allowed to give up on unanswered PINGs: with
     * maxPingsOut 1 every PONG had to be read before the next tick, 50 ms later, and a server that once
     * took longer to answer, as one on a loaded CI runner does now and then, cost the live connection an
     * extra reconnect, which then failed the tests that count them. A thousand ticks is 50 s of silence,
     * longer than any test lasts.
     *
     * @return array{NatsClient, GatedDialTransport}
     */
    protected function connectRecoverableClient(bool $waitForReconnect = true): array
    {
        $options = new NatsOptions(
            servers: [$this->integrationServerUrl()],
            reconnectEnabled: true,
            maxReconnectAttempts: 200,
            reconnectDelayMs: 10,
            reconnectMaxDelayMs: 50,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0.05,
            maxPingsOut: 1_000,
            waitForReconnect: $waitForReconnect,
        );
        $transport = new GatedDialTransport(new SeveringTransport(new AmpSocketTransport($options)));
        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        return [$client, $transport];
    }

    /**
     * Takes the server down for one client: kills its live socket and refuses new dials, then lets the
     * client's heartbeat - not a user read - notice and start the recovery in its own fiber.
     */
    protected function takeServerDownFor(NatsClient $client, GatedDialTransport $transport): void
    {
        $transport->refuseDials();
        $transport->sever();

        $deadline = $this->monotonic() + 5.0;
        while ($client->state() === ConnectionState::Open) {
            if ($this->monotonic() > $deadline) {
                throw new RuntimeException('The heartbeat did not notice the dead connection within 5 s.');
            }

            delay(0.01);
        }
    }

    /**
     * Brings the server back after $seconds, from a timer: it fires only while something waits on the
     * event loop, so an operation that failed on the spot instead of waiting would never see it.
     */
    protected function bringServerBackAfter(GatedDialTransport $transport, float $seconds): void
    {
        EventLoop::delay($seconds, static function () use ($transport): void {
            $transport->acceptDials();
        });
    }

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
     * Monotonic clock in seconds, for timing/deadline measurement in tests. Uses hrtime() rather than
     * microtime(): the host wall clock is not guaranteed monotonic (it can jump forward or backward,
     * e.g. under WSL or VM clock sync), which makes wall-clock elapsed/deadline checks flaky (#70).
     */
    protected function monotonic(): float
    {
        return hrtime(true) / 1_000_000_000;
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
     * Returns the configured WebSocket NATS server URL (ws://).
     */
    protected function integrationWsServerUrl(): string
    {
        $url = getenv('NATS_WS_URL');

        return is_string($url) && $url !== '' ? $url : 'ws://127.0.0.1:14229';
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
     * Returns the configured standard (non-handshake-first) TLS-upgrade NATS server URL.
     */
    protected function integrationTlsUpgradeServerUrl(): string
    {
        $url = getenv('NATS_TLS_UPGRADE_URL');

        return is_string($url) && $url !== '' ? $url : 'tls://127.0.0.1:14228';
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
