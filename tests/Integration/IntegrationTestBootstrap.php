<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Integration;

trait IntegrationTestBootstrap
{
    /**
     * Skips the current test unless integration tests are explicitly enabled.
     */
    protected function requireIntegrationEnabled(): void
    {
        $flag = getenv('RUN_INTEGRATION');
        if ($flag !== '1') {
            $this->markTestSkipped('Set RUN_INTEGRATION=1 to run integration tests.');
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
}
