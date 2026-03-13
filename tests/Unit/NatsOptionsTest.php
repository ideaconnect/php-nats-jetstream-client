<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsOptions;
use PHPUnit\Framework\TestCase;

final class NatsOptionsTest extends TestCase
{
    public function testFirstServerReturnsConfiguredFirstEndpoint(): void
    {
        $options = new NatsOptions(servers: ['nats://a:4222', 'nats://b:4222']);

        self::assertSame('nats://a:4222', $options->firstServer());
    }

    public function testFirstServerFallsBackWhenServersListIsEmpty(): void
    {
        $options = new NatsOptions(servers: []);

        self::assertSame('nats://127.0.0.1:4222', $options->firstServer());
    }
}
