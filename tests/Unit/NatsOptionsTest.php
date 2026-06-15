<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
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

    public function testRejectsNonPositiveConnectTimeout(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('connectTimeoutMs');
        new NatsOptions(connectTimeoutMs: 0);
    }

    public function testRejectsNonPositiveRequestTimeout(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('requestTimeoutMs');
        new NatsOptions(requestTimeoutMs: 0);
    }

    public function testRejectsZeroMaxPendingMessages(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('maxPendingMessagesPerSubscription');
        new NatsOptions(maxPendingMessagesPerSubscription: 0);
    }

    public function testRejectsNegativeReconnectValue(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('reconnectDelayMs');
        new NatsOptions(reconnectDelayMs: -1);
    }

    public function testAllowsDisabledHeartbeatAndEmptyServers(): void
    {
        // pingIntervalSeconds <= 0 disables the heartbeat, maxPingsOut 0 is aggressive-but-valid, and
        // an empty servers list falls back to the default - all legitimate, so none must be rejected.
        $options = new NatsOptions(servers: [], pingIntervalSeconds: 0, maxPingsOut: 0);

        self::assertSame(0, $options->pingIntervalSeconds);
        self::assertSame(0, $options->maxPingsOut);
    }

    /**
     * Every default documented in the README "Configuration Option Mapping" table must match the
     * actual constructor default. This is the test the table's caption cites, so it asserts the
     * full table, field by field, to keep the documentation honest.
     */
    public function testDefaultsMatchDocumentedValues(): void
    {
        $options = new NatsOptions();

        self::assertSame(['nats://127.0.0.1:4222'], $options->servers);
        self::assertSame('idct-php-nats-client', $options->name);
        self::assertSame('_INBOX', $options->inboxPrefix);
        self::assertSame(5000, $options->connectTimeoutMs);
        self::assertSame(10000, $options->requestTimeoutMs);
        self::assertTrue($options->reconnectEnabled);
        self::assertSame(10, $options->maxReconnectAttempts);
        self::assertSame(100, $options->reconnectDelayMs);
        self::assertSame(10000, $options->reconnectMaxDelayMs);
        self::assertSame(50, $options->reconnectJitterMs);
        self::assertSame(30, $options->pingIntervalSeconds);
        self::assertSame(2, $options->maxPingsOut);
        self::assertFalse($options->verbose);
        self::assertFalse($options->pedantic);
        self::assertFalse($options->noEcho);
        self::assertFalse($options->tlsRequired);
        self::assertFalse($options->tlsHandshakeFirst);
        self::assertNull($options->tlsCaFile);
        self::assertNull($options->tlsCertFile);
        self::assertNull($options->tlsKeyFile);
        self::assertNull($options->tlsKeyPassphrase);
        self::assertNull($options->tlsPeerName);
        self::assertTrue($options->tlsVerifyPeer);
        self::assertNull($options->token);
        self::assertNull($options->username);
        self::assertNull($options->password);
        self::assertNull($options->jwt);
        self::assertNull($options->nkey);
        self::assertNull($options->nonceSigner);
        self::assertSame(1024, $options->maxPendingMessagesPerSubscription);
        self::assertSame(SlowConsumerPolicy::DropOldest, $options->slowConsumerPolicy);
        self::assertNull($options->connectionListener);
        self::assertNull($options->errorListener);
        self::assertNull($options->jwtProvider);
        self::assertNull($options->tokenProvider);
        self::assertSame(8_388_608, $options->reconnectBufferSize);
        self::assertNull($options->tlsContext);
        self::assertFalse($options->randomizeServers);
        self::assertFalse($options->retryOnFailedInitialConnect);
        self::assertSame([], $options->webSocketHeaders);
        self::assertFalse($options->webSocketCompression);
        self::assertNull($options->logger);
    }
}
