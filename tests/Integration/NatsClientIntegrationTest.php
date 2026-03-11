<?php

declare(strict_types=1);

namespace Idct\Nats\Tests\Integration;

use Amp\Future;
use Idct\Nats\Connection\NatsOptions;
use Idct\Nats\Core\NatsClient;
use Idct\Nats\Core\NatsMessage;
use PHPUnit\Framework\TestCase;
use function Amp\async;

final class NatsClientIntegrationTest extends TestCase
{
    use IntegrationTestBootstrap;

    /**
     * Verifies connect and disconnect against a real NATS server.
     */
    public function testConnectAndDisconnect(): void
    {
        $this->requireIntegrationEnabled();

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        self::assertNotNull($client->serverInfo());

        $client->disconnect()->await();
        self::assertTrue(true);
    }

    /**
     * Verifies publish and subscribe delivery path against a live server.
     */
    public function testPublishAndSubscribeRoundTrip(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.subject.' . bin2hex(random_bytes(4));
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $received = null;
        $client->subscribe($subject, static function (NatsMessage $message) use (&$received): void {
            $received = $message;
        })->await();

        $client->publish($subject, 'hello')->await();
        $client->processIncoming()->await();

        self::assertInstanceOf(NatsMessage::class, $received);
        $message = $received;
        self::assertNotNull($message);
        self::assertSame('hello', $message->payload);

        $client->disconnect()->await();
    }

    /**
     * Verifies request/reply end-to-end using two live clients.
     */
    public function testRequestReply(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.echo.' . bin2hex(random_bytes(4));
        $server = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));

        $server->connect()->await();
        $client->connect()->await();

        $handled = false;
        $server->subscribe($subject, static function (NatsMessage $message) use (&$handled, $server): void {
            $handled = true;
            if ($message->replyTo !== null) {
                $server->publish($message->replyTo, 'world')->await();
            }
        })->await();

        // Process one request on the server in parallel while requester waits for its reply.
        $serverLoop = async(static function () use ($server): void {
            $server->processIncoming()->await();
        });

        $reply = $client->request($subject, 'hello', 2000)->await();
        $serverLoop->await();

        self::assertTrue($handled);
        self::assertSame('world', $reply->payload);

        $client->disconnect()->await();
        $server->disconnect()->await();
    }

    /**
     * Verifies connect can rotate to a later server entry when the first endpoint is unavailable.
     */
    public function testConnectWithServerRotationFallback(): void
    {
        $this->requireIntegrationEnabled();

        $url = $this->integrationServerUrl();
        $client = new NatsClient(
            new NatsOptions(
                servers: ['nats://127.0.0.1:5222', $url],
                reconnectEnabled: true,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 20,
                reconnectJitterMs: 0,
            ),
        );

        $client->connect()->await();

        self::assertNotNull($client->serverInfo());
        $client->disconnect()->await();
    }
}
