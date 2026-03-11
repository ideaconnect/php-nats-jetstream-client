<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

final class NatsClientTest extends TestCase
{
    /**
     * Verifies facade delegates connect/publish behavior to connection runtime.
     */
    public function testClientConnectAndPublishDelegatesToConnection(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(
            options: new NatsOptions(servers: ['nats://127.0.0.1:4222'], name: 'client-test'),
            transport: $transport,
        );

        $client->connect()->await();
        $client->publish('orders.created', '{"id":1}')->await();

        self::assertNotNull($client->serverInfo());
        $serverInfo = $client->serverInfo();
        self::assertNotNull($serverInfo);
        self::assertSame('n1', $serverInfo->serverName);
        self::assertCount(3, $transport->writes);
        self::assertSame("PUB orders.created 8\r\n{\"id\":1}\r\n", $transport->writes[2]);
    }

    /**
     * Verifies facade subscribe API receives dispatched incoming messages.
     */
    public function testClientSubscribeAndProcessIncoming(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            "MSG updates 1 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $message = null;
        $sid = $client->subscribe('updates', static function (NatsMessage $incoming) use (&$message): void {
            $message = $incoming;
        })->await();

        self::assertSame(1, $sid);
        self::assertSame(1, $client->processIncoming()->await());
        self::assertInstanceOf(NatsMessage::class, $message);
        $receivedMessage = $message;
        self::assertNotNull($receivedMessage);
        self::assertSame('hello', $receivedMessage->payload);
    }

    /**
     * Verifies facade request API resolves with the first reply message.
     */
    public function testClientRequestReturnsReply(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            "MSG _INBOX.any 1 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $reply = $client->request('svc.echo', '{"x":1}', 50)->await();

        self::assertSame('hello', $reply->payload);
    }

    /**
     * Verifies facade forwards cancellation to request implementation.
     */
    public function testClientRequestCanBeCancelled(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $deferredCancellation = new DeferredCancellation();
        $deferredCancellation->cancel();

        $this->expectException(CancelledException::class);
        $client->request('svc.echo', '{"x":1}', 1_000, $deferredCancellation->getCancellation())->await();
    }
}
