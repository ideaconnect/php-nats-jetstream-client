<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Services\Service;
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
        self::assertSame('hello', $message->payload);
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

    /**
     * Verifies facade delegates header-aware publish/request variants.
     */
    public function testClientPublishWithHeadersAndRequestWithHeaders(): void
    {
        $replyPayload = '{"ok":true}';
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            sprintf("MSG _INBOX.any 1 %d\r\n%s\r\n", strlen($replyPayload), $replyPayload),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->publishWithHeaders('orders.created', '{"id":1}', ['X-Test' => '1'])->await();
        $reply = $client->requestWithHeaders('svc.echo', '{"x":1}', ['X-Correlation-Id' => 'abc'], 50)->await();

        self::assertSame('{"ok":true}', $reply->payload);
        self::assertStringStartsWith('HPUB orders.created ', $transport->writes[2]);
        self::assertStringContainsString('X-Test:1', $transport->writes[2]);
        self::assertStringStartsWith('HPUB svc.echo _INBOX.', $transport->writes[4]);
        self::assertStringContainsString('X-Correlation-Id:abc', $transport->writes[4]);
    }

    /**
     * Verifies facade service factory and lifecycle delegates (drain/disconnect).
     */
    public function testClientServiceFactoryDisconnectAndDrain(): void
    {
        $transportA = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            'PONG',
        ]);

        $clientA = new NatsClient(new NatsOptions(), $transportA);
        $clientA->connect()->await();
        $service = $clientA->service('orders', '1.0.0', 'Order API', ['team' => 'core']);
        self::assertInstanceOf(Service::class, $service);

        $sid = $clientA->subscribe('events', static function (NatsMessage $message): void {
        })->await();
        self::assertSame(1, $sid);

        $clientA->drain()->await();
        self::assertTrue($transportA->closed);

        $transportB = new FakeTransport();
        $clientB = new NatsClient(new NatsOptions(), $transportB);
        $clientB->disconnect()->await();
        self::assertTrue($transportB->closed);
    }
}
