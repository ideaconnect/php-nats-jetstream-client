<?php

declare(strict_types=1);

namespace Idct\Nats\Tests\Unit;

use Idct\Nats\Connection\NatsOptions;
use Idct\Nats\Core\NatsClient;
use Idct\Nats\Core\NatsMessage;
use Idct\Nats\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

final class ServiceTest extends TestCase
{
    /**
     * Verifies service start registers discovery and endpoint subscriptions.
     */
    public function testStartRegistersSubscriptions(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')->addEndpoint(
            'echo',
            'svc.echo',
            static fn (NatsMessage $message): string => $message->payload,
            'q.echo',
        );

        $service->start()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('SUB $SRV.PING 1' . "\r\n", $writes);
        self::assertStringContainsString('SUB $SRV.INFO.echo', $writes);
        self::assertStringContainsString('SUB $SRV.STATS.echo', $writes);
        self::assertStringContainsString('SUB svc.echo q.echo', $writes);
    }

    /**
     * Verifies ping/info/stats discovery replies are published.
     */
    public function testDiscoveryReplies(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            "MSG \$SRV.PING 1 _INBOX.ping 0\r\n\r\n",
            "MSG \$SRV.INFO.echo 5 _INBOX.info 0\r\n\r\n",
            "MSG \$SRV.STATS.echo 8 _INBOX.stats 0\r\n\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0', 'Echo service')
            ->addEndpoint('echo', 'svc.echo', static fn (NatsMessage $message): string => $message->payload);
        $service->start()->await();

        $client->processIncoming()->await();
        $client->processIncoming()->await();
        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('PUB _INBOX.ping', $writes);
        self::assertStringContainsString('io.nats.micro.v1.ping_response', $writes);
        self::assertStringContainsString('PUB _INBOX.info', $writes);
        self::assertStringContainsString('io.nats.micro.v1.info_response', $writes);
        self::assertStringContainsString('PUB _INBOX.stats', $writes);
        self::assertStringContainsString('io.nats.micro.v1.stats_response', $writes);
    }

    /**
     * Verifies endpoint request is processed and response is published to reply subject.
     */
    public function testEndpointHandlesRequests(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            "MSG svc.echo 10 _INBOX.req 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn (NatsMessage $message): array => ['echo' => $message->payload]);
        $service->start()->await();

        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('PUB _INBOX.req', $writes);
        self::assertStringContainsString('{"echo":"hello"}', $writes);
    }

    /**
     * Verifies stop unsubscribes all registered service subscriptions.
     */
    public function testStopUnsubscribesAll(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn (NatsMessage $message): string => $message->payload);
        $service->start()->await();
        $service->stop()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString("UNSUB 1\r\n", $writes);
        self::assertStringContainsString("UNSUB 10\r\n", $writes);
    }
}
