<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\JetStream\Consumers\PullConsumerIterator;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

final class PullConsumerIteratorTest extends TestCase
{
    /** @return list<string> */
    private function infoAndPong(): array
    {
        return [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ];
    }

    private function jsMsg(string $subject, string $payload, string $replyTo): string
    {
        return sprintf("MSG %s 1 %s %d\r\n%s\r\n", $subject, $replyTo, strlen($payload), $payload);
    }

    public function testFluentBuilderSetsProperties(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $js = $client->jetStream();

        $iter = $js->pullConsumer('ORDERS', 'PROC')
            ->setBatching(10)
            ->setExpiresMs(5000)
            ->setIterations(3);

        self::assertInstanceOf(PullConsumerIterator::class, $iter);
        self::assertSame(10, $iter->getBatching());
        self::assertSame(5000, $iter->getExpiresMs());
        self::assertSame(3, $iter->getIterations());
    }

    public function testDefaultValues(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        self::assertSame(1, $iter->getBatching());
        self::assertSame(3000, $iter->getExpiresMs());
        self::assertNull($iter->getIterations());
    }

    public function testSetBatchingRejectsZero(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        $this->expectException(JetStreamException::class);
        $iter->setBatching(0);
    }

    public function testSetExpiresMsRejectsZero(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        $this->expectException(JetStreamException::class);
        $iter->setExpiresMs(0);
    }

    public function testSetIterationsRejectsZero(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C');

        $this->expectException(JetStreamException::class);
        $iter->setIterations(0);
    }

    public function testSetIterationsAcceptsNull(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $iter = $client->jetStream()->pullConsumer('S', 'C')
            ->setIterations(5)
            ->setIterations(null);

        self::assertNull($iter->getIterations());
    }

    public function testHandleProcessesOneIteration(): void
    {
        $statusHeaders = "NATS/1.0 404 No Messages\r\nStatus: 404\r\n\r\n";
        $hdrLen = strlen($statusHeaders);

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // First iteration: 1 message delivered.
            $this->jsMsg('_INBOX.JS.FETCH.any', 'order-1', '$JS.ACK.ORDERS.PROC.1.1.1.123.0'),
            // Second iteration: terminal 404 status → JetStreamException breaks loop.
            sprintf("HMSG _INBOX.JS.FETCH.any 2 %d %d\r\n%s\r\n", $hdrLen, $hdrLen, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $processed = [];
        $total = $client->jetStream()
            ->pullConsumer('ORDERS', 'PROC')
            ->setBatching(1)
            ->setExpiresMs(500)
            ->setIterations(2)
            ->handle(function (NatsMessage $msg, JetStreamContext $js) use (&$processed): void {
                $processed[] = $msg->payload;
            })->await();

        self::assertSame(1, $total);
        self::assertSame(['order-1'], $processed);
    }

    public function testHandleStopsOnNoMessages(): void
    {
        $statusHeaders = "NATS/1.0 404 No Messages\r\nStatus: 404\r\n\r\n";
        $hdrLen = strlen($statusHeaders);

        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // Immediately returns terminal 404 status — no messages.
            sprintf("HMSG _INBOX.JS.FETCH.any 1 %d %d\r\n%s\r\n", $hdrLen, $hdrLen, $statusHeaders),
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $total = $client->jetStream()
            ->pullConsumer('STREAM', 'CONS')
            ->setBatching(5)
            ->setIterations(10)
            ->setExpiresMs(100)
            ->handle(function (): void {
                self::fail('Handler should not be called when no messages are available');
            })->await();

        self::assertSame(0, $total);
    }
}
