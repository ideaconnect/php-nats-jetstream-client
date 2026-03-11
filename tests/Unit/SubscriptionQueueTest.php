<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Core\SubscriptionQueue;
use IDCT\NATS\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

final class SubscriptionQueueTest extends TestCase
{
    private function makeConnectedClient(FakeTransport $transport): NatsClient
    {
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        return $client;
    }

    /** @return list<string> */
    private function infoAndPong(): array
    {
        return [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ];
    }

    public function testSubscribeQueueReturnsSidAndFetchesMessage(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG events 1 5\r\nhello\r\n",
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('events')->await();

        self::assertInstanceOf(SubscriptionQueue::class, $queue);
        self::assertSame(1, $queue->sid);

        $msg = $queue->fetch();
        self::assertNotNull($msg);
        self::assertSame('hello', $msg->payload);
        self::assertSame('events', $msg->subject);
    }

    public function testFetchReturnsNullWhenNoMessages(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('events')->await();

        $msg = $queue->fetch();
        self::assertNull($msg);
    }

    public function testNextReturnsBufferedMessageImmediately(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG data 1 3\r\nabc\r\n",
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('data')->await();

        // Pre-load the message into the internal buffer via processIncoming.
        $client->processIncoming()->await();

        $msg = $queue->next();
        self::assertNotNull($msg);
        self::assertSame('abc', $msg->payload);
    }

    public function testNextReturnsNullOnTimeout(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('empty')->await();
        $queue->setTimeout(0.01);

        $msg = $queue->next();
        self::assertNull($msg);
    }

    public function testFetchAllCollectsMultipleMessages(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG items 1 1\r\na\r\n",
            "MSG items 1 1\r\nb\r\n",
            "MSG items 1 1\r\nc\r\n",
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('items')->await();
        $queue->setTimeout(0.1);

        $messages = $queue->fetchAll();
        self::assertCount(3, $messages);
        self::assertSame('a', $messages[0]->payload);
        self::assertSame('b', $messages[1]->payload);
        self::assertSame('c', $messages[2]->payload);
    }

    public function testFetchAllRespectsLimit(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG items 1 1\r\na\r\n",
            "MSG items 1 1\r\nb\r\n",
            "MSG items 1 1\r\nc\r\n",
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('items')->await();
        $queue->setTimeout(0.1);

        $messages = $queue->fetchAll(2);
        self::assertCount(2, $messages);
        self::assertSame('a', $messages[0]->payload);
        self::assertSame('b', $messages[1]->payload);
    }

    public function testSubscribeQueueWithQueueGroup(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG work 1 4\r\njob1\r\n",
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('work', 'workers')->await();

        // Verify the SUB command includes the queue group.
        $allWrites = implode('||', $transport->writes);
        self::assertStringContainsString('SUB work workers 1', $allWrites);

        $msg = $queue->fetch();
        self::assertNotNull($msg);
        self::assertSame('job1', $msg->payload);
    }

    public function testSetTimeoutReturnsSelf(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
        ]);

        $client = $this->makeConnectedClient($transport);
        $queue = $client->subscribeQueue('x')->await();

        $result = $queue->setTimeout(5.0);
        self::assertSame($queue, $result);
    }
}
