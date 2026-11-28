<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Integration;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\NatsException;
use IDCT\NATS\JetStream\JetStreamContext;
use PHPUnit\Framework\TestCase;

use function Amp\delay;

/**
 * JetStream through a leaf node (#273), over the nats-hub / nats-leaf fixture of docker-compose.yml: the hub runs
 * JetStream with the domain `hub`, the leaf node runs none and is connected to the hub. A client on the leaf node gets
 * no responders on `$JS.API.>` and reaches the hub's JetStream with `jetStream(domain: 'hub')`, which sends the API
 * requests to `$JS.hub.API.>`.
 *
 * The tests skip when the leaf node is not running (a fixture stack from before #273).
 */
final class JetStreamDomainIntegrationTest extends TestCase
{
    use IntegrationTestBootstrap;

    /** @var list<NatsClient> */
    private array $clients = [];

    protected function tearDown(): void
    {
        foreach ($this->clients as $client) {
            try {
                $client->disconnect()->await();
            } catch (\Throwable) {
            }
        }
        $this->clients = [];
    }

    /**
     * On the leaf node the default context finds no JetStream (503), and the domain context reaches the hub's:
     * the account info names the domain `hub`.
     */
    public function testALeafNodeReachesTheHubsJetStreamOnlyUnderItsDomain(): void
    {
        $leaf = $this->connectToLeaf();

        try {
            $leaf->jetStream()->accountInfo()->await();
            self::fail('the leaf node has no JetStream of its own');
        } catch (JetStreamException $noJetStream) {
            self::assertSame(503, $noJetStream->getCode());
            self::assertStringContainsString('$JS.API.INFO', $noJetStream->getMessage());
        }

        $account = $this->hubThroughLeaf($leaf)->accountInfo()->await();
        self::assertSame('hub', $account->raw['domain'] ?? null);
    }

    /**
     * Through the leaf node: a stream and a durable consumer are created on the hub, a message published to the
     * stream's plain subject is acknowledged by the hub's domain, pulled back and acknowledged (ackSync, so the hub
     * confirms it), a second one is consumed by a pull consume (`pullConsumer()->handle()`, whose pulls go under the
     * domain too) and acknowledged, and the consumer has nothing left to deliver or to wait for an ack on. A client
     * connected to the hub directly sees the same stream.
     */
    public function testAStreamAndAConsumerOnTheHubAreUsedThroughTheLeafNode(): void
    {
        $leaf = $this->connectToLeaf();
        $js = $this->hubThroughLeaf($leaf);
        $suffix = strtolower(bin2hex(random_bytes(3)));
        $stream = 'LEAF_' . strtoupper($suffix);
        $subject = 'leaf.' . $suffix . '.created';

        $js->createStream($stream, ['leaf.' . $suffix . '.>'])->await();

        try {
            $js->createConsumer($stream, 'reader', options: ['ack_policy' => 'explicit'])->await();

            $ack = $js->publish($subject, '{"id":1}')->await();
            self::assertSame($stream, $ack->stream);
            self::assertSame(1, $ack->seq);
            self::assertSame('hub', $ack->raw['domain'] ?? null);

            $messages = $js->fetchBatch($stream, 'reader', 1, 3000)->await();
            self::assertCount(1, $messages);
            self::assertSame($subject, $messages[0]->subject);
            self::assertSame('{"id":1}', $messages[0]->payload);
            self::assertSame(1, $js->messageMetadata($messages[0])->streamSequence);
            $js->ackSync($messages[0])->await();

            $js->publish($subject, '{"id":2}')->await();
            $handled = [];
            $consumed = $js->pullConsumer($stream, 'reader')
                ->setBatching(1)->setDepth(1)->setExpiresMs(3000)->setIterations(1)
                ->handle(static function (NatsMessage $message, JetStreamContext $context) use (&$handled): void {
                    $handled[] = $message->payload;
                    $context->ackSync($message)->await();
                })->await();
            self::assertSame(1, $consumed);
            self::assertSame(['{"id":2}'], $handled);

            $consumer = $js->getConsumer($stream, 'reader')->await();
            self::assertSame(0, (int) ($consumer->raw['num_ack_pending'] ?? -1));
            self::assertSame(0, (int) ($consumer->raw['num_pending'] ?? -1));

            $hub = $this->connect($this->integrationHubServerUrl());
            self::assertSame(2, $hub->jetStream()->getStream($stream)->await()->raw['state']['messages'] ?? null);
        } finally {
            $js->deleteStream($stream)->await();
        }
    }

    /**
     * A KV bucket on the hub is created, written, read, updated, listed and deleted through the leaf node. The writes
     * go through the domain's API prefix (`$JS.hub.API.$KV.<bucket>.<key>`, nats.go useJSPfx parity); a plain `$KV.`
     * write from the leaf node would get no responders. A client on the hub reads the value the leaf wrote.
     */
    public function testAKvBucketOnTheHubIsUsedThroughTheLeafNode(): void
    {
        $leaf = $this->connectToLeaf();
        $js = $this->hubThroughLeaf($leaf);
        $bucket = 'leaf_kv_' . bin2hex(random_bytes(3));
        $kv = $js->keyValue($bucket);
        $kv->create()->await();

        try {
            $first = $kv->put('color', 'blue')->await();
            self::assertSame('KV_' . $bucket, $first->stream);
            self::assertSame('blue', $kv->get('color')->await()?->value);

            $kv->update('color', 'green', $first->seq)->await();
            $kv->put('size', 'xl')->await();
            $kv->delete('size')->await();

            $entry = $kv->get('color')->await();
            self::assertSame('green', $entry->value);
            self::assertSame(2, $entry->revision);
            self::assertSame(['color'], $kv->keys()->await());

            $hub = $this->connect($this->integrationHubServerUrl());
            self::assertSame('green', $hub->jetStream()->keyValue($bucket)->get('color')->await()?->value);

            try {
                $leaf->request('$KV.' . $bucket . '.color', 'plain')->await();
                self::fail('a plain $KV. write from the leaf node reached the hub');
            } catch (NatsException $plainWrite) {
                self::assertStringContainsString('No responders', $plainWrite->getMessage());
            }
        } finally {
            $kv->deleteBucket()->await();
        }
    }

    /**
     * An Object Store bucket on the hub is written and read back through the leaf node, with an object of several
     * chunks: the chunk and meta publishes go to their plain `$O.` subjects, the reads through the domain's API.
     */
    public function testAnObjectStoreOnTheHubIsUsedThroughTheLeafNode(): void
    {
        $leaf = $this->connectToLeaf();
        $js = $this->hubThroughLeaf($leaf);
        $bucket = 'leaf_obj_' . bin2hex(random_bytes(3));
        $store = $js->objectStore($bucket);
        $store->create()->await();

        try {
            $data = random_bytes(300 * 1024);
            $info = $store->put('report.bin', $data)->await();
            self::assertSame(strlen($data), $info->size);
            self::assertGreaterThan(1, $info->chunks);

            $read = $store->get('report.bin')->await();
            self::assertSame($data, $read?->data);
            self::assertSame(['report.bin'], array_map(static fn($object): string => $object->name, $store->list()->await()));
        } finally {
            $store->deleteBucket()->await();
        }
    }

    /**
     * A client on the leaf node, its JetStream context for the domain `hub` once the leaf node's connection to the hub
     * carries the domain's API (the wait script checks the connection; this waits for the route as well).
     */
    private function hubThroughLeaf(NatsClient $leaf): JetStreamContext
    {
        $js = $leaf->jetStream(domain: 'hub');
        $deadline = $this->monotonic() + 15.0;
        while (true) {
            try {
                $js->accountInfo()->await();

                return $js;
            } catch (JetStreamException $notYet) {
                if ($notYet->getCode() !== 503 || $this->monotonic() > $deadline) {
                    throw $notYet;
                }
            }

            delay(0.2);
        }
    }

    private function connectToLeaf(): NatsClient
    {
        $this->requireIntegrationEnabled();

        // Probe the port without a client first: a NatsClient retries a refused first connect
        // (maxReconnectAttempts, with backoff), which would hold the test for minutes.
        $url = $this->integrationLeafServerUrl();
        $host = (string) (parse_url($url, PHP_URL_HOST) ?? '127.0.0.1');
        $port = (int) (parse_url($url, PHP_URL_PORT) ?? 4222);
        $probe = @stream_socket_client('tcp://' . $host . ':' . $port, $errorCode, $errorMessage, 2.0);
        if ($probe === false) {
            self::markTestSkipped('The nats-leaf fixture is not running at ' . $url . ' (' . $errorMessage . ').');
        }
        fclose($probe);

        return $this->connect($url);
    }

    private function connect(string $url): NatsClient
    {
        $client = new NatsClient(new NatsOptions(servers: [$url], requestTimeoutMs: 5000));
        $client->connect()->await();
        $this->clients[] = $client;

        return $client;
    }
}
