<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Integration;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\FastBatchException;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\UnsupportedFeatureException;
use IDCT\NATS\JetStream\FeatureSupport;
use IDCT\NATS\JetStream\JetStreamContext;
use PHPUnit\Framework\TestCase;

/**
 * Fast-ingest batch publish (ADR-50, #12) against a live server: nats-server 2.14+ (the `nats-214` fixture, in the
 * second stage of `composer test:e2e`), and the 2.12 fixture for what an older server does.
 */
final class FastIngestIntegrationTest extends TestCase
{
    use IntegrationTestBootstrap;

    private ?NatsClient $client = null;

    /** @var list<string> */
    private array $streams = [];

    protected function tearDown(): void
    {
        if ($this->client !== null) {
            $js = $this->client->jetStream();
            foreach ($this->streams as $stream) {
                try {
                    $js->deleteStream($stream)->await();
                } catch (JetStreamException) {
                    // Already gone.
                }
            }

            $this->client->disconnect()->await();
            $this->client = null;
        }
    }

    public function testAThousandAddsAndACommitAreStoredInOrder(): void
    {
        $js = $this->fastIngestServer();
        [$stream, $subject] = $this->stream($js, allowBatched: true);

        $fast = $js->fastPublisher();
        for ($i = 1; $i <= 1000; ++$i) {
            self::assertSame($i, $fast->add($subject, 'm-' . $i)->await()->batchSequence);
        }
        $pubAck = $fast->commit($subject, 'm-1001')->await();

        self::assertSame($stream, $pubAck->stream);
        self::assertSame(1001, $pubAck->seq);
        self::assertSame(1001, $pubAck->batchCount);
        self::assertSame($fast->batchId(), $pubAck->batchId);
        self::assertTrue($fast->isClosed());
        self::assertSame(1001, $this->storedMessages($js, $stream));
        self::assertSame('m-1', $js->getStreamMessage($stream, 1)->await()->payload);
        self::assertSame('m-1001', $js->getStreamMessage($stream, 1001)->await()->payload);
    }

    public function testCloseEndsTheBatchWithoutStoringAnotherMessage(): void
    {
        $js = $this->fastIngestServer();
        [$stream, $subject] = $this->stream($js, allowBatched: true);

        $fast = $js->fastPublisher(flow: 10);
        for ($i = 1; $i <= 5; ++$i) {
            $fast->add($subject, 'm-' . $i)->await();
        }
        $pubAck = $fast->close()->await();

        self::assertSame(5, $pubAck->batchCount);
        self::assertSame(5, $pubAck->seq);
        self::assertSame(5, $this->storedMessages($js, $stream));
    }

    public function testAStreamWithoutAllowBatchedRefusesTheBatch(): void
    {
        $js = $this->fastIngestServer();
        [$stream, $subject] = $this->stream($js, allowBatched: false);

        $fast = $js->fastPublisher();
        try {
            $fast->add($subject, 'm-1')->await();
            self::fail('expected the server to refuse the batch');
        } catch (JetStreamException $e) {
            self::assertSame(10205, $e->getErrCode());
        }

        self::assertTrue($fast->isClosed());
        self::assertSame(0, $this->storedMessages($js, $stream));
    }

    /**
     * Case K: in gap mode `ok` a message refused by its own check (Nats-Expected-Last-Sequence) reaches onError, the
     * batch goes on, and the commit counts its batch sequence (4) while 3 messages were stored.
     */
    public function testARefusedMessageInOkModeIsANoticeAndTheBatchGoesOn(): void
    {
        $js = $this->fastIngestServer();
        [$stream, $subject] = $this->stream($js, allowBatched: true);

        $notices = [];
        $fast = $js->fastPublisher(flow: 10, continueOnGap: true, onError: static function (FastBatchException $notice) use (&$notices): void {
            $notices[] = $notice;
        });
        $fast->add($subject, 'm-1')->await();
        $fast->add($subject, 'm-2', ['Nats-Expected-Last-Sequence' => '999999'])->await();
        $fast->add($subject, 'm-3')->await();
        $pubAck = $fast->commit($subject, 'm-4')->await();

        self::assertSame(4, $pubAck->batchCount);
        self::assertCount(1, $notices);
        self::assertSame(10071, $notices[0]->getErrCode());
        self::assertSame(2, $notices[0]->batchSequence);
        self::assertSame(3, $this->storedMessages($js, $stream));
    }

    /**
     * Case J: in gap mode `fail` the same refused message ends the batch at the message before it.
     */
    public function testARefusedMessageInFailModeEndsTheBatch(): void
    {
        $js = $this->fastIngestServer();
        [$stream, $subject] = $this->stream($js, allowBatched: true);

        $fast = $js->fastPublisher(flow: 10);
        $fast->add($subject, 'm-1')->await();
        $fast->add($subject, 'm-2', ['Nats-Expected-Last-Sequence' => '999999'])->await();

        try {
            $fast->commit($subject, 'm-3')->await();
            self::fail('expected the batch to have ended');
        } catch (FastBatchException $e) {
            self::assertSame(1, $e->pubAck?->batchCount);
            $refused = $e->getPrevious();
            self::assertInstanceOf(JetStreamException::class, $refused);
            self::assertSame(10071, $refused->getErrCode());
        }

        self::assertSame(1, $this->storedMessages($js, $stream));
    }

    /**
     * A server that predates fast ingest (the 2.12 fixture) stores the first message as a plain publish: the first
     * add() fails as unsupported, and the stream holds that one message. The second e2e stage leaves it out.
     */
    public function testAServerBefore214StoresTheFirstMessageAsAPlainPublish(): void
    {
        $js = $this->client()->jetStream();
        $version = $this->client?->serverInfo()?->version;
        if (!FeatureSupport::serverIsOlderThan($version, '2.14')) {
            self::markTestSkipped(sprintf('Needs a server older than 2.14; the server reports %s.', (string) $version));
        }

        [$stream, $subject] = $this->stream($js, allowBatched: false);

        $fast = $js->fastPublisher();
        try {
            $fast->add($subject, 'm-1')->await();
            self::fail('expected UnsupportedFeatureException');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame('allow_batched', $e->feature);
            self::assertSame('2.14', $e->requiredVersion);
            self::assertStringContainsString(sprintf('plain publish (stream "%s", seq 1)', $stream), $e->getMessage());
        }

        self::assertTrue($fast->isClosed());
        self::assertSame(1, $this->storedMessages($js, $stream));

        // A stream that asks for allow_batched is refused on such a server, as every version-gated field is (#266).
        try {
            $js->createStream($stream . 'B', ['it.' . strtolower($stream) . '.batched'], ['allow_batched' => true])->await();
            $this->streams[] = $stream . 'B';
            self::fail('expected UnsupportedFeatureException for allow_batched');
        } catch (UnsupportedFeatureException $e) {
            self::assertSame('allow_batched', $e->feature);
            self::assertSame('2.14', $e->requiredVersion);
        }
    }

    private function client(): NatsClient
    {
        $this->requireIntegrationEnabled();

        if ($this->client === null) {
            $this->client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()], requestTimeoutMs: 5_000));
            $this->client->connect()->await();
        }

        return $this->client;
    }

    /** The JetStream context of a connected client, the test skipped unless the server is 2.14+. */
    private function fastIngestServer(): JetStreamContext
    {
        $client = $this->client();
        $version = $client->serverInfo()?->version;
        if (FeatureSupport::serverIsOlderThan($version, '2.14')) {
            self::markTestSkipped(sprintf('Fast-ingest batch publish needs NATS server 2.14+; the server reports %s.', (string) $version));
        }

        return $client->jetStream();
    }

    /**
     * A new stream on one subject, with or without `allow_batched`; tearDown() deletes it.
     *
     * @return array{string, string}
     */
    private function stream(JetStreamContext $js, bool $allowBatched): array
    {
        $stream = 'ITFI' . strtoupper(bin2hex(random_bytes(3)));
        $subject = 'it.' . strtolower($stream) . '.events';
        $js->createStream($stream, [$subject], $allowBatched ? ['allow_batched' => true] : [])->await();
        $this->streams[] = $stream;

        return [$stream, $subject];
    }

    private function storedMessages(JetStreamContext $js, string $stream): int
    {
        $state = $js->getStream($stream)->await()->raw['state'] ?? null;

        return is_array($state) ? (int) ($state['messages'] ?? 0) : 0;
    }
}
