<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Integration;

use Amp\CancelledException;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\JetStream\ApiErrCode;
use IDCT\NATS\JetStream\JetStreamContext;
use PHPUnit\Framework\TestCase;

use function Amp\delay;

/**
 * The consumer delivery state reset of ADR-60 (#274) against a live server. nats-server 2.14 added
 * `$JS.API.CONSUMER.RESET`; the fixture stack runs 2.12, so there only the timeout of a server without it runs and
 * the rest is skipped. Point NATS_URL at a 2.14+ server to run them (e.g. `docker run -p 24222:4222 nats:2.14-alpine
 * -js` and `NATS_URL=nats://127.0.0.1:24222`). Which tests run is decided by the API level `$JS.API.INFO` reports
 * (ADR-44), not by the INFO version.
 *
 * The consume tests reset the consumer from a second client, as an operator or the CLI would.
 */
final class ConsumerResetIntegrationTest extends TestCase
{
    use IntegrationTestBootstrap;

    /** The JetStream API level of nats-server 2.14, the first with the consumer reset API. */
    private const RESET_API_LEVEL = 4;

    /** @var list<NatsClient> */
    private array $clients = [];

    /** @var list<string> */
    private array $streams = [];

    protected function tearDown(): void
    {
        foreach ($this->streams as $stream) {
            try {
                $this->clients[0]->jetStream()->deleteStream($stream)->await();
            } catch (\Throwable) {
            }
        }

        foreach ($this->clients as $client) {
            try {
                $client->disconnect()->await();
            } catch (\Throwable) {
            }
        }
    }

    /**
     * A server before 2.14 does not answer the reset: the request is sent whatever version INFO reports (ADR-44,
     * #272), the call fails with the request timeout, and the consumer is untouched. The server is judged by the API
     * level `$JS.API.INFO` reports (below 4 before 2.14), not by its INFO version.
     */
    public function testAResetTimesOutOnAServerBefore214(): void
    {
        $this->requireIntegrationEnabled();

        $client = $this->client();
        $js = $client->jetStream();
        $apiLevel = $js->accountInfo()->await()->apiLevel;
        if ($apiLevel !== null && $apiLevel >= self::RESET_API_LEVEL) {
            self::markTestSkipped(sprintf('The server (API level %d) offers the consumer reset API.', $apiLevel));
        }

        $stream = $this->stream($js, 3);
        $js->createConsumer($stream, 'worker', null, ['ack_policy' => 'explicit'])->await();

        try {
            // A short request timeout for the reset only: the setup above keeps the default.
            $this->client(1_000)->jetStream()->resetConsumer($stream, 'worker', 2)->await();
            self::fail(sprintf('expected the reset to time out on %s', (string) $client->serverInfo()?->version));
        } catch (TimeoutException) {
            // A server without the reset API does not answer it.
        }

        self::assertSame('m-1', $js->fetchNext($stream, 'worker', 2_000)->await()->payload, 'the consumer is untouched');
    }

    /**
     * A durable pull consumer that fetched and acked m-1 to m-3, reset to stream sequence 2, delivers m-2 next, with
     * consumer sequence 1; reset again without a sequence (to its ack floor, stream sequence 1 now), it delivers m-2
     * once more, since m-2 was not acked.
     */
    public function testAResetToASequenceDeliversFromThatSequence(): void
    {
        $js = $this->clientWithResetApi()->jetStream();
        $stream = $this->stream($js, 5);
        $js->createConsumer($stream, 'worker', null, ['ack_policy' => 'explicit'])->await();
        foreach ($js->fetchBatch($stream, 'worker', 3, 2_000)->await() as $message) {
            $js->ackSync($message)->await();
        }

        $info = $js->resetConsumer($stream, 'worker', 2)->await();

        self::assertSame(2, $info->resetSequence);
        self::assertSame($stream, $info->streamName);
        self::assertSame('worker', $info->name);
        self::assertSame(['consumer_seq' => 0, 'stream_seq' => 1], $info->raw['delivered'] ?? null);
        self::assertSame(4, $info->raw['num_pending'] ?? null);

        $next = $js->fetchNext($stream, 'worker', 2_000)->await();
        self::assertSame('m-2', $next->payload);
        $metadata = $js->messageMetadata($next);
        self::assertSame(2, $metadata->streamSequence);
        self::assertSame(1, $metadata->consumerSequence);

        $again = $js->resetConsumer($stream, 'worker')->await();
        self::assertSame(2, $again->resetSequence, 'without a sequence the consumer goes back to just above its ack floor');
        self::assertSame('m-2', $js->fetchNext($stream, 'worker', 2_000)->await()->payload);
    }

    /**
     * A sequence on a consumer that delivers only new messages is refused by the server with 400 and err_code 10204.
     */
    public function testAnInvalidResetFailsWithItsErrCode(): void
    {
        $js = $this->clientWithResetApi()->jetStream();
        $stream = $this->stream($js, 2);
        $js->createConsumer($stream, 'fresh', null, ['ack_policy' => 'explicit', 'deliver_policy' => 'new'])->await();

        try {
            $js->resetConsumer($stream, 'fresh', 1)->await();
            self::fail('expected the reset to be refused');
        } catch (JetStreamException $e) {
            self::assertSame(400, $e->getCode());
            self::assertSame(ApiErrCode::CONSUMER_INVALID_RESET, $e->getErrCode());
            self::assertStringStartsWith('invalid reset', $e->getMessage());
        }
    }

    /**
     * The consumer itself answers its reset, so a consumer that does not exist gets no answer: the call times out.
     */
    public function testAResetOfAConsumerThatDoesNotExistTimesOut(): void
    {
        $client = $this->clientWithResetApi(500);
        $stream = $this->stream($client->jetStream(), 1);

        $this->expectException(TimeoutException::class);

        $client->jetStream()->resetConsumer($stream, 'missing')->await();
    }

    /**
     * A running pull consume keeps delivering after another client resets its consumer to stream sequence 2: it gets
     * m-2 and m-3 again, then m-4 when it is published.
     */
    public function testARunningPullConsumeKeepsDeliveringAfterAnotherClientResetsItsConsumer(): void
    {
        $admin = $this->clientWithResetApi()->jetStream();
        $stream = $this->stream($admin, 3);
        $admin->createConsumer($stream, 'worker', null, ['ack_policy' => 'explicit'])->await();

        $worker = $this->client();
        /** @var \ArrayObject<int, string> $handled */
        $handled = new \ArrayObject();
        $iterator = $worker->jetStream()->pullConsumer($stream, 'worker')->setBatching(10)->setDepth(1)->setExpiresMs(2_000);
        $run = $iterator->handle(static function (NatsMessage $message, JetStreamContext $js) use ($handled): void {
            $handled[] = $message->payload;
            $js->ack($message)->await();
        });

        try {
            $this->waitFor(static fn(): bool => $handled->count() === 3, 'm-1 to m-3');
            $this->waitFor(static fn(): bool => ($admin->getConsumer($stream, 'worker')->await()->raw['num_ack_pending'] ?? null) === 0, 'the acks');

            $admin->resetConsumer($stream, 'worker', 2)->await();
            $this->waitFor(static fn(): bool => $handled->count() === 5, 'the replay of m-2 and m-3');
            $admin->publish($this->subject($stream), 'm-4')->await();
            $this->waitFor(static fn(): bool => $handled->count() === 6, 'm-4');
        } finally {
            $iterator->stop();
            $run->await(new TimeoutCancellation(5));
        }

        self::assertSame(['m-1', 'm-2', 'm-3', 'm-2', 'm-3', 'm-4'], $handled->getArrayCopy());
    }

    /**
     * A durable push subscription keeps delivering after another client resets its consumer to stream sequence 2: the
     * handler gets m-2 and m-3 again, then m-4, and the error listener reports no gap.
     */
    public function testADurablePushConsumeKeepsDeliveringAfterAnotherClientResetsItsConsumer(): void
    {
        $admin = $this->clientWithResetApi()->jetStream();
        $stream = $this->stream($admin, 3);

        /** @var \ArrayObject<int, string> $errors */
        $errors = new \ArrayObject();
        $worker = $this->client(errors: $errors);
        $js = $worker->jetStream();
        /** @var \ArrayObject<int, string> $handled */
        $handled = new \ArrayObject();
        $js->subscribePushConsumer(
            $stream,
            'pusher',
            static function (NatsMessage $message) use ($handled, $js): void {
                $handled[] = $message->payload;
                $js->ack($message)->await();
            },
            null,
            null,
            ['ack_policy' => 'explicit', 'idle_heartbeat' => 200_000_000],
        )->await();

        $this->pump($worker, static fn(): bool => $handled->count() === 3);
        $this->waitFor(static fn(): bool => ($admin->getConsumer($stream, 'pusher')->await()->raw['num_ack_pending'] ?? null) === 0, 'the acks');
        $admin->resetConsumer($stream, 'pusher', 2)->await();
        $this->pump($worker, static fn(): bool => $handled->count() === 5);
        $admin->publish($this->subject($stream), 'm-4')->await();
        $this->pump($worker, static fn(): bool => $handled->count() === 6);
        // A few heartbeats after the last delivery: they report what the client got.
        $this->pump($worker, static fn(): bool => false, 0.6);

        self::assertSame(['m-1', 'm-2', 'm-3', 'm-2', 'm-3', 'm-4'], $handled->getArrayCopy());
        self::assertSame([], array_values(array_filter($errors->getArrayCopy(), static fn(string $e): bool => str_contains($e, 'up to sequence'))), 'no gap is reported');
    }

    /**
     * An ordered consumer whose consumer another client resets backward (to stream sequence 2) recreates itself and
     * delivers m-1 to m-5 once each, in order; reset forward past the stream's tail while idle (to 100), it recreates
     * itself at its next heartbeat and delivers m-6 and m-7 when they are published: neither a duplicate nor a gap.
     */
    public function testAnOrderedConsumerRecreatesItselfAfterAnotherClientResetsItsConsumer(): void
    {
        $admin = $this->clientWithResetApi()->jetStream();
        $stream = $this->stream($admin, 3);

        $worker = $this->client();
        /** @var \ArrayObject<int, string> $handled */
        $handled = new \ArrayObject();
        $sid = $worker->jetStream()->subscribeOrderedConsumer(
            $stream,
            static function (NatsMessage $message) use ($handled): void {
                $handled[] = $message->payload;
            },
            idleHeartbeatNs: 200_000_000,
        )->await();

        try {
            $this->pump($worker, static fn(): bool => $handled->count() === 3);
            $admin->resetConsumer($stream, $this->onlyConsumer($admin, $stream), 2)->await();
            $admin->publish($this->subject($stream), 'm-4')->await();
            $admin->publish($this->subject($stream), 'm-5')->await();
            $this->pump($worker, static fn(): bool => $handled->count() >= 5);
            $this->pump($worker, static fn(): bool => false, 0.5);
            self::assertSame(['m-1', 'm-2', 'm-3', 'm-4', 'm-5'], $handled->getArrayCopy(), 'a reset backward: no duplicate');

            $admin->resetConsumer($stream, $this->onlyConsumer($admin, $stream), 100)->await();
            // Two heartbeat intervals: the heartbeat reporting the consumer sequence back at 0 recreates it.
            $this->pump($worker, static fn(): bool => false, 0.6);
            $admin->publish($this->subject($stream), 'm-6')->await();
            $admin->publish($this->subject($stream), 'm-7')->await();
            $this->pump($worker, static fn(): bool => $handled->count() >= 7);
            $this->pump($worker, static fn(): bool => false, 0.5);
            self::assertSame(['m-1', 'm-2', 'm-3', 'm-4', 'm-5', 'm-6', 'm-7'], $handled->getArrayCopy(), 'a reset forward past the tail: no gap');
        } finally {
            $worker->jetStream()->stopOrderedConsumer($sid)->await();
        }
    }

    /**
     * A connected client; the first one made is the one tearDown() deletes the streams with.
     *
     * @param \ArrayObject<int, string>|null $errors Collects what the error listener gets.
     */
    private function client(int $requestTimeoutMs = 5_000, ?\ArrayObject $errors = null): NatsClient
    {
        $listener = null;
        if ($errors !== null) {
            $listener = static function (\Throwable $e) use ($errors): void {
                $errors[] = $e->getMessage();
            };
        }

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()], requestTimeoutMs: $requestTimeoutMs, errorListener: $listener));
        $client->connect()->await();
        $this->clients[] = $client;

        return $client;
    }

    /** A connected client, the test skipped unless the server offers the reset API (2.14+, API level 4). */
    private function clientWithResetApi(int $requestTimeoutMs = 5_000): NatsClient
    {
        $this->requireIntegrationEnabled();

        if ($this->clients === []) {
            $this->client();
        }

        $apiLevel = $this->clients[0]->jetStream()->accountInfo()->await()->apiLevel;
        if ($apiLevel === null || $apiLevel < self::RESET_API_LEVEL) {
            self::markTestSkipped(sprintf(
                'The consumer reset API needs NATS server 2.14+ (API level %d); the server reports %s.',
                self::RESET_API_LEVEL,
                $apiLevel === null ? 'no API level' : 'API level ' . $apiLevel,
            ));
        }

        return $requestTimeoutMs === 5_000 ? $this->clients[0] : $this->client($requestTimeoutMs);
    }

    /** A new stream holding m-1 to m-$count on its one subject; tearDown() deletes it. */
    private function stream(JetStreamContext $js, int $count): string
    {
        $stream = 'ITRESET' . strtoupper(bin2hex(random_bytes(3)));
        $js->createStream($stream, [$this->subject($stream)])->await();
        $this->streams[] = $stream;
        for ($i = 1; $i <= $count; $i++) {
            $js->publish($this->subject($stream), 'm-' . $i)->await();
        }

        return $stream;
    }

    private function subject(string $stream): string
    {
        return 'it.' . strtolower($stream) . '.events';
    }

    /** The name of the one consumer on $stream (an ordered consumer's, named by the client). */
    private function onlyConsumer(JetStreamContext $js, string $stream): string
    {
        $names = $js->consumerNames($stream)->await();
        self::assertCount(1, $names);

        return $names[0];
    }

    /** Reads $client's socket until $done holds, failing after five seconds; or for $seconds, when given. */
    private function pump(NatsClient $client, \Closure $done, ?float $seconds = null): void
    {
        $deadline = $this->monotonic() + ($seconds ?? 5.0);
        while (!$done()) {
            if ($this->monotonic() > $deadline) {
                if ($seconds === null) {
                    self::fail('Waited 5 s for the deliveries');
                }

                return;
            }

            try {
                $client->processIncoming(new TimeoutCancellation(0.05))->await();
            } catch (CancelledException) {
            }
        }
    }

    /** Waits until $condition holds, failing after five seconds (monotonic) with what it waited for. */
    private function waitFor(\Closure $condition, string $what): void
    {
        $deadline = $this->monotonic() + 5.0;
        while (!$condition()) {
            if ($this->monotonic() > $deadline) {
                self::fail('Waited 5 s for ' . $what);
            }

            delay(0.01);
        }
    }
}
