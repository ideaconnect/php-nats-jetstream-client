<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Integration;

use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\Configuration\ConsumerConfiguration;
use IDCT\NATS\JetStream\Configuration\StreamConfiguration;
use IDCT\NATS\JetStream\Configuration\StreamSource;
use IDCT\NATS\JetStream\Enum\AckPolicy;
use IDCT\NATS\JetStream\Enum\RetentionPolicy;
use IDCT\NATS\JetStream\JetStreamContext;
use PHPUnit\Framework\TestCase;

use function Amp\delay;

/**
 * Durable stream sourcing of ADR-60 (#280) against a live server: a stream that sources a WorkQueue stream through
 * a durable `flow_control` consumer of your own, set up with the typed builders. nats-server 2.14 added it; the
 * default fixture (`nats`) runs 2.12, so there only the refusals run and the sourcing test is skipped. The second
 * `composer test:e2e` integration stage runs against the `nats-214` fixture (`NATS_URL=nats://127.0.0.1:14232
 * RUN_INTEGRATION=1 composer test:integration:2.14`), which runs the sourcing test and leaves the pre-2.14 test out by
 * its name (`ServerBefore214`; it skips itself from API level 4 on). Which tests run is decided by the API level
 * `$JS.API.INFO` reports (ADR-44), not by the INFO version.
 */
final class DurableSourcingIntegrationTest extends TestCase
{
    use IntegrationTestBootstrap;

    /** The JetStream API level of nats-server 2.14, the first with durable stream sourcing. */
    private const DURABLE_SOURCING_API_LEVEL = 4;

    /** The JetStream API level of nats-server 2.12.0 (2.12.15 reports 3). */
    private const SERVER_212_API_LEVEL = 2;

    private ?NatsClient $client = null;

    /** @var list<string> Deleted in reverse order: a sourcing stream before its origin. */
    private array $streams = [];

    protected function tearDown(): void
    {
        if ($this->client === null) {
            return;
        }

        foreach (array_reverse($this->streams) as $stream) {
            try {
                $this->client->jetStream()->deleteStream($stream)->await();
            } catch (\Throwable) {
            }
        }

        try {
            $this->client->disconnect()->await();
        } catch (\Throwable) {
        }
    }

    /**
     * A 2.14+ server sources a WorkQueue stream through the named consumer: the three messages reach the sourcing
     * stream and leave the work queue, and the stored source carries the consumer.
     */
    public function testAWorkQueueIsSourcedThroughANamedFlowControlConsumer(): void
    {
        $js = $this->jetStream();
        $apiLevel = $js->accountInfo()->await()->apiLevel;
        if ($apiLevel === null || $apiLevel < self::DURABLE_SOURCING_API_LEVEL) {
            self::markTestSkipped(sprintf(
                'Durable stream sourcing needs NATS server 2.14+ (API level %d); the server reports %s.',
                self::DURABLE_SOURCING_API_LEVEL,
                self::describe($apiLevel),
            ));
        }

        [$queue, $aggregate, $subject, $deliverSubject] = $this->names();
        $js->addStream(StreamConfiguration::create($queue)->subjects($subject)->retention(RetentionPolicy::WorkQueue))->await();
        $this->streams[] = $queue;

        $consumer = $js->addConsumer(
            $queue,
            ConsumerConfiguration::create()->durable('SRC')->deliverSubject($deliverSubject)->ackPolicy(AckPolicy::FlowControl),
        )->await();
        self::assertSame('flow_control', $consumer->raw['config']['ack_policy'] ?? null);

        $js->createStream($aggregate, [], [
            'sources' => [StreamSource::source($queue)->consumer('SRC', $deliverSubject)->toArray()],
        ])->await();
        $this->streams[] = $aggregate;

        for ($i = 1; $i <= 3; $i++) {
            $js->publish($subject, 'm-' . $i)->await();
        }

        // The copies arrive at once; on an idle stream the work queue sees the acknowledgement with the 1 s heartbeat.
        $this->waitFor(fn(): bool => $this->messages($js, $aggregate) === 3, 'three messages in ' . $aggregate);
        $this->waitFor(fn(): bool => $this->messages($js, $queue) === 0, 'an empty work queue ' . $queue);

        $sources = $js->getStream($aggregate)->await()->raw['config']['sources'] ?? null;
        self::assertIsArray($sources);
        self::assertSame(['name' => 'SRC', 'deliver_subject' => $deliverSubject], $sources[0]['consumer'] ?? null);
    }

    /**
     * A server from 2.12 up to 2.14 (API level 2 or 3) knows neither half: it refuses the `flow_control` ack policy
     * and a source that names a consumer, each with 400 and err_code 10025 (invalid JSON). A 2.10 or 2.11 server
     * drops the source's `consumer` instead of refusing it, so the test is skipped there.
     */
    public function testAServerBefore214RefusesTheFlowControlConsumerAndANamedConsumerSource(): void
    {
        $js = $this->jetStream();
        $apiLevel = $js->accountInfo()->await()->apiLevel;
        if ($apiLevel === null || $apiLevel < self::SERVER_212_API_LEVEL || $apiLevel >= self::DURABLE_SOURCING_API_LEVEL) {
            self::markTestSkipped(sprintf(
                'Only a server from 2.12 up to 2.14 (API level %d to %d) refuses both; the server reports %s.',
                self::SERVER_212_API_LEVEL,
                self::DURABLE_SOURCING_API_LEVEL - 1,
                self::describe($apiLevel),
            ));
        }

        // The version only names the server in a failure message; the API level above picks the test.
        $version = $this->serverVersion();
        [$queue, $aggregate, $subject, $deliverSubject] = $this->names();
        $js->addStream(StreamConfiguration::create($queue)->subjects($subject)->retention(RetentionPolicy::WorkQueue))->await();
        $this->streams[] = $queue;

        try {
            $js->addConsumer(
                $queue,
                ConsumerConfiguration::create()->durable('SRC')->deliverSubject($deliverSubject)->ackPolicy(AckPolicy::FlowControl),
            )->await();
            self::fail('expected the flow_control consumer to be refused on ' . (string) $version);
        } catch (JetStreamException $e) {
            self::assertSame(400, $e->getCode());
            self::assertSame(10025, $e->getErrCode());
        }

        try {
            $js->createStream($aggregate, [], [
                'sources' => [StreamSource::source($queue)->consumer('SRC', $deliverSubject)->toArray()],
            ])->await();
            $this->streams[] = $aggregate;
            self::fail('expected the source naming a consumer to be refused on ' . (string) $version);
        } catch (JetStreamException $e) {
            self::assertSame(400, $e->getCode());
            self::assertSame(10025, $e->getErrCode());
            self::assertStringContainsString('unknown field "consumer"', $e->getMessage());
        }
    }

    private function jetStream(): JetStreamContext
    {
        $this->requireIntegrationEnabled();

        $this->client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()], requestTimeoutMs: 5_000));
        $this->client->connect()->await();

        return $this->client->jetStream();
    }

    private function serverVersion(): ?string
    {
        return $this->client?->serverInfo()?->version;
    }

    private static function describe(?int $apiLevel): string
    {
        return $apiLevel === null ? 'no API level' : 'API level ' . $apiLevel;
    }

    /**
     * Fresh names: the work queue, the sourcing stream, the work queue's subject and a deliver subject outside both
     * streams' subjects (the server refuses one inside the origin's as a cycle).
     *
     * @return array{string, string, string, string}
     */
    private function names(): array
    {
        $suffix = strtoupper(bin2hex(random_bytes(3)));

        return [
            'ITDSRCWQ' . $suffix,
            'ITDSRCAGG' . $suffix,
            'it.dsrc.' . strtolower($suffix) . '.jobs',
            'deliver.dsrc.' . strtolower($suffix),
        ];
    }

    private function messages(JetStreamContext $js, string $stream): ?int
    {
        $messages = $js->getStream($stream)->await()->raw['state']['messages'] ?? null;

        return is_int($messages) ? $messages : null;
    }

    /** Waits until $condition holds, failing after five seconds (monotonic) with what it waited for. */
    private function waitFor(\Closure $condition, string $what): void
    {
        $deadline = $this->monotonic() + 5.0;
        while (!$condition()) {
            if ($this->monotonic() > $deadline) {
                self::fail('Waited 5 s for ' . $what);
            }

            delay(0.05);
        }
    }
}
