<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Services\BasicJsonSchemaValidator;
use IDCT\NATS\Services\ServiceEndpoint;
use IDCT\NATS\Services\ServiceEndpointHandlerInterface;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\ReconnectingTransport;
use IDCT\NATS\Transport\TransportInterface;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\delay;

final class ServiceTestObjectHandler implements ServiceEndpointHandlerInterface
{
    public function handle(NatsMessage $message): string
    {
        return 'obj:' . $message->payload;
    }
}

final class ServiceTestClassHandler implements ServiceEndpointHandlerInterface
{
    public function handle(NatsMessage $message): string
    {
        return 'class:' . $message->payload;
    }
}

final class ServiceTestInvalidClassHandler {}

final class ServiceTestCtorArgHandler implements ServiceEndpointHandlerInterface
{
    public function __construct(private readonly string $required) {}

    public function handle(NatsMessage $message): string
    {
        return $this->required . ':' . $message->payload;
    }
}

final class ServiceTest extends TestCase
{
    /** @var list<NatsClient> Clients that tearDown() closes, so that a failed test leaves no serving loop behind. */
    private array $opened = [];

    protected function tearDown(): void
    {
        foreach ($this->opened as $client) {
            try {
                $client->disconnect()->await(new TimeoutCancellation(1));
            } catch (\Throwable) {
                // Already closed.
            }
        }

        $this->opened = [];
    }

    /** @return list<string> */
    private function infoAndPong(): array
    {
        return [
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ];
    }

    /**
     * Verifies the done handler fires once when the service stops, and re-arms on restart (#57).
     */
    public function testDoneHandlerFiresOnceOnStop(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $doneCount = 0;
        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload)
            ->onDone(static function () use (&$doneCount): void {
                $doneCount++;
            });

        $service->start()->await();
        $service->stop()->await();
        $service->stop()->await(); // second stop must not re-fire

        self::assertSame(1, $doneCount);

        // Restart re-arms the handler.
        $service->start()->await();
        $service->stop()->await();
        self::assertSame(2, $doneCount);
    }

    /**
     * Verifies a per-endpoint stats supplier merges custom data into STATS (#50).
     */
    public function testEndpointStatsHandlerMergesCustomData(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $service = $client->service('metrics', '1.0.0')->addEndpoint(
            'work',
            'svc.work',
            static fn(NatsMessage $message): string => $message->payload,
            'q',
            null,
            [],
            static fn(\IDCT\NATS\Services\ServiceEndpoint $e): array => ['queue_depth' => 7, 'name' => $e->name],
        );

        $endpoint = $service->statsSnapshot()['endpoints'][0] ?? [];
        self::assertSame(['queue_depth' => 7, 'name' => 'work'], $endpoint['data'] ?? null);
    }

    /**
     * Verifies a grouped endpoint forwards metadata and the stats supplier (#40).
     */
    public function testGroupedEndpointForwardsMetadataAndStatsHandler(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $service = $client->service('grp', '1.0.0');
        $service->addGroup('v1')->addEndpoint(
            'work',
            'work',
            static fn(NatsMessage $message): string => $message->payload,
            'q',
            null,
            ['team' => 'core'],
            static fn(\IDCT\NATS\Services\ServiceEndpoint $e): array => ['ok' => true],
        );

        $endpoint = $service->statsSnapshot()['endpoints'][0] ?? [];
        self::assertSame('v1.work', $endpoint['subject'] ?? null);
        self::assertSame(['ok' => true], $endpoint['data'] ?? null);
    }

    /**
     * Verifies drain() unsubscribes endpoints and flushes, leaving the service stoppable/restartable (#51).
     */
    public function testDrainUnsubscribesAndFlushes(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "PONG\r\n", // answers the drain flush
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload);
        $service->start()->await();

        $service->drain()->await();

        $writes = implode('', $transport->writes);
        // Endpoints/discovery were unsubscribed and a PING was sent to flush the UNSUBs.
        self::assertStringContainsString('UNSUB ', $writes);
        self::assertStringContainsString("PING\r\n", $writes);
    }

    /**
     * Verifies service start registers discovery and endpoint subscriptions.
     */
    public function testStartRegistersSubscriptions(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')->addEndpoint(
            'echo',
            'svc.echo',
            static fn(NatsMessage $message): string => $message->payload,
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
    public function testInfoIncludesEndpointMetadata(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG \$SRV.INFO.echo 5 _INBOX.info 0\r\n\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0', 'Echo service')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload, metadata: ['team' => 'core']);
        $service->start()->await();

        $client->processIncoming()->await();

        // The INFO response advertises the per-endpoint metadata (NATS micro spec).
        self::assertStringContainsString('"metadata":{"team":"core"}', implode('', $transport->writes));
    }

    /**
     * ADR-32 requires endpoint names to satisfy the same token rules as service names; nats.go
     * micro rejects invalid names at registration, and an accepted invalid name was advertised
     * verbatim to conformant tooling via $SRV.INFO/$SRV.STATS.
     */
    public function testAddEndpointRejectsNonTokenName(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('ADR-32');
        $client->service('echo', '1.0.0')
            ->addEndpoint('my endpoint!', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);
    }

    /**
     * Empty `metadata` maps must serialize as JSON objects (`{}`) in every $SRV discovery response,
     * never as arrays (`[]`): ADR-32 types metadata as map[string]string, and Go-based consumers
     * (nats micro ls, nats.go micro) hard-fail unmarshalling `[]` into a map — rejecting the whole
     * discovery response, so a metadata-less service (the default) was invisible to standard
     * tooling. Covers the service-level metadata (PING/STATS) and the per-endpoint metadata (INFO).
     */
    public function testDiscoveryEncodesEmptyMetadataAsObject(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // sid 2 = $SRV.PING (endpoint SUB is sid 1), sid 5 = $SRV.INFO, sid 8 = $SRV.STATS.
            "MSG \$SRV.PING 2 _INBOX.ping 0\r\n\r\n",
            "MSG \$SRV.INFO 5 _INBOX.info 0\r\n\r\n",
            "MSG \$SRV.STATS 8 _INBOX.stats 0\r\n\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // No service metadata, no endpoint metadata: the empty-map default is exactly the broken case.
        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload);
        $service->start()->await();

        $client->processIncoming()->await();
        $client->processIncoming()->await();
        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringNotContainsString('"metadata":[]', $writes, 'ADR-32 metadata is a map; [] breaks Go unmarshalling');
        // Service-level metadata in the PING reply and per-endpoint metadata in the INFO reply.
        self::assertStringContainsString('"type":"io.nats.micro.v1.ping_response"', $writes);
        self::assertStringContainsString('"type":"io.nats.micro.v1.info_response"', $writes);
        self::assertStringContainsString('"type":"io.nats.micro.v1.stats_response"', $writes);
        self::assertGreaterThanOrEqual(
            3,
            substr_count($writes, '"metadata":{}'),
            'PING (service), INFO (service + endpoint), and STATS replies must all carry object-typed empty metadata',
        );
    }

    public function testInfoIncludesEndpointSchema(): void
    {
        // #101: ADR-32 stabilizes only PING/INFO/STATS, so a spec-conformant consumer reads schema from
        // the standard $SRV.INFO response. A declared endpoint schema must appear in the info_response.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG \$SRV.INFO.echo 5 _INBOX.info 0\r\n\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $schema = ['type' => 'object', 'properties' => ['msg' => ['type' => 'string']]];
        $service = $client->service('echo', '1.0.0', 'Echo service')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload, schema: $schema);
        $service->start()->await();

        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('io.nats.micro.v1.info_response', $writes);
        self::assertStringContainsString('"schema":{"type":"object"', $writes);
    }

    public function testInfoOmitsSchemaWhenEndpointHasNone(): void
    {
        // An endpoint without a declared schema must not emit a schema key in the INFO response.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG \$SRV.INFO.echo 5 _INBOX.info 0\r\n\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0', 'Echo service')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload);
        $service->start()->await();

        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('io.nats.micro.v1.info_response', $writes);
        self::assertStringNotContainsString('"schema"', $writes);
    }

    public function testDiscoveryReplies(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG \$SRV.PING 1 _INBOX.ping 0\r\n\r\n",
            "MSG \$SRV.INFO.echo 5 _INBOX.info 0\r\n\r\n",
            "MSG \$SRV.STATS.echo 8 _INBOX.stats 0\r\n\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0', 'Echo service')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload);
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.echo 13 _INBOX.req 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): array => ['echo' => $message->payload]);
        $service->start()->await();

        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('PUB _INBOX.req', $writes);
        self::assertStringContainsString('{"echo":"hello"}', $writes);
    }

    public function testEndpointResponseEncodeFailureDoesNotTearDownDispatch(): void
    {
        // #97: a handler returning a value json_encode cannot encode (binary / non-UTF-8) must not let the
        // JsonException escape the shared dispatch loop and abort delivery for every subscription. The bad
        // request gets a controlled 500; a subsequent request to another endpoint is still served.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // Endpoint 'bad' (sid 13) returns an un-encodable array; endpoint 'good' (sid 14) returns 'ok'.
            "MSG svc.bad 13 _INBOX.bad 5\r\nhello\r\n",
            "MSG svc.good 14 _INBOX.good 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('svc', '1.0.0')
            ->addEndpoint('bad', 'svc.bad', static fn(NatsMessage $message): array => ['data' => "\xff\xfe\xfa"])
            ->addEndpoint('good', 'svc.good', static fn(NatsMessage $message): string => 'ok');
        $service->start()->await();

        // Neither delivery may throw out of the dispatch loop.
        $client->processIncoming()->await();
        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        // The bad request received a controlled service error (500) rather than crashing the loop.
        self::assertStringContainsString('_INBOX.bad', $writes);
        self::assertStringContainsString('Nats-Service-Error', $writes);
        // The good endpoint was still served after the bad one's encode failure (dispatch survived).
        self::assertStringContainsString('PUB _INBOX.good', $writes);
        self::assertStringContainsString("ok\r\n", $writes);
    }

    /**
     * Verifies stop unsubscribes all registered service subscriptions.
     */
    public function testStopUnsubscribesAll(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload);
        $service->start()->await();
        $service->stop()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString("UNSUB 1\r\n", $writes);
        self::assertStringContainsString("UNSUB 13\r\n", $writes);
    }

    /**
     * Verifies grouped hierarchy API prefixes endpoint subjects correctly.
     */
    public function testGroupedEndpointHierarchy(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.v1.echo 13 _INBOX.req 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0');
        $service->addGroup('svc')->addGroup('v1')->addEndpoint(
            'echo-v1',
            'echo',
            static fn(NatsMessage $message): string => 'v1:' . $message->payload,
        );

        $service->start()->await();
        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('SUB svc.v1.echo q 13' . "\r\n", $writes);
        self::assertStringContainsString('PUB _INBOX.req', $writes);
        self::assertStringContainsString('v1:hello', $writes);

        $stats = $service->statsSnapshot();
        self::assertSame('svc.v1.echo', $stats['endpoints'][0]['subject'] ?? null);
    }

    /**
     * Verifies grouping trims empty dot segments when prefixes/subjects are blank.
     */
    public function testGroupJoinHandlesEmptySegments(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0');
        $service->addGroup('')->addEndpoint('root', 'echo', static fn(NatsMessage $message): string => $message->payload);
        $service->addGroup('svc')->addEndpoint('svc-root', '', static fn(NatsMessage $message): string => $message->payload);

        $subjects = array_map(
            static fn(array $endpoint): string => (string) ($endpoint['subject'] ?? ''),
            $service->statsSnapshot()['endpoints'] ?? [],
        );

        self::assertContains('echo', $subjects);
        self::assertContains('svc', $subjects);
    }

    /**
     * Verifies SCHEMA discovery subscriptions and responses include endpoint schemas.
     */
    public function testSchemaDiscoveryResponse(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG \$SRV.SCHEMA.echo 11 _INBOX.schema 0\r\n\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $schema = ['type' => 'object', 'properties' => ['msg' => ['type' => 'string']]];

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload, schema: $schema);
        $service->start()->await();

        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('SUB $SRV.SCHEMA 10' . "\r\n", $writes);
        self::assertStringContainsString('PUB _INBOX.schema', $writes);
        self::assertStringContainsString('io.nats.micro.v1.schema_response', $writes);
        self::assertStringContainsString('"schema":', $writes);
    }

    /**
     * Verifies stats include parity-oriented endpoint metrics and stable started timestamp.
     */
    public function testStatsIncludeDetailedMetrics(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.echo 13 _INBOX.req1 5\r\nhello\r\n",
            "MSG svc.echo 13 _INBOX.req2 4\r\nboom\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static function (NatsMessage $message): string {
                if ($message->payload === 'boom') {
                    throw new \RuntimeException('handler failed');
                }

                return $message->payload;
            });
        $service->start()->await();

        $client->processIncoming()->await();
        $client->processIncoming()->await();

        $stats = $service->statsSnapshot();
        $endpoint = $stats['endpoints'][0] ?? [];

        self::assertSame(2, $endpoint['num_requests'] ?? null);
        self::assertSame(1, $endpoint['num_errors'] ?? null);
        self::assertSame('handler failed', $endpoint['last_error'] ?? null);
        self::assertGreaterThanOrEqual(0, $endpoint['processing_time'] ?? -1);
        self::assertGreaterThanOrEqual(0, $endpoint['average_processing_time'] ?? -1);

        $statsAgain = $service->statsSnapshot();
        self::assertSame($stats['started'] ?? null, $statsAgain['started'] ?? null);
    }

    /**
     * Verifies the stats 'started' timestamp is UTC regardless of the process default timezone, so
     * its hardcoded Z suffix is truthful (ADR-32, #132).
     */
    public function testStartedTimestampIsUtcRegardlessOfDefaultTimezone(): void
    {
        $previousTimezone = date_default_timezone_get();
        // A zone with a large offset: a local-time timestamp stamped with Z would be hours off UTC.
        date_default_timezone_set('Pacific/Kiritimati');

        try {
            $client = new NatsClient(new NatsOptions(), new FakeTransport());
            $service = $client->service('echo', '1.0.0');

            $started = $service->statsSnapshot()['started'] ?? null;
            self::assertIsString($started);
            self::assertMatchesRegularExpression('/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/', $started);

            $parsed = \DateTimeImmutable::createFromFormat(
                'Y-m-d\TH:i:s.u\Z',
                $started,
                new \DateTimeZone('UTC'),
            );
            self::assertInstanceOf(\DateTimeImmutable::class, $parsed);
            // Parsed as UTC, the value must be "now"; a local Kiritimati (+14) time would be ~14h off.
            self::assertLessThan(60, abs($parsed->getTimestamp() - time()));
        } finally {
            date_default_timezone_set($previousTimezone);
        }
    }

    public function testHandlerCanRespondWithCustomServiceError(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.echo 13 _INBOX.req 4\r\nboom\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static function (NatsMessage $message): string {
                throw new \IDCT\NATS\Services\ServiceError(429, 'Rate limited', '{"retry_after":5}');
            });
        $service->start()->await();

        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        // The handler's chosen code/description appear in the micro-spec error headers (not 500/generic).
        self::assertStringContainsString('Nats-Service-Error:Rate limited', $writes);
        self::assertStringContainsString('Nats-Service-Error-Code:429', $writes);
        // The custom body is delivered verbatim.
        self::assertStringContainsString('{"retry_after":5}', $writes);
        self::assertStringNotContainsString('Internal server error', $writes);

        // The error is counted and recorded with the handler's description.
        $endpoint = $service->statsSnapshot()['endpoints'][0] ?? [];
        self::assertSame(1, $endpoint['num_errors'] ?? null);
        self::assertSame('Rate limited', $endpoint['last_error'] ?? null);
    }

    public function testHandlerErrorResponseDoesNotLeakExceptionMessage(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.echo 13 _INBOX.req 4\r\nboom\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static function (NatsMessage $message): string {
                throw new \RuntimeException('secret-dsn user:pass@db-host');
            });
        $service->start()->await();

        $client->processIncoming()->await();

        // The error reply sent to the (untrusted) requester must NOT contain the raw exception text.
        $writes = implode('', $transport->writes);
        self::assertStringContainsString('Internal server error', $writes);
        self::assertStringNotContainsString('secret-dsn', $writes);

        // Micro-spec error headers are present so a generic client can detect the failure.
        self::assertStringContainsString('HPUB _INBOX.req ', $writes);
        self::assertStringContainsString('Nats-Service-Error:Internal server error', $writes);
        self::assertStringContainsString('Nats-Service-Error-Code:500', $writes);

        // The real detail is still available to the operator server-side (lastError / STATS).
        $endpoint = $service->statsSnapshot()['endpoints'][0] ?? [];
        self::assertSame('secret-dsn user:pass@db-host', $endpoint['last_error'] ?? null);
    }

    public function testStatsOmitsNonSpecAliasKeys(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => 'ok');

        $endpoint = $service->statsSnapshot()['endpoints'][0] ?? [];

        // Spec field names are present; the non-spec aliases are gone.
        self::assertArrayHasKey('num_requests', $endpoint);
        self::assertArrayHasKey('num_errors', $endpoint);
        self::assertArrayNotHasKey('requests', $endpoint);
        self::assertArrayNotHasKey('errors', $endpoint);
    }

    public function testServiceRejectsInvalidName(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Service name');
        // A dot would break $SRV.PING.<name> subject construction / over-subscribe.
        $client->service('bad.name', '1.0.0');
    }

    public function testServiceRejectsNonSemverVersion(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('semantic version');
        $client->service('calc', 'v1');
    }

    public function testValidationRejectionEmitsRequestEnd(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.v 13 _INBOX.r 2\r\nhi\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $events = [];
        $service = $client->service('val', '1.0.0')
            ->addObserver(static function (string $event, ServiceEndpoint $endpoint, NatsMessage $message, array $context) use (&$events): void {
                $events[] = $event;
            })
            ->withRequestValidator(static fn(NatsMessage $message, array $schema): string => 'bad input')
            ->addEndpoint('v', 'svc.v', static fn(NatsMessage $message): string => 'ok', schema: ['type' => 'object']);
        $service->start()->await();

        $client->processIncoming()->await();

        // A schema-rejected request must still emit the terminal request_end (so observer spans/gauges
        // opened on request_start are not leaked).
        self::assertSame(['request_start', 'request_error', 'request_end'], $events);
    }

    public function testStartRollsBackAndClearsStateOnPartialFailure(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // The second endpoint's subject is invalid (whitespace): addEndpoint accepts it, but the
        // subscribe at start() fails after discovery + the first endpoint already subscribed.
        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('good', 'svc.good', static fn(NatsMessage $message): string => 'ok')
            ->addEndpoint('bad', 'bad subject', static fn(NatsMessage $message): string => 'no');

        try {
            $service->start()->await();
            self::fail('Expected start() to throw on the invalid subject');
        } catch (\Throwable) {
            // expected
        }

        // A partial failure must leave no lingering SIDs and must NOT mark the service started, so a
        // retried start() is not silently a no-op.
        self::assertSame([], (new \ReflectionProperty($service, 'subscriptionSids'))->getValue($service));
        self::assertFalse((new \ReflectionProperty($service, 'started'))->getValue($service));
    }

    /**
     * Verifies reset clears accumulated endpoint statistics.
     */
    public function testResetClearsStats(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.echo 13 _INBOX.req1 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload);
        $service->start()->await();

        $client->processIncoming()->await();
        $service->reset();

        $stats = $service->statsSnapshot();
        $endpoint = $stats['endpoints'][0] ?? [];

        self::assertSame(0, $endpoint['num_requests'] ?? null);
        self::assertSame(0, $endpoint['num_errors'] ?? null);
        self::assertNull($endpoint['last_error'] ?? null);
        self::assertSame(0, $endpoint['processing_time'] ?? null);
        self::assertSame(0, $endpoint['average_processing_time'] ?? null);
    }

    /**
     * Verifies endpoint schema validator can reject requests with a structured error response.
     */
    public function testRequestValidatorCanRejectRequests(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.echo 13 _INBOX.req1 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $handled = false;
        $service = $client->service('echo', '1.0.0')
            ->withRequestValidator(static fn(NatsMessage $message, array $schema): ?string => $schema === [] ? null : 'payload does not match schema')
            ->addEndpoint('echo', 'svc.echo', static function (NatsMessage $message) use (&$handled): string {
                $handled = true;

                return $message->payload;
            }, schema: ['type' => 'object']);
        $service->start()->await();

        $client->processIncoming()->await();

        self::assertFalse($handled);

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('"code":"VALIDATION_ERROR"', $writes);
        self::assertStringContainsString('"payload does not match schema"', $writes);
        self::assertStringContainsString('"type":"io.nats.micro.v1.error"', $writes);

        $stats = $service->statsSnapshot();
        $endpoint = $stats['endpoints'][0] ?? [];
        self::assertSame(1, $endpoint['num_requests'] ?? null);
        self::assertSame(1, $endpoint['num_errors'] ?? null);
    }

    /**
     * A request validator that throws is answered like a handler that throws: the endpoint counts the error and
     * keeps the exception's text as its last error, its observers see request_error (HANDLER_ERROR) and
     * request_end, and the requester gets a HANDLER_ERROR 500 reply without that text. The exception used to
     * escape the endpoint into the read that delivered the request: no reply, no error counted, and the read
     * failed with it.
     */
    public function testRequestValidatorThatThrowsIsAnsweredLikeAHandlerThatThrows(): void
    {
        $transport = new FakeTransport([...$this->infoAndPong(), "MSG svc.v 13 _INBOX.req 2\r\n{}\r\n"]);
        $client = new NatsClient(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $client->connect()->await();

        $events = [];
        $handled = false;
        $service = $client->service('val', '1.0.0')
            ->withRequestValidator(static function (): ?string {
                throw new \RuntimeException('validator broke');
            })
            ->addObserver(static function (string $event, ServiceEndpoint $endpoint, NatsMessage $message, array $context) use (&$events): void {
                $events[] = $event === 'request_error' ? $event . ':' . $context['code'] : $event;
            })
            ->addEndpoint('v', 'svc.v', static function () use (&$handled): string {
                $handled = true;

                return 'ok';
            }, schema: ['type' => 'object']);
        $service->start()->await();

        $client->processIncoming()->await();

        self::assertFalse($handled, 'the handler did not run');
        self::assertSame(['request_start', 'request_error:HANDLER_ERROR', 'request_end'], $events);
        $endpoint = $service->statsSnapshot()['endpoints'][0] ?? [];
        self::assertSame(1, $endpoint['num_requests'] ?? null);
        self::assertSame(1, $endpoint['num_errors'] ?? null);
        self::assertSame('validator broke', $endpoint['last_error'] ?? null);
        $writes = implode('', $transport->writes);
        self::assertStringContainsString('HPUB _INBOX.req ', $writes);
        self::assertStringContainsString('Nats-Service-Error-Code:500', $writes);
        self::assertStringContainsString('"code":"HANDLER_ERROR"', $writes);
        self::assertStringNotContainsString('validator broke', $writes, 'the exception text stays server-side');
    }

    /**
     * A request validator that throws a ServiceError gets the reply it chose, as a handler that throws one does.
     */
    public function testRequestValidatorThatThrowsAServiceErrorGetsTheReplyItChose(): void
    {
        $transport = new FakeTransport([...$this->infoAndPong(), "MSG svc.v 13 _INBOX.req 2\r\n{}\r\n"]);
        $client = new NatsClient(new NatsOptions(pingIntervalSeconds: 0), $transport);
        $client->connect()->await();

        $service = $client->service('val', '1.0.0')
            ->withRequestValidator(static function (): ?string {
                throw new \IDCT\NATS\Services\ServiceError(503, 'schema registry unavailable');
            })
            ->addEndpoint('v', 'svc.v', static fn(NatsMessage $message): string => 'ok', schema: ['type' => 'object']);
        $service->start()->await();

        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('Nats-Service-Error-Code:503', $writes);
        self::assertStringContainsString('Nats-Service-Error:schema registry unavailable', $writes);
        $endpoint = $service->statsSnapshot()['endpoints'][0] ?? [];
        self::assertSame(1, $endpoint['num_errors'] ?? null);
        self::assertSame('schema registry unavailable', $endpoint['last_error'] ?? null);
    }

    /**
     * Verifies observers receive request lifecycle events and correlation metadata.
     */
    public function testObserversReceiveLifecycleEvents(): void
    {
        $headerPayload = "NATS/1.0\r\nX-Request-Id:req-42\r\n\r\n";
        $bodyPayload = 'hello';
        $merged = $headerPayload . $bodyPayload;
        $headerBytes = strlen($headerPayload);
        $totalBytes = strlen($merged);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "HMSG svc.echo 13 _INBOX.req {$headerBytes} {$totalBytes}\r\n{$merged}\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $events = [];
        $service = $client->service('echo', '1.0.0')
            ->addObserver(static function (string $event, $endpoint, NatsMessage $message, array $context) use (&$events): void {
                $events[] = [
                    'event' => $event,
                    'correlation_id' => $context['correlation_id'] ?? null,
                    'subject' => $message->subject,
                ];
            })
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload);
        $service->start()->await();

        $client->processIncoming()->await();

        self::assertSame('request_start', $events[0]['event'] ?? null);
        self::assertSame('request_end', $events[1]['event'] ?? null);
        self::assertSame('req-42', $events[0]['correlation_id'] ?? null);
        self::assertSame('svc.echo', $events[0]['subject']);
    }

    /**
     * Verifies a successful request that carries headers on a service with NO observers still
     * replies correctly. Since #140 this path skips building the observer context entirely (no
     * header parse); there is no counting seam to assert the skip directly (NatsHeaders is a
     * static utility and NatsMessage is final), so this pins the observable behavior: headers
     * present, zero observers, correct success reply.
     */
    public function testSuccessfulRequestWithHeadersAndNoObserversRepliesCorrectly(): void
    {
        $headerPayload = "NATS/1.0\r\nX-Request-Id:req-77\r\n\r\n";
        $bodyPayload = 'ping';
        $merged = $headerPayload . $bodyPayload;
        $headerBytes = strlen($headerPayload);
        $totalBytes = strlen($merged);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "HMSG svc.echo 13 _INBOX.req {$headerBytes} {$totalBytes}\r\n{$merged}\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => 'pong:' . $message->payload);
        $service->start()->await();

        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('PUB _INBOX.req', $writes);
        self::assertStringContainsString('pong:ping', $writes);

        $stats = $service->statsSnapshot();
        $endpoint = $stats['endpoints'][0] ?? [];
        self::assertSame(1, $endpoint['num_requests'] ?? null);
        self::assertSame(0, $endpoint['num_errors'] ?? null);
    }

    /**
     * Verifies built-in schema validator adapter is applied through convenience API.
     */
    public function testWithSchemaValidatorUsesAdapter(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.echo 13 _INBOX.req1 16\r\n{\"id\":\"invalid\"}\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->withSchemaValidator(new BasicJsonSchemaValidator())
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload, schema: [
                'type' => 'object',
                'required' => ['id'],
                'properties' => [
                    'id' => ['type' => 'integer'],
                ],
            ]);
        $service->start()->await();

        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('"code":"VALIDATION_ERROR"', $writes);
        self::assertStringContainsString('$.id must be integer, got string', $writes);
        self::assertStringContainsString('"type":"io.nats.micro.v1.error"', $writes);
    }

    /**
     * A requester-controlled non-UTF-8 X-Request-Id plus a schema-failing payload used to make the
     * validation-error reply throw JsonException from INSIDE the (then unguarded) reply publish -
     * escaping the subscription callback into the shared dispatch loop and aborting delivery for
     * every subscription on the connection (#97's exact failure mode, remotely triggerable). The
     * rejection reply must go out (without the poisoned correlation id) and nothing may escape.
     */
    public function testValidationReplyToleratesNonUtf8CorrelationHeader(): void
    {
        $headerPayload = "NATS/1.0\r\nX-Request-Id:\xC3\x28\xFF\r\n\r\n";
        $bodyPayload = '{"id":"invalid"}';
        $merged = $headerPayload . $bodyPayload;
        $headerBytes = strlen($headerPayload);
        $totalBytes = strlen($merged);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "HMSG svc.echo 13 _INBOX.req {$headerBytes} {$totalBytes}\r\n{$merged}\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->withSchemaValidator(new BasicJsonSchemaValidator())
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload, schema: [
                'type' => 'object',
                'properties' => ['id' => ['type' => 'integer']],
            ]);
        $service->start()->await();

        // Pre-fix: this threw JsonException (malformed UTF-8) out of the dispatch loop.
        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('"code":"VALIDATION_ERROR"', $writes, 'the rejection reply must still be published');
        self::assertStringNotContainsString('correlation_id', $writes, 'the poisoned correlation id must be omitted, not encoded');
    }

    /**
     * A handler throwing ServiceError with a CR/LF-crafted CODE used to blow up the header build
     * inside publishResponse (InvalidArgumentException) and escape the dispatch loop - the injection
     * itself was blocked, but by throwing, not sanely. The code is now collapsed to one line like
     * the description, the reply goes out, and nothing escapes.
     */
    public function testServiceErrorCrLfCodeIsSanitizedAndReplyStillSent(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.echo 13 _INBOX.req 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static function (): string {
                throw new \IDCT\NATS\Services\ServiceError("400\r\nX-Injected: 1", 'bad request');
            });
        $service->start()->await();

        // Pre-fix: InvalidArgumentException from the header build escaped the dispatch loop.
        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('Nats-Service-Error-Code:400 X-Injected: 1', $writes, 'the code must be collapsed to one line');
        self::assertStringNotContainsString("400\r\nX-Injected", $writes, 'no header injection');
    }

    /**
     * A connection-level reply-publish failure (the connection closed between request delivery and
     * the reply) used to escape the subscription callback into the shared dispatch loop as a
     * spurious connection error. It must be recorded on the endpoint and swallowed. With a
     * SUCCESSFUL handler the reply write is the sole fault of the request, so it IS the one counted
     * error, with a request_error event carrying the publish failure.
     */
    public function testReplyPublishConnectionFailureIsRecordedNotEscaping(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.echo 13 _INBOX.req 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(reconnectEnabled: false), $transport);
        $client->connect()->await();

        $events = [];
        $service = $client->service('echo', '1.0.0')
            ->addObserver(static function (string $event, ServiceEndpoint $endpoint, NatsMessage $message, array $context) use (&$events): void {
                $events[] = ['event' => $event, 'context' => $context];
            })
            ->addEndpoint('echo', 'svc.echo', static function () use ($client): string {
                // The connection dies while the handler runs; the reply publish then fails at the
                // connection level (not JsonException) - the previously unguarded escape path.
                $client->disconnect()->await();

                return 'reply';
            });
        $service->start()->await();

        // Pre-fix: the ConnectionException from the reply publish escaped the dispatch loop.
        $client->processIncoming()->await();

        $stats = $service->statsSnapshot()['endpoints'][0] ?? [];
        self::assertSame(1, $stats['num_requests'] ?? null);
        self::assertSame(1, $stats['num_errors'] ?? null, 'the reply failure must be recorded on the endpoint');
        self::assertIsString($stats['last_error'] ?? null);
        self::assertStringContainsString('not open', (string) $stats['last_error']);

        // The reply write happens after the terminal request_end (the handler already completed), so
        // this path's single request_error trails it, carrying the publish failure itself.
        self::assertSame(['request_start', 'request_end', 'request_error'], array_column($events, 'event'));
        $errorContext = $events[2]['context'];
        self::assertSame('HANDLER_ERROR', $errorContext['code'] ?? null);
        self::assertIsString($errorContext['error'] ?? null);
        self::assertStringContainsString('not open', (string) $errorContext['error']);
    }

    /**
     * A request whose handler already errored (ServiceError -> errors++, request_error) and whose
     * error-reply publish THEN fails must still count exactly ONE error: the reply-failure catch
     * double-counted it, making $SRV.STATS num_errors exceed num_requests (>100% error rate), and
     * emitted a second request_error - after the terminal request_end, with the hardcoded
     * HANDLER_ERROR code contradicting the handler's own. Only lastError may record the publish
     * failure (issues/2nd-round/service-reply-failure-double-counts-errors.md).
     */
    public function testHandlerErrorWithFailedErrorReplyCountsSingleError(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.echo 13 _INBOX.req 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(reconnectEnabled: false), $transport);
        $client->connect()->await();

        $events = [];
        $service = $client->service('echo', '1.0.0')
            ->addObserver(static function (string $event, ServiceEndpoint $endpoint, NatsMessage $message, array $context) use (&$events): void {
                $events[] = ['event' => $event, 'context' => $context];
            })
            ->addEndpoint('echo', 'svc.echo', static function () use ($client): string {
                // The connection dies while the handler runs AND the handler fails: the error-reply
                // publish (HPUB with the micro error headers) then fails at the connection level.
                $client->disconnect()->await();

                throw new \IDCT\NATS\Services\ServiceError('418', 'teapot');
            });
        $service->start()->await();

        $client->processIncoming()->await();

        $stats = $service->statsSnapshot()['endpoints'][0] ?? [];
        // One request, ONE error: the invariant num_errors <= num_requests must hold.
        self::assertSame(1, $stats['num_requests'] ?? null);
        self::assertSame(1, $stats['num_errors'] ?? null, 'a handler error whose reply write also fails is a single error');
        // lastError still records the (later) publish failure, replacing the handler description.
        self::assertIsString($stats['last_error'] ?? null);
        self::assertStringContainsString('not open', (string) $stats['last_error']);

        // Exactly one request_error - the handler's own, with its chosen code, before the terminal
        // request_end. No late second event with a contradictory HANDLER_ERROR code follows.
        self::assertSame(['request_start', 'request_error', 'request_end'], array_column($events, 'event'));
        $errorContext = $events[1]['context'];
        self::assertSame('418', $errorContext['code'] ?? null);
        self::assertSame('teapot', $errorContext['error'] ?? null);
    }

    /**
     * Verifies error responses carry correlation id when request headers provide one.
     */
    public function testErrorEnvelopeIncludesCorrelationIdFromHeaders(): void
    {
        $headerPayload = "NATS/1.0\r\nX-Request-Id:req-123\r\n\r\n";
        $bodyPayload = 'hello';
        $merged = $headerPayload . $bodyPayload;
        $headerBytes = strlen($headerPayload);
        $totalBytes = strlen($merged);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "HMSG svc.echo 13 _INBOX.req {$headerBytes} {$totalBytes}\r\n{$merged}\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static function (): string {
                throw new \RuntimeException('boom');
            });
        $service->start()->await();

        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('"code":"HANDLER_ERROR"', $writes);
        self::assertStringContainsString('"correlation_id":"req-123"', $writes);
    }

    /**
     * Verifies object handler adapters implementing ServiceEndpointHandlerInterface are supported.
     */
    public function testEndpointAcceptsObjectHandlerAdapter(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.echo 13 _INBOX.req 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', new ServiceTestObjectHandler());
        $service->start()->await();

        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('obj:hello', $writes);
    }

    /**
     * Verifies class-string handler adapters are instantiated and executed.
     */
    public function testEndpointAcceptsClassStringHandlerAdapter(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.echo 13 _INBOX.req 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', ServiceTestClassHandler::class);
        $service->start()->await();

        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('class:hello', $writes);
    }

    /**
     * Verifies invalid class-string handlers are rejected with a clear exception.
     */
    public function testEndpointRejectsInvalidObjectHandlerAdapter(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported service endpoint handler');

        $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', new ServiceTestInvalidClassHandler());
    }

    /**
     * Verifies run helper processes incoming requests and auto-stops on timeout.
     */
    public function testRunProcessesAndStopsOnTimeout(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.echo 13 _INBOX.req 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => 'run:' . $message->payload);

        $service->run(0.03)->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('PUB _INBOX.req', $writes);
        self::assertStringContainsString('run:hello', $writes);
        self::assertStringContainsString("UNSUB 1\r\n", $writes);
    }

    /**
     * Verifies run helper can be cancelled externally and still unsubscribes service SIDs.
     */
    public function testRunSupportsExternalCancellation(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload);

        $cancellation = new DeferredCancellation();
        $runner = async(static function () use ($service, $cancellation): void {
            $service->run(cancellation: $cancellation->getCancellation())->await();
        });

        delay(0.01);
        $cancellation->cancel();
        $runner->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString("UNSUB 1\r\n", $writes);
        self::assertStringContainsString("UNSUB 13\r\n", $writes);
    }

    /**
     * Verifies endpoints default to the NATS micro spec queue group "q" so instances load-balance.
     */
    public function testEndpointDefaultsToSpecQueueGroup(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload);
        $service->start()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('SUB svc.echo q 13' . "\r\n", $writes);
        // Discovery subscriptions must stay non-queued so every instance answers discovery.
        self::assertStringContainsString('SUB $SRV.PING 1' . "\r\n", $writes);
    }

    /**
     * Verifies an empty-string queue group opts out (plain subscription, fan-out to all instances).
     */
    public function testEndpointEmptyStringQueueGroupOptsOut(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload, '');
        $service->start()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('SUB svc.echo 13' . "\r\n", $writes);
        self::assertStringNotContainsString('SUB svc.echo q', $writes);
    }

    /**
     * Verifies a null queue group also opts out of the default queue group.
     */
    public function testEndpointNullQueueGroupOptsOut(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload, null);
        $service->start()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('SUB svc.echo 13' . "\r\n", $writes);
        self::assertStringNotContainsString('SUB svc.echo q', $writes);
    }

    public function testRunPassesCancellationIntoSocketRead(): void
    {
        // blockWhenEmpty models a live idle socket: the run loop's read suspends until cancelled.
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ], blockWhenEmpty: true);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload)
            ->run(0.03)->await();

        // The idle read was given a cancellation and was actually torn down (not orphaned): every
        // started read resolved. On the unfixed code the read got a null cancellation and never
        // resolved, so startedReads > resolvedReads and lastReadHadCancellation would be false.
        self::assertTrue($transport->lastReadHadCancellation);
        self::assertGreaterThan(0, $transport->startedReads);
        self::assertSame($transport->startedReads, $transport->resolvedReads);
    }

    public function testRunLeavesConnectionReusableAfterTimeout(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ], blockWhenEmpty: true);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => $message->payload)
            ->run(0.03)->await();

        // The shared connection must not be left with a read in progress (which would short-circuit
        // every subsequent read). On the unfixed code the orphaned read leaves this true forever.
        $connectionProp = new \ReflectionProperty(NatsClient::class, 'connection');
        $connection = $connectionProp->getValue($client);
        self::assertInstanceOf(NatsConnection::class, $connection);
        $readInProgress = new \ReflectionProperty(NatsConnection::class, 'readInProgress');
        self::assertFalse($readInProgress->getValue($connection));
    }

    public function testAddEndpointRejectsDuplicateSubject(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('a', 'svc.echo', static fn(NatsMessage $m): string => 'a');

        $this->expectException(\InvalidArgumentException::class);
        $service->addEndpoint('b', 'svc.echo', static fn(NatsMessage $m): string => 'b');
    }

    public function testAddEndpointRejectsEmptyName(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0');

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('name must be non-empty and match');
        $service->addEndpoint('   ', 'svc.echo', static fn(NatsMessage $m): string => 'x');
    }

    public function testAddEndpointRejectsEmptySubject(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0');

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('subject must not be empty');
        $service->addEndpoint('echo', '', static fn(NatsMessage $m): string => 'x');
    }

    public function testClassHandlerWithRequiredConstructorArgIsRejected(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0');

        // A class-string handler with a required constructor argument cannot be auto-instantiated;
        // it must fail with a clear framework error, not a raw ArgumentCountError.
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('could not be instantiated');
        $service->addEndpoint('echo', 'svc.echo', ServiceTestCtorArgHandler::class);
    }

    public function testStopToleratesClosedConnection(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);
        $service->start()->await();

        // The connection is gone before stop(): unsubscribe() releases local state without throwing
        // on a closed connection (#116), and stop() clears its own per-SID tracking regardless.
        $client->disconnect()->await();

        // stop() must not abort; it unsubscribes per-SID (best-effort) and clears state.
        $service->stop()->await();

        $sids = new \ReflectionProperty($service, 'subscriptionSids');
        self::assertSame([], $sids->getValue($service));
    }

    public function testRunStopsWhenConnectionIsUnrecoverable(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            FakeTransport::EOF, // peer close -> recover -> reconnect disabled -> Closed
        ]);
        $client = new NatsClient(new NatsOptions(reconnectEnabled: false, pingIntervalSeconds: 0), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);

        // run() with no timeout must still return once the connection is unrecoverable, instead of
        // busy-spinning forever. The outer bound fails the test if it spins.
        $result = \Amp\Future\await([async(static function () use ($service): void {
            $service->run()->await();
        })], new TimeoutCancellation(2.0));

        self::assertSame([null], $result);
    }

    public function testDiscoveryHandlerSwallowsEncodeFailure(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // $SRV.INFO.calc is the 5th discovery subscription (sid 5); request it with a reply inbox.
            "MSG \$SRV.INFO.calc 5 _INBOX.r 0\r\n\r\n",
        ]);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // An invalid-UTF-8 description makes the INFO discovery payload fail to JSON-encode.
        $client->service('calc', '1.0.0', "bad\xB1description")
            ->addEndpoint('add', 'calc.add', static fn(NatsMessage $m): string => 'ok')
            ->start()->await();

        // The encode failure must be swallowed inside the discovery handler, not escape the dispatch
        // loop (which would abort delivery for other subscriptions). processIncoming completes.
        $frames = $client->processIncoming()->await();
        self::assertSame(1, $frames);
    }

    /**
     * Verifies that calling start() on an already-started service is a no-op (the idempotency guard at the top of start()).
     */
    public function testStartIsIdempotentWhenAlreadyStarted(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);

        $service->start()->await();
        $writesAfterFirst = $transport->writes;

        // Second start() must be a no-op: no additional SUB frames.
        $service->start()->await();
        $writesAfterSecond = $transport->writes;

        self::assertSame($writesAfterFirst, $writesAfterSecond);
    }

    /**
     * Verifies that a ServiceError with no body AND a correlation_id header emits the correlation_id
     * in the error payload.
     */
    public function testServiceErrorWithNullBodyIncludesCorrelationIdFromHeader(): void
    {
        $headerPayload = "NATS/1.0\r\nX-Request-Id:corr-99\r\n\r\n";
        $bodyPayload = 'go';
        $merged = $headerPayload . $bodyPayload;
        $headerBytes = strlen($headerPayload);
        $totalBytes = strlen($merged);

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "HMSG svc.echo 13 _INBOX.req {$headerBytes} {$totalBytes}\r\n{$merged}\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // ServiceError with null body: the runtime builds the errorPayload and must include correlation_id.
        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static function (NatsMessage $m): never {
                throw new \IDCT\NATS\Services\ServiceError('422', 'Unprocessable');
            });
        $service->start()->await();

        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('"correlation_id":"corr-99"', $writes);
        self::assertStringContainsString('"code":"422"', $writes);
        self::assertStringContainsString('Nats-Service-Error-Code:422', $writes);
    }

    /**
     * Verifies that a handler returning null causes no reply to be published.
     */
    public function testHandlerReturningNullSendsNoReply(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.fire 13 _INBOX.req 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $handled = false;
        $service = $client->service('fire', '1.0.0')
            ->addEndpoint('fire', 'svc.fire', static function (NatsMessage $m) use (&$handled): null {
                $handled = true;

                return null;
            });
        $service->start()->await();

        $writesBefore = $transport->writes;
        $client->processIncoming()->await();

        $newWrites = array_slice($transport->writes, count($writesBefore));
        // Handler ran, but no PUB should follow (null response).
        self::assertTrue($handled);
        foreach ($newWrites as $w) {
            self::assertStringNotContainsString('PUB _INBOX.req', $w);
        }

        // Request counter incremented even for null-response handlers.
        $stats = $service->statsSnapshot();
        self::assertSame(1, $stats['endpoints'][0]['num_requests'] ?? null);
    }

    /**
     * Verifies that drain() fires the done handler once (via markDone() called from drain()).
     */
    public function testDrainFiresDoneHandler(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "PONG\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $fired = 0;
        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload)
            ->onDone(static function () use (&$fired): void {
                $fired++;
            });

        $service->start()->await();
        $service->drain()->await();

        self::assertSame(1, $fired);
    }

    /**
     * Verifies that the done handler exception is swallowed (handler called inside try/catch).
     */
    public function testDoneHandlerExceptionIsSwallowed(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload)
            ->onDone(static function (): never {
                throw new \RuntimeException('done-handler-boom');
            });

        $service->start()->await();

        // stop() must complete without rethrowing the done-handler exception.
        $service->stop()->await();

        // Reaching here means the exception was swallowed (started state was cleared).
        $started = (new \ReflectionProperty($service, 'started'))->getValue($service);
        self::assertFalse($started);
    }

    /**
     * Verifies a discovery message with no replyTo is silently ignored.
     */
    public function testDiscoveryMessageWithoutReplyToIsIgnored(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // PING discovery message with no reply inbox (0-byte body, no reply subject).
            "MSG \$SRV.PING 1 0\r\n\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);
        $service->start()->await();

        $writesBefore = $transport->writes;
        $frames = $client->processIncoming()->await();

        // Frame was processed (one MSG delivered) but no PUB was emitted.
        self::assertSame(1, $frames);
        $newWrites = array_slice($transport->writes, count($writesBefore));
        foreach ($newWrites as $w) {
            self::assertStringNotContainsString('PUB $SRV', $w);
        }
    }

    /**
     * Verifies endpoint requests with no replyTo are processed but produce no PUB.
     */
    public function testEndpointRequestWithNoReplyToSendsNoResponse(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // No reply subject (fire-and-forget publish to the endpoint).
            "MSG svc.work 13 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $handled = false;
        $service = $client->service('work', '1.0.0')
            ->addEndpoint('work', 'svc.work', static function (NatsMessage $m) use (&$handled): string {
                $handled = true;

                return 'done';
            });
        $service->start()->await();

        $writesBefore = $transport->writes;
        $client->processIncoming()->await();

        self::assertTrue($handled);
        $newWrites = array_slice($transport->writes, count($writesBefore));
        foreach ($newWrites as $w) {
            self::assertStringNotContainsString('PUB', $w);
        }
    }

    /**
     * Verifies observer that throws does not interrupt request handling.
     */
    public function testObserverExceptionIsSwallowed(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            "MSG svc.echo 13 _INBOX.req 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addObserver(static function (string $event): never {
                throw new \RuntimeException('observer-boom');
            })
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => 'ok');
        $service->start()->await();

        // processIncoming must complete even though the observer throws.
        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        // The handler still ran and replied.
        self::assertStringContainsString('PUB _INBOX.req', $writes);
        self::assertStringContainsString('ok', $writes);
    }

    /**
     * Verifies buildRunCancellation throws for a non-positive timeout.
     */
    public function testRunRejectsNonPositiveTimeout(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('timeout must be greater than zero');
        $service->run(0.0)->await();
    }

    /**
     * Verifies run() with only a positive timeout (no external cancellation) uses a TimeoutCancellation.
     */
    public function testRunWithOnlyTimeoutUsesTimeoutCancellation(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
        ], blockWhenEmpty: true);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);

        // run() with only a timeout and no external cancellation must return (TimeoutCancellation path).
        $service->run(0.02)->await();

        // After run() exits the service must be stopped.
        $writes = implode('', $transport->writes);
        self::assertStringContainsString('UNSUB ', $writes);
    }

    /**
     * Verifies stats supplier that throws does not break the stats response (the catch around the stats supplier).
     */
    public function testStatsHandlerExceptionIsSwallowed(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());

        $service = $client->service('svc', '1.0.0')->addEndpoint(
            'work',
            'svc.work',
            static fn(NatsMessage $m): string => 'ok',
            'q',
            null,
            [],
            static fn(\IDCT\NATS\Services\ServiceEndpoint $e): array => throw new \RuntimeException('stats-boom'),
        );

        $snapshot = $service->statsSnapshot();
        $endpoint = $snapshot['endpoints'][0] ?? [];

        // The stats entry must be present but without a 'data' key (supplier threw).
        self::assertArrayHasKey('num_requests', $endpoint);
        self::assertArrayNotHasKey('data', $endpoint);
    }

    /**
     * Verifies the run loop breaks immediately when the cancellation is already requested on entry.
     */
    public function testRunBreaksImmediatelyWhenCancellationAlreadyRequested(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);

        $deferred = new \Amp\DeferredCancellation();
        $deferred->cancel();
        $cancellation = $deferred->getCancellation();

        // The cancellation is already triggered; run() should exit promptly without hanging.
        $service->run(null, $cancellation)->await();

        $writes = implode('', $transport->writes);
        // Service stopped: UNSUB frames must appear.
        self::assertStringContainsString('UNSUB ', $writes);
    }

    /**
     * Verifies start() rollback silently swallows unsubscribe failures when the connection is gone
     * during the catch block (inner catch in the rollback foreach).
     *
     * Scenario: two endpoints are added; the connection is dropped before start(). The first
     * discovery subscribe succeeds (the fake transport is still open) but everything breaks
     * after that... actually we need a subtler approach: connect, immediately close the transport,
     * then call start() so that even the first subscribe fails - but then the rollback tries
     * unsub on the already-queued SIDs and THAT also fails (the rollback foreach's inner catch fires).
     *
     * The simplest reliable trigger: start successfully, close the connection, then force a second
     * start() with a bad endpoint. The partial subscribe at "bad subject" throws, and the rollback
     * over the already-subscribed SIDs (discovery + first endpoint) also throws because the
     * connection is now closed - exercising the rollback's swallow path.
     */
    public function testStartRollbackSwallowsUnsubscribeFailureOnClosedConnection(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        // Add a valid endpoint so discovery subscriptions and one endpoint get registered.
        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('good', 'svc.good', static fn(NatsMessage $m): string => 'ok')
            ->addEndpoint('bad', 'bad subject', static fn(NatsMessage $m): string => 'no');

        // Make every UNSUB write fail so the rollback of the already-subscribed SIDs throws and must be
        // swallowed (the inner catch in start()'s rollback foreach). A failing write - not a closed
        // connection - is now what makes
        // unsubscribe() throw, since #116 made unsubscribe() a silent no-op on a not-open connection.
        $transport->throwOnWriteContaining = 'UNSUB';

        try {
            $service->start()->await();
            self::fail('Expected start() to throw');
        } catch (\Throwable) {
            // expected: the bad-subject subscribe throws; the rollback unsubscribe failures are swallowed.
        }

        // Service must remain not-started and SIDs must be cleared (rollback complete despite unsubscribe failures).
        self::assertSame([], (new \ReflectionProperty($service, 'subscriptionSids'))->getValue($service));
        self::assertFalse((new \ReflectionProperty($service, 'started'))->getValue($service));
    }

    /**
     * Verifies drain() silently swallows unsubscribe failures (the catch block inside the foreach in
     * drain()). Since #116 unsubscribe() no longer throws on a closed connection, the failure is now
     * triggered by a failing UNSUB write on an otherwise-open connection.
     */
    public function testDrainSwallowsUnsubscribeWriteFailure(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);
        $service->start()->await();

        // Make every UNSUB write fail so unsubscribe() throws for each SID during drain (the catch).
        $transport->throwOnWriteContaining = 'UNSUB';

        // drain() must complete without rethrowing any unsubscribe exceptions.
        $service->drain()->await();

        // State must still be cleared via the finally block.
        self::assertSame([], (new \ReflectionProperty($service, 'subscriptionSids'))->getValue($service));
        self::assertFalse((new \ReflectionProperty($service, 'started'))->getValue($service));
    }

    /**
     * Verifies drain() silently swallows flush failures when the connection is already gone
     * (catch block around flush() inside drain()).
     *
     * This path is reached when $this->started is true and flush() throws (e.g. closed connection).
     */
    public function testDrainToleratesFlushFailureOnClosedConnection(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
            // No second PONG: flush will timeout/fail because there is no PONG response queued.
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);
        $service->start()->await();

        // Close connection; now flush() inside drain() will throw (the catch around flush() fires).
        $client->disconnect()->await();

        // drain() must complete without rethrowing the flush exception; state must be cleared.
        $service->drain()->await();

        self::assertFalse((new \ReflectionProperty($service, 'started'))->getValue($service));
        self::assertSame([], (new \ReflectionProperty($service, 'subscriptionSids'))->getValue($service));
    }

    /**
     * Verifies buildRunCancellation returns a CompositeCancellation when both a timeout and an
     * external cancellation are supplied.
     */
    public function testRunWithBothTimeoutAndExternalCancellationUsesCompositeCancellation(): void
    {
        $transport = new FakeTransport($this->infoAndPong(), blockWhenEmpty: true);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);

        // Provide BOTH a timeout and an external DeferredCancellation - this exercises the
        // CompositeCancellation branch. The external cancel fires first.
        $deferred = new \Amp\DeferredCancellation();
        $runner = \Amp\async(static function () use ($service, $deferred): void {
            $service->run(10.0, $deferred->getCancellation())->await();
        });

        \Amp\delay(0.01);
        $deferred->cancel();
        $runner->await();

        // Service must have been stopped after the composite cancellation fired.
        $writes = implode('', $transport->writes);
        self::assertStringContainsString('UNSUB ', $writes);
    }

    /**
     * The VALIDATION_ERROR reply path is remotely reachable with hostile traffic, so a failing
     * error-reply publish must be contained inside the dispatch callback (#97 class): the rejection
     * stands unreplied, the endpoint records the validation error, and the terminal request_end
     * observer event still fires - the shared dispatch loop never sees the publish failure.
     */
    public function testValidationErrorReplyPublishFailureIsContained(): void
    {
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG svc.v 13 _INBOX.reply9 2\r\nhi\r\n",
        ]);
        // Every write of the error reply (an HPUB to the request's reply-to) fails at the transport.
        $transport->throwOnWriteContaining = '_INBOX.reply9';

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $handled = false;
        $events = [];
        $service = $client->service('val', '1.0.0')
            ->addObserver(static function (string $event, ServiceEndpoint $endpoint, NatsMessage $message, array $context) use (&$events): void {
                $events[] = $event;
            })
            ->withRequestValidator(static fn(NatsMessage $message, array $schema): string => 'bad input')
            ->addEndpoint('v', 'svc.v', static function (NatsMessage $message) use (&$handled): string {
                $handled = true;

                return 'ok';
            }, schema: ['type' => 'object']);
        $service->start()->await();

        // The publish failure must not escape the dispatch loop: the frame completes processing.
        self::assertSame(1, $client->processIncoming()->await());

        self::assertFalse($handled, 'a schema-rejected request must never reach the handler');
        // request_end still fires after the failed reply, so observer spans/gauges are not leaked.
        self::assertSame(['request_start', 'request_error', 'request_end'], $events);

        $stats = $service->statsSnapshot()['endpoints'][0] ?? [];
        self::assertSame(1, $stats['num_errors'] ?? null);
        self::assertSame('bad input', $stats['last_error'] ?? null);

        // The reply never reached the wire (the write threw before recording the bytes).
        self::assertStringNotContainsString('VALIDATION_ERROR', implode('', $transport->writes));
    }

    /**
     * stop() must swallow per-SID unsubscribe failures (a failing UNSUB write on an otherwise-open
     * connection), still clear all state via the finally, fire the done handler, and leave the
     * service restartable - a partial teardown must never abort stop() or wedge a later start().
     */
    public function testStopSwallowsUnsubscribeWriteFailureAndStillFiresDone(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $doneCount = 0;
        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload)
            ->onDone(static function () use (&$doneCount): void {
                $doneCount++;
            });
        $service->start()->await();

        // Make every UNSUB write fail so unsubscribe() throws for each SID during stop() (the catch).
        $transport->throwOnWriteContaining = 'UNSUB';

        $service->stop()->await();

        self::assertSame(1, $doneCount, 'the done handler must fire despite the unsubscribe failures');
        self::assertSame([], (new \ReflectionProperty($service, 'subscriptionSids'))->getValue($service));
        self::assertFalse((new \ReflectionProperty($service, 'started'))->getValue($service));

        // State was cleared cleanly: a subsequent start() re-subscribes without tripping the guard.
        $transport->throwOnWriteContaining = null;
        $service->start()->await();
        self::assertTrue((new \ReflectionProperty($service, 'started'))->getValue($service));
    }

    /**
     * run(): a handler of another subscription on the shared connection that throws while the loop reads is
     * reported to the error listener, and the loop reads on with the connection still open. The loop used to
     * catch the handler's exception, swallow it without a trace and back off.
     */
    public function testRunReportsASubscriptionHandlerThatThrowsAndReadsOn(): void
    {
        $errors = [];
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // One frame for the poisoned subscription (sid 1, subscribed before the service).
            "MSG px.poison 1 4\r\nboom\r\n",
        ], blockWhenEmpty: true);
        $client = $this->clientRecordingErrors($transport, $errors);

        $thrown = 0;
        $client->subscribe('px.poison', static function () use (&$thrown): void {
            $thrown++;

            throw new \RuntimeException('poison handler');
        })->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);

        $deferred = new DeferredCancellation();
        $runner = async(static function () use ($service, $deferred): void {
            $service->run(cancellation: $deferred->getCancellation())->await();
        });

        // The loop read on once its next read, of the idle socket, starts.
        $this->waitFor(static fn(): bool => $transport->startedReads >= 1);

        self::assertSame(1, $thrown, 'the poisoned handler must have thrown inside the run loop');
        self::assertSame(['poison handler'], $errors);

        $deferred->cancel();
        $runner->await();

        self::assertSame(ConnectionState::Open, $client->state());
        self::assertStringContainsString('UNSUB ', implode('', $transport->writes));
    }

    /**
     * run(): a request read in the same chunk as a message whose handler throws is answered by that read. It
     * used to stay queued until the server sent something else - up to the heartbeat interval - and was lost
     * when the service stopped first, since stop() discards what is queued for its endpoints.
     */
    public function testRunAnswersARequestReadBehindAMessageWhoseHandlerThrows(): void
    {
        $errors = [];
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            // The poisoned subscription is sid 1, so the endpoint is sid 14, after the twelve discovery sids.
            "MSG px.poison 1 4\r\nboom\r\nMSG svc.echo 14 _INBOX.req 5\r\nhello\r\n",
        ], blockWhenEmpty: true);
        $client = $this->clientRecordingErrors($transport, $errors);
        $client->subscribe('px.poison', static function (): void {
            throw new \RuntimeException('poison handler');
        })->await();

        $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => 'run:' . $m->payload)
            ->run(0.1)->await();

        self::assertStringContainsString('run:hello', implode('', $transport->writes));
        self::assertSame(['poison handler'], $errors);
    }

    /**
     * run(): the messages of the failing subscription read behind the one whose handler threw are delivered by
     * the same read.
     */
    public function testRunDeliversTheRestOfTheFailingSubscriptionsMessages(): void
    {
        $errors = [];
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG px.mixed 1 4\r\nboom\r\nMSG px.mixed 1 2\r\nok\r\n",
        ], blockWhenEmpty: true);
        $client = $this->clientRecordingErrors($transport, $errors);
        $seen = [];
        $client->subscribe('px.mixed', static function (NatsMessage $m) use (&$seen): void {
            $seen[] = $m->payload;
            if ($m->payload === 'boom') {
                throw new \RuntimeException('mixed handler');
            }
        })->await();

        $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload)
            ->run(0.1)->await();

        self::assertSame(['boom', 'ok'], $seen);
        self::assertSame(['mixed handler'], $errors);
    }

    /**
     * run(): an -ERR the server keeps the connection open for and a throwing handler read together are both
     * reported, and the request behind them is still answered. Which is reported first is not pinned: the
     * handler's failure is reported as the read delivers, the -ERR once it has delivered everything, though the
     * -ERR came first on the wire.
     */
    public function testRunReportsAnErrAndAHandlerFailureReadTogetherAndAnswersTheRequest(): void
    {
        $errors = [];
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "-ERR 'maximum subscriptions exceeded'\r\nMSG px.poison 1 4\r\nboom\r\nMSG svc.echo 14 _INBOX.req 5\r\nhello\r\n",
        ], blockWhenEmpty: true);
        $client = $this->clientRecordingErrors($transport, $errors);
        $client->subscribe('px.poison', static function (): void {
            throw new \RuntimeException('poison handler');
        })->await();

        $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => 'run:' . $m->payload)
            ->run(0.1)->await();

        self::assertEqualsCanonicalizing(['poison handler', "Server sent error frame: 'maximum subscriptions exceeded'"], $errors);
        self::assertStringContainsString('run:hello', implode('', $transport->writes));
    }

    /**
     * run(): a handler that throws a CancelledException while the loop reads - its own bounded wait ran out,
     * say - is reported like any other handler failure, and the service serves on. The loop used to take it
     * for its own cancellation and stop: every endpoint was unsubscribed, nothing was reported, and the request
     * that came next went unanswered.
     */
    public function testRunReportsAHandlerThatThrowsACancelledExceptionAndServesOn(): void
    {
        $errors = [];
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG px.slow 1 1\r\nx\r\n",
            "MSG svc.echo 14 _INBOX.req 5\r\nhello\r\n",
        ], blockWhenEmpty: true);
        $client = new NatsClient(new NatsOptions(
            pingIntervalSeconds: 0,
            errorListener: static function (\Throwable $error) use (&$errors): void {
                $errors[] = $error::class . ': ' . $error->getMessage();
            },
        ), $transport);
        $this->opened[] = $client;
        $client->connect()->await();
        $client->subscribe('px.slow', static function (): void {
            // A wait of the handler's own that runs out.
            delay(1.0, cancellation: new TimeoutCancellation(0.001));
        })->await();

        $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => 'run:' . $m->payload)
            ->run(0.2)->await();

        self::assertSame([CancelledException::class . ': The operation was cancelled'], $errors);
        self::assertStringContainsString('run:hello', implode('', $transport->writes), 'the service served on');
    }

    /**
     * run(): one read brings a message whose handler throws, a request and then a line that does not parse.
     * The handler's failure is reported, the request is answered by that read, the corrupt stream is reported
     * and recovered from by a reconnect, and the loop serves on. The read used to throw the handler's failure
     * once it had reconnected, for the loop to swallow.
     */
    public function testRunReportsAHandlerThatThrowsAheadOfALineThatDoesNotParseAndServesOn(): void
    {
        $errors = [];
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "MSG px.poison 1 4\r\nboom\r\nMSG svc.echo 14 _INBOX.req 5\r\nhello\r\nBOGUS\r\n",
            ...$this->infoAndPong(), // the reconnect after the corrupt stream
        ], blockWhenEmpty: true);
        $client = $this->clientRecordingErrors($transport, $errors);
        $client->subscribe('px.poison', static function (): void {
            throw new \RuntimeException('poison handler');
        })->await();

        $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => 'run:' . $m->payload)
            ->run(0.2)->await();

        self::assertSame(['poison handler', 'Unsupported control frame: BOGUS'], $errors);
        self::assertStringContainsString('run:hello', implode('', $transport->writes));
        self::assertCount(2, $transport->connectCalls, 'the corrupt stream was recovered from');
        self::assertSame(ConnectionState::Open, $client->state());
    }

    /**
     * run(): an -ERR the server keeps the connection open for, such as its answer to a SUB beyond the
     * maximum subscriptions, is reported to the error listener, and the loop serves on. The loop's catch
     * would only swallow it, and nothing said why an endpoint got no requests.
     */
    public function testRunReportsAnErrTheServerKeepsTheConnectionOpenForAndServesOn(): void
    {
        $errors = [];
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            "-ERR 'maximum subscriptions exceeded'\r\n",
            "MSG svc.echo 13 _INBOX.req 5\r\nhello\r\n",
        ]);
        $client = new NatsClient(new NatsOptions(
            pingIntervalSeconds: 0,
            errorListener: static function (\Throwable $error) use (&$errors): void {
                $errors[] = $error->getMessage();
            },
        ), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => 'run:' . $message->payload);

        $service->run(0.1)->await();

        self::assertContains("Server sent error frame: 'maximum subscriptions exceeded'", $errors);
        self::assertSame(ConnectionState::Open, $client->state());
        self::assertStringContainsString('run:hello', implode('', $transport->writes), 'the loop serves the request after the -ERR');
    }

    /**
     * run(): a subscription whose handler throws on every message no longer slows the loop down. A request
     * queued behind 25 failing messages, each in a chunk of its own, is answered within a 100 ms run; the loop
     * used to back off 20 ms after each failure, so it reached the fifth.
     */
    public function testRunDoesNotBackOffAfterAHandlerFails(): void
    {
        $errors = [];
        $transport = new FakeTransport([
            ...$this->infoAndPong(),
            ...array_fill(0, 25, "MSG px.poison 1 4\r\nboom\r\n"),
            "MSG svc.echo 14 _INBOX.req 5\r\nhello\r\n",
        ], blockWhenEmpty: true);
        $client = $this->clientRecordingErrors($transport, $errors);
        $thrown = 0;
        $client->subscribe('px.poison', static function () use (&$thrown): void {
            $thrown++;

            throw new \RuntimeException('poison handler');
        })->await();

        $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => 'run:' . $m->payload)
            ->run(0.1)->await();

        self::assertSame(25, $thrown);
        self::assertCount(25, $errors);
        self::assertStringContainsString('run:hello', implode('', $transport->writes));
    }

    /**
     * run(): cancelling while the loop backs off from a connection-level failure exits at once and stops the
     * service. The server drops the client as stale while the loop reads; the reconnect that starts holds its
     * dial, and the loop, which may not wait for it (waitForReconnect off), fails each read and backs off.
     */
    public function testRunCancellationDuringReconnectBackoffStopsService(): void
    {
        $transport = new ReconnectingTransport();
        $client = new NatsClient(new NatsOptions(connectTimeoutMs: 500, pingIntervalSeconds: 0, waitForReconnect: false), $transport);
        $this->opened[] = $client;
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);
        $deferred = new DeferredCancellation();
        $runner = async(static function () use ($service, $deferred): void {
            $service->run(cancellation: $deferred->getCancellation())->await();
        });

        $this->waitFor(static fn(): bool => $transport->sidFor('svc.echo') !== null);
        $transport->holdNextDial();
        $transport->pushFrame("-ERR 'Stale Connection'\r\n");
        $this->waitFor(static fn(): bool => $client->state() === ConnectionState::Connecting);
        delay(0.1);

        $start = hrtime(true);
        $deferred->cancel();
        $runner->await(new TimeoutCancellation(1.0));

        self::assertLessThan(0.1, (hrtime(true) - $start) / 1e9, 'cancellation must end the backoff at once');
        self::assertSame([], (new \ReflectionProperty($service, 'subscriptionSids'))->getValue($service));
        self::assertFalse((new \ReflectionProperty($service, 'started'))->getValue($service));

        $transport->releaseDial();
        $this->waitFor(static fn(): bool => $client->state() === ConnectionState::Open);
    }

    /** @return iterable<string, array{bool}> */
    public static function reconnectModes(): iterable
    {
        yield 'reconnect off' => [false];
        yield 'reconnect on' => [true];
    }

    /**
     * run(): one read brings a message whose handler throws, a request, and a fatal -ERR. The handler's failure
     * is reported and the request answered before the connection ends - on the connection the server is
     * closing, as any message read ahead of a fatal -ERR is delivered - and the connection then closes
     * (reconnect off) or reconnects. The request used to stay queued behind the failure: with reconnect off it
     * was discarded with the connection, and with reconnect on it was answered on the new connection.
     */
    #[DataProvider('reconnectModes')]
    public function testRunAnswersARequestReadAheadOfAFatalErrBehindAThrowingHandlerBeforeTheConnectionEnds(bool $reconnect): void
    {
        $errors = [];
        $transport = new ReconnectingTransport();
        $client = $this->clientRecordingErrors($transport, $errors, reconnect: $reconnect);
        $client->subscribe('px.poison', static function (): void {
            throw new \RuntimeException('poison handler');
        })->await();
        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => 'run:' . $m->payload);
        $stop = new DeferredCancellation();
        $runner = async(static fn() => $service->run(cancellation: $stop->getCancellation())->await());
        $this->waitFor(static fn(): bool => $transport->sidFor('svc.echo') !== null);
        delay(0.02);

        $transport->pushFrame(
            ReconnectingTransport::msgFrame('px.poison', (int) $transport->sidFor('px.poison'), 'boom')
            . ReconnectingTransport::msgFrame('svc.echo', (int) $transport->sidFor('svc.echo'), 'hello', '_INBOX.req')
            . "-ERR 'Stale Connection'\r\n",
        );
        if ($reconnect) {
            $this->waitFor(static fn(): bool => $transport->epoch() === 1 && $client->state() === ConnectionState::Open);
        } else {
            $this->waitFor(static fn(): bool => $client->state() === ConnectionState::Closed);
        }

        self::assertSame([0], self::epochsOfWritesContaining($transport, 'run:hello'), 'answered before the connection ended');
        self::assertSame(['poison handler'], $errors);
        $stop->cancel();
        $runner->await();
    }

    /**
     * run(): a request another fiber's read brought - a request() that waited for its reply - behind a message
     * whose handler throws is answered without waiting for the server to send more. That read fails with the
     * handler's exception and leaves the rest queued, and the loop's read, which waited for it, delivers the
     * rest. It used to wait for the server's next bytes, up to the heartbeat interval, and was lost when the
     * service stopped first. The request() itself still fails with the other subscription's exception.
     */
    public function testRunAnswersARequestAnotherFibersReadLeftQueuedBehindAThrowingHandler(): void
    {
        $errors = [];
        $transport = new ReconnectingTransport();
        $client = $this->clientRecordingErrors($transport, $errors, reconnect: false);
        $client->subscribe('px.poison', static function (): void {
            throw new \RuntimeException('poison handler');
        })->await();
        // A request that owns the socket read while it waits for a reply that never comes.
        $request = async(static fn() => $client->request('backend.op', 'q', 2_000)->await());
        $this->waitFor(static fn(): bool => $transport->controlLinesStartingWith('PUB backend.op') !== []);
        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => 'run:' . $m->payload);
        $stop = new DeferredCancellation();
        $runner = async(static fn() => $service->run(cancellation: $stop->getCancellation())->await());
        $this->waitFor(static fn(): bool => $transport->sidFor('svc.echo') !== null);
        delay(0.02);

        $transport->pushFrame(
            ReconnectingTransport::msgFrame('px.poison', (int) $transport->sidFor('px.poison'), 'boom')
            . ReconnectingTransport::msgFrame('svc.echo', (int) $transport->sidFor('svc.echo'), 'hello', '_INBOX.req'),
        );

        $this->waitFor(static fn(): bool => self::epochsOfWritesContaining($transport, 'run:hello') !== []);
        try {
            $request->await();
            self::fail('expected the request to fail with the other subscription\'s exception');
        } catch (\RuntimeException $e) {
            self::assertSame('poison handler', $e->getMessage());
        }
        $stop->cancel();
        $runner->await();
    }

    /**
     * run(): a request that the delivery after a reconnect left queued behind a message whose handler throws is
     * answered by the loop's next read, without waiting for the server to send more. That delivery reports the
     * failure and stops there; the request used to wait for the server's next bytes.
     */
    public function testRunAnswersARequestTheDeliveryAfterAReconnectLeftQueuedBehindAThrowingHandler(): void
    {
        $errors = [];
        $transport = new ReconnectingTransport();
        $client = $this->clientRecordingErrors($transport, $errors);
        $client->subscribe('px.poison', static function (): void {
            throw new \RuntimeException('poison handler');
        })->await();
        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => 'run:' . $m->payload);
        $stop = new DeferredCancellation();
        $runner = async(static fn() => $service->run(cancellation: $stop->getCancellation())->await());
        $this->waitFor(static fn(): bool => $transport->sidFor('svc.echo') !== null);
        $poisonSid = (int) $transport->sidFor('px.poison');
        $echoSid = (int) $transport->sidFor('svc.echo');
        delay(0.02);
        // The new connection answers the replayed subscriptions with both messages, which the replay reads.
        $transport->afterWrite = static function (string $bytes) use ($transport, $poisonSid, $echoSid): void {
            if ($transport->epoch() === 1 && str_contains($bytes, 'SUB svc.echo')) {
                $transport->afterWrite = null;
                $transport->pushFrame(
                    ReconnectingTransport::msgFrame('px.poison', $poisonSid, 'boom')
                    . ReconnectingTransport::msgFrame('svc.echo', $echoSid, 'hello', '_INBOX.req'),
                );
            }
        };

        $transport->dropConnection();

        $this->waitFor(static fn(): bool => self::epochsOfWritesContaining($transport, 'run:hello') !== []);
        self::assertSame([1], self::epochsOfWritesContaining($transport, 'run:hello'));
        self::assertSame(['Socket closed by peer (EOF)', 'poison handler'], $errors);
        $stop->cancel();
        $runner->await();
    }

    /**
     * drain(): a handler of another subscription that throws while the flush after the UNSUBs reads is reported,
     * and the flush reads on to its PONG, which confirms the server has processed the UNSUBs. The flush used to
     * end with that handler's exception, before its PONG, and drain() swallowed it.
     */
    public function testDrainReportsAHandlerThatThrowsDuringItsFlushAndReadsOnToItsPong(): void
    {
        $errors = [];
        $transport = new FakeTransport($this->infoAndPong(), blockWhenEmpty: true);
        $client = $this->clientRecordingErrors($transport, $errors);
        $client->subscribe('px.poison', static function (): void {
            throw new \RuntimeException('poison handler');
        })->await();
        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);
        $service->start()->await();
        // The server answers the flush's PING with a message for the poisoned subscription, then the PONG.
        $transport->enqueueOnWriteContaining["PING\r\n"] = ["MSG px.poison 1 4\r\nboom\r\n", "PONG\r\n"];

        $service->drain()->await();

        self::assertSame(['poison handler'], $errors);
        self::assertSame([], (new \ReflectionProperty($transport, 'readQueue'))->getValue($transport), 'the flush read its PONG');
    }

    /**
     * run(): an application that passes its shutdown token both to run() and to a wait inside a subscription
     * handler, and shuts down while that handler waits. run() stops, and the handler's CancelledException is
     * reported, once, like any handler failure the loop's read meets. Before, the loop had stopped waiting for
     * that read by then, and the exception went nowhere.
     */
    public function testRunStoppedWhileAHandlerAwaitsTheSameCancellationReportsThatHandlersCancelledException(): void
    {
        $errors = [];
        $transport = new FakeTransport([...$this->infoAndPong(), "MSG px.work 1 1\r\nx\r\n"], blockWhenEmpty: true);
        $client = new NatsClient(new NatsOptions(
            pingIntervalSeconds: 0,
            errorListener: static function (\Throwable $error) use (&$errors): void {
                $errors[] = $error::class . ': ' . $error->getMessage();
            },
        ), $transport);
        $this->opened[] = $client;
        $client->connect()->await();
        $shutdown = new DeferredCancellation();
        $waiting = false;
        $client->subscribe('px.work', static function () use ($shutdown, &$waiting): void {
            $waiting = true;
            delay(5.0, cancellation: $shutdown->getCancellation());
        })->await();
        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);
        $runner = async(static fn() => $service->run(cancellation: $shutdown->getCancellation())->await());
        $this->waitFor(static function () use (&$waiting): bool {
            return $waiting;
        });

        $shutdown->cancel();
        $runner->await();
        $this->waitFor(static function () use (&$errors): bool {
            return $errors !== [];
        });

        self::assertSame([CancelledException::class . ': The operation was cancelled'], $errors);
    }

    /**
     * A class-string endpoint handler that does NOT implement ServiceEndpointHandlerInterface is
     * rejected at registration with an exception naming both the class and the required interface -
     * not silently wrapped into a handler that fails at request time. The declared parameter type
     * (class-string<ServiceEndpointHandlerInterface>) already forbids this statically, so the
     * runtime guard defends untyped callers; it is reached via reflection rather than by violating
     * the public signature in-repo (the suite's established internals pattern).
     */
    public function testEndpointRejectsClassStringNotImplementingHandlerInterface(): void
    {
        $client = new NatsClient(new NatsOptions(), new FakeTransport());
        $service = $client->service('echo', '1.0.0');

        try {
            (new \ReflectionMethod($service, 'resolveHandler'))
                ->invoke($service, ServiceTestInvalidClassHandler::class);
            self::fail('a class-string handler not implementing the interface must be rejected');
        } catch (\InvalidArgumentException $e) {
            self::assertSame(sprintf(
                'Service endpoint class handler %s must implement %s.',
                ServiceTestInvalidClassHandler::class,
                ServiceEndpointHandlerInterface::class,
            ), $e->getMessage());
        }
    }

    /**
     * A connected client whose error listener collects the message of everything the connection reports.
     *
     * @param list<string> $errors
     */
    private function clientRecordingErrors(TransportInterface $transport, array &$errors, bool $reconnect = true): NatsClient
    {
        $client = new NatsClient(new NatsOptions(
            connectTimeoutMs: 500,
            reconnectEnabled: $reconnect,
            reconnectDelayMs: 1,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
            errorListener: static function (\Throwable $error) use (&$errors): void {
                $errors[] = $error->getMessage();
            },
        ), $transport);
        $this->opened[] = $client;
        $client->connect()->await();

        return $client;
    }

    /**
     * @param \Closure(): bool $condition
     */
    private function waitFor(\Closure $condition): void
    {
        $deadlineNs = hrtime(true) + 2_000_000_000;
        while (!$condition() && hrtime(true) < $deadlineNs) {
            delay(0.005);
        }

        self::assertTrue($condition(), 'the condition was not met in time');
    }

    /**
     * The epochs of the writes a live session of $transport accepted that contain $needle, in order.
     *
     * @return list<int>
     */
    private static function epochsOfWritesContaining(ReconnectingTransport $transport, string $needle): array
    {
        $epochs = [];
        foreach ($transport->writes as $write) {
            if (str_contains($write['bytes'], $needle)) {
                $epochs[] = $write['epoch'];
            }
        }

        return $epochs;
    }
}
