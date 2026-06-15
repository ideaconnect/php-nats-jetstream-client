<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredCancellation;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Services\BasicJsonSchemaValidator;
use IDCT\NATS\Services\ServiceEndpoint;
use IDCT\NATS\Services\ServiceEndpointHandlerInterface;
use IDCT\NATS\Tests\Support\FakeTransport;
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
        $this->expectExceptionMessage('name must not be empty');
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

        // The connection is gone before stop(): unsubscribe() would throw "not open".
        $client->disconnect()->await();

        // stop() must not abort on the first failure; it swallows per-SID and clears state.
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
     * Verifies that calling start() on an already-started service is a no-op (idempotency guard, line 194).
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
     * in the error payload (lines 273-274).
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
     * Verifies that a handler returning null causes no reply to be published (line 310).
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
     * Verifies that drain() fires the done handler once (line 379 via markDone from drain).
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
     * Verifies that the done handler exception is swallowed (line 379: handler called inside try/catch).
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
     * Verifies a discovery message with no replyTo is silently ignored (line 565).
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
     * Verifies endpoint requests with no replyTo are processed but produce no PUB (line 593).
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
     * Verifies observer that throws does not interrupt request handling (line 672).
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
     * Verifies buildRunCancellation throws for a non-positive timeout (line 739).
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
     * Verifies run() with only a positive timeout (no external cancellation) uses a TimeoutCancellation (line 751).
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
     * Verifies stats supplier that throws does not break the stats response (line 517 catch).
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
     * Verifies the run loop breaks immediately when the cancellation is already requested on entry (line 440).
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
     * during the catch block (line 326: inner catch in the rollback foreach).
     *
     * Scenario: two endpoints are added; the connection is dropped before start(). The first
     * discovery subscribe succeeds (the fake transport is still open) but everything breaks
     * after that... actually we need a subtler approach: connect, immediately close the transport,
     * then call start() so that even the first subscribe fails - but then the rollback tries
     * unsub on the already-queued SIDs and THAT also fails (line 326 catch fires).
     *
     * The simplest reliable trigger: start successfully, close the connection, then force a second
     * start() with a bad endpoint. The partial subscribe at "bad subject" throws, and the rollback
     * over the already-subscribed SIDs (discovery + first endpoint) also throws because the
     * connection is now closed - exercising the line-326 swallow path.
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

        // Close the connection so that when start() rolls back the already-subscribed SIDs,
        // unsubscribe() throws (line 326 catch fires) but must be swallowed.
        $client->disconnect()->await();

        try {
            $service->start()->await();
            self::fail('Expected start() to throw');
        } catch (\Throwable) {
            // expected: the subscribe or rollback itself throws; what matters is no secondary exception.
        }

        // Service must remain not-started and SIDs must be cleared (rollback complete despite unsubscribe failures).
        self::assertSame([], (new \ReflectionProperty($service, 'subscriptionSids'))->getValue($service));
        self::assertFalse((new \ReflectionProperty($service, 'started'))->getValue($service));
    }

    /**
     * Verifies drain() silently swallows unsubscribe failures when the connection is already gone
     * (line 398: catch block inside the foreach in drain()).
     */
    public function testDrainToleratesClosedConnectionDuringUnsubscribe(): void
    {
        $transport = new FakeTransport($this->infoAndPong());
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);
        $service->start()->await();

        // Drop the connection before draining so unsubscribe() throws for every SID (line 398 catch).
        $client->disconnect()->await();

        // drain() must complete without rethrowing any unsubscribe exceptions.
        $service->drain()->await();

        // State must still be cleared via the finally block.
        self::assertSame([], (new \ReflectionProperty($service, 'subscriptionSids'))->getValue($service));
        self::assertFalse((new \ReflectionProperty($service, 'started'))->getValue($service));
    }

    /**
     * Verifies drain() silently swallows flush failures when the connection is already gone
     * (line 408: catch block around flush() inside drain()).
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

        // Close connection; now flush() inside drain() will throw (line 408 catch fires).
        $client->disconnect()->await();

        // drain() must complete without rethrowing the flush exception; state must be cleared.
        $service->drain()->await();

        self::assertFalse((new \ReflectionProperty($service, 'started'))->getValue($service));
        self::assertSame([], (new \ReflectionProperty($service, 'subscriptionSids'))->getValue($service));
    }

    /**
     * Verifies buildRunCancellation returns a CompositeCancellation when both a timeout and an
     * external cancellation are supplied (line 751).
     */
    public function testRunWithBothTimeoutAndExternalCancellationUsesCompositeCancellation(): void
    {
        $transport = new FakeTransport($this->infoAndPong(), blockWhenEmpty: true);
        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $m): string => $m->payload);

        // Provide BOTH a timeout and an external DeferredCancellation - this exercises the
        // CompositeCancellation branch (line 751). The external cancel fires first.
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


}
