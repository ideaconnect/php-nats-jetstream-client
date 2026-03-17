<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Unit;

use Amp\DeferredCancellation;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Services\BasicJsonSchemaValidator;
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

final class ServiceTestInvalidClassHandler
{
}

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
            "MSG svc.echo 13 _INBOX.req 5\r\nhello\r\n",
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
        self::assertStringContainsString("UNSUB 13\r\n", $writes);
    }

    /**
     * Verifies grouped hierarchy API prefixes endpoint subjects correctly.
     */
    public function testGroupedEndpointHierarchy(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            "MSG svc.v1.echo 13 _INBOX.req 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0');
        $service->addGroup('svc')->addGroup('v1')->addEndpoint(
            'echo-v1',
            'echo',
            static fn (NatsMessage $message): string => 'v1:' . $message->payload,
        );

        $service->start()->await();
        $client->processIncoming()->await();

        $writes = implode('', $transport->writes);
        self::assertStringContainsString('SUB svc.v1.echo 13' . "\r\n", $writes);
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0');
        $service->addGroup('')->addEndpoint('root', 'echo', static fn (NatsMessage $message): string => $message->payload);
        $service->addGroup('svc')->addEndpoint('svc-root', '', static fn (NatsMessage $message): string => $message->payload);

        $subjects = array_map(
            static fn (array $endpoint): string => (string) ($endpoint['subject'] ?? ''),
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            "MSG \$SRV.SCHEMA.echo 11 _INBOX.schema 0\r\n\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $schema = ['type' => 'object', 'properties' => ['msg' => ['type' => 'string']]];

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn (NatsMessage $message): string => $message->payload, schema: $schema);
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            "MSG svc.echo 13 _INBOX.req1 5\r\nhello\r\n",
            "MSG svc.echo 13 _INBOX.req2 4\r\nboom\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static function (NatsMessage $message): string {
                usleep(1000);

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
     * Verifies reset clears accumulated endpoint statistics.
     */
    public function testResetClearsStats(): void
    {
        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            "MSG svc.echo 13 _INBOX.req1 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn (NatsMessage $message): string => $message->payload);
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            "MSG svc.echo 13 _INBOX.req1 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $handled = false;
        $service = $client->service('echo', '1.0.0')
            ->withRequestValidator(static fn (NatsMessage $message, array $schema): ?string => $schema === [] ? null : 'payload does not match schema')
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
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
            ->addEndpoint('echo', 'svc.echo', static fn (NatsMessage $message): string => $message->payload);
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            "MSG svc.echo 13 _INBOX.req1 16\r\n{\"id\":\"invalid\"}\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->withSchemaValidator(new BasicJsonSchemaValidator())
            ->addEndpoint('echo', 'svc.echo', static fn (NatsMessage $message): string => $message->payload, schema: [
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            "MSG svc.echo 13 _INBOX.req 5\r\nhello\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn (NatsMessage $message): string => 'run:' . $message->payload);

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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $service = $client->service('echo', '1.0.0')
            ->addEndpoint('echo', 'svc.echo', static fn (NatsMessage $message): string => $message->payload);

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
}
