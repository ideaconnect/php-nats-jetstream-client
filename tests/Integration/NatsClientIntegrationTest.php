<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Integration;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Future;
use IDCT\NATS\Auth\NkeySeedSigner;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsHeaders;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\NatsException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Services\BasicJsonSchemaValidator;
use IDCT\NATS\Tests\Support\FakeTransport;
use IDCT\NATS\Tests\Support\FlakyTransport;
use PHPUnit\Framework\TestCase;
use function Amp\async;
use function Amp\delay;

final class NatsClientIntegrationTest extends TestCase
{
    use IntegrationTestBootstrap;

    /**
     * Verifies connect and disconnect against a real NATS server.
     */
    public function testConnectAndDisconnect(): void
    {
        $this->requireIntegrationEnabled();

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        self::assertNotNull($client->serverInfo());

        $client->disconnect()->await();
    }

    /**
     * Verifies publish and subscribe delivery path against a live server.
     */
    public function testPublishAndSubscribeRoundTrip(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.subject.' . bin2hex(random_bytes(4));
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $received = null;
        $client->subscribe($subject, static function (NatsMessage $message) use (&$received): void {
            $received = $message;
        })->await();

        $client->publish($subject, 'hello')->await();
        $client->processIncoming()->await();

        self::assertInstanceOf(NatsMessage::class, $received);
        /** @var NatsMessage $message */
        $message = $received;
        self::assertSame('hello', $message->payload);

        $client->disconnect()->await();
    }

    /**
     * Verifies request/reply end-to-end using two live clients.
     */
    public function testRequestReply(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.echo.' . bin2hex(random_bytes(4));
        $server = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));

        $server->connect()->await();
        $client->connect()->await();

        $handled = false;
        $server->subscribe($subject, static function (NatsMessage $message) use (&$handled, $server): void {
            $handled = true;
            if ($message->replyTo !== null) {
                $server->publish($message->replyTo, 'world')->await();
            }
        })->await();

        // Process one request on the server in parallel while requester waits for its reply.
        $serverLoop = async(static function () use ($server): void {
            $server->processIncoming()->await();
        });

        $reply = $client->request($subject, 'hello', 2000)->await();
        $serverLoop->await();

        self::assertTrue($handled);
        self::assertSame('world', $reply->payload);

        $client->disconnect()->await();
        $server->disconnect()->await();
    }

    /**
     * Verifies publishWithHeaders preserves custom headers for subscribers.
     */
    public function testPublishWithHeadersRoundTrip(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.headers.' . bin2hex(random_bytes(4));
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $received = null;
        $client->subscribe($subject, static function (NatsMessage $message) use (&$received): void {
            $received = $message;
        })->await();

        $client->publishWithHeaders($subject, 'hello', [
            'X-Request-Id' => 'it-' . bin2hex(random_bytes(3)),
            'Content-Type' => 'text/plain',
        ])->await();

        $deadline = microtime(true) + 2.0;
        while ($received === null && microtime(true) < $deadline) {
            $client->processIncoming()->await();
            usleep(20_000);
        }

        self::assertInstanceOf(NatsMessage::class, $received);
        /** @var NatsMessage $message */
        $message = $received;
        $headers = NatsHeaders::fromWireBlock($message->rawHeaders);

        self::assertSame('hello', $message->payload);
        self::assertArrayHasKey('X-Request-Id', $headers);
        self::assertSame('text/plain', $headers['Content-Type'] ?? null);

        $client->disconnect()->await();
    }

    /**
     * Verifies requestWithHeaders forwards custom headers to service handlers.
     */
    public function testRequestWithHeadersPropagatesHeaders(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.req.headers.' . bin2hex(random_bytes(4));
        $requestId = 'req-' . bin2hex(random_bytes(3));

        $server = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));

        $server->connect()->await();
        $client->connect()->await();

        $seenRequestId = null;
        $server->subscribe($subject, static function (NatsMessage $message) use (&$seenRequestId, $server): void {
            $headers = NatsHeaders::fromWireBlock($message->rawHeaders);
            $seenRequestId = $headers['X-Request-Id'] ?? null;

            if ($message->replyTo !== null) {
                $server->publish($message->replyTo, 'ok')->await();
            }
        })->await();

        $serverLoop = async(static function () use ($server): void {
            $server->processIncoming()->await();
        });

        $reply = $client->requestWithHeaders($subject, 'hello', ['X-Request-Id' => $requestId], 2000)->await();
        $serverLoop->await();

        self::assertSame('ok', $reply->payload);
        self::assertSame($requestId, $seenRequestId);

        $client->disconnect()->await();
        $server->disconnect()->await();
    }

    /**
     * Verifies no_echo suppresses delivery of a client's own publishes.
     */
    public function testNoEchoSuppressesSelfPublishedMessages(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.noecho.' . bin2hex(random_bytes(4));
        $client = new NatsClient(new NatsOptions(
            servers: [$this->integrationServerUrl()],
            noEcho: true,
        ));
        $client->connect()->await();

        $received = false;
        $client->subscribe($subject, static function (NatsMessage $message) use (&$received): void {
            $received = true;
        })->await();

        $client->publish($subject, 'self')->await();

        $deadline = microtime(true) + 0.8;
        while (microtime(true) < $deadline) {
            $client->processIncoming()->await();
            if ($received) {
                break;
            }

            delay(0.01);
        }

        self::assertFalse($received);

        $client->disconnect()->await();
    }

    /**
     * Verifies connect can rotate to a later server entry when the first endpoint is unavailable.
     */
    public function testConnectWithServerRotationFallback(): void
    {
        $this->requireIntegrationEnabled();

        $url = $this->integrationServerUrl();
        $client = new NatsClient(
            new NatsOptions(
                servers: ['nats://127.0.0.1:5222', $url],
                reconnectEnabled: true,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 20,
                reconnectJitterMs: 0,
            ),
        );

        $client->connect()->await();

        self::assertNotNull($client->serverInfo());
        $client->disconnect()->await();
    }

    /**
    * Verifies services framework endpoint request/reply on a live server.
     */
    public function testServiceDiscoveryAndEndpoint(): void
    {
        $this->requireIntegrationEnabled();

        $serviceClient = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $requester = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));

        $serviceClient->connect()->await();
        $requester->connect()->await();

        $service = $serviceClient->service('echo', '1.0.0', 'Echo demo')
            ->addEndpoint('echo', 'svc.echo', static fn (NatsMessage $message): string => 'reply:' . $message->payload);
        $service->start()->await();

        $servicePumpCancellation = new DeferredCancellation();
        $servicePump = async(static function () use ($serviceClient, $servicePumpCancellation): void {
            $cancellation = $servicePumpCancellation->getCancellation();

            while (!$cancellation->isRequested()) {
                try {
                    $serviceClient->processIncoming()->await($cancellation);
                } catch (CancelledException) {
                    break;
                } catch (\Throwable) {
                    usleep(20_000);
                }
            }
        });

        try {
            // Retry to handle race condition: the service subscription
            // may not be ready on the NATS server yet.
            $echoReply = null;
            for ($attempt = 0; $attempt < 10; $attempt++) {
                try {
                    $echoReply = $requester->request('svc.echo', 'hello', 2_000)->await();
                    break;
                } catch (NatsException $e) {
                    if ($attempt === 9 || !str_contains($e->getMessage(), 'No responders')) {
                        throw $e;
                    }
                    delay(0.1);
                }
            }
        } finally {
            $servicePumpCancellation->cancel();
            $servicePump->await();
        }

        self::assertInstanceOf(NatsMessage::class, $echoReply);
        /** @var NatsMessage $echoReplyMessage */
        $echoReplyMessage = $echoReply;

        self::assertSame('reply:hello', $echoReplyMessage->payload);
        self::assertSame('echo', (string) ($service->statsSnapshot()['name'] ?? ''));

        $service->stop()->await();
        $requester->disconnect()->await();
        $serviceClient->disconnect()->await();
    }

    /**
     * Verifies service stats parity fields and observer correlation metadata on live server.
     */
    public function testServiceStatsAndObserversWithHeaders(): void
    {
        $this->requireIntegrationEnabled();

        $suffix = bin2hex(random_bytes(3));
        $serviceName = 'echo-' . $suffix;
        $subject = 'svc.echo.' . $suffix;

        $serviceClient = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $requester = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));

        $serviceClient->connect()->await();
        $requester->connect()->await();

        $events = [];
        $service = $serviceClient->service($serviceName, '1.0.0', 'Echo stats')
            ->withSchemaValidator(new BasicJsonSchemaValidator())
            ->addObserver(static function (string $event, $endpoint, NatsMessage $message, array $context) use (&$events): void {
                $events[] = [
                    'event' => $event,
                    'correlation_id' => $context['correlation_id'] ?? null,
                    'subject' => $message->subject,
                ];
            })
            ->addEndpoint('echo', $subject, static fn (NatsMessage $message): string => 'reply:' . $message->payload, schema: [
                'type' => 'object',
                'required' => ['id'],
                'properties' => [
                    'id' => ['type' => 'integer'],
                ],
            ]);
        $service->start()->await();

        $servicePumpCancellation = new DeferredCancellation();
        $servicePump = async(static function () use ($serviceClient, $servicePumpCancellation): void {
            $cancellation = $servicePumpCancellation->getCancellation();

            while (!$cancellation->isRequested()) {
                try {
                    $serviceClient->processIncoming()->await($cancellation);
                } catch (CancelledException) {
                    break;
                } catch (\Throwable) {
                    usleep(20_000);
                }
            }
        });

        try {
            $invalidReply = null;
            $validReply = null;

            for ($attempt = 0; $attempt < 10; $attempt++) {
                try {
                    $invalidReply = $requester->requestWithHeaders(
                        $subject,
                        '{"id":"bad"}',
                        ['X-Request-Id' => 'it-invalid-' . $suffix],
                        2_000,
                    )->await();

                    $validReply = $requester->requestWithHeaders(
                        $subject,
                        '{"id":1}',
                        ['X-Request-Id' => 'it-valid-' . $suffix],
                        2_000,
                    )->await();
                    break;
                } catch (NatsException $e) {
                    if ($attempt === 9 || !str_contains($e->getMessage(), 'No responders')) {
                        throw $e;
                    }

                    delay(0.1);
                }
            }
        } finally {
            $servicePumpCancellation->cancel();
            $servicePump->await();
        }

        self::assertInstanceOf(NatsMessage::class, $invalidReply);
        self::assertInstanceOf(NatsMessage::class, $validReply);

        $invalidPayload = json_decode((string) $invalidReply->payload, true);
        self::assertIsArray($invalidPayload);
        self::assertSame('io.nats.micro.v1.error', $invalidPayload['type'] ?? null);
        self::assertSame('VALIDATION_ERROR', $invalidPayload['code'] ?? null);
        self::assertSame('it-invalid-' . $suffix, $invalidPayload['correlation_id'] ?? null);

        self::assertSame('reply:{"id":1}', $validReply->payload);

        $stats = $service->statsSnapshot();
        $endpoint = $stats['endpoints'][0] ?? [];
        self::assertSame(2, $endpoint['num_requests'] ?? null);
        self::assertSame(1, $endpoint['num_errors'] ?? null);
        self::assertNotSame('', (string) ($endpoint['last_error'] ?? ''));
        self::assertGreaterThanOrEqual(0, (int) ($endpoint['processing_time'] ?? -1));
        self::assertGreaterThanOrEqual(0, (int) ($endpoint['average_processing_time'] ?? -1));

        $eventNames = array_map(static fn (array $event): string => (string) $event['event'], $events);
        self::assertContains('request_start', $eventNames);
        self::assertContains('request_error', $eventNames);
        self::assertContains('request_end', $eventNames);

        $correlationIds = array_values(array_filter(array_map(
            static fn (array $event): ?string => is_string($event['correlation_id'] ?? null) ? $event['correlation_id'] : null,
            $events,
        )));
        self::assertContains('it-invalid-' . $suffix, $correlationIds);
        self::assertContains('it-valid-' . $suffix, $correlationIds);

        $service->stop()->await();
        $requester->disconnect()->await();
        $serviceClient->disconnect()->await();
    }

    /**
     * Verifies service discovery subject contract for PING/INFO/STATS/SCHEMA.
     */
    public function testServiceDiscoverySubjectsContract(): void
    {
        $this->requireIntegrationEnabled();

        $suffix = bin2hex(random_bytes(3));
        $serviceName = 'svc-' . $suffix;
        $subjectWithSchema = 'svc.' . $suffix . '.schema';
        $subjectWithoutSchema = 'svc.' . $suffix . '.plain';

        $serviceClient = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $requester = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));

        $serviceClient->connect()->await();
        $requester->connect()->await();

        $service = $serviceClient->service($serviceName, '1.2.3', 'Discovery contract')
            ->addEndpoint('schema-endpoint', $subjectWithSchema, static fn (NatsMessage $message): string => 'ok:' . $message->payload, schema: [
                'type' => 'object',
                'required' => ['id'],
                'properties' => [
                    'id' => ['type' => 'integer'],
                ],
            ])
            ->addEndpoint('plain-endpoint', $subjectWithoutSchema, static fn (NatsMessage $message): string => 'ok:' . $message->payload);
        $service->start()->await();

        $servicePumpCancellation = new DeferredCancellation();
        $servicePump = async(static function () use ($serviceClient, $servicePumpCancellation): void {
            $cancellation = $servicePumpCancellation->getCancellation();

            while (!$cancellation->isRequested()) {
                try {
                    $serviceClient->processIncoming()->await($cancellation);
                } catch (CancelledException) {
                    break;
                } catch (\Throwable) {
                    usleep(20_000);
                }
            }
        });

        try {
            $subjects = [
                '$SRV.PING.' . $serviceName,
                '$SRV.INFO.' . $serviceName,
                '$SRV.STATS.' . $serviceName,
                '$SRV.SCHEMA.' . $serviceName,
            ];

            $responses = [];
            foreach ($subjects as $discoverySubject) {
                $response = null;
                for ($attempt = 0; $attempt < 10; $attempt++) {
                    try {
                        $response = $requester->request($discoverySubject, '', 2_000)->await();
                        break;
                    } catch (NatsException $e) {
                        if ($attempt === 9 || !str_contains($e->getMessage(), 'No responders')) {
                            throw $e;
                        }

                        delay(0.1);
                    }
                }

                self::assertInstanceOf(NatsMessage::class, $response);
                /** @var NatsMessage $responseMessage */
                $responseMessage = $response;
                $decoded = json_decode((string) $responseMessage->payload, true);

                self::assertIsArray($decoded);
                $responses[$discoverySubject] = $decoded;
            }
        } finally {
            $servicePumpCancellation->cancel();
            $servicePump->await();
        }

        $ping = $responses['$SRV.PING.' . $serviceName] ?? null;
        self::assertIsArray($ping);
        self::assertSame('io.nats.micro.v1.ping_response', $ping['type'] ?? null);
        self::assertSame($serviceName, $ping['name'] ?? null);
        self::assertSame('1.2.3', $ping['version'] ?? null);
        self::assertNotSame('', (string) ($ping['id'] ?? ''));

        $info = $responses['$SRV.INFO.' . $serviceName] ?? null;
        self::assertIsArray($info);
        self::assertSame('io.nats.micro.v1.info_response', $info['type'] ?? null);
        self::assertSame('Discovery contract', $info['description'] ?? null);
        self::assertSame($serviceName, $info['name'] ?? null);
        self::assertIsArray($info['endpoints'] ?? null);

        $stats = $responses['$SRV.STATS.' . $serviceName] ?? null;
        self::assertIsArray($stats);
        self::assertSame('io.nats.micro.v1.stats_response', $stats['type'] ?? null);
        self::assertSame($serviceName, $stats['name'] ?? null);
        self::assertIsArray($stats['endpoints'] ?? null);
        self::assertCount(2, $stats['endpoints']);

        $schema = $responses['$SRV.SCHEMA.' . $serviceName] ?? null;
        self::assertIsArray($schema);
        self::assertSame('io.nats.micro.v1.schema_response', $schema['type'] ?? null);
        self::assertSame($serviceName, $schema['name'] ?? null);
        self::assertIsArray($schema['endpoints'] ?? null);
        self::assertCount(2, $schema['endpoints']);

        $schemaBySubject = [];
        foreach ($schema['endpoints'] as $endpoint) {
            if (!is_array($endpoint) || !isset($endpoint['subject']) || !is_string($endpoint['subject'])) {
                continue;
            }

            $schemaBySubject[$endpoint['subject']] = $endpoint;
        }

        self::assertArrayHasKey($subjectWithSchema, $schemaBySubject);
        self::assertArrayHasKey($subjectWithoutSchema, $schemaBySubject);
        self::assertIsArray($schemaBySubject[$subjectWithSchema]['schema'] ?? null);
        self::assertArrayNotHasKey('schema', $schemaBySubject[$subjectWithoutSchema]);

        $service->stop()->await();
        $requester->disconnect()->await();
        $serviceClient->disconnect()->await();
    }

    /**
     * Verifies service dispatches correctly across multiple registered endpoints.
     */
    public function testServiceMultipleEndpoints(): void
    {
        $this->requireIntegrationEnabled();

        $suffix = bin2hex(random_bytes(3));
        $subjectAlpha = 'svc.' . $suffix . '.alpha';
        $subjectBeta = 'svc.' . $suffix . '.beta';

        $serviceClient = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $requester = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));

        $serviceClient->connect()->await();
        $requester->connect()->await();

        $service = $serviceClient->service('multi-' . $suffix, '1.0.0', 'Multi endpoint')
            ->addEndpoint('alpha', $subjectAlpha, static fn (NatsMessage $message): string => 'alpha:' . $message->payload)
            ->addEndpoint('beta', $subjectBeta, static fn (NatsMessage $message): string => 'beta:' . $message->payload);
        $service->start()->await();

        $servicePumpCancellation = new DeferredCancellation();
        $servicePump = async(static function () use ($serviceClient, $servicePumpCancellation): void {
            $cancellation = $servicePumpCancellation->getCancellation();

            while (!$cancellation->isRequested()) {
                try {
                    $serviceClient->processIncoming()->await($cancellation);
                } catch (CancelledException) {
                    break;
                } catch (\Throwable) {
                    usleep(20_000);
                }
            }
        });

        try {
            $replyAlpha = null;
            $replyBeta = null;

            for ($attempt = 0; $attempt < 10; $attempt++) {
                try {
                    $replyAlpha = $requester->request($subjectAlpha, 'one', 2_000)->await();
                    $replyBeta = $requester->request($subjectBeta, 'two', 2_000)->await();
                    break;
                } catch (NatsException $e) {
                    if ($attempt === 9 || !str_contains($e->getMessage(), 'No responders')) {
                        throw $e;
                    }

                    delay(0.1);
                }
            }
        } finally {
            $servicePumpCancellation->cancel();
            $servicePump->await();
        }

        self::assertInstanceOf(NatsMessage::class, $replyAlpha);
        self::assertInstanceOf(NatsMessage::class, $replyBeta);
        self::assertSame('alpha:one', $replyAlpha->payload);
        self::assertSame('beta:two', $replyBeta->payload);

        $stats = $service->statsSnapshot();
        self::assertCount(2, $stats['endpoints']);

        $service->stop()->await();
        $requester->disconnect()->await();
        $serviceClient->disconnect()->await();
    }

    /**
     * Verifies grouped service endpoints use hierarchical subject prefixes.
     */
    public function testServiceGroupedEndpointsHierarchy(): void
    {
        $this->requireIntegrationEnabled();

        $suffix = bin2hex(random_bytes(3));
        $subjectV1 = 'svc.' . $suffix . '.v1.echo';
        $subjectV2 = 'svc.' . $suffix . '.v2.echo';

        $serviceClient = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $requester = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));

        $serviceClient->connect()->await();
        $requester->connect()->await();

        $service = $serviceClient->service('grouped-' . $suffix, '1.0.0', 'Grouped endpoints');
        $root = $service->addGroup('svc.' . $suffix);
        $root->addGroup('v1')->addEndpoint('echo-v1', 'echo', static fn (NatsMessage $message): string => 'v1:' . $message->payload);
        $root->addGroup('v2')->addEndpoint('echo-v2', 'echo', static fn (NatsMessage $message): string => 'v2:' . $message->payload);
        $service->start()->await();

        $servicePumpCancellation = new DeferredCancellation();
        $servicePump = async(static function () use ($serviceClient, $servicePumpCancellation): void {
            $cancellation = $servicePumpCancellation->getCancellation();

            while (!$cancellation->isRequested()) {
                try {
                    $serviceClient->processIncoming()->await($cancellation);
                } catch (CancelledException) {
                    break;
                } catch (\Throwable) {
                    usleep(20_000);
                }
            }
        });

        try {
            $replyV1 = null;
            $replyV2 = null;

            for ($attempt = 0; $attempt < 10; $attempt++) {
                try {
                    $replyV1 = $requester->request($subjectV1, 'hello', 2_000)->await();
                    $replyV2 = $requester->request($subjectV2, 'hello', 2_000)->await();
                    break;
                } catch (NatsException $e) {
                    if ($attempt === 9 || !str_contains($e->getMessage(), 'No responders')) {
                        throw $e;
                    }

                    delay(0.1);
                }
            }
        } finally {
            $servicePumpCancellation->cancel();
            $servicePump->await();
        }

        self::assertInstanceOf(NatsMessage::class, $replyV1);
        self::assertInstanceOf(NatsMessage::class, $replyV2);
        self::assertSame('v1:hello', $replyV1->payload);
        self::assertSame('v2:hello', $replyV2->payload);

        $subjects = array_map(
            static fn (array $endpoint): string => (string) ($endpoint['subject'] ?? ''),
            $service->statsSnapshot()['endpoints'] ?? [],
        );
        self::assertContains($subjectV1, $subjects);
        self::assertContains($subjectV2, $subjects);

        $service->stop()->await();
        $requester->disconnect()->await();
        $serviceClient->disconnect()->await();
    }

    /**
     * Verifies service handles multiple concurrent requests from different clients.
     */
    public function testServiceConcurrentRequests(): void
    {
        $this->requireIntegrationEnabled();

        $suffix = bin2hex(random_bytes(3));
        $subject = 'svc.' . $suffix . '.concurrent';
        $requestCount = 8;

        $serviceClient = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $serviceClient->connect()->await();

        $service = $serviceClient->service('conc-' . $suffix, '1.0.0', 'Concurrent service')
            ->addEndpoint('concurrent', $subject, static function (NatsMessage $message): string {
                usleep(25_000);

                return 'ok:' . $message->payload;
            });
        $service->start()->await();

        $servicePumpCancellation = new DeferredCancellation();
        $servicePump = async(static function () use ($serviceClient, $servicePumpCancellation): void {
            $cancellation = $servicePumpCancellation->getCancellation();

            while (!$cancellation->isRequested()) {
                try {
                    $serviceClient->processIncoming()->await($cancellation);
                } catch (CancelledException) {
                    break;
                } catch (\Throwable) {
                    usleep(20_000);
                }
            }
        });

        $requesters = [];
        for ($i = 0; $i < $requestCount; $i++) {
            $requester = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
            $requester->connect()->await();
            $requesters[] = $requester;
        }

        try {
            $futures = [];
            foreach ($requesters as $idx => $requester) {
                $futures[] = async(static function () use ($requester, $subject, $idx): string {
                    $reply = $requester->request($subject, (string) $idx, 2_000)->await();

                    return $reply->payload;
                });
            }

            $results = array_map(static fn ($future): string => $future->await(), $futures);
        } finally {
            foreach ($requesters as $requester) {
                $requester->disconnect()->await();
            }

            $servicePumpCancellation->cancel();
            $servicePump->await();
        }

        sort($results);
        $expected = [];
        for ($i = 0; $i < $requestCount; $i++) {
            $expected[] = 'ok:' . $i;
        }
        sort($expected);

        self::assertSame($expected, $results);

        $stats = $service->statsSnapshot();
        $endpoint = $stats['endpoints'][0] ?? [];
        self::assertSame($requestCount, $endpoint['num_requests'] ?? null);

        $service->stop()->await();
        $serviceClient->disconnect()->await();
    }

    /**
     * Verifies fragmented wire chunks are reassembled and dispatched to subscribers.
     */
    public function testFragmentedFramesStillDispatch(): void
    {
        $this->requireIntegrationEnabled();

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            "MSG updates 1 5\r\nhe",
            "llo\r\n",
        ]);

        $client = new NatsClient(new NatsOptions(), $transport);
        $client->connect()->await();

        $received = [];
        $client->subscribe('updates', static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        // First chunk is incomplete; second chunk completes message payload/frame.
        self::assertSame(0, $client->processIncoming()->await());
        self::assertSame(1, $client->processIncoming()->await());
        self::assertSame(['hello'], $received);

        $client->disconnect()->await();
    }

    /**
     * Verifies slow-consumer error policy surfaces queue overflow through client API.
     */
    public function testSlowConsumerPolicyBehavior(): void
    {
        $this->requireIntegrationEnabled();

        $transport = new FakeTransport([
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
            'PONG',
            "MSG updates 1 5\r\nfirst\r\nMSG updates 1 6\r\nsecond\r\n",
        ]);

        $options = new NatsOptions(
            maxPendingMessagesPerSubscription: 1,
            slowConsumerPolicy: SlowConsumerPolicy::Error,
        );

        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $client->subscribe('updates', static function (NatsMessage $message): void {
            // Intentionally no-op: overflow is driven by pending queue constraints.
        })->await();

        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('Subscription queue overflow');
        $client->processIncoming()->await();
    }

    /**
     * Verifies tlsHandshakeFirst workflow against a TLS-enabled integration endpoint.
     */
    public function testTlsHandshakeFirstConnection(): void
    {
        $this->requireIntegrationEnabled();

        $url = $this->integrationTlsServerUrl();
        $caFile = $this->integrationTlsCaFile();
        $certFile = $this->integrationTlsCertFile();
        $keyFile = $this->integrationTlsKeyFile();

        if ($caFile === null || $certFile === null || $keyFile === null) {
            $this->markTestSkipped('Set TLS env vars or generate local TLS fixtures to run the TLS handshake-first integration test.');
        }

        $client = new NatsClient(new NatsOptions(
            servers: [$url],
            tlsRequired: true,
            tlsHandshakeFirst: true,
            tlsCaFile: $caFile,
            tlsCertFile: $certFile,
            tlsKeyFile: $keyFile,
            tlsVerifyPeer: (getenv('NATS_TLS_SKIP_VERIFY') !== '1'),
        ));

        $client->connect()->await();
        self::assertNotNull($client->serverInfo());
        $client->disconnect()->await();
    }

    /**
     * Verifies the TLS fixture rejects clients that do not present the required certificate.
     */
    public function testTlsConnectionFailsWithoutClientCertificate(): void
    {
        $this->requireIntegrationEnabled();

        $url = $this->integrationTlsServerUrl();
        $caFile = $this->integrationTlsCaFile();

        if ($caFile === null) {
            $this->markTestSkipped('Set NATS_TLS_CA_FILE or generate local TLS fixtures to run the TLS client-certificate failure test.');
        }

        $client = new NatsClient(new NatsOptions(
            servers: [$url],
            tlsRequired: true,
            tlsHandshakeFirst: true,
            tlsCaFile: $caFile,
            tlsVerifyPeer: true,
            reconnectEnabled: false,
        ));

        $this->expectException(ConnectionException::class);
        $client->connect()->await();
    }

    /**
     * Verifies strict TLS peer validation fails when the client trusts the wrong CA.
     */
    public function testTlsConnectionFailsWithWrongCa(): void
    {
        $this->requireIntegrationEnabled();

        if (getenv('NATS_TLS_SKIP_VERIFY') === '1') {
            $this->markTestSkipped('Strict TLS verification failure test is disabled when NATS_TLS_SKIP_VERIFY=1.');
        }

        $url = $this->integrationTlsServerUrl();
        $certFile = $this->integrationTlsCertFile();
        $keyFile = $this->integrationTlsKeyFile();
        $wrongCaFile = $this->repoRoot() . '/build/tls/server-cert.pem';

        if ($certFile === null || $keyFile === null || !is_file($wrongCaFile)) {
            $this->markTestSkipped('Generate local TLS fixtures to run the TLS wrong-CA failure test.');
        }

        $client = new NatsClient(new NatsOptions(
            servers: [$url],
            tlsRequired: true,
            tlsHandshakeFirst: true,
            tlsCaFile: $wrongCaFile,
            tlsCertFile: $certFile,
            tlsKeyFile: $keyFile,
            tlsVerifyPeer: true,
            reconnectEnabled: false,
        ));

        $this->expectException(ConnectionException::class);
        $client->connect()->await();
    }

    /**
     * Verifies strict TLS hostname validation fails when the configured peer name does not match the server certificate.
     */
    public function testTlsConnectionFailsWithPeerNameMismatch(): void
    {
        $this->requireIntegrationEnabled();

        if (getenv('NATS_TLS_SKIP_VERIFY') === '1') {
            $this->markTestSkipped('Strict TLS verification failure test is disabled when NATS_TLS_SKIP_VERIFY=1.');
        }

        $url = $this->integrationTlsServerUrl();
        $caFile = $this->integrationTlsCaFile();
        $certFile = $this->integrationTlsCertFile();
        $keyFile = $this->integrationTlsKeyFile();

        if ($caFile === null || $certFile === null || $keyFile === null) {
            $this->markTestSkipped('Generate local TLS fixtures to run the TLS peer-name mismatch test.');
        }

        $client = new NatsClient(new NatsOptions(
            servers: [$url],
            tlsRequired: true,
            tlsHandshakeFirst: true,
            tlsCaFile: $caFile,
            tlsCertFile: $certFile,
            tlsKeyFile: $keyFile,
            tlsPeerName: 'mismatch.invalid.local',
            tlsVerifyPeer: true,
            reconnectEnabled: false,
        ));

        $this->expectException(ConnectionException::class);
        $client->connect()->await();
    }

    /**
     * Verifies token auth succeeds with valid token and fails with invalid token.
     */
    public function testTokenAuthSuccessAndFailure(): void
    {
        $this->requireIntegrationEnabled();

        $url = $this->integrationTokenServerUrl();
        $validToken = $this->integrationToken();
        $invalidToken = $this->integrationInvalidToken();

        $authorized = new NatsClient(new NatsOptions(
            servers: [$url],
            token: $validToken,
        ));
        $authorized->connect()->await();
        self::assertNotNull($authorized->serverInfo());
        $authorized->disconnect()->await();

        $unauthorized = new NatsClient(new NatsOptions(
            servers: [$url],
            token: $invalidToken,
            reconnectEnabled: false,
        ));

        $this->expectException(ConnectionException::class);
        $unauthorized->connect()->await();
    }

    /**
     * Verifies username/password auth succeeds with valid credentials and fails with invalid credentials.
     */
    public function testUserPasswordAuthSuccessAndFailure(): void
    {
        $this->requireIntegrationEnabled();

        $url = $this->integrationUserPassServerUrl();
        $username = $this->integrationUsername();
        $password = $this->integrationPassword();
        $badPassword = $this->integrationBadPassword();

        $authorized = new NatsClient(new NatsOptions(
            servers: [$url],
            username: $username,
            password: $password,
        ));
        $authorized->connect()->await();
        self::assertNotNull($authorized->serverInfo());
        $authorized->disconnect()->await();

        $unauthorized = new NatsClient(new NatsOptions(
            servers: [$url],
            username: $username,
            password: $badPassword,
            reconnectEnabled: false,
        ));

        $this->expectException(ConnectionException::class);
        $unauthorized->connect()->await();
    }

    /**
     * Verifies JWT auth succeeds when the server nonce is signed with the matching user seed.
     */
    public function testJwtNonceAuthenticationFlow(): void
    {
        $this->requireIntegrationEnabled();

        $url = $this->integrationJwtServerUrl();
        $jwt = $this->integrationJwt();
        $seed = $this->integrationJwtSeed();

        if ($jwt === null || $seed === null) {
            $this->markTestSkipped('Provide local build/nats/jwt fixture files or set NATS_JWT and NATS_JWT_NKEY_SEED for JWT auth integration test.');
        }

        $signer = new NkeySeedSigner($seed);
        $client = new NatsClient(new NatsOptions(
            servers: [$url],
            jwt: $jwt,
            nkey: $signer->publicKey(),
            nonceSigner: $signer,
        ));

        $client->connect()->await();
        self::assertNotNull($client->serverInfo());
        $client->disconnect()->await();
    }

    /**
     * Verifies standalone NKey auth succeeds when the server challenge is signed with the configured seed.
     */
    public function testStandaloneNkeyAuthenticationFlow(): void
    {
        $this->requireIntegrationEnabled();

        $url = $this->integrationNkeyServerUrl();
        $seed = $this->integrationNkeySeed();

        $signer = new NkeySeedSigner($seed);
        $client = new NatsClient(new NatsOptions(
            servers: [$url],
            nkey: $signer->publicKey(),
            nonceSigner: $signer,
        ));

        $client->connect()->await();
        self::assertNotNull($client->serverInfo());
        $client->disconnect()->await();
    }

    /**
     * Verifies request surfaces server no_responders as NatsException.
     */
    public function testNoRespondersErrorSurface(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.noresponders.' . bin2hex(random_bytes(4));
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        try {
            $client->request($subject, 'hello', 2_000)->await();
            self::fail('Expected no_responders error to be raised.');
        } catch (NatsException $e) {
            self::assertStringContainsString('No responders', $e->getMessage());
            self::assertStringContainsString($subject, $e->getMessage());
        }

        $client->disconnect()->await();
    }

    /**
     * Verifies reconnect after transport loss replays subscriptions and resumes delivery.
     */
    public function testReconnectAfterTransportLossReplaysSubscriptions(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'updates';
        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
                    'PONG',
                    '__THROW__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
                    'PONG',
                    "MSG {$subject} 1 5\r\nhello\r\n",
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $client = new NatsClient(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 3,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );
        $client->connect()->await();

        $received = [];
        $client->subscribe($subject, static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        // First read fails and triggers reconnect path.
        self::assertSame(0, $client->processIncoming()->await());
        // Next read processes delivery on reconnected transport.
        $client->processIncoming()->await();

        self::assertSame(['hello'], $received);
        self::assertCount(2, $transport->connectCalls);

        $subWrites = array_values(array_filter(
            $transport->writes,
            static fn (string $write): bool => str_starts_with($write, 'SUB ' . $subject . ' '),
        ));
        self::assertCount(2, $subWrites);

        $client->disconnect()->await();
    }

    /**
     * Verifies max outstanding pings triggers reconnect path and preserves open state.
     */
    public function testMaxPingsOutTriggersReconnect(): void
    {
        $this->requireIntegrationEnabled();

        $transport = new FlakyTransport(
            readQueuesByConnection: [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
                    'PONG',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
                    'PONG',
                ],
            ],
            connectFailures: 0,
            readFailures: 0,
        );

        $client = new NatsClient(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 1,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 1,
                maxPingsOut: 0,
            ),
            $transport,
        );
        $client->connect()->await();

        // Let ping timer fire once; with maxPingsOut=0 this must trigger reconnect.
        delay(1.1);

        self::assertCount(2, $transport->connectCalls);
        self::assertNotNull($client->serverInfo());
        self::assertSame('S2', $client->serverInfo()->serverId);

        $client->disconnect()->await();
    }

    /**
     * Verifies reconnect attempts exhausted path surfaces connection failure after transport loss.
     */
    public function testReconnectAttemptsExhaustedReturnsClosed(): void
    {
        $this->requireIntegrationEnabled();

        $transport = new class () implements \IDCT\NATS\Transport\TransportInterface {
            public int $connectAttempts = 0;

            /** @var list<string> */
            private array $readQueue = [
                'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
                'PONG',
            ];

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function (): void {
                    $this->connectAttempts++;
                    if ($this->connectAttempts > 1) {
                        throw new \RuntimeException('connect failed');
                    }
                });
            }

            public function write(string $bytes): Future
            {
                return async(static function (): void {
                });
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    if ($this->readQueue !== []) {
                        return array_shift($this->readQueue);
                    }

                    throw new \RuntimeException('read failed');
                });
            }

            public function close(): Future
            {
                return async(static function (): void {
                });
            }
        };

        $client = new NatsClient(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 2,
                reconnectDelayMs: 1,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );
        $client->connect()->await();

        $caught = false;
        try {
            $client->processIncoming()->await();
        } catch (ConnectionException $e) {
            $caught = true;
            self::assertStringContainsString('Reconnect attempts exhausted', $e->getMessage());
        }

        self::assertTrue($caught);

        try {
            $client->publish('updates', 'hello')->await();
            self::fail('Expected closed connection after reconnect exhaustion.');
        } catch (ConnectionException $e) {
            self::assertStringContainsString('Connection is not open', $e->getMessage());
        }
    }

    /**
     * Verifies reconnect backoff delay progression contributes measurable wait before recovery.
     */
    public function testReconnectBackoffDelayProgression(): void
    {
        $this->requireIntegrationEnabled();

        $transport = new class () implements \IDCT\NATS\Transport\TransportInterface {
            public int $connectAttempts = 0;

            /** @var array<int, list<string>> */
            private array $readQueues = [
                [
                    'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
                    'PONG',
                    '__THROW__',
                ],
                [
                    'INFO {"server_id":"S2","server_name":"n2","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}',
                    'PONG',
                ],
            ];

            private int $successfulConnects = 0;

            public function connect(string $dsn, int $timeoutMs): Future
            {
                return async(function (): void {
                    $this->connectAttempts++;

                    // Initial connect succeeds. Reconnect attempts 2 and 3 fail, then attempt 4 succeeds.
                    if ($this->connectAttempts === 2 || $this->connectAttempts === 3) {
                        throw new \RuntimeException('connect failed');
                    }

                    $this->successfulConnects++;
                });
            }

            public function write(string $bytes): Future
            {
                return async(static function (): void {
                });
            }

            public function readLine(?\Amp\Cancellation $cancellation = null): Future
            {
                return async(function (): string {
                    $index = max(0, $this->successfulConnects - 1);
                    if (!isset($this->readQueues[$index])) {
                        return '';
                    }

                    $next = array_shift($this->readQueues[$index]);
                    if ($next === null) {
                        return '';
                    }

                    if ($next === '__THROW__') {
                        throw new \RuntimeException('read failed');
                    }

                    return $next;
                });
            }

            public function close(): Future
            {
                return async(static function (): void {
                });
            }
        };

        $client = new NatsClient(
            new NatsOptions(
                reconnectEnabled: true,
                maxReconnectAttempts: 5,
                reconnectDelayMs: 20,
                reconnectJitterMs: 0,
                pingIntervalSeconds: 0,
            ),
            $transport,
        );
        $client->connect()->await();

        $start = microtime(true);
        self::assertSame(0, $client->processIncoming()->await());
        $elapsed = microtime(true) - $start;

        self::assertSame(4, $transport->connectAttempts);
        self::assertGreaterThanOrEqual(0.055, $elapsed);
        self::assertSame('S2', $client->serverInfo()?->serverId);

        $client->disconnect()->await();
    }

    /**
     * Verifies queue subscriptions distribute messages across group members without duplicates.
     */
    public function testQueueGroupDistributesMessages(): void
    {
        $this->requireIntegrationEnabled();

        $suffix = bin2hex(random_bytes(4));
        $subject = 'it.queue.' . $suffix;
        $group = 'q.' . $suffix;
        $messageCount = 40;

        $publisher = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $workerA = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $workerB = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));

        $publisher->connect()->await();
        $workerA->connect()->await();
        $workerB->connect()->await();

        $seenA = [];
        $seenB = [];
        $workerA->subscribe($subject, static function (NatsMessage $message) use (&$seenA): void {
            $seenA[] = $message->payload;
        }, $group)->await();
        $workerB->subscribe($subject, static function (NatsMessage $message) use (&$seenB): void {
            $seenB[] = $message->payload;
        }, $group)->await();

        for ($i = 0; $i < $messageCount; $i++) {
            $publisher->publish($subject, (string) $i)->await();
        }

        $deadline = microtime(true) + 5.0;
        while ((count($seenA) + count($seenB)) < $messageCount && microtime(true) < $deadline) {
            $workerA->processIncoming()->await();
            $workerB->processIncoming()->await();
            usleep(20_000);
        }

        $allSeen = array_merge($seenA, $seenB);
        sort($allSeen);
        $unique = array_values(array_unique($allSeen));

        self::assertCount($messageCount, $allSeen);
        self::assertCount($messageCount, $unique);
        self::assertGreaterThan(0, count($seenA));
        self::assertGreaterThan(0, count($seenB));

        $publisher->disconnect()->await();
        $workerA->disconnect()->await();
        $workerB->disconnect()->await();
    }

    /**
     * Verifies request timeout path when a responder receives requests but does not reply.
     */
    public function testRequestTimeoutReturnsTimeoutError(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.timeout.' . bin2hex(random_bytes(4));
        $server = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));

        $server->connect()->await();
        $client->connect()->await();

        $received = 0;
        $server->subscribe($subject, static function (NatsMessage $message) use (&$received): void {
            $received++;
            // Intentionally no reply to trigger requester timeout path.
        })->await();

        $serverPumpCancellation = new DeferredCancellation();
        $serverPump = async(static function () use ($server, $serverPumpCancellation): void {
            $cancellation = $serverPumpCancellation->getCancellation();
            while (!$cancellation->isRequested()) {
                try {
                    $server->processIncoming()->await($cancellation);
                } catch (CancelledException) {
                    break;
                } catch (\Throwable) {
                    usleep(20_000);
                }
            }
        });

        try {
            $client->request($subject, 'hello', 300)->await();
            self::fail('Expected request timeout exception.');
        } catch (TimeoutException $e) {
            self::assertStringContainsString('Request timed out', $e->getMessage());
            self::assertStringContainsString($subject, $e->getMessage());
        } finally {
            $serverPumpCancellation->cancel();
            $serverPump->await();
        }

        self::assertGreaterThanOrEqual(1, $received);

        $client->disconnect()->await();
        $server->disconnect()->await();
    }

    /**
     * Verifies drain flushes in-flight deliveries before closing the connection.
     */
    public function testDrainDuringInflightDelivery(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.drain.' . bin2hex(random_bytes(4));

        $subscriber = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $subscriber->connect()->await();

        $received = [];
        $subscriber->subscribe($subject, static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        // Publish from the same connection to avoid inter-client subscription propagation race.
        $subscriber->publish($subject, 'inflight-1')->await();

        // Drain should process in-flight messages and then close cleanly.
        $subscriber->drain()->await();

        self::assertSame(['inflight-1'], $received);

        try {
            $subscriber->publish($subject, 'after-drain')->await();
            self::fail('Expected closed connection after drain.');
        } catch (ConnectionException $e) {
            self::assertStringContainsString('Connection is not open', $e->getMessage());
        }
    }

    /**
     * Verifies publish rejects payloads that exceed server max_payload.
     */
    public function testOversizedPublishIsRejected(): void
    {
        $this->requireIntegrationEnabled();

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $serverInfo = $client->serverInfo();
        self::assertNotNull($serverInfo);
        $maxPayload = $serverInfo->maxPayload;
        self::assertGreaterThan(0, $maxPayload);

        $subject = 'it.maxpayload.' . bin2hex(random_bytes(4));
        $oversized = str_repeat('x', $maxPayload + 1);

        try {
            $client->publish($subject, $oversized)->await();
            self::fail('Expected oversized payload to be rejected.');
        } catch (ProtocolException $e) {
            self::assertStringContainsString('exceeds server max_payload', $e->getMessage());
        }

        $client->disconnect()->await();
    }

    /**
     * Verifies wildcard subscriptions only receive matching subjects.
     */
    public function testWildcardSubscriptionReceivesExpectedSubjects(): void
    {
        $this->requireIntegrationEnabled();

        $suffix = bin2hex(random_bytes(4));
        $pattern = 'it.wild.' . $suffix . '.*';
        $matchA = 'it.wild.' . $suffix . '.a';
        $matchB = 'it.wild.' . $suffix . '.b';
        $nonMatch = 'it.wild.' . $suffix . '.a.tail';

        $subscriber = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $publisher = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));

        $subscriber->connect()->await();
        $publisher->connect()->await();

        $subjects = [];
        $payloads = [];
        $subscriber->subscribe($pattern, static function (NatsMessage $message) use (&$subjects, &$payloads): void {
            $subjects[] = $message->subject;
            $payloads[] = $message->payload;
        })->await();

        $publisher->publish($matchA, 'a')->await();
        $publisher->publish($matchB, 'b')->await();
        $publisher->publish($nonMatch, 'x')->await();

        $deadline = microtime(true) + 3.0;
        while (count($subjects) < 2 && microtime(true) < $deadline) {
            $subscriber->processIncoming()->await();
            usleep(20_000);
        }

        sort($subjects);
        sort($payloads);

        self::assertSame([$matchA, $matchB], $subjects);
        self::assertSame(['a', 'b'], $payloads);

        $subscriber->disconnect()->await();
        $publisher->disconnect()->await();
    }

    /**
     * Verifies request cancellation aborts waiting before timeout.
     */
    public function testRequestCancellationStopsAwait(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.cancel.' . bin2hex(random_bytes(4));
        $server = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));

        $server->connect()->await();
        $client->connect()->await();

        $server->subscribe($subject, static function (NatsMessage $message): void {
            // Intentionally do not reply; requester should be cancelled first.
        })->await();

        $serverPumpCancellation = new DeferredCancellation();
        $serverPump = async(static function () use ($server, $serverPumpCancellation): void {
            $cancellation = $serverPumpCancellation->getCancellation();
            while (!$cancellation->isRequested()) {
                try {
                    $server->processIncoming()->await($cancellation);
                } catch (CancelledException) {
                    break;
                } catch (\Throwable) {
                    usleep(20_000);
                }
            }
        });

        $requestCancellation = new DeferredCancellation();
        async(static function () use ($requestCancellation): void {
            usleep(150_000);
            $requestCancellation->cancel();
        });

        $start = microtime(true);
        try {
            $client->request($subject, 'cancel-me', 2_500, $requestCancellation->getCancellation())->await();
            self::fail('Expected request cancellation.');
        } catch (CancelledException) {
            self::assertLessThan(2.0, microtime(true) - $start);
        } finally {
            $serverPumpCancellation->cancel();
            $serverPump->await();
        }

        $client->disconnect()->await();
        $server->disconnect()->await();
    }
}
