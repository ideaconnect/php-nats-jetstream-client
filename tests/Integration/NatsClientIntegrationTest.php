<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Integration;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\TimeoutCancellation;
use IDCT\NATS\Auth\NkeySeedSigner;
use IDCT\NATS\Connection\Enum\ConnectionState;
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
use IDCT\NATS\Tests\Support\SeveringTransport;
use IDCT\NATS\Transport\AmpSocketTransport;
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

        // processIncoming() blocks until data arrives or the cancellation fires, so this is
        // event-driven, not a poll: no sleep, and bounded so a lost message fails fast.
        $cancellation = new TimeoutCancellation(2.0);
        try {
            while ($received === null) {
                $client->processIncoming($cancellation)->await();
            }
        } catch (CancelledException) {
            // No message within the window; the assertion below reports it.
        }

        self::assertInstanceOf(NatsMessage::class, $received);
        /** @var NatsMessage $message */
        $message = $received;
        self::assertSame('hello', $message->payload);

        $client->disconnect()->await();
    }

    /**
     * Verifies auto-unsubscribe semantics against a live server: after `unsubscribe($sid, $max)` the
     * remaining deliveries up to $max total still reach the handler (previously they were silently
     * discarded), and nothing past the max arrives (#112).
     */
    public function testAutoUnsubscribeDeliversUpToMaxThenStops(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.autounsub.' . bin2hex(random_bytes(4));
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $received = [];
        $deliveries = 0;
        $sid = $client->subscribe($subject, static function (NatsMessage $message) use (&$received, &$deliveries): void {
            $received[] = $message->payload;
            $deliveries++;
        })->await();

        $client->publish($subject, 'm1')->await();

        $cancellation = new TimeoutCancellation(2.0);
        try {
            while ($deliveries < 1) {
                $client->processIncoming($cancellation)->await();
            }
        } catch (CancelledException) {
            // No message within the window; the assertion below reports it.
        }
        self::assertSame(['m1'], $received);

        // One message already delivered: allow two more (3 total), then the server stops sending.
        $client->unsubscribe($sid, 3)->await();

        $client->publish($subject, 'm2')->await();
        $client->publish($subject, 'm3')->await();
        $client->publish($subject, 'm4')->await();

        $cancellation = new TimeoutCancellation(2.0);
        try {
            while ($deliveries < 3) {
                $client->processIncoming($cancellation)->await();
            }
        } catch (CancelledException) {
            // Fewer than three within the window; the assertion below reports it.
        }
        self::assertSame(['m1', 'm2', 'm3'], $received);

        // Bounded settle window: m4 must never arrive - the server stopped at the max and the client
        // dropped the subscription after the third delivery.
        $settle = new TimeoutCancellation(0.3);
        try {
            while (!$settle->isRequested()) {
                $client->processIncoming($settle)->await();
            }
        } catch (CancelledException) {
            // Window elapsed with no further delivery.
        }
        self::assertSame(['m1', 'm2', 'm3'], $received);

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
        // Ensure the SUB is registered server-side before the requester publishes (avoid a 503 race).
        $server->flush()->await();

        // Pump the server continuously while the requester waits; a single read is not enough
        // because one socket read does not necessarily contain a whole frame. The read is
        // cancellation-bound, so stopping the pump needs no sleep and any real error still surfaces.
        $serverPumpCancel = new DeferredCancellation();
        $serverPump = async(static function () use ($server, $serverPumpCancel): void {
            $cancellation = $serverPumpCancel->getCancellation();
            try {
                while (!$cancellation->isRequested()) {
                    $server->processIncoming($cancellation)->await();
                }
            } catch (CancelledException) {
                // Stopped once the request completed.
            }
        });

        try {
            $reply = $client->request($subject, 'hello', 2000)->await();
        } finally {
            $serverPumpCancel->cancel();
            $serverPump->await();
        }

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

        $cancellation = new TimeoutCancellation(2.0);
        try {
            while ($received === null) {
                $client->processIncoming($cancellation)->await();
            }
        } catch (CancelledException) {
            // No message within the window; the assertion below reports it.
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
        // Ensure the SUB is registered server-side before the requester publishes (avoid a 503 race).
        $server->flush()->await();

        $serverPumpCancel = new DeferredCancellation();
        $serverPump = async(static function () use ($server, $serverPumpCancel): void {
            $cancellation = $serverPumpCancel->getCancellation();
            try {
                while (!$cancellation->isRequested()) {
                    $server->processIncoming($cancellation)->await();
                }
            } catch (CancelledException) {
                // Stopped once the request completed.
            }
        });

        try {
            $reply = $client->requestWithHeaders($subject, 'hello', ['X-Request-Id' => $requestId], 2000)->await();
        } finally {
            $serverPumpCancel->cancel();
            $serverPump->await();
        }

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

        $deadline = $this->monotonic() + 0.8;
        while ($this->monotonic() < $deadline) {
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
            ->addEndpoint('echo', 'svc.echo', static fn(NatsMessage $message): string => 'reply:' . $message->payload);
        $service->start()->await();

        $servicePumpCancellation = new DeferredCancellation();
        $servicePump = async(static function () use ($serviceClient, $servicePumpCancellation): void {
            $cancellation = $servicePumpCancellation->getCancellation();

            try {
                while (!$cancellation->isRequested()) {
                    $serviceClient->processIncoming($cancellation)->await();
                }
            } catch (CancelledException) {
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
            ->addEndpoint('echo', $subject, static fn(NatsMessage $message): string => 'reply:' . $message->payload, schema: [
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

            try {
                while (!$cancellation->isRequested()) {
                    $serviceClient->processIncoming($cancellation)->await();
                }
            } catch (CancelledException) {
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

        $eventNames = array_map(static fn(array $event): string => (string) $event['event'], $events);
        self::assertContains('request_start', $eventNames);
        self::assertContains('request_error', $eventNames);
        self::assertContains('request_end', $eventNames);

        $correlationIds = array_values(array_filter(array_map(
            static fn(array $event): ?string => is_string($event['correlation_id'] ?? null) ? $event['correlation_id'] : null,
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
            ->addEndpoint('schema-endpoint', $subjectWithSchema, static fn(NatsMessage $message): string => 'ok:' . $message->payload, schema: [
                'type' => 'object',
                'required' => ['id'],
                'properties' => [
                    'id' => ['type' => 'integer'],
                ],
            ])
            ->addEndpoint('plain-endpoint', $subjectWithoutSchema, static fn(NatsMessage $message): string => 'ok:' . $message->payload);
        $service->start()->await();

        $servicePumpCancellation = new DeferredCancellation();
        $servicePump = async(static function () use ($serviceClient, $servicePumpCancellation): void {
            $cancellation = $servicePumpCancellation->getCancellation();

            try {
                while (!$cancellation->isRequested()) {
                    $serviceClient->processIncoming($cancellation)->await();
                }
            } catch (CancelledException) {
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
            ->addEndpoint('alpha', $subjectAlpha, static fn(NatsMessage $message): string => 'alpha:' . $message->payload)
            ->addEndpoint('beta', $subjectBeta, static fn(NatsMessage $message): string => 'beta:' . $message->payload);
        $service->start()->await();

        $servicePumpCancellation = new DeferredCancellation();
        $servicePump = async(static function () use ($serviceClient, $servicePumpCancellation): void {
            $cancellation = $servicePumpCancellation->getCancellation();

            try {
                while (!$cancellation->isRequested()) {
                    $serviceClient->processIncoming($cancellation)->await();
                }
            } catch (CancelledException) {
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
        $root->addGroup('v1')->addEndpoint('echo-v1', 'echo', static fn(NatsMessage $message): string => 'v1:' . $message->payload);
        $root->addGroup('v2')->addEndpoint('echo-v2', 'echo', static fn(NatsMessage $message): string => 'v2:' . $message->payload);
        $service->start()->await();

        $servicePumpCancellation = new DeferredCancellation();
        $servicePump = async(static function () use ($serviceClient, $servicePumpCancellation): void {
            $cancellation = $servicePumpCancellation->getCancellation();

            try {
                while (!$cancellation->isRequested()) {
                    $serviceClient->processIncoming($cancellation)->await();
                }
            } catch (CancelledException) {
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
            static fn(array $endpoint): string => (string) ($endpoint['subject'] ?? ''),
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
                // Simulate work without blocking the event loop, so the in-flight requests overlap.
                delay(0.025);

                return 'ok:' . $message->payload;
            });
        $service->start()->await();

        $servicePumpCancellation = new DeferredCancellation();
        $servicePump = async(static function () use ($serviceClient, $servicePumpCancellation): void {
            $cancellation = $servicePumpCancellation->getCancellation();

            try {
                while (!$cancellation->isRequested()) {
                    $serviceClient->processIncoming($cancellation)->await();
                }
            } catch (CancelledException) {
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

            $results = array_map(static fn($future): string => $future->await(), $futures);
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
            'INFO {"server_id":"S1","server_name":"n1","version":"2.12.0","jetstream":true,"max_payload":1048576,"headers":true}' . "\r\n",
            "PONG\r\n",
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
     * Verifies the standard NATS TLS upgrade (read plaintext INFO, then upgrade) against a fixture
     * that is NOT configured handshake-first, using the default tlsHandshakeFirst=false.
     */
    public function testStandardTlsUpgradeConnection(): void
    {
        $this->requireIntegrationEnabled();

        $url = $this->integrationTlsUpgradeServerUrl();
        $caFile = $this->integrationTlsCaFile();
        $certFile = $this->integrationTlsCertFile();
        $keyFile = $this->integrationTlsKeyFile();

        if ($caFile === null || $certFile === null || $keyFile === null) {
            $this->markTestSkipped('Set TLS env vars or generate local TLS fixtures to run the standard TLS upgrade integration test.');
        }

        $client = new NatsClient(new NatsOptions(
            servers: [$url],
            tlsRequired: true,
            tlsHandshakeFirst: false,
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
     * Kills a LIVE TCP session mid-idle (client-side force close of the real socket via
     * SeveringTransport) and asserts the full client-level recovery composition against the real
     * server: the next read observes a genuine EOF through the production AmpSocketTransport path,
     * the reconnect dials the server pool through the same decorated transport, the subscription is
     * replayed with a real SUB, and a publish from a SECOND independent client reaches the
     * resubscribed handler (#141).
     */
    public function testSeveredLiveConnectionMidIdleReconnectsAndResumesDelivery(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.sever.idle.' . bin2hex(random_bytes(4));
        $options = new NatsOptions(
            servers: [$this->integrationServerUrl()],
            reconnectEnabled: true,
            maxReconnectAttempts: 5,
            reconnectDelayMs: 10,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
        );
        $transport = new SeveringTransport(new AmpSocketTransport($options));
        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $client->subscribe($subject, static function (NatsMessage $message) use (&$received): void {
            $received[] = $message->payload;
        })->await();

        // Force-close the live socket while the connection is idle: the next read sees a real EOF
        // from a genuinely dead TCP connection, not a scripted fake.
        $transport->sever();

        // Drive the read loop until the client has observed the EOF and completed recovery, bounded
        // by a monotonic deadline (no sleeps; recovery runs synchronously inside processIncoming).
        $deadline = $this->monotonic() + 10.0;
        while ($client->statistics()->reconnects < 1 && $this->monotonic() < $deadline) {
            try {
                $client->processIncoming(new TimeoutCancellation(0.5))->await();
            } catch (CancelledException) {
                // No bytes in this window; keep driving until the deadline.
            }
        }
        self::assertGreaterThanOrEqual(1, $client->statistics()->reconnects, 'severed socket must trigger a reconnect');

        // Round-trip a PING so the replayed SUB is registered server-side before the second client
        // publishes; otherwise the publish could race the resubscribe.
        $client->flush()->await();

        $publisher = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $publisher->connect()->await();
        $publisher->publish($subject, 'post-reconnect')->await();

        $cancellation = new TimeoutCancellation(5.0);
        try {
            while ($received === []) {
                $client->processIncoming($cancellation)->await();
            }
        } catch (CancelledException) {
            // Window elapsed; the assertion below reports it.
        }

        self::assertSame(['post-reconnect'], $received, 'the replayed subscription must deliver against the real server');

        $publisher->disconnect()->await();
        $client->disconnect()->await();
    }

    /**
     * Kills a LIVE TCP session mid-traffic: after one delivered message the real socket is severed
     * while a second client keeps publishing. Messages published between the sever and the SUB
     * replay are lost by design (core NATS does not resend), so the test asserts recovery
     * (reconnects >= 1) plus delivery of NEW post-reconnect traffic to the resubscribed handler,
     * bounded by a monotonic deadline (#141).
     */
    public function testSeveredLiveConnectionMidTrafficRecoversAndDeliversNewTraffic(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.sever.traffic.' . bin2hex(random_bytes(4));
        $options = new NatsOptions(
            servers: [$this->integrationServerUrl()],
            reconnectEnabled: true,
            maxReconnectAttempts: 5,
            reconnectDelayMs: 10,
            reconnectJitterMs: 0,
            pingIntervalSeconds: 0,
        );
        $transport = new SeveringTransport(new AmpSocketTransport($options));
        $client = new NatsClient($options, $transport);
        $client->connect()->await();

        $received = [];
        $deliveries = 0;
        $client->subscribe($subject, static function (NatsMessage $message) use (&$received, &$deliveries): void {
            $received[] = $message->payload;
            $deliveries++;
        })->await();
        // Ensure the SUB is registered server-side before the publisher starts.
        $client->flush()->await();

        $publisher = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $publisher->connect()->await();

        // Prove the pipeline is live before severing: one message publishes and arrives.
        $publisher->publish($subject, 'pre-sever')->await();
        $cancellation = new TimeoutCancellation(5.0);
        try {
            while ($deliveries < 1) {
                $client->processIncoming($cancellation)->await();
            }
        } catch (CancelledException) {
            // Window elapsed; the assertion below reports it.
        }
        self::assertSame(['pre-sever'], $received);

        // Kill the live TCP session while the second client's traffic continues.
        $transport->sever();

        $deadline = $this->monotonic() + 10.0;
        $sequence = 0;
        while ($deliveries < 2 && $this->monotonic() < $deadline) {
            $sequence++;
            $publisher->publish($subject, 'post-sever-' . $sequence)->await();
            try {
                $client->processIncoming(new TimeoutCancellation(0.25))->await();
            } catch (CancelledException) {
                // No delivery in this window; publish again and keep driving.
            }
        }

        self::assertGreaterThanOrEqual(1, $client->statistics()->reconnects, 'severed socket must trigger a reconnect');
        self::assertGreaterThanOrEqual(2, $deliveries, 'post-reconnect traffic must reach the resubscribed handler');
        self::assertSame('pre-sever', $received[0]);
        self::assertStringStartsWith('post-sever-', $received[count($received) - 1]);

        $publisher->disconnect()->await();
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
        $done = new DeferredFuture();
        $workerA->subscribe($subject, static function (NatsMessage $message) use (&$seenA, &$seenB, $messageCount, $done): void {
            $seenA[] = $message->payload;
            if (count($seenA) + count($seenB) >= $messageCount && !$done->isComplete()) {
                $done->complete();
            }
        }, $group)->await();
        $workerB->subscribe($subject, static function (NatsMessage $message) use (&$seenA, &$seenB, $messageCount, $done): void {
            $seenB[] = $message->payload;
            if (count($seenA) + count($seenB) >= $messageCount && !$done->isComplete()) {
                $done->complete();
            }
        }, $group)->await();

        // Round-trip each worker so its queue-group subscription is registered on the server before
        // publishing from a separate connection; otherwise the first messages could be missed.
        $flush = static function (NatsClient $client): void {
            try {
                $client->request('it.queue.flush.' . bin2hex(random_bytes(4)), '', 1_000)->await();
            } catch (\Throwable) {
                // A no-responder reply is expected; the round-trip itself flushes the subscription.
            }
        };
        $flush($workerA);
        $flush($workerB);

        // Drain both workers continuously; the completion future is resolved from the handlers above.
        $pumpCancellation = new DeferredCancellation();
        $pumpToken = $pumpCancellation->getCancellation();
        $pumpA = async(static function () use ($workerA, $pumpToken): void {
            try {
                while (!$pumpToken->isRequested()) {
                    $workerA->processIncoming($pumpToken)->await();
                }
            } catch (CancelledException) {
            }
        });
        $pumpB = async(static function () use ($workerB, $pumpToken): void {
            try {
                while (!$pumpToken->isRequested()) {
                    $workerB->processIncoming($pumpToken)->await();
                }
            } catch (CancelledException) {
            }
        });

        for ($i = 0; $i < $messageCount; $i++) {
            $publisher->publish($subject, (string) $i)->await();
        }

        try {
            $done->getFuture()->await(new TimeoutCancellation(5.0));
        } catch (CancelledException) {
            // Window elapsed; the assertions below report what was distributed.
        } finally {
            $pumpCancellation->cancel();
            $pumpA->await();
            $pumpB->await();
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
        // Ensure the SUB is registered server-side before the requester publishes, otherwise the
        // no-responders sentinel races the subscription and the request fails with 503 instead of
        // timing out.
        $server->flush()->await();

        $serverPumpCancellation = new DeferredCancellation();
        $serverPump = async(static function () use ($server, $serverPumpCancellation): void {
            $cancellation = $serverPumpCancellation->getCancellation();
            try {
                while (!$cancellation->isRequested()) {
                    $server->processIncoming($cancellation)->await();
                }
            } catch (CancelledException) {
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
     * #150 - a push-consumer-style handler that ACKs (publishes to an ack subject, exactly as a
     * JetStream ack or NatsMessage::respond() does) while the connection is draining. The in-flight
     * message is delivered during Draining, and the handler's ack must still reach the wire (rather
     * than throwing ConnectionException because the publish path refused a non-Open connection); the
     * subscriber then ends Closed. An observer connection confirms the ack left the client.
     */
    public function testDrainDeliversHandlerAckToTheWire(): void
    {
        $this->requireIntegrationEnabled();

        $suffix = bin2hex(random_bytes(4));
        $work = 'it.drainack.work.' . $suffix;
        $ackSubject = 'it.drainack.ack.' . $suffix;

        $observer = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $subscriber = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $observer->connect()->await();
        $subscriber->connect()->await();

        $acks = [];
        $observer->subscribe($ackSubject, static function (NatsMessage $message) use (&$acks): void {
            $acks[] = $message->payload;
        })->await();
        // Register the observer's SUB server-side before any ack is published.
        $observer->flush()->await();

        $handled = false;
        $ackError = null;
        $subscriber->subscribe($work, static function (NatsMessage $message) use (&$handled, &$ackError, $subscriber, $ackSubject): void {
            $handled = true;
            try {
                $subscriber->publish($ackSubject, 'ACK')->await();
            } catch (\Throwable $e) {
                // On the buggy path the publish throws ConnectionException (Draining is not Open).
                $ackError = $e;
            }
        })->await();
        $subscriber->flush()->await();

        // Publish the in-flight message from the same connection, then drain: it is delivered to the
        // handler during Draining, and the handler's ack fires mid-drain.
        $subscriber->publish($work, 'do-it')->await();

        // Pump the observer so it can receive the ack while the subscriber drains.
        $observerPumpCancel = new DeferredCancellation();
        $observerPump = async(static function () use ($observer, $observerPumpCancel): void {
            $cancellation = $observerPumpCancel->getCancellation();
            try {
                while (!$cancellation->isRequested()) {
                    $observer->processIncoming($cancellation)->await();
                }
            } catch (CancelledException) {
                // Stopped once the ack was observed.
            }
        });

        try {
            $subscriber->drain()->await();

            // Wait (bounded, monotonic) for the ack to land on the observer connection.
            $deadline = $this->monotonic() + 2.0;
            while ($acks === [] && $this->monotonic() < $deadline) {
                delay(0.01);
            }
        } finally {
            $observerPumpCancel->cancel();
            $observerPump->await();
        }

        self::assertTrue($handled, 'the in-flight message must be delivered to the handler during drain');
        self::assertNull($ackError, 'the handler ack must not throw while the connection is Draining');
        self::assertSame(['ACK'], $acks, 'the handler ack must reach the wire before drain closes the socket');
        self::assertSame(ConnectionState::Closed, $subscriber->state());

        $observer->disconnect()->await();
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

        // One connection: the SUB is ordered before these publishes and the server echoes them
        // back to this subscription, so there is no cross-connection registration race.
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        $subjects = [];
        $payloads = [];
        $client->subscribe($pattern, static function (NatsMessage $message) use (&$subjects, &$payloads): void {
            $subjects[] = $message->subject;
            $payloads[] = $message->payload;
        })->await();

        $client->publish($matchA, 'a')->await();
        $client->publish($matchB, 'b')->await();
        $client->publish($nonMatch, 'x')->await();

        $cancellation = new TimeoutCancellation(3.0);
        try {
            while (count($subjects) < 2) {
                $client->processIncoming($cancellation)->await();
            }
        } catch (CancelledException) {
            // Window elapsed; the assertions below report what was received.
        }

        sort($subjects);
        sort($payloads);

        self::assertSame([$matchA, $matchB], $subjects);
        self::assertSame(['a', 'b'], $payloads);

        $client->disconnect()->await();
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

        // Ensure the SUB is registered server-side before the requester publishes, otherwise the
        // server's no-responders sentinel races the subscription and the request fails with a 503
        // instead of being cancelled.
        $server->flush()->await();

        $serverPumpCancellation = new DeferredCancellation();
        $serverPump = async(static function () use ($server, $serverPumpCancellation): void {
            $cancellation = $serverPumpCancellation->getCancellation();
            try {
                while (!$cancellation->isRequested()) {
                    $server->processIncoming($cancellation)->await();
                }
            } catch (CancelledException) {
            }
        });

        $requestCancellation = new DeferredCancellation();
        async(static function () use ($requestCancellation): void {
            // Non-blocking wait so the request is genuinely in-flight before it is cancelled;
            // a blocking usleep here would freeze the event loop, not just this fiber.
            delay(0.15);
            $requestCancellation->cancel();
        });

        // Use a deliberately long timeout: the responder never replies, so the wait can only end two
        // ways - the external cancellation (CancelledException) or the deadline (TimeoutException).
        // request() throws CancelledException ONLY when the external token aborted the wait, so
        // catching it is itself proof that cancellation (not the timeout) stopped the await. We assert
        // on the EXCEPTION TYPE rather than wall-clock latency: an occasional event-loop/GC stall can
        // delay timer firing by seconds, which made a tight wall-clock bound flaky (#70) even though
        // cancellation behaved correctly. The generous timeout keeps the deadline from ever winning
        // under that jitter, while a genuinely broken cancellation would surface as a TimeoutException.
        $timeoutMs = 30_000;
        $start = $this->monotonic();
        try {
            $client->request($subject, 'cancel-me', $timeoutMs, $requestCancellation->getCancellation())->await();
            self::fail('Expected request cancellation.');
        } catch (CancelledException) {
            // Cancellation aborted the wait before the deadline - proven by the exception type.
            self::assertLessThan($timeoutMs / 1000, $this->monotonic() - $start, 'Cancellation must abort before the request deadline');
        } finally {
            $serverPumpCancellation->cancel();
            $serverPump->await();
        }

        $client->disconnect()->await();
        $server->disconnect()->await();
    }

    /**
     * Verifies two instances of the same service load-balance requests via the default queue group:
     * each request is handled by exactly one instance, so total handled equals the request count.
     */
    public function testServiceEndpointsLoadBalanceAcrossInstances(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'svc.lb.' . bin2hex(random_bytes(3));
        $requests = 20;

        $a = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $b = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $requester = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $a->connect()->await();
        $b->connect()->await();
        $requester->connect()->await();

        // Identical service definitions; default queue group ("q") should load-balance.
        $serviceA = $a->service('lbworker', '1.0.0')
            ->addEndpoint('work', $subject, static fn(NatsMessage $message): string => 'a');
        $serviceB = $b->service('lbworker', '1.0.0')
            ->addEndpoint('work', $subject, static fn(NatsMessage $message): string => 'b');
        $serviceA->start()->await();
        $serviceB->start()->await();

        $pumpCancel = new DeferredCancellation();
        $pump = static function (NatsClient $client) use ($pumpCancel): void {
            $cancellation = $pumpCancel->getCancellation();
            try {
                while (!$cancellation->isRequested()) {
                    $client->processIncoming($cancellation)->await();
                }
            } catch (CancelledException) {
                // Stopped once all requests completed.
            }
        };
        $pumpA = async(static fn() => $pump($a));
        $pumpB = async(static fn() => $pump($b));

        try {
            for ($i = 0; $i < $requests; $i++) {
                for ($attempt = 0; $attempt < 10; $attempt++) {
                    try {
                        $requester->request($subject, "r{$i}", 2_000)->await();
                        break;
                    } catch (NatsException $e) {
                        if ($attempt === 9 || !str_contains($e->getMessage(), 'No responders')) {
                            throw $e;
                        }
                        delay(0.1);
                    }
                }
            }
        } finally {
            $pumpCancel->cancel();
            $pumpA->await();
            $pumpB->await();
        }

        $handledA = (int) ($serviceA->statsSnapshot()['endpoints'][0]['num_requests'] ?? 0);
        $handledB = (int) ($serviceB->statsSnapshot()['endpoints'][0]['num_requests'] ?? 0);

        // Load is shared across both instances (not all to one), and each request is handled
        // about once - decisively NOT fan-out, which would deliver every request to both
        // instances for a total of 2x the request count.
        self::assertGreaterThan(0, $handledA, 'instance A should handle part of the load');
        self::assertGreaterThan(0, $handledB, 'instance B should handle part of the load');
        self::assertGreaterThanOrEqual($requests, $handledA + $handledB);
        self::assertLessThan($requests * 2, $handledA + $handledB, 'queue group load-balances, not fan-out');

        $serviceA->stop()->await();
        $serviceB->stop()->await();
        $a->disconnect()->await();
        $b->disconnect()->await();
        $requester->disconnect()->await();
    }

    /**
     * flush() completes a PING/PONG round-trip against the live server, confirming prior writes were
     * processed (#66 - flush had no dedicated functional test).
     */
    public function testFlushRoundTripConfirmsServerProcessing(): void
    {
        $this->requireIntegrationEnabled();

        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        try {
            $subject = 'it.flush.' . bin2hex(random_bytes(4));
            $client->subscribe($subject, static function (): void {})->await();
            $client->publish($subject, 'x')->await();

            // flush() drives its own read loop for the PONG; it must resolve without error.
            $client->flush()->await();
            self::assertSame(ConnectionState::Open, $client->state());
        } finally {
            $client->disconnect()->await();
        }
    }

    /**
     * The polling SubscriptionQueue API delivers live messages via next() with a timeout (#66 -
     * SubscriptionQueue had only unit/Behat coverage, no integration test).
     */
    public function testSubscriptionQueuePollingDeliversLive(): void
    {
        $this->requireIntegrationEnabled();

        $subject = 'it.pollq.' . bin2hex(random_bytes(4));
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client->connect()->await();

        try {
            $queue = $client->subscribeQueue($subject)->await();
            $client->flush()->await(); // ensure the SUB is registered before publishing

            $client->publish($subject, 'q1')->await();
            $client->publish($subject, 'q2')->await();

            $queue->setTimeout(3.0); // next() self-pumps processIncoming within this window
            $first = $queue->next();
            $second = $queue->next();

            self::assertSame('q1', $first?->payload);
            self::assertSame('q2', $second?->payload);
        } finally {
            $client->disconnect()->await();
        }
    }

    /**
     * An idle (publisher-only) connection stays open across multiple ping intervals without the app
     * pumping processIncoming, because the heartbeat self-read consumes the PONGs (#67 - P0-2 guard).
     */
    public function testIdleConnectionStaysOpenViaHeartbeat(): void
    {
        $this->requireIntegrationEnabled();

        // Reconnect stays at its default (enabled), matching a real publisher; asserting reconnects == 0
        // ensures the connection genuinely survived via the heartbeat self-read rather than being
        // silently torn down and reconnected (which would also leave state() == Open and hide the bug).
        $client = new NatsClient(new NatsOptions(
            servers: [$this->integrationServerUrl()],
            pingIntervalSeconds: 1,
            maxPingsOut: 2,
        ));
        $client->connect()->await();

        try {
            // Stay completely idle for longer than maxPingsOut * pingInterval. If the heartbeat did not
            // self-read PONGs, outstandingPings would exceed maxPingsOut and force a disconnect/reconnect.
            delay(3.5);

            self::assertSame(ConnectionState::Open, $client->state(), 'Idle connection must survive via heartbeat self-read');
            self::assertSame(0, $client->statistics()->reconnects, 'Heartbeat self-read must keep the connection alive without a reconnect');
            $client->flush()->await(); // still usable
        } finally {
            $client->disconnect()->await();
        }
    }

    /**
     * A request timeout cancels the in-flight read and does NOT poison the transport: a subsequent
     * request on the same connection succeeds (#67 - P0-1 guard).
     */
    public function testRequestTimeoutDoesNotPoisonConnection(): void
    {
        $this->requireIntegrationEnabled();

        $silent = 'it.silent.' . bin2hex(random_bytes(4));
        $echo = 'it.echo.' . bin2hex(random_bytes(4));
        $server = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $client = new NatsClient(new NatsOptions(servers: [$this->integrationServerUrl()]));
        $server->connect()->await();
        $client->connect()->await();

        try {
            // One responder receives but never replies (forces a timeout, not a 503 no-responders).
            $server->subscribe($silent, static function (): void {})->await();
            // A second responder echoes, to prove the connection is healthy after the timeout.
            $server->subscribe($echo, static function (NatsMessage $message): void {
                $message->respond('pong:' . $message->payload)->await();
            })->await();
            $server->flush()->await();

            $pump = new DeferredCancellation();
            $pumpFuture = async(static function () use ($server, $pump): void {
                $cancellation = $pump->getCancellation();
                try {
                    while (!$cancellation->isRequested()) {
                        $server->processIncoming($cancellation)->await();
                    }
                } catch (CancelledException) {
                }
            });

            try {
                try {
                    $client->request($silent, 'hello', 300)->await();
                    self::fail('Expected request timeout exception.');
                } catch (TimeoutException) {
                    // Expected - the read should have been cancelled, not orphaned.
                }

                // The connection must still be open and usable: the next request succeeds.
                self::assertSame(ConnectionState::Open, $client->state());
                $reply = $client->request($echo, 'after-timeout', 3000)->await();
                self::assertSame('pong:after-timeout', $reply->payload);
            } finally {
                $pump->cancel();
                $pumpFuture->await();
            }
        } finally {
            $client->disconnect()->await();
            $server->disconnect()->await();
        }
    }
}
