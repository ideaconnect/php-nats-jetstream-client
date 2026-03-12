<?php

declare(strict_types=1);

namespace IDCT\NATS\Tests\Integration;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\Future;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\NatsException;
use IDCT\NATS\Services\BasicJsonSchemaValidator;
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
        self::assertTrue(true);
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
}
