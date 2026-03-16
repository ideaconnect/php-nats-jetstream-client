<?php

declare(strict_types=1);

namespace IDCT\NATS\Services;

use Amp\Future;
use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\TimeoutCancellation;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsHeaders;
use IDCT\NATS\Core\NatsMessage;
use function Amp\async;
use function Amp\delay;

/**
 * NATS microservice runtime implementing discovery and endpoint handling.
 */
final class Service
{
    /** @var array<int, int> */
    private array $subscriptionSids = [];

    /** @var array<string, ServiceEndpoint> */
    private array $endpoints = [];

    /** @var array<string, callable(NatsMessage):(string|array<string,mixed>|null)> */
    private array $handlers = [];

    /** @var list<callable(string,ServiceEndpoint,NatsMessage,array<string,mixed>):void> */
    private array $observers = [];

    /** @var null|callable(NatsMessage,array<string,mixed>):(null|string) */
    private $requestValidator = null;

    private readonly string $id;
    private readonly string $startedAt;

    /**
     * Creates a service runtime bound to a NATS client.
     *
     * @param NatsClient $client Connected NATS client used for endpoint subscriptions and replies.
     * @param string $name Logical service name exposed in `$SRV.*` discovery responses.
     * @param string $version Service semantic version exposed by discovery and stats endpoints.
     * @param ?string $description Optional human-readable description returned by INFO responses.
     * @param array<string,string> $metadata
     */
    public function __construct(
        private readonly NatsClient $client,
        private readonly string $name,
        private readonly string $version,
        private readonly ?string $description = null,
        private readonly array $metadata = [],
    ) {
        $this->id = bin2hex(random_bytes(8));
        $this->startedAt = gmdate('Y-m-d\TH:i:s\Z');
    }

    /**
     * Registers a request handler endpoint.
     *
     * @param callable(NatsMessage):(string|array<string,mixed>|null)|ServiceEndpointHandlerInterface|class-string<ServiceEndpointHandlerInterface>|object $handler
     * @param array<string,mixed>|null $schema Optional JSON Schema for the endpoint.
     */
    public function addEndpoint(string $name, string $subject, callable|object|string $handler, ?string $queueGroup = null, ?array $schema = null): self
    {
        $endpoint = new ServiceEndpoint($name, $subject, $queueGroup, $schema);
        $this->endpoints[$subject] = $endpoint;
        $this->handlers[$subject] = $this->resolveHandler($handler);

        return $this;
    }

    /**
     * Adds a request lifecycle observer callback.
     *
     * @param callable(string,ServiceEndpoint,NatsMessage,array<string,mixed>):void $observer
     */
    public function addObserver(callable $observer): self
    {
        $this->observers[] = $observer;

        return $this;
    }

    /**
     * Enables opt-in request validation for endpoints with a declared schema.
     *
     * @param callable(NatsMessage,array<string,mixed>):(null|string) $validator
     */
    public function withRequestValidator(callable $validator): self
    {
        $this->requestValidator = $validator;

        return $this;
    }

    /**
     * Enables built-in schema validation adapter support.
     */
    public function withSchemaValidator(ServiceSchemaValidatorInterface $validator): self
    {
        return $this->withRequestValidator(
            static fn (NatsMessage $message, array $schema): ?string => $validator->validate($message, $schema),
        );
    }

    /**
     * Creates grouped endpoint builder with subject prefix.
     */
    public function addGroup(string $name): ServiceGroup
    {
        return new ServiceGroup($this, $name);
    }

    /**
     * Starts discovery and endpoint subscriptions.
     *
     * @return Future<void>
     */
    public function start(): Future
    {
        return async(function (): void {
            if ($this->subscriptionSids !== []) {
                return;
            }

            $this->subscribeDiscovery()->await();

            foreach ($this->endpoints as $subject => $endpoint) {
                $sid = $this->client->subscribe(
                    $subject,
                    function (NatsMessage $message) use ($subject, $endpoint): void {
                        $endpoint->requests++;
                        $started = hrtime(true);
                        $context = $this->buildObserverContext($message, $subject);

                        $this->notifyObservers('request_start', $endpoint, $message, $context);

                        if ($endpoint->schema !== null && $this->requestValidator !== null) {
                            $validationError = ($this->requestValidator)($message, $endpoint->schema);
                            if ($validationError !== null) {
                                $endpoint->errors++;
                                $endpoint->lastError = $validationError;
                                $endpoint->processingTimeNs += (int) max(0, hrtime(true) - $started);

                                $this->notifyObservers('request_error', $endpoint, $message, $context + [
                                    'code' => 'VALIDATION_ERROR',
                                    'error' => $validationError,
                                ]);

                                $this->publishResponse($message->replyTo, $this->errorPayload(
                                    code: 'VALIDATION_ERROR',
                                    message: $validationError,
                                    correlationId: is_string($context['correlation_id'] ?? null)
                                        ? $context['correlation_id']
                                        : null,
                                ));

                                return;
                            }
                        }

                        try {
                            $response = ($this->handlers[$subject])($message);
                        } catch (\Throwable $e) {
                            $endpoint->errors++;
                            $endpoint->lastError = $e->getMessage();
                            $this->notifyObservers('request_error', $endpoint, $message, $context + [
                                'code' => 'HANDLER_ERROR',
                                'error' => $e->getMessage(),
                            ]);

                            $response = $this->errorPayload(
                                code: 'HANDLER_ERROR',
                                message: $e->getMessage(),
                                correlationId: is_string($context['correlation_id'] ?? null)
                                    ? $context['correlation_id']
                                    : null,
                            );
                        } finally {
                            $duration = (int) max(0, hrtime(true) - $started);
                            $endpoint->processingTimeNs += $duration;

                            $this->notifyObservers('request_end', $endpoint, $message, $context + [
                                'duration_ns' => $duration,
                            ]);
                        }

                        if ($response === null) {
                            return;
                        }

                        $this->publishResponse($message->replyTo, $response);
                    },
                    $endpoint->queueGroup,
                )->await();

                $this->subscriptionSids[] = $sid;
            }
        });
    }

    /**
     * Stops service subscriptions.
     *
     * @return Future<void>
     */
    public function stop(): Future
    {
        return async(function (): void {
            foreach ($this->subscriptionSids as $sid) {
                $this->client->unsubscribe($sid)->await();
            }

            $this->subscriptionSids = [];
        });
    }

    /**
     * Starts the service and continuously processes incoming messages.
     *
     * The loop exits when timeout/cancellation is requested, then the service
     * is unsubscribed automatically.
     *
     * @param float|null $timeoutSeconds Optional run timeout in seconds.
     * @param Cancellation|null $cancellation Optional external cancellation token.
     * @return Future<void>
     */
    public function run(?float $timeoutSeconds = null, ?Cancellation $cancellation = null): Future
    {
        return async(function () use ($timeoutSeconds, $cancellation): void {
            $this->start()->await();

            $effectiveCancellation = $this->buildRunCancellation($timeoutSeconds, $cancellation);

            try {
                while (true) {
                    if ($effectiveCancellation?->isRequested() ?? false) {
                        break;
                    }

                    try {
                        $processed = $this->client->processIncoming()->await($effectiveCancellation);
                        if ($processed === 0) {
                            // Yield briefly to avoid a tight loop when transport is idle.
                            delay(0.01, cancellation: $effectiveCancellation);
                        }
                    } catch (CancelledException) {
                        break;
                    } catch (\Throwable) {
                        delay(0.02);
                    }
                }
            } finally {
                $this->stop()->await();
            }
        });
    }

    /**
     * Resets endpoint runtime statistics counters.
     */
    public function reset(): void
    {
        foreach ($this->endpoints as $endpoint) {
            $endpoint->requests = 0;
            $endpoint->errors = 0;
            $endpoint->lastError = null;
            $endpoint->processingTimeNs = 0;
        }
    }

    /**
     * Returns current service statistics payload.
     *
     * @return array<string,mixed>
     */
    public function statsSnapshot(): array
    {
        $endpointStats = [];
        foreach ($this->endpoints as $endpoint) {
            $averageProcessingTime = $endpoint->requests > 0
                ? intdiv($endpoint->processingTimeNs, $endpoint->requests)
                : 0;

            $endpointStats[] = [
                'name' => $endpoint->name,
                'subject' => $endpoint->subject,
                'queue_group' => $endpoint->queueGroup,
                'requests' => $endpoint->requests,
                'errors' => $endpoint->errors,
                'num_requests' => $endpoint->requests,
                'num_errors' => $endpoint->errors,
                'last_error' => $endpoint->lastError,
                'processing_time' => $endpoint->processingTimeNs,
                'average_processing_time' => $averageProcessingTime,
            ];
        }

        return [
            'type' => 'io.nats.micro.v1.stats_response',
            'name' => $this->name,
            'id' => $this->id,
            'version' => $this->version,
            'started' => $this->startedAt,
            'endpoints' => $endpointStats,
            'metadata' => $this->metadata,
        ];
    }

    /**
     * @return Future<void>
     */
    private function subscribeDiscovery(): Future
    {
        return async(function (): void {
            $subjects = [
                '$SRV.PING',
                '$SRV.PING.' . $this->name,
                '$SRV.PING.' . $this->name . '.' . $this->id,
                '$SRV.INFO',
                '$SRV.INFO.' . $this->name,
                '$SRV.INFO.' . $this->name . '.' . $this->id,
                '$SRV.STATS',
                '$SRV.STATS.' . $this->name,
                '$SRV.STATS.' . $this->name . '.' . $this->id,
                '$SRV.SCHEMA',
                '$SRV.SCHEMA.' . $this->name,
                '$SRV.SCHEMA.' . $this->name . '.' . $this->id,
            ];

            foreach ($subjects as $subject) {
                $sid = $this->client->subscribe($subject, function (NatsMessage $message) use ($subject): void {
                    if ($message->replyTo === null || $message->replyTo === '') {
                        return;
                    }

                    $payload = $this->discoveryPayloadForSubject($subject);
                    $this->client->publish($message->replyTo, json_encode($payload, JSON_THROW_ON_ERROR))->await();
                })->await();

                $this->subscriptionSids[] = $sid;
            }
        });
    }

    /**
     * @param string|array<string,mixed> $response
     */
    private function publishResponse(?string $replyTo, string|array $response): void
    {
        if ($replyTo === null || $replyTo === '') {
            return;
        }

        if (is_array($response)) {
            $this->client->publish($replyTo, json_encode($response, JSON_THROW_ON_ERROR))->await();

            return;
        }

        $this->client->publish($replyTo, $response)->await();
    }

    /**
     * @return array<string,mixed>
     */
    private function errorPayload(string $code, string $message, ?string $correlationId): array
    {
        $payload = [
            'type' => 'io.nats.micro.v1.error',
            'code' => $code,
            'message' => $message,
            'error' => $message,
        ];

        if ($correlationId !== null && $correlationId !== '') {
            $payload['correlation_id'] = $correlationId;
        }

        return $payload;
    }

    /**
     * @return array<string,mixed>
     */
    private function buildObserverContext(NatsMessage $message, string $subject): array
    {
        $headers = NatsHeaders::fromWireBlock($message->rawHeaders);
        $normalizedHeaders = [];

        foreach ($headers as $name => $value) {
            $normalizedHeaders[strtolower($name)] = $value;
        }

        return [
            'subject' => $subject,
            'reply_to' => $message->replyTo,
            'correlation_id' => $normalizedHeaders['x-request-id']
                ?? $normalizedHeaders['traceparent']
                ?? $normalizedHeaders['nats-msg-id']
                ?? null,
        ];
    }

    /**
     * @param array<string,mixed> $context
     */
    private function notifyObservers(string $event, ServiceEndpoint $endpoint, NatsMessage $message, array $context): void
    {
        foreach ($this->observers as $observer) {
            try {
                $observer($event, $endpoint, $message, $context);
            } catch (\Throwable) {
                // Observer failures must not impact service request handling.
            }
        }
    }

    /**
     * @return array<string,mixed>
     */
    private function discoveryPayloadForSubject(string $subject): array
    {
        $base = [
            'name' => $this->name,
            'id' => $this->id,
            'version' => $this->version,
            'metadata' => $this->metadata,
        ];

        if (str_starts_with($subject, '$SRV.SCHEMA')) {
            $endpoints = [];
            foreach ($this->endpoints as $endpoint) {
                $entry = [
                    'name' => $endpoint->name,
                    'subject' => $endpoint->subject,
                ];
                if ($endpoint->schema !== null) {
                    $entry['schema'] = $endpoint->schema;
                }
                $endpoints[] = $entry;
            }

            return [
                'type' => 'io.nats.micro.v1.schema_response',
                'endpoints' => $endpoints,
            ] + $base;
        }

        if (str_starts_with($subject, '$SRV.STATS')) {
            return $this->statsSnapshot();
        }

        if (str_starts_with($subject, '$SRV.INFO')) {
            $endpoints = [];
            foreach ($this->endpoints as $endpoint) {
                $endpoints[] = [
                    'name' => $endpoint->name,
                    'subject' => $endpoint->subject,
                    'queue_group' => $endpoint->queueGroup,
                ];
            }

            return [
                'type' => 'io.nats.micro.v1.info_response',
                'description' => $this->description,
                'endpoints' => $endpoints,
            ] + $base;
        }

        return [
            'type' => 'io.nats.micro.v1.ping_response',
        ] + $base;
    }

    private function buildRunCancellation(?float $timeoutSeconds, ?Cancellation $cancellation): ?Cancellation
    {
        if ($timeoutSeconds !== null && $timeoutSeconds <= 0) {
            throw new \InvalidArgumentException('Service run timeout must be greater than zero seconds.');
        }

        if ($timeoutSeconds === null) {
            return $cancellation;
        }

        $timeoutCancellation = new TimeoutCancellation($timeoutSeconds);
        if ($cancellation === null) {
            return $timeoutCancellation;
        }

        return new CompositeCancellation($timeoutCancellation, $cancellation);
    }

    /**
     * @param callable(NatsMessage):(string|array<string,mixed>|null)|ServiceEndpointHandlerInterface|class-string<ServiceEndpointHandlerInterface>|object $handler
     * @return callable(NatsMessage):(string|array<string,mixed>|null)
     */
    private function resolveHandler(callable|object|string $handler): callable
    {
        if (is_string($handler) && class_exists($handler)) {
            $instance = new $handler();
            if (!$instance instanceof ServiceEndpointHandlerInterface) {
                throw new \InvalidArgumentException(sprintf(
                    'Service endpoint class handler %s must implement %s.',
                    $handler,
                    ServiceEndpointHandlerInterface::class,
                ));
            }

            return static fn (NatsMessage $message): string|array|null => $instance->handle($message);
        }

        if ($handler instanceof ServiceEndpointHandlerInterface) {
            return static fn (NatsMessage $message): string|array|null => $handler->handle($message);
        }

        if (is_callable($handler)) {
            return $handler;
        }

        throw new \InvalidArgumentException(sprintf(
            'Unsupported service endpoint handler of type %s. Expected callable, %s instance, or class-string implementing %s.',
            get_debug_type($handler),
            ServiceEndpointHandlerInterface::class,
            ServiceEndpointHandlerInterface::class,
        ));
    }
}
