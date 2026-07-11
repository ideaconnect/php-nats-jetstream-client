<?php

declare(strict_types=1);

namespace IDCT\NATS\Services;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
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
    /** Default endpoint queue group per the NATS micro spec; instances load-balance requests. */
    public const DEFAULT_QUEUE_GROUP = 'q';

    /** @var array<int, int> */
    private array $subscriptionSids = [];

    /** Whether start() has fully completed; distinct from the SID list so a partial failure is not mistaken for "running". */
    private bool $started = false;

    /** @var array<string, ServiceEndpoint> */
    private array $endpoints = [];

    /** @var array<string, callable(NatsMessage):(string|array<string,mixed>|null)> */
    private array $handlers = [];

    /** @var list<callable(string,ServiceEndpoint,NatsMessage,array<string,mixed>):void> */
    private array $observers = [];

    /** @var null|callable(NatsMessage,array<string,mixed>):(null|string) */
    private $requestValidator = null;

    /** @var null|callable(Service):void */
    private $doneHandler = null;

    /** Whether the done handler has already fired, so stop()+drain() do not double-invoke it. */
    private bool $doneFired = false;

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
        // The name is interpolated into discovery subjects ($SRV.PING.<name>, $SRV.INFO.<name>, ...),
        // so a dot/space/wildcard would crash start() mid-loop or over-subscribe to unrelated
        // services. Validate it (and require a semantic version, per the NATS micro spec) up front.
        if (preg_match('/^[A-Za-z0-9_-]+$/', $name) !== 1) {
            throw new \InvalidArgumentException('Service name must be non-empty and match ^[A-Za-z0-9_-]+$');
        }

        if (preg_match('/^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/', $version) !== 1) {
            throw new \InvalidArgumentException('Service version must be a semantic version, e.g. "1.2.3"');
        }

        $this->id = bin2hex(random_bytes(8));
        // RFC3339 with microseconds, matching the Go/JS micro clients' sub-second precision. The
        // hardcoded Z suffix requires the timestamp itself to be UTC regardless of the process
        // default timezone (ADR-32, #132).
        $this->startedAt = (new \DateTimeImmutable('now', new \DateTimeZone('UTC')))->format('Y-m-d\TH:i:s.u\Z');
    }

    /**
     * Registers a request handler endpoint.
     *
     * @param callable(NatsMessage):(string|array<string,mixed>|null)|ServiceEndpointHandlerInterface|class-string<ServiceEndpointHandlerInterface>|object $handler
     * @param array<string,mixed>|null $schema Optional JSON Schema for the endpoint.
     * @param array<string,string> $metadata Optional per-endpoint metadata advertised in $SRV.INFO.
     * @param (callable(ServiceEndpoint):array<string,mixed>)|null $statsHandler Optional supplier of
     *        extra per-endpoint data merged into $SRV.STATS (nats.go StatsHandler).
     */
    public function addEndpoint(string $name, string $subject, callable|object|string $handler, ?string $queueGroup = self::DEFAULT_QUEUE_GROUP, ?array $schema = null, array $metadata = [], ?callable $statsHandler = null): self
    {
        // Per the NATS micro spec endpoints share a queue group ("q" by default) so multiple
        // service instances load-balance requests. Pass null or '' to opt out (fan-out: every
        // instance receives every request).
        if (trim($name) === '') {
            throw new \InvalidArgumentException('Service endpoint name must not be empty');
        }

        if (trim($subject) === '') {
            throw new \InvalidArgumentException('Service endpoint subject must not be empty');
        }

        if (isset($this->endpoints[$subject])) {
            // Two endpoints resolving to the same subject would silently overwrite the first (and
            // its handler), and under-report it in INFO/SCHEMA/STATS. Fail fast at registration.
            throw new \InvalidArgumentException(sprintf('An endpoint is already registered for subject "%s"', $subject));
        }

        $resolvedQueueGroup = ($queueGroup === null || $queueGroup === '') ? null : $queueGroup;
        $endpoint = new ServiceEndpoint(
            $name,
            $subject,
            $resolvedQueueGroup,
            $schema,
            $metadata,
            statsHandler: $statsHandler !== null ? \Closure::fromCallable($statsHandler) : null,
        );
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
     * Registers a callback invoked once when the service stops (via {@see stop()}, {@see drain()}, or
     * {@see run()} exiting). Mirrors nats.go micro `DoneHandler` (#57).
     *
     * @param callable(Service):void $handler
     */
    public function onDone(callable $handler): self
    {
        $this->doneHandler = $handler;

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
            static fn(NatsMessage $message, array $schema): ?string => $validator->validate($message, $schema),
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
            if ($this->started) {
                return;
            }

            // Arm the done handler for this run (it fires once on the next stop/drain).
            $this->doneFired = false;

            // Collect new SIDs locally and only commit them on full success, so a subscribe failing
            // mid-loop does not leave the service half-initialized (with the idempotency guard then
            // masking a retry as a no-op). On failure, roll back and rethrow so start() can be retried.
            $sids = [];

            try {
                $this->subscribeDiscovery($sids)->await();

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
                                    $duration = (int) max(0, hrtime(true) - $started);
                                    $endpoint->errors++;
                                    $endpoint->lastError = $validationError;
                                    $endpoint->processingTimeNs += $duration;

                                    $this->notifyObservers('request_error', $endpoint, $message, $context + [
                                        'code' => 'VALIDATION_ERROR',
                                        'error' => $validationError,
                                    ]);

                                    $this->publishResponse(
                                        $message->replyTo,
                                        $this->errorPayload(
                                            code: 'VALIDATION_ERROR',
                                            message: $validationError,
                                            correlationId: is_string($context['correlation_id'] ?? null)
                                                ? $context['correlation_id']
                                                : null,
                                        ),
                                        $this->serviceErrorHeaders('400', $validationError),
                                    );

                                    // Emit the terminal request_end on the rejection path too, so an
                                    // observer that opened a span/timer/gauge on request_start does not
                                    // leak it for schema-rejected (often hostile) traffic.
                                    $this->notifyObservers('request_end', $endpoint, $message, $context + [
                                        'duration_ns' => $duration,
                                    ]);

                                    return;
                                }
                            }

                            $errorHeaders = null;

                            try {
                                $response = ($this->handlers[$subject])($message);
                            } catch (ServiceError $serviceError) {
                                // The handler explicitly chose to fail with a custom code/description
                                // (and optional body). Honor it verbatim - this is a deliberate error
                                // reply, not an internal fault, so the chosen detail IS sent to the caller.
                                $endpoint->errors++;
                                $endpoint->lastError = $serviceError->description;
                                $this->notifyObservers('request_error', $endpoint, $message, $context + [
                                    'code' => $serviceError->serviceErrorCode,
                                    'error' => $serviceError->description,
                                ]);

                                $response = $serviceError->body ?? $this->errorPayload(
                                    code: $serviceError->serviceErrorCode,
                                    message: $serviceError->description,
                                    correlationId: is_string($context['correlation_id'] ?? null)
                                        ? $context['correlation_id']
                                        : null,
                                );
                                $errorHeaders = $this->serviceErrorHeaders(
                                    $serviceError->serviceErrorCode,
                                    $serviceError->description,
                                );
                            } catch (\Throwable $e) {
                                $endpoint->errors++;
                                $endpoint->lastError = $e->getMessage();
                                $this->notifyObservers('request_error', $endpoint, $message, $context + [
                                    'code' => 'HANDLER_ERROR',
                                    'error' => $e->getMessage(),
                                ]);

                                // Do not leak the raw exception text to the (possibly untrusted)
                                // requester: it can disclose internal paths, queries, or secrets. The
                                // requester gets a generic message under the HANDLER_ERROR code; the
                                // real detail stays server-side in lastError and the request_error event.
                                $response = $this->errorPayload(
                                    code: 'HANDLER_ERROR',
                                    message: 'Internal server error',
                                    correlationId: is_string($context['correlation_id'] ?? null)
                                        ? $context['correlation_id']
                                        : null,
                                );
                                $errorHeaders = $this->serviceErrorHeaders('500', 'Internal server error');
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

                            try {
                                $this->publishResponse($message->replyTo, $response, $errorHeaders);
                            } catch (\JsonException $e) {
                                // The response cannot be JSON-encoded (e.g. a handler returned binary /
                                // non-UTF-8 data, or NAN/INF). A JsonException here would otherwise escape
                                // the shared dispatch loop and abort delivery for every subscription on the
                                // connection (#97). Treat it like an internal fault: record it and reply
                                // with a controlled 500, mirroring the handler-exception path. The error
                                // payload is plain strings, so it always encodes.
                                $endpoint->errors++;
                                $endpoint->lastError = $e->getMessage();
                                $this->notifyObservers('request_error', $endpoint, $message, $context + [
                                    'code' => 'HANDLER_ERROR',
                                    'error' => $e->getMessage(),
                                ]);

                                try {
                                    $this->publishResponse(
                                        $message->replyTo,
                                        $this->errorPayload(
                                            code: 'HANDLER_ERROR',
                                            message: 'Internal server error',
                                            correlationId: is_string($context['correlation_id'] ?? null)
                                                ? $context['correlation_id']
                                                : null,
                                        ),
                                        $this->serviceErrorHeaders('500', 'Internal server error'),
                                    );
                                } catch (\Throwable) {
                                    // Best effort: never let a reply-publish failure escape the loop.
                                }
                            }
                        },
                        $endpoint->queueGroup,
                    )->await();

                    $sids[] = $sid;
                }
            } catch (\Throwable $e) {
                // Roll back any subscriptions made so far so a retried start() is not blocked and no
                // half-initialized endpoints linger.
                foreach ($sids as $sid) {
                    try {
                        $this->client->unsubscribe($sid)->await();
                    } catch (\Throwable) {
                        // Best-effort rollback; the connection may already be gone.
                    }
                }

                throw $e;
            }

            $this->subscriptionSids = $sids;
            $this->started = true;
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
            try {
                foreach ($this->subscriptionSids as $sid) {
                    try {
                        $this->client->unsubscribe($sid)->await();
                    } catch (\Throwable) {
                        // The connection may already be closed/lost during shutdown (unsubscribe
                        // throws when not open). Keep unsubscribing the rest rather than aborting on
                        // the first failure and leaking the remaining SIDs.
                    }
                }
            } finally {
                // Always clear state so a subsequent start() can re-subscribe cleanly.
                $this->subscriptionSids = [];
                $this->started = false;
                $this->markDone();
            }
        });
    }

    /**
     * Invokes the done handler exactly once (reset by {@see start()}). Handler exceptions are swallowed
     * so shutdown is not derailed.
     */
    private function markDone(): void
    {
        if ($this->doneFired || $this->doneHandler === null) {
            return;
        }

        $this->doneFired = true;
        try {
            ($this->doneHandler)($this);
        } catch (\Throwable) {
            // A faulty done handler must not break shutdown.
        }
    }

    /**
     * Gracefully drains the service: unsubscribes every endpoint and flushes so the server stops
     * delivering new requests, while in-flight handlers (already dispatched on the event loop) run to
     * completion, then clears state. Mirrors nats.go / nats.java micro `Stop()` drain semantics (#51).
     *
     * @return Future<void>
     */
    public function drain(): Future
    {
        return async(function (): void {
            try {
                foreach ($this->subscriptionSids as $sid) {
                    try {
                        $this->client->unsubscribe($sid)->await();
                    } catch (\Throwable) {
                        // The connection may already be gone; keep draining the rest.
                    }
                }

                if ($this->started) {
                    try {
                        // Flush so the UNSUBs are processed server-side before we consider the drain
                        // complete (no new request will be delivered after this resolves).
                        $this->client->flush()->await();
                    } catch (\Throwable) {
                        // Best effort: a closed connection needs no flush.
                    }
                }
            } finally {
                $this->subscriptionSids = [];
                $this->started = false;
                $this->markDone();
            }
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
                        // Thread the cancellation INTO processIncoming (not just the outer await) so
                        // the underlying socket read is actually bounded and torn down on cancel -
                        // otherwise an idle read is orphaned and leaves the shared connection wedged.
                        $processed = $this->client->processIncoming($effectiveCancellation)->await($effectiveCancellation);
                        if ($processed === 0) {
                            // Yield briefly to avoid a tight loop when transport is idle.
                            delay(0.01, cancellation: $effectiveCancellation);
                        }
                    } catch (CancelledException) {
                        break;
                    } catch (\Throwable) {
                        // A connection-level failure. If the connection is unrecoverable (closed for
                        // good), stop instead of busy-spinning forever silently swallowing the error;
                        // otherwise back off - interruptibly - and let reconnect catch up.
                        if ($this->client->state() === ConnectionState::Closed) {
                            break;
                        }

                        try {
                            delay(0.02, cancellation: $effectiveCancellation);
                        } catch (CancelledException) {
                            break;
                        }
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

            $entry = [
                'name' => $endpoint->name,
                'subject' => $endpoint->subject,
                'queue_group' => $endpoint->queueGroup,
                'num_requests' => $endpoint->requests,
                'num_errors' => $endpoint->errors,
                'last_error' => $endpoint->lastError,
                'processing_time' => $endpoint->processingTimeNs,
                'average_processing_time' => $averageProcessingTime,
            ];

            // Merge any custom per-endpoint data from the configured stats supplier (#50). A throwing
            // supplier must not break the discovery response.
            if ($endpoint->statsHandler !== null) {
                try {
                    $entry['data'] = ($endpoint->statsHandler)($endpoint);
                } catch (\Throwable) {
                    // Skip custom data on failure.
                }
            }

            $endpointStats[] = $entry;
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
    /**
     * @param array<int, int> $sids Collector for the created subscription SIDs (by reference) so the
     *                              caller can commit or roll them back atomically.
     * @return Future<void>
     */
    private function subscribeDiscovery(array &$sids): Future
    {
        return async(function () use (&$sids): void {
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

                    try {
                        $payload = $this->discoveryPayloadForSubject($subject);
                        $this->client->publish($message->replyTo, json_encode($payload, JSON_THROW_ON_ERROR))->await();
                    } catch (\Throwable) {
                        // A discovery encode/publish failure (e.g. invalid-UTF-8 metadata) must not
                        // escape the shared dispatch loop and abort delivery of buffered frames for
                        // other subscriptions. Best-effort: skip this discovery response.
                    }
                })->await();

                $sids[] = $sid;
            }
        });
    }

    /**
     * @param string|array<string,mixed> $response
     */
    /**
     * @param string|array<string,mixed> $response
     * @param array<string,string>|null $errorHeaders NATS micro error headers (Nats-Service-Error[-Code])
     */
    private function publishResponse(?string $replyTo, string|array $response, ?array $errorHeaders = null): void
    {
        if ($replyTo === null || $replyTo === '') {
            return;
        }

        $body = is_array($response) ? json_encode($response, JSON_THROW_ON_ERROR) : $response;

        // The NATS micro spec signals endpoint failures via reply HEADERS, so a generic client
        // (Go micro, nats CLI) can detect the error without parsing the JSON body.
        if ($errorHeaders !== null && $errorHeaders !== []) {
            $this->client->publishWithHeaders($replyTo, $body, $errorHeaders)->await();

            return;
        }

        $this->client->publish($replyTo, $body)->await();
    }

    /**
     * Builds the micro-spec error reply headers. The description is collapsed to a single line because
     * header values cannot contain CR/LF - a crafted handler/validation message must not break framing.
     *
     * @return array<string,string>
     */
    private function serviceErrorHeaders(string $code, string $message): array
    {
        return [
            'Nats-Service-Error' => trim(preg_replace('/\s+/', ' ', $message) ?? $message),
            'Nats-Service-Error-Code' => $code,
        ];
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
                $entry = [
                    'name' => $endpoint->name,
                    'subject' => $endpoint->subject,
                    'queue_group' => $endpoint->queueGroup,
                    'metadata' => $endpoint->metadata,
                ];

                // Surface the declared endpoint schema via the standard $SRV.INFO verb. ADR-32 stabilizes
                // only PING/INFO/STATS, so a spec-conformant consumer (nats CLI micro, nats.go) never
                // queries the non-spec $SRV.SCHEMA channel; carrying the schema here makes it reachable.
                // Conformant clients ignore the extra field. The $SRV.SCHEMA verb is retained for
                // backward compatibility. (#101)
                if ($endpoint->schema !== null) {
                    $entry['schema'] = $endpoint->schema;
                }

                $endpoints[] = $entry;
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
            try {
                $instance = new $handler();
            } catch (\Throwable $e) {
                throw new \InvalidArgumentException(sprintf(
                    'Service endpoint class handler %s could not be instantiated (it must have a no-argument constructor): %s',
                    $handler,
                    $e->getMessage(),
                ), 0, $e);
            }

            if (!$instance instanceof ServiceEndpointHandlerInterface) {
                throw new \InvalidArgumentException(sprintf(
                    'Service endpoint class handler %s must implement %s.',
                    $handler,
                    ServiceEndpointHandlerInterface::class,
                ));
            }

            return static fn(NatsMessage $message): string|array|null => $instance->handle($message);
        }

        if ($handler instanceof ServiceEndpointHandlerInterface) {
            return static fn(NatsMessage $message): string|array|null => $handler->handle($message);
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
