<?php

declare(strict_types=1);

namespace IDCT\NATS\Core;

use Amp\Cancellation;
use Amp\Future;
use IDCT\NATS\Connection\ConnectionStats;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\NatsConnection;
use IDCT\NATS\Connection\NatsOptions;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\Protocol\ServerInfo;
use IDCT\NATS\Services\Service;
use IDCT\NATS\Transport\AmpSocketTransport;
use IDCT\NATS\Transport\TransportInterface;

use function Amp\async;

/**
 * Facade client exposing high-level NATS publish/subscribe and request APIs.
 */
final class NatsClient
{
    private readonly NatsConnection $connection;
    private readonly NatsOptions $options;
    private ?JetStreamContext $jetStreamContext = null;

    /**
     * Creates a high-level client facade over the connection runtime.
     *
     * @param NatsOptions $options Runtime options for NATS connectivity, authentication, heartbeat, and reconnect behavior.
     * @param TransportInterface|null $transport Optional custom transport. When null, AmpSocketTransport is used.
     */
    public function __construct(
        NatsOptions $options = new NatsOptions(),
        ?TransportInterface $transport = null,
    ) {
        $this->options = $options;
        $this->connection = new NatsConnection(
            options: $options,
            transport: $transport ?? new AmpSocketTransport($options),
        );
    }

    /**
     * Opens a connection to the configured NATS server.
     *
     * @return Future<void>
     */
    public function connect(): Future
    {
        return $this->connection->connect();
    }

    /**
     * Closes the active connection and releases underlying transport resources.
     *
     * Locally queued, undelivered messages are discarded without being delivered - nats.go
     * Close() parity (#134). Use {@see drain()} for the lossless path: it delivers the buffered
     * backlog before closing.
     *
     * @return Future<void>
     */
    public function disconnect(): Future
    {
        return $this->connection->disconnect();
    }

    /**
     * Gracefully drains all subscriptions, flushes pending messages, and closes.
     *
     * @return Future<void>
     */
    public function drain(): Future
    {
        return $this->connection->drain();
    }

    /**
     * Publishes a payload to a subject.
     *
     * @return Future<void>
     */
    public function publish(string $subject, string $payload, ?string $replyTo = null): Future
    {
        return $this->connection->publish($subject, $payload, $replyTo);
    }

    /**
     * Publishes a payload with NATS headers to a subject. A header value may be a single string or a
     * list of strings for multi-value (multimap) headers (ADR-4).
     *
     * @param array<string,string|list<string>> $headers
     * @return Future<void>
     */
    public function publishWithHeaders(
        string $subject,
        string $payload,
        array $headers,
        ?string $replyTo = null,
    ): Future {
        return $this->connection->publishWithHeaders($subject, $payload, $headers, $replyTo);
    }

    /**
     * Registers a subscription handler and returns its SID.
     *
     * @param callable(NatsMessage):void $handler
     * @return Future<int>
     */
    public function subscribe(string $subject, callable $handler, ?string $queue = null): Future
    {
        return $this->connection->subscribe($subject, $handler, $queue);
    }

    /**
     * Subscribes and returns a SubscriptionQueue for polling-style message consumption.
     *
     * @return Future<SubscriptionQueue>
     */
    public function subscribeQueue(string $subject, ?string $queue = null): Future
    {
        return async(function () use ($subject, $queue): SubscriptionQueue {
            /** @var SubscriptionQueue|null $subscriptionQueue */
            $subscriptionQueue = null;
            /** @var \SplQueue<NatsMessage> $early */
            $early = new \SplQueue();
            $sid = $this->connection->subscribe(
                $subject,
                static function (NatsMessage $msg) use (&$subscriptionQueue, $early): void {
                    // The sid is routable the moment SUB hits the wire - before the queue object
                    // exists. A delivery in that window (a concurrent read or the heartbeat
                    // self-read) must be buffered, not silently dropped (#129).
                    if ($subscriptionQueue === null) {
                        $early->enqueue($msg);

                        return;
                    }

                    $subscriptionQueue->enqueue($msg);
                },
                $queue,
            )->await();
            $subscriptionQueue = new SubscriptionQueue(
                $this,
                $sid,
                $this->options->maxPendingMessagesPerSubscription,
                $this->options->slowConsumerPolicy,
            );

            // Replay through enqueue() so the cap and slow-consumer policy apply as usual.
            while (!$early->isEmpty()) {
                $subscriptionQueue->enqueue($early->dequeue());
            }

            return $subscriptionQueue;
        });
    }

    /**
     * Removes a subscription by SID.
     *
     * With $maxMessages, arms auto-unsubscribe: delivery continues until that many TOTAL messages
     * (counting already-delivered ones) have reached the handler, then the subscription is removed
     * automatically - matching the server-side `UNSUB <sid> <max>` semantics (#112). On a connection
     * that is not open, local state is released silently instead of throwing (#116).
     *
     * A plain unsubscribe (no $maxMessages) discards the sid's locally queued, undelivered backlog -
     * nats.go Unsubscribe() parity (#134). Use {@see drainSubscription()} (or a full {@see drain()})
     * for the lossless path that delivers the backlog first.
     *
     * @return Future<void>
     */
    public function unsubscribe(int $sid, ?int $maxMessages = null): Future
    {
        return $this->connection->unsubscribe($sid, $maxMessages);
    }

    /**
     * Drains a single subscription: stops new deliveries (UNSUB), flushes so in-flight messages are
     * dispatched to the handler, then removes the subscription. Mirrors per-subscription Drain().
     *
     * @return Future<void>
     */
    public function drainSubscription(int $sid): Future
    {
        return $this->connection->drainSubscription($sid);
    }

    /**
     * Processes a single incoming transport chunk and dispatches parsed frames.
     *
     * @param Cancellation|null $cancellation Optional token that cancels the underlying socket read.
     * @return Future<int>
     */
    public function processIncoming(?Cancellation $cancellation = null): Future
    {
        return $this->connection->processIncoming($cancellation);
    }

    /**
     * Routes an asynchronous error through the connection's error listener and logger.
     *
     * @internal Used by SubscriptionQueue to surface slow-consumer drops (#134); not part of the
     *           supported API.
     */
    public function emitError(\Throwable $error, string $logLevel = 'error'): void
    {
        $this->connection->emitError($error, $logLevel);
    }

    /**
     * Flushes outbound writes and waits for the server's PONG, confirming the server has processed
     * everything sent so far (e.g. a SUBSCRIBE before a dependent request). Bounded by the request
     * timeout.
     *
     * @return Future<void>
     */
    public function flush(): Future
    {
        return $this->connection->flush();
    }

    /**
     * Returns the current connection state (Open, Connecting, Draining, Closed, ...).
     */
    public function state(): ConnectionState
    {
        return $this->connection->state();
    }

    /**
     * Sends a request and resolves with the first reply message.
     *
     * @param Cancellation|null $cancellation Optional external cancellation token.
     * @return Future<NatsMessage>
     */
    public function request(
        string $subject,
        string $payload,
        ?int $timeoutMs = null,
        ?Cancellation $cancellation = null,
    ): Future {
        return $this->connection->request($subject, $payload, $timeoutMs, $cancellation);
    }

    /**
     * Sends a request with headers and resolves with the first reply message.
     *
     * @param array<string,string> $headers
     * @param Cancellation|null $cancellation Optional external cancellation token.
     * @return Future<NatsMessage>
     */
    public function requestWithHeaders(
        string $subject,
        string $payload,
        array $headers,
        ?int $timeoutMs = null,
        ?Cancellation $cancellation = null,
    ): Future {
        return $this->connection->requestWithHeaders($subject, $payload, $headers, $timeoutMs, $cancellation);
    }

    /**
     * Sends one request and collects multiple replies (scatter-gather), terminating on the first of:
     * {@see $maxResponses} replies, a no-responders sentinel, the per-message {@see $stallMs} gap, or
     * the total timeout. Mirrors nats.go `RequestMany` / nats.java `Connection.requestMany`.
     *
     * @param array<string,string>|null $headers Optional request headers (null = plain request).
     * @param int|null $maxResponses Stop after this many replies (null = time-bounded only).
     * @param int|null $totalTimeoutMs Overall budget in ms (null = configured request timeout).
     * @param int|null $stallMs Stop once this long passes with no new reply (null = disabled).
     * @return Future<list<NatsMessage>>
     */
    public function requestMany(
        string $subject,
        string $payload,
        ?array $headers = null,
        ?int $maxResponses = null,
        ?int $totalTimeoutMs = null,
        ?int $stallMs = null,
        ?Cancellation $cancellation = null,
    ): Future {
        return $this->connection->requestMany($subject, $payload, $headers, $maxResponses, $totalTimeoutMs, $stallMs, $cancellation);
    }

    /**
     * Returns server capabilities advertised during the INFO handshake.
     */
    public function serverInfo(): ?ServerInfo
    {
        return $this->connection->serverInfo();
    }

    /**
     * The server URL currently connected to, or null when not connected.
     */
    public function connectedUrl(): ?string
    {
        return $this->connection->connectedUrl();
    }

    /**
     * Cluster endpoints discovered from the server's INFO `connect_urls`.
     *
     * @return list<string>
     */
    public function discoveredServers(): array
    {
        return $this->connection->discoveredServers();
    }

    /**
     * The server's maximum accepted payload size, or null when unknown.
     */
    public function maxPayload(): ?int
    {
        return $this->connection->maxPayload();
    }

    /**
     * A snapshot of connection traffic counters.
     */
    public function statistics(): ConnectionStats
    {
        return $this->connection->statistics();
    }

    /**
     * The runtime options this client was constructed with.
     */
    public function options(): NatsOptions
    {
        return $this->options;
    }

    /**
     * Measures the round-trip time to the server (PING/PONG), in seconds.
     *
     * @return Future<float>
     */
    public function rtt(): Future
    {
        return $this->connection->rtt();
    }

    /**
     * Returns a JetStream API context bound to this client instance.
     */
    public function jetStream(): JetStreamContext
    {
        if ($this->jetStreamContext === null) {
            $this->jetStreamContext = new JetStreamContext($this);
        }

        return $this->jetStreamContext;
    }

    /**
     * Creates a services-framework runtime bound to this client.
     *
     * @param array<string,string> $metadata
     */
    public function service(string $name, string $version, ?string $description = null, array $metadata = []): Service
    {
        return new Service($this, $name, $version, $description, $metadata);
    }
}
