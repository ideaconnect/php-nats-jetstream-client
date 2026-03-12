<?php

declare(strict_types=1);

namespace IDCT\NATS\Core;

use Amp\Cancellation;
use Amp\Future;
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
     * Publishes a payload with NATS headers to a subject.
     *
     * @param array<string,string> $headers
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
            $sid = $this->connection->subscribe(
                $subject,
                static function (NatsMessage $msg) use (&$subscriptionQueue): void {
                    $subscriptionQueue?->enqueue($msg);
                },
                $queue,
            )->await();
            $subscriptionQueue = new SubscriptionQueue($this, $sid);

            return $subscriptionQueue;
        });
    }

    /**
     * Removes a subscription by SID.
     *
     * @return Future<void>
     */
    public function unsubscribe(int $sid, ?int $maxMessages = null): Future
    {
        return $this->connection->unsubscribe($sid, $maxMessages);
    }

    /**
     * Processes a single incoming transport chunk and dispatches parsed frames.
     *
     * @return Future<int>
     */
    public function processIncoming(): Future
    {
        return $this->connection->processIncoming();
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
    ): Future
    {
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
     * Returns server capabilities advertised during the INFO handshake.
     */
    public function serverInfo(): ?ServerInfo
    {
        return $this->connection->serverInfo();
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
