<?php

declare(strict_types=1);

namespace Idct\Nats\Services;

use Amp\Future;
use Idct\Nats\Core\NatsClient;
use Idct\Nats\Core\NatsMessage;
use function Amp\async;

final class Service
{
    /** @var array<int, int> */
    private array $subscriptionSids = [];

    /** @var array<string, ServiceEndpoint> */
    private array $endpoints = [];

    /** @var array<string, callable(NatsMessage):(string|array<string,mixed>|null)> */
    private array $handlers = [];

    private readonly string $id;

    /**
     * Creates a service runtime bound to a NATS client.
     *
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
    }

    /**
     * Registers a request handler endpoint.
     *
    * @param callable(NatsMessage):(string|array<string,mixed>|null) $handler
     */
    public function addEndpoint(string $name, string $subject, callable $handler, ?string $queueGroup = null): self
    {
        $endpoint = new ServiceEndpoint($name, $subject, $queueGroup);
        $this->endpoints[$subject] = $endpoint;
        $this->handlers[$subject] = $handler;

        return $this;
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

                        try {
                            $response = ($this->handlers[$subject])($message);
                        } catch (\Throwable $e) {
                            $endpoint->errors++;
                            $response = ['error' => $e->getMessage()];
                        }

                        if ($message->replyTo === null || $message->replyTo === '') {
                            return;
                        }

                        if ($response === null) {
                            return;
                        }

                        if (is_array($response)) {
                            $this->client->publish($message->replyTo, json_encode($response, JSON_THROW_ON_ERROR))->await();

                            return;
                        }

                        $this->client->publish($message->replyTo, $response)->await();
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
     * Returns current service statistics payload.
     *
     * @return array<string,mixed>
     */
    public function statsSnapshot(): array
    {
        $endpointStats = [];
        foreach ($this->endpoints as $endpoint) {
            $endpointStats[] = [
                'name' => $endpoint->name,
                'subject' => $endpoint->subject,
                'queue_group' => $endpoint->queueGroup,
                'requests' => $endpoint->requests,
                'errors' => $endpoint->errors,
            ];
        }

        return [
            'type' => 'io.nats.micro.v1.stats_response',
            'name' => $this->name,
            'id' => $this->id,
            'version' => $this->version,
            'started' => gmdate('Y-m-d\TH:i:s\Z'),
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
     * @return array<string,mixed>
     */
    private function discoveryPayloadForSubject(string $subject): array
    {
        if (str_starts_with($subject, '$SRV.STATS')) {
            return $this->statsSnapshot();
        }

        $base = [
            'name' => $this->name,
            'id' => $this->id,
            'version' => $this->version,
            'metadata' => $this->metadata,
        ];

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
}
