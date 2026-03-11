<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

use Amp\Future;
use IDCT\NATS\Core\Inbox;
use IDCT\NATS\Core\NatsHeaders;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use function Amp\async;

final class JetStreamContext
{
    /** @var array<string,KeyValueBucket> */
    private array $kvBuckets = [];
    /** @var array<string,ObjectStoreBucket> */
    private array $objectBuckets = [];

    /**
     * Creates a JetStream API context bound to a NATS client.
     */
    public function __construct(private readonly NatsClient $client)
    {
    }

    /**
     * Retrieves account-wide JetStream metrics and limits.
     *
     * @return Future<AccountInfo>
     */
    public function accountInfo(): Future
    {
        return async(function (): AccountInfo {
            $payload = $this->requestJson(JetStreamApi::ACCOUNT_INFO, []);

            return AccountInfo::fromArray($payload);
        });
    }

    /**
     * Returns a KeyValue bucket context.
     */
    public function keyValue(string $bucket): KeyValueBucket
    {
        if (!isset($this->kvBuckets[$bucket])) {
            $this->kvBuckets[$bucket] = new KeyValueBucket($this->client, $this, $bucket);
        }

        return $this->kvBuckets[$bucket];
    }

    /**
     * Returns an Object Store bucket context.
     */
    public function objectStore(string $bucket): ObjectStoreBucket
    {
        if (!isset($this->objectBuckets[$bucket])) {
            $this->objectBuckets[$bucket] = new ObjectStoreBucket($this->client, $this, $bucket);
        }

        return $this->objectBuckets[$bucket];
    }

    /**
     * Creates or updates a stream using a minimal configuration payload.
     *
     * @param list<string> $subjects
     * @param array<string,mixed> $options Additional stream config fields.
     * @return Future<StreamInfo>
     */
    public function createStream(string $name, array $subjects, array $options = []): Future
    {
        return async(function () use ($name, $subjects, $options): StreamInfo {
            if ($subjects === []) {
                throw new JetStreamException('Stream subjects must not be empty');
            }

            $payload = array_merge($options, [
                'name' => $name,
                'subjects' => $subjects,
            ]);

            $response = $this->requestJson(JetStreamApi::STREAM_CREATE_PREFIX . $name, $payload);

            return StreamInfo::fromArray($response);
        });
    }

    /**
     * Updates an existing stream configuration.
     *
     * @param array<string,mixed> $config Full stream config to apply.
     * @return Future<StreamInfo>
     */
    public function updateStream(string $name, array $config): Future
    {
        return async(function () use ($name, $config): StreamInfo {
            $payload = array_merge($config, ['name' => $name]);

            $response = $this->requestJson(JetStreamApi::STREAM_UPDATE_PREFIX . $name, $payload);

            return StreamInfo::fromArray($response);
        });
    }

    /**
     * Retrieves stream metadata by name.
     *
     * @return Future<StreamInfo>
     */
    public function getStream(string $name): Future
    {
        return async(function () use ($name): StreamInfo {
            $response = $this->requestJson(JetStreamApi::STREAM_INFO_PREFIX . $name, []);

            return StreamInfo::fromArray($response);
        });
    }

    /**
     * Deletes a stream and returns operation success.
     *
     * @return Future<bool>
     */
    public function deleteStream(string $name): Future
    {
        return async(function () use ($name): bool {
            $response = $this->requestJson(JetStreamApi::STREAM_DELETE_PREFIX . $name, []);

            return (bool) ($response['success'] ?? false);
        });
    }

    /**
     * Creates or updates a durable consumer for a stream.
     *
     * @param array<string,mixed> $options Additional consumer config fields (max_deliver, ack_wait, etc.).
     * @return Future<ConsumerInfo>
     */
    public function createConsumer(string $stream, string $consumer, ?string $filterSubject = null, array $options = []): Future
    {
        return async(function () use ($stream, $consumer, $filterSubject, $options): ConsumerInfo {
            if ($filterSubject === '') {
                throw new JetStreamException('Consumer filter subject must not be empty (use null to omit)');
            }

            $config = array_merge($options, [
                'durable_name' => $consumer,
                'ack_policy' => 'explicit',
            ]);

            if ($filterSubject !== null) {
                $config['filter_subject'] = $filterSubject;
            }

            $response = $this->requestJson(
                JetStreamApi::CONSUMER_CREATE_PREFIX . $stream . '.' . $consumer,
                ['stream_name' => $stream, 'config' => $config],
            );

            return ConsumerInfo::fromArray($response);
        });
    }

    /**
     * Creates an ephemeral pull consumer.
     *
     * @param array<string,mixed> $options Additional consumer config fields.
     * @return Future<ConsumerInfo>
     */
    public function createEphemeralConsumer(string $stream, ?string $filterSubject = null, array $options = []): Future
    {
        return async(function () use ($stream, $filterSubject, $options): ConsumerInfo {
            $config = array_merge($options, [
                'ack_policy' => 'explicit',
            ]);

            if ($filterSubject !== null && $filterSubject !== '') {
                $config['filter_subject'] = $filterSubject;
            }

            $response = $this->requestJson(
                JetStreamApi::CONSUMER_CREATE_PREFIX . $stream,
                ['stream_name' => $stream, 'config' => $config],
            );

            return ConsumerInfo::fromArray($response);
        });
    }

    /**
     * Creates or updates a durable push consumer.
     *
     * @param array<string,mixed> $options Additional consumer config fields.
     * @return Future<ConsumerInfo>
     */
    public function createPushConsumer(
        string $stream,
        string $consumer,
        string $deliverSubject,
        ?string $filterSubject = null,
        array $options = [],
    ): Future {
        return async(function () use ($stream, $consumer, $deliverSubject, $filterSubject, $options): ConsumerInfo {
            $config = array_merge($options, [
                'durable_name' => $consumer,
                'ack_policy' => 'explicit',
                'deliver_subject' => $deliverSubject,
            ]);

            if ($filterSubject !== null && $filterSubject !== '') {
                $config['filter_subject'] = $filterSubject;
            }

            $response = $this->requestJson(
                JetStreamApi::CONSUMER_CREATE_PREFIX . $stream . '.' . $consumer,
                ['stream_name' => $stream, 'config' => $config],
            );

            return ConsumerInfo::fromArray($response);
        });
    }

    /**
     * Creates an ephemeral push consumer.
     *
     * @param array<string,mixed> $options Additional consumer config fields.
     * @return Future<ConsumerInfo>
     */
    public function createEphemeralPushConsumer(
        string $stream,
        string $deliverSubject,
        ?string $filterSubject = null,
        array $options = [],
    ): Future {
        return async(function () use ($stream, $deliverSubject, $filterSubject, $options): ConsumerInfo {
            $config = array_merge($options, [
                'ack_policy' => 'explicit',
                'deliver_subject' => $deliverSubject,
            ]);

            if ($filterSubject !== null && $filterSubject !== '') {
                $config['filter_subject'] = $filterSubject;
            }

            $response = $this->requestJson(
                JetStreamApi::CONSUMER_CREATE_PREFIX . $stream,
                ['stream_name' => $stream, 'config' => $config],
            );

            return ConsumerInfo::fromArray($response);
        });
    }

    /**
     * Creates a durable push consumer and subscribes with JetStream control-frame handling.
     *
     * @param callable(NatsMessage):void $handler
     * @param array<string,mixed> $consumerOptions Additional consumer config fields.
     * @return Future<int>
     */
    public function subscribePushConsumer(
        string $stream,
        string $consumer,
        callable $handler,
        ?string $deliverSubject = null,
        ?string $filterSubject = null,
        array $consumerOptions = [],
    ): Future {
        return async(function () use ($stream, $consumer, $handler, $deliverSubject, $filterSubject, $consumerOptions): int {
            $deliver = $deliverSubject ?? Inbox::generate('_INBOX.JS.PUSH');

            $this->createPushConsumer($stream, $consumer, $deliver, $filterSubject, $consumerOptions)->await();

            return $this->client->subscribe($deliver, function (NatsMessage $message) use ($handler): void {
                if ($this->handlePushControlMessage($message)->await()) {
                    return;
                }

                $handler($message);
            })->await();
        });
    }

    /**
     * Creates an ephemeral push consumer and subscribes with JetStream control-frame handling.
     *
     * @param callable(NatsMessage):void $handler
     * @param array<string,mixed> $consumerOptions Additional consumer config fields.
     * @return Future<int>
     */
    public function subscribeEphemeralPushConsumer(
        string $stream,
        callable $handler,
        ?string $deliverSubject = null,
        ?string $filterSubject = null,
        array $consumerOptions = [],
    ): Future {
        return async(function () use ($stream, $handler, $deliverSubject, $filterSubject, $consumerOptions): int {
            $deliver = $deliverSubject ?? Inbox::generate('_INBOX.JS.PUSH');

            $this->createEphemeralPushConsumer($stream, $deliver, $filterSubject, $consumerOptions)->await();

            return $this->client->subscribe($deliver, function (NatsMessage $message) use ($handler): void {
                if ($this->handlePushControlMessage($message)->await()) {
                    return;
                }

                $handler($message);
            })->await();
        });
    }

    /**
     * Retrieves consumer metadata by stream and durable name.
     *
     * @return Future<ConsumerInfo>
     */
    public function getConsumer(string $stream, string $consumer): Future
    {
        return async(function () use ($stream, $consumer): ConsumerInfo {
            $response = $this->requestJson(JetStreamApi::CONSUMER_INFO_PREFIX . $stream . '.' . $consumer, []);

            return ConsumerInfo::fromArray($response);
        });
    }

    /**
     * Deletes a consumer and returns operation success.
     *
     * @return Future<bool>
     */
    public function deleteConsumer(string $stream, string $consumer): Future
    {
        return async(function () use ($stream, $consumer): bool {
            $response = $this->requestJson(JetStreamApi::CONSUMER_DELETE_PREFIX . $stream . '.' . $consumer, []);

            return (bool) ($response['success'] ?? false);
        });
    }

    /**
     * Publishes to a stream subject and returns JetStream publish acknowledgment.
     *
     * @return Future<PubAck>
     */
    public function publish(string $subject, string $payload): Future
    {
        return async(function () use ($subject, $payload): PubAck {
            $message = $this->client->request($subject, $payload)->await();

            /** @var array<string,mixed> $data */
            $data = json_decode($message->payload, true, 512, JSON_THROW_ON_ERROR);

            /** @var array<string,mixed>|null $error */
            $error = is_array($data['error'] ?? null) ? $data['error'] : null;
            if ($error !== null) {
                $description = (string) ($error['description'] ?? 'JetStream publish error');
                $code = (int) ($error['code'] ?? 0);
                throw new JetStreamException($description, $code);
            }

            return PubAck::fromArray($data);
        });
    }

    /**
     * Publishes a scheduled message using NATS 2.12 scheduler headers.
     *
     * @return Future<PubAck>
     */
    public function publishScheduled(
        string $scheduleSubject,
        string $targetSubject,
        string $payload,
        string $schedule,
        ?string $scheduleTtl = null,
    ): Future {
        return async(function () use ($scheduleSubject, $targetSubject, $payload, $schedule, $scheduleTtl): PubAck {
            $this->assertSupportedSchedulePattern($schedule);

            $headers = [
                'Nats-Schedule' => $schedule,
                'Nats-Schedule-Target' => $targetSubject,
            ];

            if ($scheduleTtl !== null && $scheduleTtl !== '') {
                $headers['Nats-Schedule-TTL'] = $scheduleTtl;
            }

            $message = $this->client->requestWithHeaders($scheduleSubject, $payload, $headers)->await();

            /** @var array<string,mixed> $data */
            $data = json_decode($message->payload, true, 512, JSON_THROW_ON_ERROR);

            /** @var array<string,mixed>|null $error */
            $error = is_array($data['error'] ?? null) ? $data['error'] : null;
            if ($error !== null) {
                $description = (string) ($error['description'] ?? 'JetStream schedule publish error');
                $code = (int) ($error['code'] ?? 0);
                throw new JetStreamException($description, $code);
            }

            return PubAck::fromArray($data);
        });
    }

    /**
     * Fetches the next message for a pull consumer.
     *
     * @return Future<NatsMessage>
     */
    public function fetchNext(string $stream, string $consumer, int $expiresMs = 3000): Future
    {
        return async(function () use ($stream, $consumer, $expiresMs): NatsMessage {
            $messages = $this->fetchBatch($stream, $consumer, 1, $expiresMs)->await();

            return $messages[0];
        });
    }

    /**
     * Fetches a batch of messages for a pull consumer.
     *
     * @return Future<list<NatsMessage>>
     */
    public function fetchBatch(string $stream, string $consumer, int $batch, int $expiresMs = 3000): Future
    {
        return async(function () use ($stream, $consumer, $batch, $expiresMs): array {
            if ($expiresMs <= 0) {
                throw new JetStreamException('Pull fetch expiresMs must be greater than zero');
            }

            if ($batch <= 0) {
                throw new JetStreamException('Pull fetch batch must be greater than zero');
            }

            $payload = [
                'batch' => $batch,
                'expires' => $expiresMs * 1_000_000,
            ];

            $subject = JetStreamApi::CONSUMER_MSG_NEXT_PREFIX . $stream . '.' . $consumer;
            $json = json_encode($payload, JSON_THROW_ON_ERROR);

            $inbox = Inbox::generate('_INBOX.JS.FETCH');
            $messages = [];

            $sid = $this->client->subscribe($inbox, static function (NatsMessage $msg) use (&$messages): void {
                $messages[] = $msg;
            })->await();

            try {
                $this->client->publish($subject, $json, $inbox)->await();

                $deadline = microtime(true) + ($expiresMs + 1000) / 1000;
                while (count($messages) < $batch && microtime(true) < $deadline) {
                    $this->client->processIncoming()->await();
                }
            } finally {
                $this->client->unsubscribe($sid)->await();
            }

            if ($messages === []) {
                throw new JetStreamException('No messages received within timeout');
            }

            return $messages;
        });
    }

    /**
     * Sends a JetStream explicit ACK for a previously delivered message.
     *
     * @return Future<void>
     */
    public function ack(NatsMessage $message): Future
    {
        return $this->publishAckToken($message, '+ACK');
    }

    /**
     * Sends a JetStream NAK for a previously delivered message.
     *
     * @return Future<void>
     */
    public function nak(NatsMessage $message): Future
    {
        return $this->publishAckToken($message, '-NAK');
    }

    /**
     * Sends a JetStream delayed NAK for a previously delivered message.
     *
     * @return Future<void>
     */
    public function nakWithDelay(NatsMessage $message, int $delayMs): Future
    {
        return async(function () use ($message, $delayMs): void {
            if ($delayMs <= 0) {
                throw new JetStreamException('JetStream delayed NAK requires delayMs greater than zero');
            }

            $payload = '-NAK ' . json_encode(['delay' => $delayMs * 1_000_000], JSON_THROW_ON_ERROR);
            $this->publishAckToken($message, $payload)->await();
        });
    }

    /**
     * Sends a JetStream terminal ACK for a previously delivered message.
     *
     * @return Future<void>
     */
    public function term(NatsMessage $message): Future
    {
        return $this->publishAckToken($message, '+TERM');
    }

    /**
     * Sends a JetStream work-in-progress signal for a previously delivered message.
     *
     * @return Future<void>
     */
    public function inProgress(NatsMessage $message): Future
    {
        return $this->publishAckToken($message, '+WPI');
    }

    /**
     * Validates the schedule expression format supported by current server behavior.
     */
    private function assertSupportedSchedulePattern(string $schedule): void
    {
        // NATS server currently supports @at only for scheduled messages.
        if (!preg_match('/^@at\s+\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/', $schedule)) {
            throw new JetStreamException('Only @at schedule expressions are currently supported');
        }
    }

    /**
     * Publishes an ACK protocol token to a message reply subject.
     *
     * @return Future<void>
     */
    private function publishAckToken(NatsMessage $message, string $token): Future
    {
        return async(function () use ($message, $token): void {
            if ($message->replyTo === null || $message->replyTo === '') {
                throw new JetStreamException('JetStream ACK requires a reply subject on the delivered message');
            }

            $this->client->publish($message->replyTo, $token)->await();
        });
    }

    /**
     * Handles JetStream push-control messages (heartbeat/flow-control).
     *
     * @return Future<bool> True when the message is a control message and was handled.
     */
    private function handlePushControlMessage(NatsMessage $message): Future
    {
        return async(function () use ($message): bool {
            $headers = NatsHeaders::fromWireBlock($message->rawHeaders);
            $status = (int) ($headers['Status'] ?? 0);

            if ($status !== 100) {
                return false;
            }

            $description = strtolower((string) ($headers['Description'] ?? ''));
            $isFlowControl = str_contains($description, 'flow') || array_key_exists('Nats-Consumer-Stalled', $headers);

            if ($isFlowControl && $message->replyTo !== null && $message->replyTo !== '') {
                $this->client->publish($message->replyTo, '')->await();
            }

            // Status 100 control messages are not user payload deliveries.
            return true;
        });
    }

    /**
     * Executes a JetStream API request and returns decoded JSON response.
     *
     * @param array<string,mixed> $body
     * @return array<string,mixed>
     */
    private function requestJson(string $subject, array $body): array
    {
        $jsonBody = $body === [] ? (object) [] : $body;
        $json = json_encode($jsonBody, JSON_THROW_ON_ERROR);
        $message = $this->client->request($subject, $json)->await();

        try {
            /** @var array<string,mixed> $data */
            $data = json_decode($message->payload, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new JetStreamException('Malformed JetStream API response: ' . $e->getMessage(), 0, $e);
        }

        /** @var array<string,mixed>|null $error */
        $error = is_array($data['error'] ?? null) ? $data['error'] : null;
        if ($error !== null) {
            $description = (string) ($error['description'] ?? 'JetStream API error');
            $code = (int) ($error['code'] ?? 0);
            throw new JetStreamException($description, $code);
        }

        return $data;
    }
}
