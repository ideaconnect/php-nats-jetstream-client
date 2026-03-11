<?php

declare(strict_types=1);

namespace Idct\Nats\JetStream;

use Amp\Future;
use Idct\Nats\Core\NatsClient;
use Idct\Nats\Core\NatsHeaders;
use Idct\Nats\Core\NatsMessage;
use Idct\Nats\Exception\JetStreamException;
use function Amp\async;

final class KeyValueBucket
{
    /**
     * Creates a KV bucket context bound to a client and bucket name.
     */
    public function __construct(
        private readonly NatsClient $client,
        private readonly JetStreamContext $jetStream,
        private readonly string $bucket,
    ) {
    }

    /**
     * Creates or updates the underlying KV stream for this bucket.
     *
     * @param array<string,mixed> $options
     * @return Future<StreamInfo>
     */
    public function create(array $options = []): Future
    {
        return async(function () use ($options): StreamInfo {
            $defaults = [
                'description' => 'KV bucket ' . $this->bucket,
                'max_msgs_per_subject' => 1,
                'allow_direct' => true,
                'allow_rollup_hdrs' => true,
            ];

            return $this->jetStream->createStream(
                $this->streamName(),
                [$this->subjectPrefix() . '>'],
                array_merge($defaults, $options),
            )->await();
        });
    }

    /**
     * Deletes the underlying KV stream.
     *
     * @return Future<bool>
     */
    public function deleteBucket(): Future
    {
        return $this->jetStream->deleteStream($this->streamName());
    }

    /**
     * Puts a value for the provided key.
     *
     * @return Future<PubAck>
     */
    public function put(string $key, string $value): Future
    {
        return async(function () use ($key, $value): PubAck {
            $this->assertValidKey($key);

            return $this->jetStream->publish($this->subjectForKey($key), $value)->await();
        });
    }

    /**
     * Marks a key as deleted.
     *
     * @return Future<PubAck>
     */
    public function delete(string $key): Future
    {
        return async(function () use ($key): PubAck {
            $this->assertValidKey($key);

            return $this->publishWithHeadersAck(
                $this->subjectForKey($key),
                '',
                ['KV-Operation' => 'DEL'],
            )->await();
        });
    }

    /**
     * Loads the latest entry for a key, or null when no key exists.
     *
     * @return Future<KeyValueEntry|null>
     */
    public function get(string $key): Future
    {
        return async(function () use ($key): ?KeyValueEntry {
            $this->assertValidKey($key);

            try {
                $response = $this->requestKvMessage($key);
            } catch (JetStreamException $e) {
                if ($e->getCode() === 404) {
                    return null;
                }

                throw $e;
            }

            /** @var array<string,mixed>|null $message */
            $message = is_array($response['message'] ?? null) ? $response['message'] : null;
            if ($message === null) {
                return null;
            }

            $data = (string) ($message['data'] ?? '');
            $decoded = $data === '' ? '' : (string) base64_decode($data, true);
            $headers = $this->decodeHeadersFromApiMessage($message);
            $operation = strtoupper((string) ($headers['KV-Operation'] ?? 'PUT'));

            return new KeyValueEntry(
                bucket: $this->bucket,
                key: $key,
                value: $operation === 'DEL' || $operation === 'PURGE' ? null : $decoded,
                operation: $operation,
                revision: isset($message['seq']) ? (int) $message['seq'] : null,
            );
        });
    }

    /**
     * Watches keys using wildcard pattern and forwards entries to a callback.
     *
     * @param callable(KeyValueEntry):void $handler
     * @return Future<int>
     */
    public function watch(callable $handler, string $pattern = '>'): Future
    {
        return async(function () use ($handler, $pattern): int {
            $subject = $this->subjectPrefix() . $pattern;

            return $this->client->subscribe($subject, function (NatsMessage $message) use ($handler): void {
                $key = $this->keyFromSubject($message->subject);
                if ($key === null) {
                    return;
                }

                $headers = NatsHeaders::fromWireBlock($message->rawHeaders);
                $operation = strtoupper((string) ($headers['KV-Operation'] ?? 'PUT'));

                $handler(new KeyValueEntry(
                    bucket: $this->bucket,
                    key: $key,
                    value: $operation === 'DEL' || $operation === 'PURGE' ? null : $message->payload,
                    operation: $operation,
                    revision: null,
                ));
            })->await();
        });
    }

    /**
     * Returns KV stream name for this bucket.
     */
    public function streamName(): string
    {
        return 'KV_' . $this->bucket;
    }

    /**
     * Returns KV subject prefix for this bucket.
     */
    public function subjectPrefix(): string
    {
        return '$KV.' . $this->bucket . '.';
    }

    /**
     * Resolves a full subject for a key.
     */
    private function subjectForKey(string $key): string
    {
        return $this->subjectPrefix() . $key;
    }

    /**
     * @return array<string,mixed>
     */
    private function requestKvMessage(string $key): array
    {
        $subject = JetStreamApi::STREAM_MSG_GET_PREFIX . $this->streamName();
        $payload = json_encode(['last_by_subj' => $this->subjectForKey($key)], JSON_THROW_ON_ERROR);
        $message = $this->client->request($subject, $payload)->await();

        /** @var array<string,mixed> $data */
        $data = json_decode($message->payload, true, 512, JSON_THROW_ON_ERROR);

        /** @var array<string,mixed>|null $error */
        $error = is_array($data['error'] ?? null) ? $data['error'] : null;
        if ($error !== null) {
            $description = (string) ($error['description'] ?? 'JetStream API error');
            $code = (int) ($error['code'] ?? 0);
            throw new JetStreamException($description, $code);
        }

        return $data;
    }

    /**
     * @param array<string,mixed> $message
     * @return array<string,string>
     */
    private function decodeHeadersFromApiMessage(array $message): array
    {
        $encodedHeaders = (string) ($message['hdrs'] ?? '');
        if ($encodedHeaders === '') {
            return [];
        }

        $rawHeaders = base64_decode($encodedHeaders, true);
        if ($rawHeaders === false) {
            return [];
        }

        return NatsHeaders::fromWireBlock($rawHeaders);
    }

    /**
     * Parses a key from a KV subject.
     */
    private function keyFromSubject(string $subject): ?string
    {
        $prefix = $this->subjectPrefix();
        if (!str_starts_with($subject, $prefix)) {
            return null;
        }

        return substr($subject, strlen($prefix));
    }

    /**
     * @param array<string,string> $headers
     * @return Future<PubAck>
     */
    private function publishWithHeadersAck(string $subject, string $payload, array $headers): Future
    {
        return async(function () use ($subject, $payload, $headers): PubAck {
            $message = $this->client->requestWithHeaders($subject, $payload, $headers)->await();

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
     * Ensures key name follows basic NATS KV key constraints.
     */
    private function assertValidKey(string $key): void
    {
        if ($key === '' || str_contains($key, ' ') || str_contains($key, '*') || str_contains($key, '>')) {
            throw new JetStreamException('Invalid KV key');
        }
    }
}
