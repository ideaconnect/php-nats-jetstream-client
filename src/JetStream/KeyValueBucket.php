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
     * Updates a key only when expected last revision matches.
     *
     * @return Future<PubAck>
     */
    public function update(string $key, string $value, int $expectedRevision): Future
    {
        return async(function () use ($key, $value, $expectedRevision): PubAck {
            $this->assertValidKey($key);
            if ($expectedRevision <= 0) {
                throw new JetStreamException('Expected revision must be greater than zero');
            }

            return $this->publishWithHeadersAck(
                $this->subjectForKey($key),
                $value,
                ['Nats-Expected-Last-Subject-Sequence' => (string) $expectedRevision],
            )->await();
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
     * Purges key history and writes a purge marker.
     *
     * @return Future<PubAck>
     */
    public function purge(string $key): Future
    {
        return async(function () use ($key): PubAck {
            $this->assertValidKey($key);

            return $this->publishWithHeadersAck(
                $this->subjectForKey($key),
                '',
                ['KV-Operation' => 'PURGE', 'Nats-Rollup' => 'sub'],
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
            $decodedRaw = $data === '' ? '' : base64_decode($data, true);
            $decoded = $decodedRaw === false ? $data : $decodedRaw;
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
     * Returns latest values for all keys currently present in bucket.
     *
     * @return Future<array<string,string>>
     */
    public function getAll(): Future
    {
        return async(function (): array {
            $status = $this->getStatus()->await();
            $lastSequence = (int) ($status['last_sequence'] ?? 0);
            $latest = [];

            for ($seq = 1; $seq <= $lastSequence; $seq++) {
                $message = $this->requestKvMessageBySequence($seq);
                if ($message === null) {
                    continue;
                }

                $subject = (string) ($message['subject'] ?? '');
                $key = $this->keyFromSubject($subject);
                if ($key === null || $key === '') {
                    continue;
                }

                $headers = $this->decodeHeadersFromApiMessage($message);
                $operation = strtoupper((string) ($headers['KV-Operation'] ?? 'PUT'));

                if ($operation === 'DEL' || $operation === 'PURGE') {
                    unset($latest[$key]);
                    continue;
                }

                $encoded = (string) ($message['data'] ?? '');
                $decodedRaw = $encoded === '' ? '' : base64_decode($encoded, true);
                $decoded = $decodedRaw === false ? $encoded : $decodedRaw;

                $latest[$key] = $decoded;
            }

            return $latest;
        });
    }

    /**
     * Returns bucket status derived from stream state.
     *
     * @return Future<array<string,mixed>>
     */
    public function getStatus(): Future
    {
        return async(function (): array {
            $stream = $this->jetStream->getStream($this->streamName())->await();
            /** @var array<string,mixed> $state */
            $state = is_array($stream->raw['state'] ?? null) ? $stream->raw['state'] : [];

            return [
                'bucket' => $this->bucket,
                'stream' => $this->streamName(),
                'messages' => (int) ($state['messages'] ?? 0),
                'last_sequence' => (int) ($state['last_seq'] ?? ($state['messages'] ?? 0)),
                'bytes' => (int) ($state['bytes'] ?? 0),
                'subjects' => is_array($state['subjects'] ?? null) ? $state['subjects'] : [],
            ];
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
     * @return array<string,mixed>|null
     */
    private function requestKvMessageBySequence(int $sequence): ?array
    {
        $subject = JetStreamApi::STREAM_MSG_GET_PREFIX . $this->streamName();
        $payload = json_encode(['seq' => $sequence], JSON_THROW_ON_ERROR);
        $message = $this->client->request($subject, $payload)->await();

        /** @var array<string,mixed> $data */
        $data = json_decode($message->payload, true, 512, JSON_THROW_ON_ERROR);

        /** @var array<string,mixed>|null $error */
        $error = is_array($data['error'] ?? null) ? $data['error'] : null;
        if ($error !== null) {
            $code = (int) ($error['code'] ?? 0);
            if ($code === 404) {
                return null;
            }

            $description = (string) ($error['description'] ?? 'JetStream API error');
            throw new JetStreamException($description, $code);
        }

        /** @var array<string,mixed>|null $payloadMessage */
        $payloadMessage = is_array($data['message'] ?? null) ? $data['message'] : null;

        return $payloadMessage;
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
