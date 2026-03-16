<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\ObjectStore;

use Amp\Future;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\JetStreamApi;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\JetStream\Models\StreamInfo;
use function Amp\async;

/**
 * Implements NATS JetStream Object Store bucket operations.
 */
final class ObjectStoreBucket
{
    private const DEFAULT_CHUNK_SIZE = 131072; // 128 KiB

    /**
     * Creates an Object Store bucket context bound to a client and bucket name.
     *
     * @param NatsClient $client Connected client used for chunk publish and metadata retrieval operations.
     * @param JetStreamContext $jetStream JetStream context used to manage backing object-store streams.
     * @param string $bucket Object Store bucket name used to build stream and metadata/chunk subject prefixes.
     * @param int $chunkSize Chunk size in bytes used when splitting object payloads before publishing chunk messages.
     */
    public function __construct(
        private readonly NatsClient $client,
        private readonly JetStreamContext $jetStream,
        private readonly string $bucket,
        private readonly int $chunkSize = self::DEFAULT_CHUNK_SIZE,
    ) {
    }

    /**
     * Creates or updates the underlying Object Store stream.
     *
     * @param array<string,mixed> $options
     * @return Future<StreamInfo>
     */
    public function create(array $options = []): Future
    {
        return async(function () use ($options): StreamInfo {
            $defaults = [
                'description' => 'Object Store bucket ' . $this->bucket,
                'allow_direct' => true,
                'discard' => 'new',
            ];

            return $this->jetStream->createStream(
                $this->streamName(),
                [$this->chunkPrefix() . '>', $this->metaPrefix() . '>'],
                array_merge($defaults, $options),
            )->await();
        });
    }

    /**
     * Deletes the underlying Object Store stream.
     *
     * @return Future<bool>
     */
    public function deleteBucket(): Future
    {
        return $this->jetStream->deleteStream($this->streamName());
    }

    /**
     * Stores an object payload and publishes metadata.
     *
     * @param array<string,string> $metadata
     * @return Future<ObjectInfo>
     */
    public function put(string $name, string $data, array $metadata = []): Future
    {
        return async(function () use ($name, $data, $metadata): ObjectInfo {
            $this->assertValidName($name);

            $chunkSubject = $this->chunkPrefix() . bin2hex(random_bytes(8));
            $totalSize = strlen($data);
            $chunks = 0;

            if ($totalSize <= $this->chunkSize) {
                $this->jetStream->publish($chunkSubject, $data)->await();
                $chunks = 1;
            } else {
                $offset = 0;
                while ($offset < $totalSize) {
                    $chunk = substr($data, $offset, $this->chunkSize);
                    $this->jetStream->publish($chunkSubject, $chunk)->await();
                    $offset += strlen($chunk);
                    $chunks++;
                }
            }

            $info = [
                'name' => $name,
                'size' => $totalSize,
                'chunks' => $chunks,
                'digest' => 'SHA-256=' . base64_encode(hash('sha256', $data, true)),
                'mtime' => gmdate('Y-m-d\TH:i:s\Z'),
                'deleted' => false,
                'chunk_subject' => $chunkSubject,
                'metadata' => $metadata,
            ];

            $this->jetStream->publish($this->metaSubject($name), json_encode($info, JSON_THROW_ON_ERROR))->await();

            return ObjectInfo::fromArray($this->bucket, $info);
        });
    }

    /**
     * Retrieves object metadata and payload.
     *
     * @return Future<ObjectData|null>
     */
    public function get(string $name): Future
    {
        return async(function () use ($name): ?ObjectData {
            $info = $this->info($name)->await();
            if ($info === null) {
                return null;
            }

            if ($info->deleted) {
                return new ObjectData($info, null);
            }

            $expectedChunks = $info->chunks ?? 1;
            $assembled = '';

            $consumerName = null;
            try {
                $consumer = $this->jetStream->createEphemeralConsumer(
                    $this->streamName(),
                    $info->chunkSubject,
                    ['deliver_policy' => 'all'],
                )->await();
                $consumerName = $consumer->name;

                for ($i = 0; $i < $expectedChunks; $i++) {
                    try {
                        $message = $this->jetStream->fetchNext($this->streamName(), $consumerName, 2_000)->await();
                    } catch (JetStreamException $e) {
                        if ($e->getCode() === 408) {
                            break;
                        }

                        throw $e;
                    }

                    $assembled .= $message->payload;
                    if ($message->replyTo !== null && $message->replyTo !== '') {
                        $this->jetStream->ack($message)->await();
                    }
                }
            } finally {
                if ($consumerName !== null && $consumerName !== '') {
                    try {
                        $this->jetStream->deleteConsumer($this->streamName(), $consumerName)->await();
                    } catch (JetStreamException) {
                        // Best-effort ephemeral consumer cleanup.
                    }
                }
            }

            // Verify digest integrity when metadata contains one.
            if ($info->digest !== '') {
                $expected = $info->digest;
                $actual = 'SHA-256=' . base64_encode(hash('sha256', $assembled, true));
                if ($expected !== $actual) {
                    throw new JetStreamException('Object digest mismatch: expected ' . $expected . ', got ' . $actual);
                }
            }

            return new ObjectData($info, $assembled);
        });
    }

    /**
     * Streams object payload to a callback.
     *
     * @param callable(string):void $chunkHandler
     * @return Future<ObjectInfo|null>
     */
    public function getToCallback(string $name, callable $chunkHandler): Future
    {
        return async(function () use ($name, $chunkHandler): ?ObjectInfo {
            $objectData = $this->get($name)->await();
            if ($objectData === null) {
                return null;
            }

            if ($objectData->data !== null && $objectData->data !== '') {
                $chunkHandler($objectData->data);
            }

            return $objectData->info;
        });
    }

    /**
     * Retrieves object metadata only.
     *
     * @return Future<ObjectInfo|null>
     */
    public function info(string $name): Future
    {
        return async(function () use ($name): ?ObjectInfo {
            $this->assertValidName($name);

            try {
                $response = $this->requestStreamMessage($this->metaSubject($name));
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

            $encodedData = (string) ($message['data'] ?? '');
            $metadataJson = $encodedData === '' ? '' : base64_decode($encodedData, true);
            if ($metadataJson === false || $metadataJson === '') {
                return null;
            }

            /** @var array<string,mixed> $metadata */
            $metadata = json_decode($metadataJson, true, 512, JSON_THROW_ON_ERROR);

            return ObjectInfo::fromArray($this->bucket, $metadata);
        });
    }

    /**
     * Marks an object as deleted by writing a metadata tombstone.
     *
     * @return Future<ObjectInfo>
     */
    public function delete(string $name): Future
    {
        return async(function () use ($name): ObjectInfo {
            $this->assertValidName($name);

            $info = [
                'name' => $name,
                'size' => 0,
                'digest' => '',
                'mtime' => gmdate('Y-m-d\TH:i:s\Z'),
                'deleted' => true,
                'chunk_subject' => '',
                'metadata' => [],
            ];

            $this->jetStream->publish($this->metaSubject($name), json_encode($info, JSON_THROW_ON_ERROR))->await();

            return ObjectInfo::fromArray($this->bucket, $info);
        });
    }

    /**
     * Watches metadata subjects and emits object metadata updates.
     *
     * @param callable(ObjectInfo):void $handler
     * @return Future<int>
     */
    public function watch(callable $handler, string $pattern = '>'): Future
    {
        return async(function () use ($handler, $pattern): int {
            return $this->client->subscribe($this->metaPrefix() . $pattern, function (NatsMessage $message) use ($handler): void {
                /** @var array<string,mixed> $metadata */
                $metadata = json_decode($message->payload, true, 512, JSON_THROW_ON_ERROR);
                $handler(ObjectInfo::fromArray($this->bucket, $metadata));
            })->await();
        });
    }

    /**
     * Lists latest metadata records for objects in this bucket.
     *
     * @return Future<list<ObjectInfo>>
     */
    public function list(bool $includeDeleted = false): Future
    {
        return async(function () use ($includeDeleted): array {
            $status = $this->getStatus()->await();
            $lastSequence = (int) ($status['last_sequence'] ?? 0);
            $latestByName = [];

            for ($seq = 1; $seq <= $lastSequence; $seq++) {
                $message = $this->requestObjectMessageBySequence($seq);
                if ($message === null) {
                    continue;
                }

                $subject = (string) ($message['subject'] ?? '');
                if (!str_starts_with($subject, $this->metaPrefix())) {
                    continue;
                }

                $metadata = $this->decodeMetadataFromApiMessage($message);
                if ($metadata === null) {
                    continue;
                }

                $info = ObjectInfo::fromArray($this->bucket, $metadata);
                if ($info->name === '') {
                    continue;
                }

                $latestByName[$info->name] = $info;
            }

            $result = [];
            foreach ($latestByName as $info) {
                if (!$includeDeleted && $info->deleted) {
                    continue;
                }

                $result[] = $info;
            }

            return $result;
        });
    }

    /**
     * Returns object bucket status derived from stream state.
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
                'last_sequence' => (int) ($state['last_seq'] ?? 0),
                'bytes' => (int) ($state['bytes'] ?? 0),
                'subjects' => is_array($state['subjects'] ?? null) ? $state['subjects'] : [],
            ];
        });
    }

    /**
     * Returns Object Store stream name.
     */
    public function streamName(): string
    {
        return 'OBJ_' . $this->bucket;
    }

    /**
     * Returns chunk subject prefix.
     */
    public function chunkPrefix(): string
    {
        return '$O.' . $this->bucket . '.C.';
    }

    /**
     * Returns metadata subject prefix.
     */
    public function metaPrefix(): string
    {
        return '$O.' . $this->bucket . '.M.';
    }

    /**
     * Resolves metadata subject for an object name.
     */
    private function metaSubject(string $name): string
    {
        return $this->metaPrefix() . $name;
    }

    /**
     * @return array<string,mixed>
     */
    private function requestStreamMessage(string $subject): array
    {
        $apiSubject = JetStreamApi::STREAM_MSG_GET_PREFIX . $this->streamName();
        $payload = json_encode(['last_by_subj' => $subject], JSON_THROW_ON_ERROR);
        $message = $this->client->request($apiSubject, $payload)->await();

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
    private function requestObjectMessageBySequence(int $sequence): ?array
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
     * @return array<string,mixed>|null
     */
    private function decodeMetadataFromApiMessage(array $message): ?array
    {
        $encodedData = (string) ($message['data'] ?? '');
        if ($encodedData === '') {
            return null;
        }

        $decoded = base64_decode($encodedData, true);
        if ($decoded === false || $decoded === '') {
            return null;
        }

        /** @var array<string,mixed>|null $metadata */
        $metadata = json_decode($decoded, true);

        return is_array($metadata) ? $metadata : null;
    }

    /**
     * Validates object names against wildcard and whitespace usage.
     */
    private function assertValidName(string $name): void
    {
        if ($name === '' || preg_match('/[\s*>]/', $name)) {
            throw new JetStreamException('Invalid object name');
        }
    }
}
