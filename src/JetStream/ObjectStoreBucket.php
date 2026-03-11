<?php

declare(strict_types=1);

namespace Idct\Nats\JetStream;

use Amp\Future;
use Idct\Nats\Core\NatsClient;
use Idct\Nats\Core\NatsMessage;
use Idct\Nats\Exception\JetStreamException;
use function Amp\async;

final class ObjectStoreBucket
{
    /**
     * Creates an Object Store bucket context bound to a client and bucket name.
     */
    public function __construct(
        private readonly NatsClient $client,
        private readonly JetStreamContext $jetStream,
        private readonly string $bucket,
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
            $this->jetStream->publish($chunkSubject, $data)->await();

            $info = [
                'name' => $name,
                'size' => strlen($data),
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

            $chunkResponse = $this->requestStreamMessage($info->chunkSubject);

            /** @var array<string,mixed>|null $message */
            $message = is_array($chunkResponse['message'] ?? null) ? $chunkResponse['message'] : null;
            if ($message === null) {
                return new ObjectData($info, null);
            }

            $encodedData = (string) ($message['data'] ?? '');
            $payload = $encodedData === '' ? '' : base64_decode($encodedData, true);

            return new ObjectData($info, $payload === false ? null : $payload);
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
     * Validates object names against wildcard and whitespace usage.
     */
    private function assertValidName(string $name): void
    {
        if ($name === '' || str_contains($name, ' ') || str_contains($name, '*') || str_contains($name, '>')) {
            throw new JetStreamException('Invalid object name');
        }
    }
}
