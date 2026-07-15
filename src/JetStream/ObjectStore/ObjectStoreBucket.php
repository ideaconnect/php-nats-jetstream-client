<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\ObjectStore;

use Amp\Future;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsHeaders;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\ApiErrCode;
use IDCT\NATS\JetStream\JetStreamApi;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\JetStream\JetStreamRequest;
use IDCT\NATS\JetStream\Models\StreamInfo;

use function Amp\async;

/**
 * Implements NATS JetStream Object Store bucket operations using the official on-wire layout
 * (meta subjects keyed by base64url(name), chunks under a per-object NUID subject, SHA-256 digest
 * in base64url, and rollup meta), so buckets interoperate with the `nats` CLI and other clients.
 */
final class ObjectStoreBucket
{
    private const DEFAULT_CHUNK_SIZE = 131072; // 128 KiB

    /**
     * Number of chunks pulled per download batch. Bounds peak memory to roughly
     * DOWNLOAD_BATCH_CHUNKS * chunkSize while replacing the previous one-request-per-chunk pattern,
     * so large objects download in O(chunks / DOWNLOAD_BATCH_CHUNKS) round-trips instead of O(chunks).
     */
    private const DOWNLOAD_BATCH_CHUNKS = 64;

    /** Per-batch pull expiry in milliseconds for chunk downloads. */
    private const DOWNLOAD_BATCH_EXPIRES_MS = 10000;

    /**
     * Maximum number of chunk publishes kept in flight at once during an upload. Bounds outstanding
     * PubAcks/memory while letting their round-trips overlap instead of running strictly serially.
     */
    private const UPLOAD_PIPELINE_DEPTH = 16;

    /** Max link hops followed when resolving an object link, to bound link cycles (#59). */
    private const MAX_LINK_HOPS = 8;

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
        // A non-positive chunk size would make put()/putStream() loop forever (no chunk is ever full).
        if ($this->chunkSize <= 0) {
            throw new JetStreamException('Object Store chunk size must be a positive number of bytes');
        }
    }

    /**
     * Creates or updates the underlying Object Store stream. Accepts either a raw stream-config array
     * or a typed {@see ObjectStoreConfig} (#39: TTL / MaxBytes / Storage / Replicas / Placement /
     * compression).
     *
     * @param array<string,mixed>|ObjectStoreConfig $options
     * @return Future<StreamInfo>
     */
    public function create(array|ObjectStoreConfig $options = []): Future
    {
        $resolved = $options instanceof ObjectStoreConfig ? $options->toStreamConfig() : $options;

        return async(function () use ($resolved): StreamInfo {
            $defaults = [
                'description' => 'Object Store bucket ' . $this->bucket,
                'allow_direct' => true,
                'allow_rollup_hdrs' => true,
                'discard' => 'new',
            ];

            return $this->jetStream->createStream(
                $this->streamName(),
                [$this->chunkPrefix() . '>', $this->metaPrefix() . '>'],
                array_merge($defaults, $resolved),
            )->await();
        });
    }

    /**
     * Seals the bucket: makes it permanently read-only (no further writes/deletes). Irreversible.
     * Mirrors nats.go / nats.java `ObjectStore.Seal` (#38).
     *
     * @return Future<bool>
     */
    public function seal(): Future
    {
        return async(function (): bool {
            $info = $this->jetStream->getStream($this->streamName())->await();
            /** @var array<string,mixed> $config */
            $config = is_array($info->raw['config'] ?? null) ? $info->raw['config'] : [];

            // Empty JSON objects in the fetched config (e.g. consumer_limits {}) decode to PHP [] and
            // would re-encode as a JSON array the server rejects. Drop empty-array fields - the server
            // re-applies their defaults - while preserving scalar/non-empty config (max_bytes, etc.).
            $config = array_filter($config, static fn($value): bool => $value !== []);
            $config['sealed'] = true;

            $this->jetStream->updateStream($this->streamName(), $config)->await();

            return true;
        });
    }

    /**
     * Creates a link object pointing at another object (by name) in this or another bucket. Mirrors
     * nats.go / nats.java `ObjectStore.AddLink` (#48). The link stores no content; resolving it on
     * read is tracked separately.
     *
     * @return Future<ObjectInfo>
     */
    public function addLink(string $name, string $targetName, ?string $targetBucket = null): Future
    {
        return async(function () use ($name, $targetName, $targetBucket): ObjectInfo {
            $this->assertValidName($name);

            $info = $this->linkMeta($name, ['bucket' => $targetBucket ?? $this->bucket, 'name' => $targetName]);
            $this->publishMeta($name, $info);

            return ObjectInfo::fromArray($this->bucket, $info);
        });
    }

    /**
     * Creates a link object pointing at a whole bucket. Mirrors nats.go / nats.java
     * `ObjectStore.AddBucketLink` (#48).
     *
     * @return Future<ObjectInfo>
     */
    public function addBucketLink(string $name, string $targetBucket): Future
    {
        return async(function () use ($name, $targetBucket): ObjectInfo {
            $this->assertValidName($name);

            $info = $this->linkMeta($name, ['bucket' => $targetBucket]);
            $this->publishMeta($name, $info);

            return ObjectInfo::fromArray($this->bucket, $info);
        });
    }

    /**
     * Builds a link meta record (no content; carries options.link).
     *
     * @param array{bucket:string,name?:string} $link
     * @return array<string,mixed>
     */
    private function linkMeta(string $name, array $link): array
    {
        return [
            'name' => $name,
            'bucket' => $this->bucket,
            'nuid' => '',
            'size' => 0,
            'chunks' => 0,
            'digest' => '',
            'mtime' => gmdate('Y-m-d\TH:i:s\Z'),
            'deleted' => false,
            'options' => ['link' => $link],
            'metadata' => [],
        ];
    }

    /**
     * Builds a live (non-deleted) object meta record with this bucket's chunk-size option and a fresh
     * mtime. The caller supplies the fields that vary per operation; put()/putStream()/rename() differ
     * only in where those come from. Add 'description' to the returned record afterwards when present.
     *
     * @param array<string,mixed> $metadata
     * @return array<string,mixed>
     */
    private function objectMeta(string $name, string $nuid, int $size, int $chunks, string $digest, array $metadata): array
    {
        return [
            'name' => $name,
            'bucket' => $this->bucket,
            'nuid' => $nuid,
            'size' => $size,
            'chunks' => $chunks,
            'digest' => $digest,
            'mtime' => gmdate('Y-m-d\TH:i:s\Z'),
            'deleted' => false,
            'options' => ['max_chunk_size' => $this->chunkSize],
            'metadata' => $metadata,
        ];
    }

    /**
     * Builds a deleted-object tombstone meta record (empty content, deleted=true) - published by both
     * delete() and rename()'s old-name tombstone, which were byte-identical hand-built copies.
     *
     * @return array<string,mixed>
     */
    private function tombstoneMeta(string $name): array
    {
        return [
            'name' => $name,
            'bucket' => $this->bucket,
            'nuid' => '',
            'size' => 0,
            'chunks' => 0,
            'digest' => '',
            'mtime' => gmdate('Y-m-d\TH:i:s\Z'),
            'deleted' => true,
            'options' => ['max_chunk_size' => $this->chunkSize],
            'metadata' => [],
        ];
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
     * Stores an object payload and publishes metadata, purging any previous revision's chunks.
     *
     * @param array<string,string> $metadata
     * @param string|null $description Optional human-readable object description (#58).
     * @return Future<ObjectInfo>
     */
    public function put(string $name, string $data, array $metadata = [], ?string $description = null): Future
    {
        return async(function () use ($name, $data, $metadata, $description): ObjectInfo {
            $this->assertValidName($name);

            // Run the previous-revision lookup concurrently with the upload below: it is only needed
            // to purge the old chunks afterwards, so awaiting it here would add a serial round-trip.
            $previousFuture = $this->lookupExisting($name);

            $nuid = $this->nuid();
            $chunkSubject = $this->chunkSubjectForNuid($nuid);
            $totalSize = strlen($data);
            $chunks = 0;

            if ($totalSize === 0) {
                // An empty object stores no chunks (matches the official Object Store layout, where a
                // 0-byte object has chunks=0). Previously it published one empty chunk and set chunks=1.
                $chunks = 0;
            } elseif ($totalSize <= $this->chunkSize) {
                $this->jetStream->publish($chunkSubject, $data)->await();
                $chunks = 1;
            } else {
                // Pipeline chunk publishes in bounded in-flight windows instead of one full PubAck
                // round-trip per chunk. The PUB frames are written to the single connection in chunk
                // order (writes are serialized in call order), so stream order - and therefore
                // download reassembly - is preserved; the acks for a window are awaited together.
                $pending = [];
                $offset = 0;
                while ($offset < $totalSize) {
                    $chunk = substr($data, $offset, $this->chunkSize);
                    $pending[] = $this->jetStream->publish($chunkSubject, $chunk);
                    $offset += strlen($chunk);
                    $chunks++;

                    if (count($pending) >= self::UPLOAD_PIPELINE_DEPTH) {
                        Future\await($pending);
                        $pending = [];
                    }
                }

                if ($pending !== []) {
                    Future\await($pending);
                }
            }

            $info = $this->objectMeta($name, $nuid, $totalSize, $chunks, $this->digestOf($data), $metadata);
            if ($description !== null && $description !== '') {
                $info['description'] = $description;
            }

            $this->publishMeta($name, $info);

            // Best-effort cleanup of the previous revision's chunks (rollup keeps only the latest
            // meta, but chunk subjects are NUID-specific and must be purged explicitly).
            $previous = $previousFuture->await();
            if ($previous !== null && $previous->nuid !== '' && $previous->nuid !== $nuid) {
                $this->purgeChunks($previous->nuid);
            }

            return ObjectInfo::fromArray($this->bucket, $info);
        });
    }

    /**
     * Stores an object by pulling its bytes from a producer callback, so the whole payload need not
     * be held in memory at once (the streaming counterpart to getToCallback()). The producer returns
     * successive data blocks of any size and null when the object is complete; the bytes are
     * re-chunked to chunkSize, published in bounded in-flight windows, and the SHA-256 digest is
     * computed incrementally. Memory is bounded to roughly chunkSize plus the pipeline window.
     *
     * @param callable(): ?string $producer Returns the next data block, or null at end of stream.
     * @param array<string,string> $metadata
     * @return Future<ObjectInfo>
     */
    public function putStream(string $name, callable $producer, array $metadata = []): Future
    {
        return async(function () use ($name, $producer, $metadata): ObjectInfo {
            $this->assertValidName($name);

            // Run the previous-revision lookup concurrently with the upload (only needed for purge).
            $previousFuture = $this->lookupExisting($name);

            $nuid = $this->nuid();
            $chunkSubject = $this->chunkSubjectForNuid($nuid);
            $hashContext = hash_init('sha256');
            $totalSize = 0;
            $chunks = 0;
            $buffer = '';
            /** @var list<Future<\IDCT\NATS\JetStream\Models\PubAck>> $pending */
            $pending = [];

            $publishChunk = function (string $chunk) use (&$pending, &$chunks, $chunkSubject): void {
                $pending[] = $this->jetStream->publish($chunkSubject, $chunk);
                ++$chunks;

                if (count($pending) >= self::UPLOAD_PIPELINE_DEPTH) {
                    Future\await($pending);
                    $pending = [];
                }
            };

            while (true) {
                $block = $producer();
                if ($block === null) {
                    break;
                }

                if ($block === '') {
                    continue;
                }

                hash_update($hashContext, $block);
                $totalSize += strlen($block);
                $buffer .= $block;

                // Emit whole chunks by advancing an offset, then drop the consumed prefix ONCE per
                // producer block. Recopying the shrinking tail per chunk (substr after each publish)
                // would be O(n^2) for a block much larger than chunkSize.
                if (strlen($buffer) >= $this->chunkSize) {
                    $offset = 0;
                    while (strlen($buffer) - $offset >= $this->chunkSize) {
                        $publishChunk(substr($buffer, $offset, $this->chunkSize));
                        $offset += $this->chunkSize;
                    }

                    $buffer = substr($buffer, $offset);
                }
            }

            if ($buffer !== '') {
                $publishChunk($buffer);
            }

            if ($pending !== []) {
                Future\await($pending);
            }

            $info = $this->objectMeta($name, $nuid, $totalSize, $chunks, $this->finalizeDigest($hashContext), $metadata);

            $this->publishMeta($name, $info);

            $previous = $previousFuture->await();
            if ($previous !== null && $previous->nuid !== '' && $previous->nuid !== $nuid) {
                $this->purgeChunks($previous->nuid);
            }

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
        return $this->getInternal($name, 0);
    }

    /**
     * Reads an object, transparently following an object link to its target (in this or another
     * bucket), bounded against link cycles. Mirrors nats.go / nats.java link-aware `Get` (#59).
     *
     * @return Future<ObjectData|null>
     */
    private function getInternal(string $name, int $depth): Future
    {
        return async(function () use ($name, $depth): ?ObjectData {
            if ($depth > self::MAX_LINK_HOPS) {
                throw new JetStreamException('Too many Object Store link hops resolving "' . $name . '"');
            }

            $info = $this->info($name)->await();
            if ($info === null) {
                return null;
            }

            if ($info->isLink()) {
                $target = $this->linkTargetBucket($info, $name);

                return $target->getInternal((string) ($info->link['name'] ?? ''), $depth + 1)->await();
            }

            if ($info->deleted) {
                // A deleted (tombstoned) object has no content; reading it returns null, consistent
                // with a missing object and with the official client's not-found semantics. The
                // tombstone metadata remains observable via info().
                return null;
            }

            $assembled = '';
            $actualDigest = $this->streamChunks($info, static function (string $chunk) use (&$assembled): void {
                $assembled .= $chunk;
            });

            $this->verifyDigest($info, $actualDigest);

            return new ObjectData($info, $assembled);
        });
    }

    /**
     * Resolves the bucket holding a link's target object, rejecting bucket links (which have no object
     * content to read).
     */
    private function linkTargetBucket(ObjectInfo $info, string $name): self
    {
        if (!isset($info->link['name']) || $info->link['name'] === '') {
            throw new JetStreamException('Cannot get() the bucket link "' . $name . '": it points to a bucket, not an object');
        }

        $targetBucket = $info->link['bucket'] ?? $this->bucket;

        return $targetBucket === $this->bucket ? $this : $this->jetStream->objectStore($targetBucket);
    }

    /**
     * Streams object payload to a callback chunk-by-chunk without buffering the whole object.
     *
     * The callback is invoked once per stored chunk as it is downloaded, so large objects do not
     * have to be held in memory. The content digest is computed incrementally and verified after
     * the final chunk.
     *
     * @param callable(string):void $chunkHandler
     * @return Future<ObjectInfo|null>
     */
    public function getToCallback(string $name, callable $chunkHandler): Future
    {
        return $this->getToCallbackInternal($name, $chunkHandler, 0);
    }

    /**
     * Streaming counterpart to {@see getInternal()}: follows object links to the target, bounded
     * against cycles, then streams the target's chunks to the callback (#59).
     *
     * @param callable(string):void $chunkHandler
     * @return Future<ObjectInfo|null>
     */
    private function getToCallbackInternal(string $name, callable $chunkHandler, int $depth): Future
    {
        return async(function () use ($name, $chunkHandler, $depth): ?ObjectInfo {
            if ($depth > self::MAX_LINK_HOPS) {
                throw new JetStreamException('Too many Object Store link hops resolving "' . $name . '"');
            }

            $info = $this->info($name)->await();
            if ($info === null) {
                return null;
            }

            if ($info->isLink()) {
                $target = $this->linkTargetBucket($info, $name);

                return $target->getToCallbackInternal((string) ($info->link['name'] ?? ''), $chunkHandler, $depth + 1)->await();
            }

            if ($info->deleted) {
                // A deleted (tombstoned) object has no content to stream; return null (the callback is
                // not invoked), consistent with get() and a missing object. info() still reveals the
                // tombstone.
                return null;
            }

            $actualDigest = $this->streamChunks($info, $chunkHandler);
            $this->verifyDigest($info, $actualDigest);

            return $info;
        });
    }

    /**
     * Downloads an object's chunks via a transient ephemeral consumer, forwarding each chunk to the
     * callback and computing the SHA-256 content digest incrementally. Returns the computed digest
     * string ("SHA-256=...") so callers can verify integrity without re-buffering the payload.
     *
     * Chunks are pulled in bounded batches (DOWNLOAD_BATCH_CHUNKS at a time) rather than one pull
     * request per chunk, so large objects download in far fewer round-trips while peak memory stays
     * bounded (the whole object is never held in memory). JetStream delivers chunks of a single
     * filtered consumer in stream order, preserving in-order assembly.
     *
     * @param callable(string):void $onChunk
     */
    private function streamChunks(ObjectInfo $info, callable $onChunk): string
    {
        $expected = $info->chunks;
        $hashContext = hash_init('sha256');

        // An empty object stores no chunks: there is nothing to pull and the digest is over zero
        // bytes. Pulling anyway would block until the batch expiry (no chunk ever arrives), so
        // short-circuit to the empty-content digest (in the same SHA-256=base64url format as below).
        if ($expected <= 0) {
            return $this->finalizeDigest($hashContext);
        }

        // Fast path for a single-chunk object (the common case for small objects): the lone chunk is
        // the only message on its NUID subject, so fetch it with one Direct Get instead of creating,
        // pulling from, and deleting a transient ephemeral consumer (4 round-trips -> 1).
        if ($expected === 1) {
            try {
                $message = $this->jetStream
                    ->directGetLastMessageForSubject($this->streamName(), $this->chunkSubjectForNuid($info->nuid))
                    ->await();

                hash_update($hashContext, $message->payload);
                $onChunk($message->payload);

                return $this->finalizeDigest($hashContext);
            } catch (JetStreamException $e) {
                if ($e->getCode() === 404) {
                    throw new JetStreamException('Incomplete object download: expected 1 chunks, received 0');
                }

                if ($e->getCode() !== 503) {
                    throw $e;
                }

                // Direct Get unavailable (allow_direct disabled / legacy stream): fall through to the
                // ephemeral-consumer path below. The await threw before hashing, so the digest is clean.
            }
        }

        $remaining = $expected;
        $received = 0;

        $consumerName = null;
        try {
            // A read-only download uses ack_policy=none: there is nothing to ack, and an explicit
            // policy risks ack_wait redelivery on a slow link re-hashing a chunk into a digest mismatch.
            $consumer = $this->jetStream->createEphemeralConsumer(
                $this->streamName(),
                $this->chunkSubjectForNuid($info->nuid),
                ['deliver_policy' => 'all', 'ack_policy' => 'none'],
            )->await();
            $consumerName = $consumer->name;

            while ($remaining > 0) {
                $batch = min(self::DOWNLOAD_BATCH_CHUNKS, $remaining);

                try {
                    $messages = $this->jetStream->fetchBatch(
                        $this->streamName(),
                        $consumerName,
                        $batch,
                        self::DOWNLOAD_BATCH_EXPIRES_MS,
                    )->await();
                } catch (JetStreamException $e) {
                    if ($e->getCode() === 408) {
                        // No further chunks available; the completeness check below catches a shortfall.
                        break;
                    }

                    throw $e;
                }

                foreach ($messages as $message) {
                    hash_update($hashContext, $message->payload);
                    $onChunk($message->payload);
                    ++$received;
                }

                // Decrement by what actually arrived and pull again for the rest. A short batch is
                // not treated as "drained" here: fetchBatch() can return fewer than requested when
                // its window expires mid-delivery on a slow link, so the next iteration keeps going.
                // The empty case surfaces as a 408 above and breaks the loop.
                $remaining -= count($messages);
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

        // Digest-independent completeness gate: a truncated download must fail even when the
        // metadata carries no digest to verify against (e.g. an object written by another client).
        // ($expected is > 0 here; the empty-object case returned early above.)
        if ($received < $expected) {
            throw new JetStreamException(sprintf(
                'Incomplete object download: expected %d chunks, received %d',
                $expected,
                $received,
            ));
        }

        return $this->finalizeDigest($hashContext);
    }

    /**
     * Verifies a downloaded object's computed digest against the digest recorded in its metadata.
     */
    private function verifyDigest(ObjectInfo $info, string $actualDigest): void
    {
        if ($info->digest === '') {
            return;
        }

        // Compare the decoded digest BYTES, not the encoded strings: the metadata digest may use
        // unpadded base64url (some non-Go clients) while we compute padded, so a plain string compare
        // would spuriously reject a byte-identical object. hash_equals is constant-time.
        $expected = $this->decodeDigest($info->digest);
        $actual = $this->decodeDigest($actualDigest);

        if ($expected === null || $actual === null || !hash_equals($expected, $actual)) {
            throw new JetStreamException(
                'Object digest mismatch: expected ' . $info->digest . ', got ' . $actualDigest,
            );
        }
    }

    /**
     * Decodes a "SHA-256=<base64url>" digest to its raw bytes, tolerating missing base64url padding.
     * Returns null when the value is not a recognizable SHA-256 digest.
     */
    private function decodeDigest(string $digest): ?string
    {
        $prefix = 'SHA-256=';
        if (!str_starts_with($digest, $prefix)) {
            return null;
        }

        $encoded = strtr(substr($digest, strlen($prefix)), '-_', '+/');
        $remainder = strlen($encoded) % 4;
        if ($remainder > 0) {
            $encoded .= str_repeat('=', 4 - $remainder);
        }

        $raw = base64_decode($encoded, true);

        return $raw === false ? null : $raw;
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

            // Read the latest meta record via the Direct Get API (served by any replica, not just the
            // leader), consistent with list(). The Direct Get body is the raw meta JSON; 404 = absent.
            try {
                $message = $this->jetStream
                    ->directGetLastMessageForSubject($this->streamName(), $this->metaSubject($name))
                    ->await();
            } catch (JetStreamException $e) {
                if ($e->getCode() === 404) {
                    return null;
                }

                if ($e->getCode() === 503) {
                    // Direct Get unavailable (allow_direct disabled / legacy stream); fall back to the
                    // leader STREAM.MSG.GET path so reads still work on interop buckets.
                    return $this->fetchInfo($name, false);
                }

                throw $e;
            }

            // The record's stream sequence (its revision) travels in the Direct Get Nats-Sequence
            // header, not in the meta JSON; surface it on ObjectInfo.
            $headers = NatsHeaders::fromWireBlock($message->rawHeaders);

            if (($headers['Nats-Marker-Reason'] ?? '') !== '') {
                // Server-written subject delete-marker (ADR-43): the object's meta aged out / was
                // purged, so the object is effectively absent.
                return null;
            }

            /** @var array<string,mixed>|null $metadata */
            $metadata = json_decode($message->payload, true);
            if (!is_array($metadata)) {
                return null;
            }

            $revision = isset($headers['Nats-Sequence']) ? (int) $headers['Nats-Sequence'] : null;

            return ObjectInfo::fromArray($this->bucket, $metadata, $revision);
        });
    }

    /**
     * Best-effort metadata lookup for the put()/delete() chunk-purge cleanup (or null when absent).
     * ANY JetStream error maps to null. Kept on the leader STREAM.MSG.GET path (not Direct Get) and
     * synchronous within the calling coroutine so a concurrently-launched lookup issues its request
     * at the same nesting depth as the upload's publishes, keeping the request/SID order deterministic.
     */
    private function fetchInfo(string $name, bool $swallowErrors): ?ObjectInfo
    {
        $this->assertValidName($name);

        try {
            $response = $this->requestStreamMessage($this->metaSubject($name));
        } catch (JetStreamException $e) {
            if ($swallowErrors || $e->getCode() === 404) {
                return null;
            }

            throw $e;
        }

        /** @var array<string,mixed>|null $message */
        $message = is_array($response['message'] ?? null) ? $response['message'] : null;
        if ($message === null) {
            return null;
        }

        $metadata = $this->decodeMetadataFromApiMessage($message);
        if ($metadata === null) {
            return null;
        }

        $revision = isset($message['seq']) ? (int) $message['seq'] : null;

        return ObjectInfo::fromArray($this->bucket, $metadata, $revision);
    }

    /**
     * Marks an object as deleted by writing a metadata tombstone and purging its chunks.
     *
     * @return Future<ObjectInfo>
     */
    public function delete(string $name): Future
    {
        return async(function () use ($name): ObjectInfo {
            $this->assertValidName($name);

            // Run the lookup concurrently with the tombstone publish; only needed for chunk purge.
            $previousFuture = $this->lookupExisting($name);

            $info = $this->tombstoneMeta($name);

            $this->publishMeta($name, $info);

            $previous = $previousFuture->await();
            if ($previous !== null && $previous->nuid !== '') {
                $this->purgeChunks($previous->nuid);
            }

            return ObjectInfo::fromArray($this->bucket, $info);
        });
    }

    /**
     * Updates an object's metadata (rename and/or replace its metadata bag) WITHOUT re-uploading its
     * bytes - the stored chunks are referenced by NUID, which is preserved. Mirrors nats.go
     * `ObjectStore.UpdateMeta` / nats.java `ObjectStore.updateMeta` (#28).
     *
     * On rename the new meta is written under the new name and the old name is tombstoned (its chunks
     * are NOT purged - they now belong to the renamed object). Renaming onto an existing live object is
     * rejected.
     *
     * @param string|null               $newName  New object name, or null to keep the current name.
     * @param array<string,string>|null $metadata Replacement metadata bag, or null to keep the current one.
     * @return Future<ObjectInfo>
     */
    public function updateMeta(string $name, ?string $newName = null, ?array $metadata = null): Future
    {
        return async(function () use ($name, $newName, $metadata): ObjectInfo {
            $this->assertValidName($name);

            $existing = $this->info($name)->await();
            if ($existing === null || $existing->deleted) {
                throw new JetStreamException('Object not found: ' . $name, 404);
            }

            $isRename = $newName !== null && $newName !== $name;
            if ($isRename) {
                $this->assertValidName($newName);
                $clash = $this->info($newName)->await();
                if ($clash !== null && !$clash->deleted) {
                    throw new JetStreamException('Cannot rename to an existing object: ' . $newName);
                }
            }

            $targetName = $newName ?? $name;
            $info = $this->objectMeta($targetName, $existing->nuid, $existing->size, $existing->chunks, $existing->digest, $metadata ?? $existing->metadata);

            $this->publishMeta($targetName, $info);

            if ($isRename) {
                // Tombstone the old name so it no longer resolves; the chunks stay (same NUID, now
                // owned by the renamed object), so this must NOT purge chunks like delete() does.
                $this->publishMeta($name, $this->tombstoneMeta($name));
            }

            return ObjectInfo::fromArray($this->bucket, $info);
        });
    }

    /**
     * Watches metadata subjects and emits object metadata updates.
     *
     * With `$options = null` only updates published after the watch starts are delivered
     * (deliver_policy=new), preserving the original behavior. Pass an {@see ObjectStoreWatchOptions}
     * instance to opt into the reference ObjectStore.Watch semantics - replay the current metadata of
     * every existing object first, then follow (last_per_subject), or full history / explicit
     * updates-only (#98).
     *
     * @param callable(ObjectInfo):void $handler
     * @return Future<int>
     */
    public function watch(callable $handler, string $pattern = '>', ?ObjectStoreWatchOptions $options = null): Future
    {
        return async(function () use ($handler, $pattern, $options): int {
            $filter = $this->metaPrefix() . $pattern;

            // Deliver via a JetStream push consumer (not a plain core subscription) so each update
            // carries its stream sequence, exposed as the ObjectInfo revision (this push-consumer
            // mechanism is the part consistent with the KeyValue watch()). The read is ack-free.
            // Default (no options) keeps live-updates-only semantics (deliver_policy=new); supplying
            // options selects the reference snapshot-then-follow default (last_per_subject), full
            // history (all), or explicit updates-only (new).
            $consumerOptions = $options?->toConsumerConfig()
                ?? ['deliver_policy' => 'new', 'ack_policy' => 'none'];

            return $this->jetStream->subscribeEphemeralPushConsumer(
                $this->streamName(),
                function (NatsMessage $message) use ($handler): void {
                    $headers = NatsHeaders::fromWireBlock($message->rawHeaders);
                    if (($headers['Nats-Marker-Reason'] ?? '') !== '') {
                        // Server-written subject delete-marker (the object's meta aged out / was
                        // purged, ADR-43); not a real metadata update, so do not emit it.
                        return;
                    }

                    $metadata = json_decode($message->payload, true);
                    if (!is_array($metadata)) {
                        // Tolerate a malformed meta delivery instead of throwing out of the dispatch
                        // loop, which would abort delivery of buffered frames for other subscriptions.
                        return;
                    }

                    /** @var array<string,mixed> $metadata */
                    $handler(ObjectInfo::fromArray($this->bucket, $metadata, $this->jetStream->streamSequenceOf($message)));
                },
                filterSubject: $filter,
                consumerOptions: $consumerOptions,
            )->await();
        });
    }

    /**
     * Lists latest metadata records for objects in this bucket.
     *
     * Enumerates only the meta subjects (via a subjects filter) and reads the latest record per
     * subject, so cost is O(objects) rather than O(all stream messages).
     *
     * @return Future<list<ObjectInfo>>
     */
    public function list(bool $includeDeleted = false): Future
    {
        return async(function () use ($includeDeleted): array {
            $subjects = $this->metaSubjects();
            if ($subjects === []) {
                return [];
            }

            // Fast path (#110): one batched multi_last Direct Get (ADR-31) returns the latest meta record
            // of every object in a single request/reply, instead of one Direct Get round-trip per object.
            // Gated on server support (batched Direct Get needs NATS 2.11+); older servers take the
            // per-subject fan-out below. The returned objects and deleted-filtering are identical.
            if ($this->jetStream->supportsBatchedDirectGet()) {
                /** @var list<?ObjectInfo> $infos */
                $infos = $this->listBatched($subjects);
            } else {
                // Read the latest meta record per object CONCURRENTLY via the Direct Get API (served by
                // any replica; the round-trips overlap) instead of N+1 serial leader reads.
                $lookups = [];
                foreach ($subjects as $subject) {
                    $lookups[] = async(function () use ($subject): ?ObjectInfo {
                        try {
                            $message = $this->jetStream->directGetLastMessageForSubject($this->streamName(), $subject)->await();
                        } catch (JetStreamException $e) {
                            if ($e->getCode() === 404) {
                                return null;
                            }

                            throw $e;
                        }

                        // The Direct Get body is the raw meta JSON (not the base64 STREAM.MSG.GET envelope).
                        /** @var array<string,mixed>|null $metadata */
                        $metadata = json_decode($message->payload, true);
                        if (!is_array($metadata)) {
                            return null;
                        }

                        $headers = NatsHeaders::fromWireBlock($message->rawHeaders);
                        $revision = isset($headers['Nats-Sequence']) ? (int) $headers['Nats-Sequence'] : null;
                        $info = ObjectInfo::fromArray($this->bucket, $metadata, $revision);

                        return $info->name === '' ? null : $info;
                    });
                }

                /** @var list<?ObjectInfo> $infos */
                $infos = Future\await($lookups);
            }

            $result = [];
            foreach ($infos as $info) {
                if ($info === null) {
                    continue;
                }

                if (!$includeDeleted && $info->deleted) {
                    continue;
                }

                $result[] = $info;
            }

            return $result;
        });
    }

    /**
     * list() fast path: fetch the latest meta record of every object with ONE batched multi_last
     * Direct Get (ADR-31) instead of a per-subject fan-out. Produces the identical `list<ObjectInfo>`
     * before the caller's deleted-filtering: each reply body is the raw meta JSON (as with a single
     * Direct Get), the revision is read off the Nats-Sequence header, a nameless/malformed record is
     * dropped, and objects with no stored record are simply absent from the batch reply (mirroring the
     * fan-out's 404-skip). Only reached when {@see JetStreamContext::supportsBatchedDirectGet()}.
     *
     * @param list<string> $subjects
     * @return list<ObjectInfo>
     */
    private function listBatched(array $subjects): array
    {
        $messages = $this->jetStream->directGetLastForSubjects($this->streamName(), $subjects)->await();

        $infos = [];
        foreach ($messages as $message) {
            // The Direct Get body is the raw meta JSON (not the base64 STREAM.MSG.GET envelope).
            /** @var array<string,mixed>|null $metadata */
            $metadata = json_decode($message->payload, true);
            if (!is_array($metadata)) {
                continue;
            }

            $headers = NatsHeaders::fromWireBlock($message->rawHeaders);
            $revision = isset($headers['Nats-Sequence']) ? (int) $headers['Nats-Sequence'] : null;
            $info = ObjectInfo::fromArray($this->bucket, $metadata, $revision);
            if ($info->name === '') {
                continue;
            }

            $infos[] = $info;
        }

        return $infos;
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
     * Resolves the metadata subject for an object name (official base64url-of-name encoding).
     */
    private function metaSubject(string $name): string
    {
        return $this->metaPrefix() . $this->encodeName($name);
    }

    /**
     * Resolves the chunk subject for a given object NUID.
     */
    private function chunkSubjectForNuid(string $nuid): string
    {
        return $this->chunkPrefix() . $nuid;
    }

    /**
     * Looks up an existing object's metadata, returning null when absent (best-effort, swallows
     * not-found and lookup errors so a fresh put/delete is never blocked by it).
     */
    /**
     * Best-effort lookup of the existing object's metadata, returned as a Future so callers can run
     * it concurrently with the upload/meta publish and only await it before purging the previous
     * revision's chunks - saving a serial round-trip on the write path. Any error maps to null.
     *
     * @return Future<ObjectInfo|null>
     */
    private function lookupExisting(string $name): Future
    {
        return async(fn(): ?ObjectInfo => $this->fetchInfo($name, true));
    }

    /**
     * Publishes an object metadata record with a rollup header so only the latest record per object
     * is retained, then validates the publish acknowledgement.
     *
     * @param array<string,mixed> $info
     */
    private function publishMeta(string $name, array $info): void
    {
        // The official Object Store meta schema declares `metadata` as a map with `omitempty`. PHP's
        // json_encode([]) emits a JSON array (`"metadata":[]`), which the Go client cannot unmarshal into
        // map[string]string ("object-store meta information invalid"). Omit an empty map so the record
        // stays interoperable with the `nats` CLI / nats.go (#109).
        if (($info['metadata'] ?? null) === []) {
            unset($info['metadata']);
        }

        // Normalize a no-responders failure to JetStreamException(503) like jsRequest(), so a
        // JetStream-disabled server / unbound subject is catchable as JetStreamException (#161).
        $message = JetStreamRequest::withHeaders(
            $this->client,
            $this->metaSubject($name),
            json_encode($info, JSON_THROW_ON_ERROR),
            ['Nats-Rollup' => 'sub'],
        );

        /** @var array<string,mixed> $data */
        $data = json_decode($message->payload, true, 512, JSON_THROW_ON_ERROR);

        /** @var array<string,mixed>|null $error */
        $error = is_array($data['error'] ?? null) ? $data['error'] : null;
        if ($error !== null) {
            throw new JetStreamException(
                (string) ($error['description'] ?? 'JetStream publish error'),
                (int) ($error['code'] ?? 0),
                null,
                ApiErrCode::fromEnvelope($error),
            );
        }
    }

    /**
     * Best-effort purge of all chunk messages for a given object NUID.
     */
    private function purgeChunks(string $nuid): void
    {
        try {
            $this->jetStream->purgeStream(
                $this->streamName(),
                ['filter' => $this->chunkSubjectForNuid($nuid)],
            )->await();
        } catch (JetStreamException) {
            // Best-effort cleanup; never fail the surrounding operation on purge errors.
        }
    }

    /**
     * Enumerates the meta subjects currently present in the bucket using a subjects filter.
     *
     * @return list<string>
     */
    private function metaSubjects(): array
    {
        $apiSubject = JetStreamApi::STREAM_INFO_PREFIX . $this->streamName();

        // The STREAM.INFO subjects map is capped by the server, so a bucket with many objects must be
        // enumerated across pages (offset) - otherwise list() would silently truncate. The
        // no-new-subjects guard terminates safely even against a server that ignores `offset` (it
        // then returns the whole first page and the second page adds nothing).
        $collected = [];
        $offset = 0;

        do {
            $payload = json_encode([
                'subjects_filter' => $this->metaPrefix() . '>',
                'offset' => $offset,
            ], JSON_THROW_ON_ERROR);
            $message = $this->client->request($apiSubject, $payload)->await();

            /** @var array<string,mixed> $data */
            $data = json_decode($message->payload, true, 512, JSON_THROW_ON_ERROR);

            /** @var array<string,mixed>|null $error */
            $error = is_array($data['error'] ?? null) ? $data['error'] : null;
            if ($error !== null) {
                throw new JetStreamException(
                    (string) ($error['description'] ?? 'JetStream API error'),
                    (int) ($error['code'] ?? 0),
                    null,
                    ApiErrCode::fromEnvelope($error),
                );
            }

            /** @var array<string,mixed> $state */
            $state = is_array($data['state'] ?? null) ? $data['state'] : [];
            /** @var array<string,int> $subjects */
            $subjects = is_array($state['subjects'] ?? null) ? $state['subjects'] : [];

            $page = array_map('strval', array_keys($subjects));
            $newCount = 0;
            foreach ($page as $subject) {
                if (!isset($collected[$subject])) {
                    $collected[$subject] = true;
                    ++$newCount;
                }
            }

            $offset += count($page);
        } while ($page !== [] && $newCount > 0);

        return array_keys($collected);
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
            throw new JetStreamException($description, $code, null, ApiErrCode::fromEnvelope($error));
        }

        return $data;
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
     * Computes the official Object Store content digest ("SHA-256=" + base64url) for a buffer.
     */
    private function digestOf(string $data): string
    {
        return $this->formatDigest(hash('sha256', $data, true));
    }

    /**
     * Finalizes a streaming SHA-256 {@see \HashContext} into the official Object Store content digest
     * ("SHA-256=" + base64url) - the terminal step for chunked uploads/downloads.
     */
    private function finalizeDigest(\HashContext $hashContext): string
    {
        return $this->formatDigest(hash_final($hashContext, true));
    }

    /**
     * Formats a raw 32-byte SHA-256 digest as the official Object Store "SHA-256=<base64url>" string.
     */
    private function formatDigest(string $rawDigest): string
    {
        return 'SHA-256=' . $this->base64Url($rawDigest);
    }

    /**
     * Encodes an object name into its meta-subject token using URL-safe base64 (official layout).
     */
    private function encodeName(string $name): string
    {
        return $this->base64Url($name);
    }

    /**
     * URL-safe base64 encoding (with padding), matching the official Object Store encoding.
     */
    private function base64Url(string $bytes): string
    {
        return strtr(base64_encode($bytes), '+/', '-_');
    }

    /**
     * Generates a unique object NUID used as the chunk subject token and stored in metadata.
     */
    private function nuid(): string
    {
        return bin2hex(random_bytes(11));
    }

    /**
     * Validates object names. Names are base64url-encoded into the meta subject, so any non-empty
     * name is acceptable (the encoding keeps the wire subject valid).
     */
    private function assertValidName(string $name): void
    {
        if ($name === '') {
            throw new JetStreamException('Invalid object name');
        }
    }
}
