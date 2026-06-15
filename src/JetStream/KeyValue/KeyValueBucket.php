<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream\KeyValue;

use Amp\CancelledException;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Core\Inbox;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsHeaders;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\JetStream\JetStreamApi;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\JetStream\MessageTtl;
use IDCT\NATS\JetStream\Models\ConsumerInfo;
use IDCT\NATS\JetStream\Models\JsMessageMetadata;
use IDCT\NATS\JetStream\Models\PubAck;
use IDCT\NATS\JetStream\Models\StreamInfo;

use function Amp\async;
use function Amp\delay;

/**
 * Implements NATS JetStream Key-Value bucket operations.
 */
final class KeyValueBucket
{
    /**
     * Creates a KV bucket context bound to a client and bucket name.
     *
     * @param NatsClient $client Connected client used for publish/subscribe operations behind KV APIs.
     * @param JetStreamContext $jetStream JetStream context used for stream management and API request routing.
     * @param string $bucket Logical bucket name. It is mapped to KV stream and subject prefixes (`KV_<bucket>`, `$KV.<bucket>.>`).
     */
    public function __construct(
        private readonly NatsClient $client,
        private readonly JetStreamContext $jetStream,
        private readonly string $bucket,
    ) {}

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

            $mapped = $this->mapKvOptions($options);

            // Mirror/source KV buckets reference other buckets by name; translate them to the backing
            // `KV_`-prefixed stream names (#62). A mirrored bucket defines no subjects of its own.
            $subjects = [$this->subjectPrefix() . '>'];
            if (isset($mapped['mirror'])) {
                $mapped['mirror'] = $this->kvSourceConfig($mapped['mirror']);
                $subjects = [];
            }
            if (isset($mapped['sources']) && is_array($mapped['sources'])) {
                $mapped['sources'] = array_values(array_map(
                    fn(string|array $source): array => $this->kvSourceConfig($source),
                    $mapped['sources'],
                ));
            }

            return $this->jetStream->createStream(
                $this->streamName(),
                $subjects,
                array_merge($defaults, $mapped),
            )->await();
        });
    }

    /**
     * Normalizes a KV mirror/source entry to a stream-source config: a bare bucket name (or a `bucket`
     * key) becomes the backing `KV_`-prefixed stream name; an explicit `name` is used as-is.
     *
     * @param string|array<string,mixed> $source
     * @return array<string,mixed>
     */
    private function kvSourceConfig(string|array $source): array
    {
        if (is_string($source)) {
            return ['name' => 'KV_' . $source];
        }

        if (isset($source['bucket'])) {
            $source['name'] = 'KV_' . $source['bucket'];
            unset($source['bucket']);
        }

        return $source;
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
     * Puts a value for the provided key, optionally with a per-key TTL after which the entry expires
     * (`Nats-TTL`; requires the bucket/stream to be created with `allow_msg_ttl`). The TTL accepts an
     * integer number of seconds, a Go duration string, or "never".
     *
     * @param int|string|null $ttl Optional per-key TTL.
     * @return Future<PubAck>
     */
    public function put(string $key, string $value, int|string|null $ttl = null): Future
    {
        return async(function () use ($key, $value, $ttl): PubAck {
            $this->assertValidKey($key);

            return $this->jetStream->publish($this->subjectForKey($key), $value, ttl: $ttl)->await();
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
     * Marks a key as deleted, optionally with a tombstone TTL after which the delete marker itself
     * ages out (`Nats-TTL`; requires `allow_msg_ttl` on the bucket/stream).
     *
     * @param int|string|null $tombstoneTtl Optional TTL for the delete marker.
     * @param int|null $expectedRevision Optional compare-and-delete: only delete when this is the key's
     *                                   current revision (else the server rejects with a JetStreamException).
     * @return Future<PubAck>
     */
    public function delete(string $key, int|string|null $tombstoneTtl = null, ?int $expectedRevision = null): Future
    {
        return async(function () use ($key, $tombstoneTtl, $expectedRevision): PubAck {
            $this->assertValidKey($key);

            $headers = ['KV-Operation' => 'DEL'];
            if ($tombstoneTtl !== null) {
                $headers['Nats-TTL'] = MessageTtl::format($tombstoneTtl);
            }
            if ($expectedRevision !== null) {
                $headers['Nats-Expected-Last-Subject-Sequence'] = (string) $expectedRevision;
            }

            return $this->publishWithHeadersAck($this->subjectForKey($key), '', $headers)->await();
        });
    }

    /**
     * Purges key history and writes a purge marker, optionally with a tombstone TTL after which the
     * purge marker itself ages out (`Nats-TTL`; requires `allow_msg_ttl` on the bucket/stream).
     *
     * @param int|string|null $tombstoneTtl Optional TTL for the purge marker.
     * @param int|null $expectedRevision Optional compare-and-purge: only purge when this is the key's
     *                                   current revision (else the server rejects with a JetStreamException).
     * @return Future<PubAck>
     */
    public function purge(string $key, int|string|null $tombstoneTtl = null, ?int $expectedRevision = null): Future
    {
        return async(function () use ($key, $tombstoneTtl, $expectedRevision): PubAck {
            $this->assertValidKey($key);

            $headers = ['KV-Operation' => 'PURGE', 'Nats-Rollup' => 'sub'];
            if ($tombstoneTtl !== null) {
                $headers['Nats-TTL'] = MessageTtl::format($tombstoneTtl);
            }
            if ($expectedRevision !== null) {
                $headers['Nats-Expected-Last-Subject-Sequence'] = (string) $expectedRevision;
            }

            return $this->publishWithHeadersAck($this->subjectForKey($key), '', $headers)->await();
        });
    }

    /**
     * Loads the latest entry for a key.
     *
     * Returns null only when the key has no record at all (never written, or its history was purged
     * out). When the latest record is a delete/purge marker the entry is still returned, with
     * operation `DEL`/`PURGE` and a null value - inspect `$entry->operation` to tell a live value from
     * a tombstone. (`getAll()`, by contrast, omits deleted keys entirely.)
     *
     * @return Future<KeyValueEntry|null>
     */
    public function get(string $key): Future
    {
        return async(function () use ($key): ?KeyValueEntry {
            $this->assertValidKey($key);

            // Read the latest record via the Direct Get API (served by any replica, not just the
            // leader), consistent with getAll(). Direct Get returns the stored value as the message
            // body with Nats-* (and KV-Operation) headers; a 404 means the key does not exist.
            try {
                $message = $this->jetStream
                    ->directGetLastMessageForSubject($this->streamName(), $this->subjectForKey($key))
                    ->await();
            } catch (JetStreamException $e) {
                if ($e->getCode() === 404) {
                    return null;
                }

                if ($e->getCode() === 503) {
                    // Direct Get unavailable (allow_direct disabled / legacy stream); fall back to the
                    // leader STREAM.MSG.GET path so reads still work on interop buckets.
                    return $this->getViaStreamMessage($key);
                }

                throw $e;
            }

            $headers = NatsHeaders::fromWireBlock($message->rawHeaders);
            $operation = $this->operationFromHeaders($headers);
            $revision = isset($headers['Nats-Sequence']) ? (int) $headers['Nats-Sequence'] : null;

            return $this->buildEntry($key, $message->payload, $operation, $revision);
        });
    }

    /**
     * Loads a specific historical revision (stream sequence) of a key. Returns null when no message
     * exists at that sequence, or when the message at that sequence belongs to a different key.
     * Mirrors nats.go / nats.java `KeyValue.GetRevision` (#33).
     *
     * @return Future<KeyValueEntry|null>
     */
    public function getRevision(string $key, int $revision): Future
    {
        return async(function () use ($key, $revision): ?KeyValueEntry {
            $this->assertValidKey($key);
            if ($revision <= 0) {
                throw new JetStreamException('Revision must be greater than zero');
            }

            try {
                $message = $this->jetStream->getStreamMessage($this->streamName(), $revision)->await();
            } catch (JetStreamException $e) {
                if ($e->getCode() === 404) {
                    return null;
                }

                throw $e;
            }

            // A sequence is global to the stream: ensure it actually stores this key's subject.
            if ($message->subject !== $this->subjectForKey($key)) {
                return null;
            }

            $headers = NatsHeaders::fromWireBlock($message->rawHeaders);

            return $this->buildEntry($key, $message->payload, $this->operationFromHeaders($headers), $revision);
        });
    }

    /**
     * Resolves the KV operation for a record. A server-written subject delete-marker
     * (`Nats-Marker-Reason`: MaxAge/Remove/Purge, ADR-43) carries no `KV-Operation`; it is treated as
     * a PURGE tombstone so a reader/watcher sees a deletion rather than a live empty value.
     *
     * @param array<string,string> $headers
     */
    private function operationFromHeaders(array $headers): string
    {
        if (($headers['Nats-Marker-Reason'] ?? '') !== '') {
            return 'PURGE';
        }

        return strtoupper((string) ($headers['KV-Operation'] ?? 'PUT'));
    }

    private function buildEntry(string $key, string $value, string $operation, ?int $revision): KeyValueEntry
    {
        return new KeyValueEntry(
            bucket: $this->bucket,
            key: $key,
            value: $operation === 'DEL' || $operation === 'PURGE' ? null : $value,
            operation: $operation,
            revision: $revision,
        );
    }

    /**
     * Leader STREAM.MSG.GET fallback for get() when Direct Get is unavailable (allow_direct disabled
     * or a server without Direct Get support). Returns null when the key does not exist.
     */
    private function getViaStreamMessage(string $key): ?KeyValueEntry
    {
        $subject = JetStreamApi::STREAM_MSG_GET_PREFIX . $this->streamName();
        $payload = json_encode(['last_by_subj' => $this->subjectForKey($key)], JSON_THROW_ON_ERROR);
        $data = $this->decodeReply($this->client->request($subject, $payload)->await()->payload);

        /** @var array<string,mixed>|null $error */
        $error = is_array($data['error'] ?? null) ? $data['error'] : null;
        if ($error !== null) {
            $code = (int) ($error['code'] ?? 0);
            if ($code === 404) {
                return null;
            }

            throw new JetStreamException((string) ($error['description'] ?? 'JetStream API error'), $code);
        }

        /** @var array<string,mixed>|null $message */
        $message = is_array($data['message'] ?? null) ? $data['message'] : null;
        if ($message === null) {
            return null;
        }

        $encoded = (string) ($message['data'] ?? '');
        $value = $encoded === '' ? '' : base64_decode($encoded, true);
        if ($value === false) {
            throw new JetStreamException('Malformed KV payload for key ' . $key);
        }

        $headers = [];
        $encodedHeaders = (string) ($message['hdrs'] ?? '');
        if ($encodedHeaders !== '') {
            $rawHeaders = base64_decode($encodedHeaders, true);
            if ($rawHeaders !== false) {
                $headers = NatsHeaders::fromWireBlock($rawHeaders);
            }
        }

        $operation = $this->operationFromHeaders($headers);
        $revision = isset($message['seq']) ? (int) $message['seq'] : null;

        return $this->buildEntry($key, $value, $operation, $revision);
    }

    /**
     * Watches keys using wildcard pattern and forwards entries to a callback.
     *
     * With `$options = null` only updates published after the watch starts are delivered
     * (deliver_policy=new); pass a {@see KeyWatchOptions} instance (even with no flags) to replay the
     * current value of every matching key first (last_per_subject), or to select history / meta-only /
     * resume-from-revision / ignore-deletes and an end-of-initial-data signal.
     *
     * @param callable(KeyValueEntry):void $handler
     * @return Future<int>
     */
    public function watch(callable $handler, string $pattern = '>', ?KeyWatchOptions $options = null): Future
    {
        return async(function () use ($handler, $pattern, $options): int {
            $filter = $this->subjectPrefix() . $pattern;

            // Default (no options) preserves the original live-updates-only behavior (deliver_policy=new,
            // ack-free). Options select history/last-per-subject replay, meta-only, resume-from-revision,
            // delete suppression, and an end-of-initial-data signal.
            $consumerOptions = $options?->toConsumerConfig()
                ?? ['deliver_policy' => 'new', 'ack_policy' => 'none'];
            $ignoreDeletes = $options !== null && $options->ignoreDeletes;
            $onCaughtUp = $options?->onCaughtUp;
            $caughtUpFired = false;

            // Deliver via a JetStream push consumer (not a plain core subscription) so each update
            // carries its stream sequence - i.e. the KV revision - which a watcher needs to feed back
            // into update()/CAS.
            return $this->jetStream->subscribeEphemeralPushConsumer(
                $this->streamName(),
                function (NatsMessage $message) use ($handler, $ignoreDeletes, $onCaughtUp, &$caughtUpFired): void {
                    $key = $this->keyFromSubject($message->subject);
                    if ($key === null) {
                        return;
                    }

                    $headers = NatsHeaders::fromWireBlock($message->rawHeaders);
                    $operation = $this->operationFromHeaders($headers);
                    $isDelete = $operation === 'DEL' || $operation === 'PURGE';

                    if (!($ignoreDeletes && $isDelete)) {
                        $handler(new KeyValueEntry(
                            bucket: $this->bucket,
                            key: $key,
                            value: $isDelete ? null : $message->payload,
                            operation: $operation,
                            revision: $this->jetStream->streamSequenceOf($message),
                        ));
                    }

                    // End-of-initial-data signal: fire once when the replay has caught up to the
                    // current end of the stream (this delivery reports no further pending messages).
                    if ($onCaughtUp !== null && !$caughtUpFired) {
                        // Use the null-tolerant metadata parse: a non-conformant delivery without a
                        // parseable $JS.ACK subject must NOT throw out of the shared dispatch loop and
                        // tear down every subscription on the connection (#90) - skip the check instead.
                        $metadata = JsMessageMetadata::fromMessage($message);
                        if ($metadata !== null && $metadata->numPending === 0) {
                            $caughtUpFired = true;
                            $onCaughtUp();
                        }
                    }
                },
                filterSubject: $filter,
                consumerOptions: $consumerOptions,
                onConsumerCreated: static function (ConsumerInfo $consumer) use ($onCaughtUp, &$caughtUpFired): void {
                    // End-of-initial-data on an empty / no-match bucket: the consumer starts with nothing
                    // pending, so no delivery will ever arrive to drive the in-handler caught-up check -
                    // fire the signal now instead of leaving a blocked caller hanging (#99).
                    if ($onCaughtUp !== null && !$caughtUpFired && (int) ($consumer->raw['num_pending'] ?? 0) === 0) {
                        $caughtUpFired = true;
                        $onCaughtUp();
                    }
                },
            )->await();
        });
    }

    /**
     * Creates a key only when it does not already exist (exclusive create), mirroring nats.go /
     * nats.java `KeyValue.Create`. A key counts as absent when it was never written or its latest
     * record is a delete/purge tombstone; otherwise a {@see JetStreamException} ("key exists") is
     * thrown. Named `createKey` to avoid colliding with the bucket-level {@see create()}.
     *
     * @param int|string|null $ttl Optional per-key TTL (`Nats-TTL`; requires `allow_msg_ttl`).
     * @return Future<PubAck>
     */
    public function createKey(string $key, string $value, int|string|null $ttl = null): Future
    {
        return async(function () use ($key, $value, $ttl): PubAck {
            $this->assertValidKey($key);

            // First attempt: exclusive create - succeeds only when the subject has no prior message.
            try {
                return $this->putExpectingSubjectSeq($key, $value, 0, $ttl)->await();
            } catch (JetStreamException $e) {
                if (!$this->isWrongLastSequenceError($e)) {
                    throw $e;
                }
            }

            // The subject already has history. That is allowed only if the latest record is a
            // delete/purge tombstone: recreate the key against that tombstone's revision.
            $entry = $this->get($key)->await();
            if ($entry !== null && $entry->operation !== 'DEL' && $entry->operation !== 'PURGE') {
                throw new JetStreamException('Key already exists: ' . $key, 10071);
            }

            $revision = $entry !== null ? ($entry->revision ?? 0) : 0;

            return $this->putExpectingSubjectSeq($key, $value, $revision, $ttl)->await();
        });
    }

    /**
     * Returns the names of all keys currently present in the bucket (deleted/purged keys excluded),
     * mirroring nats.go / nats.java `KeyValue.Keys()`. Use {@see getAll()} when the values are needed.
     *
     * @return Future<list<string>>
     */
    public function keys(): Future
    {
        return async(fn(): array => array_keys($this->getAll()->await()));
    }

    /**
     * Alias of {@see keys()} (nats.java naming).
     *
     * @return Future<list<string>>
     */
    public function listKeys(): Future
    {
        return $this->keys();
    }

    /**
     * Returns the full ordered history of a key - every stored revision (puts and delete/purge
     * tombstones), oldest first. Requires the bucket to retain history (created with a `history` > 1);
     * a history-1 bucket yields only the latest record. Mirrors nats.go / nats.java `KeyValue.History`
     * (#41).
     *
     * @return Future<list<KeyValueEntry>>
     */
    public function history(string $key): Future
    {
        return async(function () use ($key): array {
            $this->assertValidKey($key);

            $deliver = Inbox::generate('_INBOX.KV.HIST');
            $consumer = $this->jetStream->createEphemeralPushConsumer(
                $this->streamName(),
                $deliver,
                $this->subjectForKey($key),
                ['deliver_policy' => 'all', 'ack_policy' => 'none'],
            )->await();

            $pending = (int) ($consumer->raw['num_pending'] ?? 0);
            if ($pending === 0) {
                return [];
            }

            /** @var list<KeyValueEntry> $entries */
            $entries = [];
            $caughtUp = false;
            $sid = $this->client->subscribe($deliver, function (NatsMessage $message) use (&$entries, &$caughtUp, $key): void {
                // Use the null-tolerant metadata parse: a non-conformant / control delivery without a
                // parseable $JS.ACK reply subject must NOT throw out of the shared dispatch loop and tear
                // down every subscription on the connection (the #90 class, here for history() - #96).
                // Skip such a frame instead of recording it as a bogus history entry.
                $meta = JsMessageMetadata::fromMessage($message);
                if ($meta === null) {
                    return;
                }

                $headers = NatsHeaders::fromWireBlock($message->rawHeaders);
                $entries[] = $this->buildEntry($key, $message->payload, $this->operationFromHeaders($headers), $meta->streamSequence);

                if ($meta->numPending === 0) {
                    $caughtUp = true;
                }
            })->await();

            $cancellation = new TimeoutCancellation(5.0);
            try {
                while (!$caughtUp) {
                    $frames = $this->client->processIncoming($cancellation)->await();
                    if ($frames === 0) {
                        delay(0.001, cancellation: $cancellation);
                    }
                }
            } catch (CancelledException) {
                // Bounded wait elapsed; return whatever history was collected.
            } finally {
                $this->client->unsubscribe($sid)->await();
            }

            return $entries;
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
            // Request stream info with subjects filter to get per-subject counts.
            $streamInfo = $this->requestStreamInfoWithSubjects();

            /** @var array<string,int> $subjects */
            $subjects = $streamInfo['subjects'];
            if ($subjects === []) {
                return [];
            }

            // Look up the latest record per key CONCURRENTLY via the Direct Get API (last_by_subj).
            // Direct Get is served by any replica (not just the leader) and the round-trips overlap,
            // turning O(keys) serial reads into roughly one round-trip of wall-clock.
            $lookups = [];
            foreach (array_keys($subjects) as $subject) {
                $key = $this->keyFromSubject((string) $subject);
                if ($key === null || $key === '') {
                    continue;
                }

                $lookups[$key] = async(function () use ($key): ?string {
                    try {
                        $message = $this->jetStream
                            ->directGetLastMessageForSubject($this->streamName(), $this->subjectForKey($key))
                            ->await();
                    } catch (JetStreamException $e) {
                        if ($e->getCode() === 404) {
                            return null;
                        }

                        throw $e;
                    }

                    $headers = NatsHeaders::fromWireBlock($message->rawHeaders);
                    $operation = $this->operationFromHeaders($headers);
                    if ($operation === 'DEL' || $operation === 'PURGE') {
                        return null;
                    }

                    return $message->payload;
                });
            }

            /** @var array<string,?string> $results */
            $results = Future\await($lookups);

            $latest = [];
            foreach ($results as $key => $value) {
                if ($value !== null) {
                    $latest[$key] = $value;
                }
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
     * Requests stream info with subjects filter to get per-subject counts.
     *
     * @return array{subjects: array<string,int>}
     */
    private function requestStreamInfoWithSubjects(): array
    {
        $subject = JetStreamApi::STREAM_INFO_PREFIX . $this->streamName();

        // The STREAM.INFO subjects map is server-capped, so a bucket with many keys must be enumerated
        // across pages (offset) or getAll() would silently truncate. The no-new-subjects guard also
        // terminates safely against a server that ignores `offset`.
        $collected = [];
        $offset = 0;

        do {
            $payload = json_encode([
                'subjects_filter' => $this->subjectPrefix() . '>',
                'offset' => $offset,
            ], JSON_THROW_ON_ERROR);
            $data = $this->decodeReply($this->client->request($subject, $payload)->await()->payload);

            /** @var array<string,mixed>|null $error */
            $error = is_array($data['error'] ?? null) ? $data['error'] : null;
            if ($error !== null) {
                throw new JetStreamException(
                    (string) ($error['description'] ?? 'JetStream API error'),
                    (int) ($error['code'] ?? 0),
                );
            }

            /** @var array<string,mixed> $state */
            $state = is_array($data['state'] ?? null) ? $data['state'] : [];

            /** @var array<string,int> $subjects */
            $subjects = is_array($state['subjects'] ?? null) ? $state['subjects'] : [];

            $newCount = 0;
            foreach ($subjects as $name => $count) {
                $name = (string) $name;
                if (!isset($collected[$name])) {
                    $collected[$name] = (int) $count;
                    ++$newCount;
                }
            }

            $offset += count($subjects);
        } while ($subjects !== [] && $newCount > 0);

        return ['subjects' => $collected];
    }

    /**
     * Decodes a JetStream JSON reply, mapping a malformed (non-JSON) body to a JetStreamException
     * instead of leaking a raw \JsonException to the caller (consistent with the other API calls).
     *
     * @return array<string,mixed>
     */
    private function decodeReply(string $payload): array
    {
        try {
            /** @var array<string,mixed> $data */
            $data = json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new JetStreamException('Malformed JetStream reply: ' . $e->getMessage(), 0, $e);
        }

        return $data;
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
     * Publishes a value asserting the subject's expected last sequence (optimistic concurrency), with
     * an optional per-key TTL. Used by {@see createKey()} (expected seq 0 / tombstone revision).
     *
     * @param int|string|null $ttl
     * @return Future<PubAck>
     */
    private function putExpectingSubjectSeq(string $key, string $value, int $expectedSeq, int|string|null $ttl): Future
    {
        $headers = ['Nats-Expected-Last-Subject-Sequence' => (string) $expectedSeq];
        if ($ttl !== null) {
            $headers['Nats-TTL'] = MessageTtl::format($ttl);
        }

        return $this->publishWithHeadersAck($this->subjectForKey($key), $value, $headers);
    }

    /**
     * Whether a JetStream error is the server's "wrong last sequence" rejection (err_code 10071),
     * which an exclusive create uses to detect that the key already has a record.
     */
    private function isWrongLastSequenceError(JetStreamException $e): bool
    {
        return $e->getCode() === 10071 || stripos($e->getMessage(), 'wrong last sequence') !== false;
    }

    /**
     * @param array<string,string> $headers
     * @return Future<PubAck>
     */
    private function publishWithHeadersAck(string $subject, string $payload, array $headers): Future
    {
        return async(function () use ($subject, $payload, $headers): PubAck {
            $message = $this->client->requestWithHeaders($subject, $payload, $headers)->await();

            $data = $this->decodeReply($message->payload);

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
     * Maps semantic KV options to underlying stream configuration fields.
     *
     * @param array<string,mixed> $options
     * @return array<string,mixed>
     */
    private function mapKvOptions(array $options): array
    {
        $mapped = [];

        foreach ($options as $key => $value) {
            $mapped[match ($key) {
                'history' => 'max_msgs_per_subject',
                'ttl' => 'max_age',
                'max_value_size' => 'max_msg_size',
                'storage' => 'storage',
                'num_replicas' => 'num_replicas',
                'description' => 'description',
                'max_bytes' => 'max_bytes',
                default => $key,
            }] = $value;
        }

        return $mapped;
    }

    /**
     * Ensures key name follows basic NATS KV key constraints.
     */
    private function assertValidKey(string $key): void
    {
        if ($key === '' || preg_match('/[\s*>]/', $key)) {
            throw new JetStreamException('Invalid KV key');
        }

        // Leading, trailing, or consecutive dots produce empty subject tokens, i.e. a malformed
        // $KV.<bucket>.<key> subject. Reject them up front with a clear KV error.
        if (str_starts_with($key, '.') || str_ends_with($key, '.') || str_contains($key, '..')) {
            throw new JetStreamException('Invalid KV key');
        }
    }
}
