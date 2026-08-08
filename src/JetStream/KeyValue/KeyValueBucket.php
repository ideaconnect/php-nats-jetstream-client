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
use IDCT\NATS\JetStream\ApiErrCode;
use IDCT\NATS\JetStream\DecodesApiErrors;
use IDCT\NATS\JetStream\JetStreamApi;
use IDCT\NATS\JetStream\JetStreamContext;
use IDCT\NATS\JetStream\JetStreamRequest;
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
    use DecodesApiErrors;

    /**
     * Default idle_heartbeat (ns) the KV watch consumer requests so the missed-heartbeat watchdog (#113)
     * can notice a silent/reaped watch. nats.go KV watchers receive heartbeats via an ordered consumer;
     * the default watch config here otherwise requested none, leaving total silence indistinguishable
     * from an idle stream. Heartbeats are status-100 control frames, filtered before delivery, so this
     * does not change what a watcher observes. Overridable via KeyWatchOptions::$idleHeartbeat.
     */
    public const WATCH_IDLE_HEARTBEAT_NS = 5_000_000_000;

    /**
     * Default bound (seconds) for a single history() replay. A replay that does not catch up within
     * the bound surfaces as an incomplete-result error rather than a silently truncated prefix (#121).
     * Overridable per call via the timeout argument.
     */
    private const HISTORY_TIMEOUT_S = 5.0;

    /**
     * READ-side subject prefix override, set only when this handle fronts an EXTERNAL
     * (cross-domain) mirror: mirrored records keep the origin's `$KV.<origin>.` subjects, so reads
     * against the local mirror stream must use that prefix. Null = own `$KV.<bucket>.` prefix.
     * Resolved by create() (from its own config) or {@see bind()} (from STREAM.INFO).
     */
    private ?string $readPrefix = null;

    /**
     * WRITE-side subject prefix override, set for ANY mirror (nats.go putPre parity): a mirror
     * stream ingests no subjects of its own, so put/delete/purge publish through to the origin -
     * `$KV.<origin>.` same-domain, `<external api>.$KV.<origin>.` cross-domain. Null = writes use
     * the read prefix.
     */
    private ?string $writePrefix = null;

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
     * ADR-8 / nats.go CreateKeyValue parity: `deny_delete` protects revision history from
     * STREAM.MSG.DELETE (KV delete/purge use DEL markers and Nats-Rollup, never MSG.DELETE) and
     * `discard: new` makes a full bucket reject writes instead of silently evicting old keys.
     * `discard: new` on a max_msgs_per_subject stream expects NATS server 2.7.2+ (nats.go gates it
     * on that version). Both remain user-overridable via `$options`.
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
                'deny_delete' => true,
                'discard' => 'new',
            ];

            $mapped = $this->mapKvOptions($options);

            // Mirror/source KV buckets reference other buckets by name; translate them to the backing
            // `KV_`-prefixed stream names (#62). A mirrored bucket defines no subjects of its own and
            // gets mirror_direct; a sourced bucket keeps its own subjects and each source gets the
            // ADR-57 subject transform re-subjecting copied records into THIS bucket's prefix -
            // without it, sourced entries sit in the stream under the origin subject where every
            // read/watch path (filtered on this bucket's prefix) is blind to them (nats.go parity).
            $subjects = ['$KV.' . $this->bucket . '.>'];
            $mirror = null;
            if (isset($mapped['mirror'])) {
                $mirror = $this->kvMirrorConfig($mapped['mirror']);
                $mapped['mirror'] = $mirror;
                // nats.go CreateKeyValue parity: mirrors answer origin DIRECT.GET traffic.
                $mapped['mirror_direct'] ??= true;
                $subjects = [];
            }
            if (isset($mapped['sources']) && is_array($mapped['sources'])) {
                $mapped['sources'] = array_values(array_map(
                    fn(string|array $source): array => $this->kvSourceConfig($source),
                    $mapped['sources'],
                ));
            }

            $info = $this->jetStream->createStream(
                $this->streamName(),
                $subjects,
                array_merge($defaults, $mapped),
            )->await();

            // A handle must never carry mirror prefixes from a configuration the server did not
            // confirm - and, symmetrically, a FAILED create() must never disturb prefixes the
            // server DID confirm earlier (via a prior create() or bind()): a working mirror handle
            // whose re-create fails would otherwise go silently blind (reads at the wrong
            // subjects, writes into a stream that ingests nothing). So the reset happens only
            // HERE, after the await proved the new configuration, atomically with the re-apply -
            // no suspension point separates them, mirroring bind()'s reset placement
            // (nats.go builds the KV handle only from a successful create - mapStreamToKVS parity).
            $this->readPrefix = null;
            $this->writePrefix = null;
            if ($mirror !== null) {
                // The mirror is CONFIRMED server-side: this handle now fronts it - writes go
                // through to the ORIGIN bucket's subjects (the mirror stream ingests nothing
                // itself), reads follow nats.go's prefix rules. bind() applies the same
                // resolution for attached handles.
                $this->applyMirrorPrefixes($mirror);
            }

            return $info;
        });
    }

    /**
     * Normalizes a KV SOURCE entry per nats.go/ADR-57: the referenced bucket becomes the backing
     * `KV_`-prefixed stream name, a `domain` is converted to the external API prefix, and - unless
     * the caller supplied its own `subject_transforms` (full-control pass-through: the name is then
     * used as-is, allowing non-KV streams) - the mandatory KV transform is attached so sourced
     * records are re-subjected into THIS bucket's prefix:
     * `{src: "$KV.<srcBucket>.>", dest: "$KV.<thisBucket>.>"}`. The transform is skipped only for an
     * external source of the SAME bucket name, whose subjects already match (nats.go rule).
     *
     * Name resolution: the `bucket` alias explicitly declares a KV BUCKET (nats.go has no such
     * alias), so its value is `KV_`-prefixed UNCONDITIONALLY - a bucket legitimately named `KV_x`
     * maps to its backing stream `KV_KV_x`, never to bucket x's stream. A bare-string entry and the
     * `name` key follow nats.go's ambiguous stream-name semantics instead: a value already starting
     * with `KV_` is taken as the backing stream name itself (idempotent, no double prefix).
     *
     * @param string|array<string,mixed> $source
     * @return array<string,mixed>
     */
    private function kvSourceConfig(string|array $source): array
    {
        if (is_string($source)) {
            $source = ['name' => $source];
        }

        $bucketAlias = null;
        if (isset($source['bucket'])) {
            $bucketAlias = (string) $source['bucket'];
            $source['name'] = 'KV_' . $bucketAlias;
            unset($source['bucket']);
        }

        $source = $this->convertKvDomain($source);

        // Caller-supplied transforms pass through verbatim - full control of the source (e.g. a
        // non-KV stream); the name is NOT KV_-prefixed in this branch (nats.go jetstream parity).
        // The `bucket` alias was already translated to its backing stream name above.
        if (isset($source['subject_transforms']) && $source['subject_transforms'] !== []) {
            return $source;
        }

        if ($bucketAlias !== null) {
            $sourceBucket = $bucketAlias;
        } else {
            $name = (string) ($source['name'] ?? '');
            if (str_starts_with($name, 'KV_')) {
                $sourceBucket = substr($name, 3);
            } else {
                $sourceBucket = $name;
                $source['name'] = 'KV_' . $name;
            }
        }

        if (!isset($source['external']) || $sourceBucket !== $this->bucket) {
            $source['subject_transforms'] = [[
                'src' => '$KV.' . $sourceBucket . '.>',
                'dest' => '$KV.' . $this->bucket . '.>',
            ]];
        }

        return $source;
    }

    /**
     * Normalizes a KV MIRROR entry: resolves the origin's backing `KV_`-prefixed stream name and
     * converts a `domain` to the external API prefix. Unlike sources, a mirror gets NO subject
     * transforms - mirrored records keep the ORIGIN's subjects, which is why the runtime prefixes
     * are re-resolved via {@see applyMirrorPrefixes()} (nats.go parity).
     *
     * Name resolution matches {@see kvSourceConfig()}: the `bucket` alias declares a KV BUCKET and
     * is prefixed unconditionally (bucket `KV_x` mirrors stream `KV_KV_x`), while a bare string and
     * the `name` key keep nats.go's stream-name semantics (a `KV_`-prefixed value is already the
     * backing stream name).
     *
     * @param string|array<string,mixed> $mirror
     * @return array<string,mixed>
     */
    private function kvMirrorConfig(string|array $mirror): array
    {
        if (is_string($mirror)) {
            $mirror = ['name' => $mirror];
        }

        if (isset($mirror['bucket'])) {
            $mirror['name'] = 'KV_' . (string) $mirror['bucket'];
            unset($mirror['bucket']);
        } else {
            $name = (string) ($mirror['name'] ?? '');
            if (!str_starts_with($name, 'KV_')) {
                $mirror['name'] = 'KV_' . $name;
            }
        }

        return $this->convertKvDomain($mirror);
    }

    /**
     * Converts a mirror/source `domain` shorthand to the external JetStream API prefix
     * (`external: {api: "$JS.<domain>.API"}`), rejecting entries that set both (nats.go parity).
     *
     * @param array<string,mixed> $entry
     * @return array<string,mixed>
     */
    private function convertKvDomain(array $entry): array
    {
        $domain = $entry['domain'] ?? null;
        if (!is_string($domain) || $domain === '') {
            return $entry;
        }

        if (isset($entry['external'])) {
            throw new JetStreamException('A KV mirror/source cannot set both domain and external');
        }

        unset($entry['domain']);
        $entry['external'] = ['api' => '$JS.' . $domain . '.API'];

        return $entry;
    }

    /**
     * Resolves the read/write subject prefixes for a mirror bucket from its mirror config, exactly
     * as nats.go's mapStreamToKVS does: mirrored records keep the ORIGIN's `$KV.<origin>.` subjects
     * and the mirror stream ingests no subjects of its own, so
     *   - writes (put/create/update/delete/purge) always go through to the origin - via
     *     `<external api>.$KV.<origin>.` cross-domain, or `$KV.<origin>.` same-domain (the origin
     *     stream, not the mirror, stores them; the mirror replicates them back);
     *   - reads switch to the origin prefix only for an EXTERNAL (cross-domain) mirror, where the
     *     local mirror stream serves them under the origin subjects; a same-domain mirror handle
     *     keeps its own prefix for reads (nats.go makes no read-side adjustment there either).
     *
     * @param array<string,mixed> $mirror
     */
    private function applyMirrorPrefixes(array $mirror): void
    {
        $origin = (string) ($mirror['name'] ?? '');
        if (str_starts_with($origin, 'KV_')) {
            $origin = substr($origin, 3);
        }

        if ($origin === '') {
            return;
        }

        $externalApi = null;
        if (isset($mirror['external']) && is_array($mirror['external'])) {
            $api = $mirror['external']['api'] ?? null;
            if (is_string($api) && $api !== '') {
                $externalApi = $api;
            }
        }

        if ($externalApi !== null) {
            $this->readPrefix = '$KV.' . $origin . '.';
            $this->writePrefix = rtrim($externalApi, '.') . '.$KV.' . $origin . '.';
        } else {
            $this->writePrefix = '$KV.' . $origin . '.';
        }
    }

    /**
     * Binds this handle to the EXISTING bucket's server-side configuration: fetches STREAM.INFO,
     * sanity-checks the stream is KV-shaped (nats.go ErrBadBucket parity), and resolves the mirror
     * read/write prefixes when the stream is a mirror. Required for a handle ATTACHED to a mirror
     * bucket created elsewhere - without it, writes would target this bucket's own prefix, which no
     * stream ingests (503), and cross-domain reads would miss the origin-prefixed records.
     * create() applies the same resolution from its own config, so created handles need no bind().
     *
     * @return Future<self>
     */
    public function bind(): Future
    {
        return async(function (): self {
            $info = $this->jetStream->getStream($this->streamName())->await();

            /** @var array<string,mixed> $config */
            $config = is_array($info->raw['config'] ?? null) ? $info->raw['config'] : [];

            if ((int) ($config['max_msgs_per_subject'] ?? 0) < 1) {
                throw new JetStreamException(sprintf(
                    'Stream "%s" is not a KV bucket (max_msgs_per_subject < 1)',
                    $this->streamName(),
                ));
            }

            // Re-resolve from the CONFIRMED server config: clear first so a handle previously
            // fronting a mirror (created or bound earlier) drops its stale prefixes when the
            // stream it re-binds to is no longer a mirror.
            $this->readPrefix = null;
            $this->writePrefix = null;
            if (isset($config['mirror']) && is_array($config['mirror'])) {
                $this->applyMirrorPrefixes($config['mirror']);
            }

            return $this;
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

            return $this->jetStream->publish($this->writeSubjectForKey($key), $value, ttl: $ttl)->await();
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
                $this->writeSubjectForKey($key),
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

            return $this->publishWithHeadersAck($this->writeSubjectForKey($key), '', $headers)->await();
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

            return $this->publishWithHeadersAck($this->writeSubjectForKey($key), '', $headers)->await();
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

        $error = is_array($data['error'] ?? null) ? $data['error'] : null;
        if ($error !== null && (int) ($error['code'] ?? 0) === 404) {
            return null;
        }
        $this->throwIfApiError($data);

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
     * @return Future<int> The watch handle sid. The watch rides an ordered consumer whose automatic
     *         recreates rotate the internal sid, so stop it with
     *         {@see JetStreamContext::stopOrderedConsumer()} - a plain `unsubscribe($sid)` only
     *         works until the first recreate.
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
            // Pin the idle-heartbeat interval this watch requests (KeyWatchOptions::$idleHeartbeat may
            // override it; subscribeOrderedConsumer would otherwise apply its own default). The interval
            // drives the ordered consumer's missed-heartbeat watchdog (#113): after two intervals with
            // no inbound frame at all (data, heartbeat or flow control) the consumer counts as silent or
            // reaped, so the watch is RECREATED and resumes just after the last delivered revision
            // instead of staying quiet forever. An error reaches the client only if every recreate
            // attempt fails. Heartbeats are status-100 control frames, filtered before delivery.
            $consumerOptions['idle_heartbeat'] ??= self::WATCH_IDLE_HEARTBEAT_NS;
            $ignoreDeletes = $options !== null && $options->ignoreDeletes;
            $onCaughtUp = $options?->onCaughtUp;
            $caughtUpFired = false;

            // Deliver via an ORDERED JetStream push consumer (not a plain ephemeral): each update
            // carries its stream sequence - i.e. the KV revision - and the ordered machinery adds
            // what a lossless watch requires (nats.go KeyWatcher parity): consumer-sequence gap
            // detection with recreate-from-lastStreamSeq+1 (once a delivery has fixed a resume
            // point, a slow-consumer drop or reconnect window is REPLAYED instead of becoming a
            // silent permanent gap - ack-none deliveries are never redelivered on their own; a
            // recreate BEFORE the first delivery re-applies the initial policy, matching nats.go's
            // no-cursor reset), flow control, and watchdog-driven recreation of a silent/reaped
            // consumer. Stop a watch with stopOrderedConsumer($sid): recreates rotate the internal
            // sid, so a plain unsubscribe() only works until the first recreate.
            $idleHeartbeat = $consumerOptions['idle_heartbeat'];
            unset($consumerOptions['idle_heartbeat']);

            return $this->jetStream->subscribeOrderedConsumer(
                $this->streamName(),
                function (NatsMessage $message) use ($handler, $ignoreDeletes, $onCaughtUp, &$caughtUpFired): void {
                    $key = $this->keyFromSubject($message->subject);
                    if ($key === null) {
                        return;
                    }

                    // The common KV put carries no headers, so this parse is a free early return. For
                    // headered deliveries (DEL/PURGE tombstones, TTL puts) it re-parses the block the
                    // push dispatch already parsed for its control-frame check: threading that parse
                    // through would change the public callable(NatsMessage):void handler contract of
                    // subscribeOrderedConsumer, so the rare double parse stays (#139). The two
                    // reply-subject parses below also stay separate: streamSequenceOf() yields null
                    // for a malformed $JS.ACK sequence token where JsMessageMetadata (int)-casts it,
                    // and the metadata parse only runs until the caught-up signal has fired.
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
                idleHeartbeatNs: is_int($idleHeartbeat) ? $idleHeartbeat : null,
                consumerOverrides: $consumerOptions,
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
                // Shaped like the server's own rejection (code 400 / err_code 10071, nats.go
                // ErrKeyExists) so getCode() keeps a single taxonomy (#154).
                throw new JetStreamException(
                    'Key already exists: ' . $key,
                    400,
                    null,
                    ApiErrCode::STREAM_WRONG_LAST_SEQUENCE,
                );
            }

            $revision = $entry !== null ? ($entry->revision ?? 0) : 0;

            return $this->putExpectingSubjectSeq($key, $value, $revision, $ttl)->await();
        });
    }

    /**
     * Default no-progress bound (seconds) for the keys() headers-only enumeration replay, mirroring
     * history()'s stall guard: a replay that makes NO progress for the whole interval surfaces as an
     * incomplete-result error rather than a silently truncated key list (#110/#121).
     */
    private const KEYS_TIMEOUT_S = 5.0;

    /**
     * Returns the names of all keys currently present in the bucket (deleted/purged keys excluded),
     * mirroring nats.go / nats.java `KeyValue.Keys()`. Use {@see getAll()} when the values are needed.
     *
     * Enumerates names WITHOUT downloading any value bytes: a `last_per_subject` + `headers_only`
     * ephemeral push consumer replays the latest record of every key carrying only its headers
     * (`KV-Operation`/`Nats-Sequence`), so tombstones are filtered by the `KV-Operation` header alone
     * and no value body crosses the wire (nats.go's `Keys()` uses the same headers-only watcher). The
     * previous `array_keys(getAll())` transferred every value just to discard it (#110). The returned
     * set of names is identical to `array_keys(getAll())`; the order (stream-sequence order of the
     * latest record per key) is unspecified, as it was before.
     *
     * @param float|null $progressTimeoutSeconds No-progress bound in seconds (null uses the default).
     * @return Future<list<string>>
     */
    public function keys(?float $progressTimeoutSeconds = null): Future
    {
        $progressTimeoutSeconds ??= self::KEYS_TIMEOUT_S;

        return async(function () use ($progressTimeoutSeconds): array {
            $deliver = Inbox::generate('_INBOX.KV.KEYS');
            $consumer = $this->jetStream->createEphemeralPushConsumer(
                $this->streamName(),
                $deliver,
                $this->subjectPrefix() . '>',
                // last_per_subject replays the latest record per key; headers_only strips the value
                // body so only the KV-Operation/Nats-Sequence headers return (the metaOnly watch path).
                ['deliver_policy' => 'last_per_subject', 'ack_policy' => 'none', 'headers_only' => true],
            )->await();

            $pending = (int) ($consumer->raw['num_pending'] ?? 0);
            if ($pending === 0) {
                return [];
            }

            /** @var list<string> $keys */
            $keys = [];
            $caughtUp = false;
            $sid = $this->client->subscribe($deliver, function (NatsMessage $message) use (&$keys, &$caughtUp): void {
                // Null-tolerant metadata parse: a non-conformant / control delivery without a parseable
                // $JS.ACK reply subject must NOT throw out of the shared dispatch loop and tear down every
                // subscription on the connection (the #90 class, as in history()). Skip such a frame.
                $meta = JsMessageMetadata::fromMessage($message);
                if ($meta === null) {
                    return;
                }

                $key = $this->keyFromSubject($message->subject);
                if ($key !== null && $key !== '') {
                    $headers = NatsHeaders::fromWireBlock($message->rawHeaders);
                    $operation = $this->operationFromHeaders($headers);
                    // Exclude DEL/PURGE tombstones (and server delete-markers) - the same records
                    // getAll() omits - using only the headers; the value body is never fetched.
                    if ($operation !== 'DEL' && $operation !== 'PURGE') {
                        $keys[] = $key;
                    }
                }

                if ($meta->numPending === 0) {
                    $caughtUp = true;
                }
            })->await();

            // Progress-based bound identical to history(): reset the stall clock on each replayed record,
            // so a healthy-but-slow large bucket that keeps making progress is never failed - only a
            // server that makes NO progress for the whole interval throws (#121).
            $progressIntervalNs = (int) ($progressTimeoutSeconds * 1_000_000_000);
            $lastActivityNs = hrtime(true);
            $seen = count($keys);
            try {
                while (!$caughtUp) {
                    $nowNs = hrtime(true);
                    if ($nowNs - $lastActivityNs >= $progressIntervalNs) {
                        throw new JetStreamException(sprintf(
                            'Key enumeration stalled: no progress for %.3f s (collected %d key(s), '
                                . 'replay not caught up)',
                            $progressTimeoutSeconds,
                            count($keys),
                        ));
                    }

                    $waitCancellation = new TimeoutCancellation(($progressIntervalNs - ($nowNs - $lastActivityNs)) / 1e9);
                    try {
                        $read = $this->client->readIncoming($waitCancellation)->await();
                        if (!$read->consumedBytes) {
                            // Only a genuinely idle read yields 1 ms; a read that consumed bytes but
                            // completed no key-record frame yet (a chunked replay) loops immediately,
                            // draining the already-buffered rest with no idle sleep (#119).
                            delay(0.001, cancellation: $waitCancellation);
                        }
                    } catch (CancelledException) {
                        // This wait segment ended; loop around to re-evaluate the stall deadline.
                    }

                    if (count($keys) > $seen) {
                        $seen = count($keys);
                        $lastActivityNs = hrtime(true);
                    }
                }
            } finally {
                $this->client->unsubscribe($sid)->await();
            }

            return $keys;
        });
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
     * The replay is bounded by a PROGRESS timeout, $progressTimeoutSeconds (default
     * {@see self::HISTORY_TIMEOUT_S}): the bound resets on every replayed revision, so a healthy-but-
     * slow large history that keeps making progress never fails - only a server that makes NO progress
     * for the whole interval throws a JetStreamException rather than returning a truncated prefix, so a
     * genuinely stalled server surfaces as an error instead of a silent partial result (#121).
     *
     * @param float|null $progressTimeoutSeconds No-progress bound in seconds (null uses the default).
     * @return Future<list<KeyValueEntry>>
     */
    public function history(string $key, ?float $progressTimeoutSeconds = null): Future
    {
        $progressTimeoutSeconds ??= self::HISTORY_TIMEOUT_S;

        return async(function () use ($key, $progressTimeoutSeconds): array {
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

            // Progress-based bound: reset the stall clock on each replayed revision, so a healthy-but-
            // slow large history that keeps making progress is never failed - only a server that makes
            // NO progress for the whole interval throws (mirrors #153's missed-heartbeat approach).
            $progressIntervalNs = (int) ($progressTimeoutSeconds * 1_000_000_000);
            $lastActivityNs = hrtime(true);
            $seen = count($entries);
            try {
                while (!$caughtUp) {
                    $nowNs = hrtime(true);
                    if ($nowNs - $lastActivityNs >= $progressIntervalNs) {
                        // No revision arrived for the whole progress interval (num_pending never reached
                        // 0). Returning the collected prefix would silently truncate the history as if
                        // complete; a stalled server surfaces as an incomplete-result error (#121).
                        throw new JetStreamException(sprintf(
                            'Key history for "%s" stalled: no progress for %.3f s (collected %d '
                                . 'revision(s), replay not caught up)',
                            $key,
                            $progressTimeoutSeconds,
                            count($entries),
                        ));
                    }

                    $waitCancellation = new TimeoutCancellation(($progressIntervalNs - ($nowNs - $lastActivityNs)) / 1e9);
                    try {
                        $read = $this->client->readIncoming($waitCancellation)->await();
                        if (!$read->consumedBytes) {
                            // Only a genuinely idle read yields 1 ms; a read that consumed bytes but
                            // completed no revision frame yet (a chunked history payload) loops
                            // immediately, draining the already-buffered rest with no idle sleep (#119).
                            delay(0.001, cancellation: $waitCancellation);
                        }
                    } catch (CancelledException) {
                        // This wait segment ended; loop around to re-evaluate the stall deadline.
                    }

                    if (count($entries) > $seen) {
                        // A revision landed: progress. Rebase the stall clock from now.
                        $seen = count($entries);
                        $lastActivityNs = hrtime(true);
                    }
                }
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

            // Fast path (#110): one batched multi_last Direct Get (ADR-31) returns the latest record of
            // every key in a single request/reply, instead of one Direct Get round-trip per key. Gated
            // on server support (batched Direct Get needs NATS 2.11+); older servers take the per-subject
            // fan-out below. The returned data, tombstone filtering, and 404/missing handling are identical.
            if ($this->jetStream->supportsBatchedDirectGet()) {
                try {
                    return $this->getAllBatched(array_keys($subjects));
                } catch (JetStreamException $e) {
                    if ($e->getCode() !== 503) {
                        throw $e;
                    }
                    // The STREAM (not the server) lacks Direct Get - allow_direct disabled / legacy
                    // interop bucket: the server-version gate above cannot see that. Fall through to
                    // the per-subject fan-out, whose lookups fall back to the leader STREAM.MSG.GET.
                }
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

                        if ($e->getCode() === 503) {
                            // Direct Get unavailable (allow_direct disabled / legacy stream): fall
                            // back to the leader read like get() does, instead of failing the whole
                            // enumeration on a bucket whose single-key reads work. getAll() omits
                            // tombstones, so a DEL/PURGE entry (null value) maps to null here.
                            $entry = $this->getViaStreamMessage($key);

                            return $entry?->value;
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
     * getAll() fast path: fetch the latest record of every key with ONE batched multi_last Direct Get
     * (ADR-31) instead of a per-subject fan-out. Produces the identical `key => value` map: the same
     * candidate subjects are considered (non-key subjects skipped), DEL/PURGE tombstones are filtered
     * by the KV-Operation header, and subjects with no stored record are simply absent from the batch
     * reply (mirroring the fan-out's 404-skip). Only reached when {@see JetStreamContext::supportsBatchedDirectGet()}.
     *
     * @param list<int|string> $subjectNames STREAM.INFO subject-map keys (`$KV.<bucket>.<key>`).
     * @return array<string,string>
     */
    private function getAllBatched(array $subjectNames): array
    {
        $subjects = [];
        foreach ($subjectNames as $subjectName) {
            $key = $this->keyFromSubject((string) $subjectName);
            if ($key === null || $key === '') {
                continue;
            }

            $subjects[] = $this->subjectForKey($key);
        }

        if ($subjects === []) {
            return [];
        }

        $messages = $this->jetStream->directGetLastForSubjects($this->streamName(), $subjects)->await();

        $latest = [];
        foreach ($messages as $message) {
            $key = $this->keyFromSubject($message->subject);
            if ($key === null || $key === '') {
                continue;
            }

            $headers = NatsHeaders::fromWireBlock($message->rawHeaders);
            $operation = $this->operationFromHeaders($headers);
            if ($operation === 'DEL' || $operation === 'PURGE') {
                continue;
            }

            $latest[$key] = $message->payload;
        }

        return $latest;
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
     * Returns the READ-side KV subject prefix for this bucket: its own `$KV.<bucket>.` normally, or
     * the origin's prefix when this handle fronts an EXTERNAL (cross-domain) mirror - mirrored
     * records keep the origin's subjects (see {@see applyMirrorPrefixes()}).
     */
    public function subjectPrefix(): string
    {
        return $this->readPrefix ?? '$KV.' . $this->bucket . '.';
    }

    /**
     * Resolves a full READ subject for a key (gets, history/watch filters, purge-delete filters).
     */
    private function subjectForKey(string $key): string
    {
        return $this->subjectPrefix() . $key;
    }

    /**
     * Resolves a full WRITE subject for a key (put/create/update/delete/purge publishes). Identical
     * to the read subject except on a mirror bucket, where writes go through to the ORIGIN
     * (nats.go putPre parity) - the mirror stream itself ingests no subjects.
     */
    private function writeSubjectForKey(string $key): string
    {
        return ($this->writePrefix ?? $this->subjectPrefix()) . $key;
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

            $this->throwIfApiError($data);

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

        return $this->publishWithHeadersAck($this->writeSubjectForKey($key), $value, $headers);
    }

    /**
     * Whether a JetStream error is the server's "wrong last sequence" rejection (err_code 10071),
     * which an exclusive create uses to detect that the key already has a record. Falls back to
     * description wording only when the envelope carried no err_code (an old server) - the server's
     * getCode() is the HTTP-like 400, never 10071, and wording changes between versions (#154).
     */
    private function isWrongLastSequenceError(JetStreamException $e): bool
    {
        if ($e->getErrCode() !== null) {
            return $e->getErrCode() === ApiErrCode::STREAM_WRONG_LAST_SEQUENCE;
        }

        return stripos($e->getMessage(), 'wrong last sequence') !== false;
    }

    /**
     * @param array<string,string> $headers
     * @return Future<PubAck>
     */
    private function publishWithHeadersAck(string $subject, string $payload, array $headers): Future
    {
        return async(function () use ($subject, $payload, $headers): PubAck {
            // Normalize a no-responders failure to JetStreamException(503) like jsRequest(), so a
            // JetStream-disabled server / unbound subject is catchable as JetStreamException (#161).
            $message = JetStreamRequest::withHeaders($this->client, $subject, $payload, $headers);

            $data = $this->decodeReply($message->payload);

            $this->throwIfApiError($data, 'JetStream publish error');

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
     * Ensures key name follows the ADR-8 KV key constraints. Keys outside the ADR-8 charset are
     * unreadable via nats.go/nats.java/the `nats` CLI (their clients reject them with ErrInvalidKey),
     * so admitting them here would strand entries only this client can access (#132).
     */
    private function assertValidKey(string $key): void
    {
        if (preg_match('/\A[-\/_=.a-zA-Z0-9]+\z/', $key) !== 1) {
            throw new JetStreamException('Invalid KV key: keys must be non-empty and contain only [-/_=.a-zA-Z0-9] (ADR-8)');
        }

        // Leading, trailing, or consecutive dots produce empty subject tokens, i.e. a malformed
        // $KV.<bucket>.<key> subject. Reject them up front with a clear KV error.
        if (str_starts_with($key, '.') || str_ends_with($key, '.') || str_contains($key, '..')) {
            throw new JetStreamException('Invalid KV key: leading, trailing, or consecutive dots are not allowed');
        }

        // ADR-8 reserves the "_kv" prefix for internal use.
        if (str_starts_with($key, '_kv')) {
            throw new JetStreamException('Invalid KV key: the "_kv" prefix is reserved (ADR-8)');
        }
    }
}
