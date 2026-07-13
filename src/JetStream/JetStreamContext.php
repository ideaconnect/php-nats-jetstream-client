<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

use Amp\CancelledException;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Core\Inbox;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Core\NatsHeaders;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\NatsException;
use IDCT\NATS\JetStream\Configuration\ConsumerConfiguration;
use IDCT\NATS\JetStream\Configuration\StreamConfiguration;
use IDCT\NATS\JetStream\Consumers\PullConsumerIterator;
use IDCT\NATS\JetStream\KeyValue\KeyValueBucket;
use IDCT\NATS\JetStream\Models\AccountInfo;
use IDCT\NATS\JetStream\Models\ConsumerInfo;
use IDCT\NATS\JetStream\Models\JsMessageMetadata;
use IDCT\NATS\JetStream\Models\PubAck;
use IDCT\NATS\JetStream\Models\StreamInfo;
use IDCT\NATS\JetStream\ObjectStore\ObjectStoreBucket;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\delay;

/**
 * High-level JetStream client for stream, consumer, KV, and Object Store operations.
 */
final class JetStreamContext
{
    /** Idle window (ns) after which the server reaps an ephemeral push consumer with no interest. */
    private const EPHEMERAL_INACTIVE_THRESHOLD_NS = 300_000_000_000; // 5 minutes

    /** Create attempts when recreating an ordered consumer after a gap, before declaring it dead (#114). */
    private const ORDERED_RECREATE_ATTEMPTS = 3;

    /** Base backoff between ordered-consumer recreate attempts, in seconds (#114). */
    private const ORDERED_RECREATE_RETRY_DELAY_S = 0.05;

    /**
     * Default idle_heartbeat (ns) an ordered consumer requests (nats.go ordered.go parity). The
     * missed-heartbeat watchdog fires a recreate when 2 x this elapse with no inbound frame (#113).
     * Overridable per subscription via subscribeOrderedConsumer()'s $idleHeartbeatNs.
     */
    private const ORDERED_IDLE_HEARTBEAT_NS = 5_000_000_000;

    /**
     * Creates a JetStream API context bound to a NATS client.
      *
      * @param NatsClient $client Connected NATS client used to issue JetStream API request/reply calls.
      * @param int $publishRetryAttempts Max publish attempts when the JetStream API momentarily has no
      *                                  responder (503). 1 disables retry. Only 503s are retried - a real
      *                                  publish error (precondition mismatch, bad subject) is not.
      * @param int $publishRetryWaitMs Delay between publish retry attempts, in milliseconds.
     */
    public function __construct(
        private readonly NatsClient $client,
        private readonly int $publishRetryAttempts = 3,
        private readonly int $publishRetryWaitMs = 250,
    ) {}

    /**
     * Returns a fluent pull-consumer iterator builder.
     */
    public function pullConsumer(string $stream, string $consumer): PullConsumerIterator
    {
        self::assertValidJsName($stream, 'stream');
        self::assertValidJsName($consumer, 'consumer');

        return new PullConsumerIterator($this, $stream, $consumer);
    }

    /**
     * Starts an atomic (all-or-nothing) publish batch (ADR-50). The target stream must be created with
     * `allow_atomic` enabled. Pass a batch id (1..64 chars) to use your own, or omit it for a
     * generated one.
     *
     * Requires NATS server 2.12+: `commit()` pre-flights the INFO-advertised server version and
     * throws an UnsupportedFeatureException before anything is published to an older server (#152).
     * A batch-incapable server that slips past that gate (unparseable version, mixed-version
     * cluster) acknowledges the batch start/commit as plain publishes; `commit()` detects that and
     * throws instead of silently storing the batch message-by-message (#130).
     */
    public function batch(?string $batchId = null): BatchPublisher
    {
        if ($batchId !== null && ($batchId === '' || strlen($batchId) > 64)) {
            throw new JetStreamException('Batch id must be between 1 and 64 characters');
        }

        return new BatchPublisher($this->client, $batchId ?? bin2hex(random_bytes(16)));
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
     * Lists the names of all KeyValue buckets (the `KV_`-prefixed streams), with the prefix stripped.
     * Mirrors nats.go / nats.java KV bucket discovery (#60).
     *
     * @return Future<list<string>>
     */
    public function keyValueBucketNames(): Future
    {
        return async(function (): array {
            $names = [];
            foreach ($this->streamNames()->await() as $stream) {
                if (str_starts_with($stream, 'KV_')) {
                    $names[] = substr($stream, 3);
                }
            }

            return $names;
        });
    }

    /**
     * Lists the names of all Object Store buckets (the `OBJ_`-prefixed streams), with the prefix
     * stripped. Mirrors nats.go / nats.java Object Store bucket discovery (#60).
     *
     * @return Future<list<string>>
     */
    public function objectStoreBucketNames(): Future
    {
        return async(function (): array {
            $names = [];
            foreach ($this->streamNames()->await() as $stream) {
                if (str_starts_with($stream, 'OBJ_')) {
                    $names[] = substr($stream, 4);
                }
            }

            return $names;
        });
    }

    /**
     * Returns a KeyValue bucket context. The wrapper is an all-readonly value object and cheap to
     * build, so it is constructed per call rather than memoized: a per-name cache on this
     * long-lived context had no eviction (not even deleteBucket()), so a client touching many
     * bucket names (e.g. one per tenant) retained one wrapper per name forever (#133).
     */
    public function keyValue(string $bucket): KeyValueBucket
    {
        self::assertValidBucket($bucket);

        return new KeyValueBucket($this->client, $this, $bucket);
    }

    /**
     * Returns an Object Store bucket context. Constructed per call, not memoized - same rationale
     * as {@see keyValue()} (#133).
     */
    public function objectStore(string $bucket): ObjectStoreBucket
    {
        self::assertValidBucket($bucket);

        return new ObjectStoreBucket($this->client, $this, $bucket);
    }

    /**
     * Validates a KV/Object Store bucket name. The name is interpolated into the backing stream name
     * (`KV_<bucket>`/`OBJ_<bucket>`) and subject prefixes (`$KV.<bucket>.>`/`$O.<bucket>.>`), so a name
     * with dots or wildcards would silently mis-scope those subjects; restrict it to the same safe set
     * the official clients use.
     */
    private static function assertValidBucket(string $bucket): void
    {
        if ($bucket === '' || preg_match('/^[A-Za-z0-9_-]+$/', $bucket) !== 1) {
            throw new JetStreamException(
                'Invalid bucket name "' . $bucket . '": only letters, digits, "-" and "_" are allowed',
            );
        }
    }

    /**
     * Validates a stream or consumer (durable) name before it is interpolated into a `$JS.API.*`
     * subject. A subject token separator or wildcard in the name silently changes which API endpoint
     * the request hits: `createConsumer('S', 'a.b')` is routed by the server as the filtered-create
     * form (consumer "a", filter "b"), a lookup/delete of that name hits no API route at all (a
     * misleading 503), and a dotted stream name on the Direct Get path can be routed as
     * `DIRECT.GET.<stream>.<last_by_subject>` and silently return data from a SIBLING stream.
     * Mirrors nats.go `checkStreamName` / `checkConsumerName` (#131).
     */
    private static function assertValidJsName(string $name, string $kind): void
    {
        if ($name === '' || preg_match('/[ \t\r\n.*>\/\\\\]/', $name) === 1) {
            throw new JetStreamException(
                'Invalid ' . $kind . ' name "' . $name . '": must be non-empty and must not contain spaces, tabs, '
                . "'.', '*', '>', '/' or '\\'",
            );
        }
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
        self::assertValidJsName($name, 'stream');

        return async(function () use ($name, $subjects, $options): StreamInfo {
            // A stream may legitimately have no subjects of its own when it ingests from a mirror or from
            // one or more sources (a pure aggregate/replica stream); only reject empty subjects otherwise.
            $hasMirrorConfig = is_array($options['mirror'] ?? null);
            $hasSourcesConfig = is_array($options['sources'] ?? null) && $options['sources'] !== [];

            if ($subjects === [] && !$hasMirrorConfig && !$hasSourcesConfig) {
                throw new JetStreamException('Stream subjects must not be empty unless mirror or sources configuration is provided');
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
     * Creates a stream from a typed {@see StreamConfiguration} builder (#53), the discoverable
     * alternative to the array-based {@see createStream()}.
     *
     * @return Future<StreamInfo>
     */
    public function addStream(StreamConfiguration $config): Future
    {
        self::assertValidJsName($config->name(), 'stream');

        return async(function () use ($config): StreamInfo {
            $response = $this->requestJson(JetStreamApi::STREAM_CREATE_PREFIX . $config->name(), $config->toArray());

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
        self::assertValidJsName($name, 'stream');

        return async(function () use ($name, $config): StreamInfo {
            $payload = array_merge($config, ['name' => $name]);

            $response = $this->requestJson(JetStreamApi::STREAM_UPDATE_PREFIX . $name, $payload);

            return StreamInfo::fromArray($response);
        });
    }

    /**
     * Creates a stream, falling back to an update when it already exists - an idempotent upsert.
     * Mirrors nats.go / nats.java `CreateOrUpdateStream` (#44).
     *
     * @param list<string> $subjects
     * @param array<string,mixed> $options Additional stream config fields.
     * @return Future<StreamInfo>
     */
    public function createOrUpdateStream(string $name, array $subjects, array $options = []): Future
    {
        self::assertValidJsName($name, 'stream');

        return async(function () use ($name, $subjects, $options): StreamInfo {
            try {
                return $this->createStream($name, $subjects, $options)->await();
            } catch (JetStreamException $e) {
                if (!self::isStreamNameInUseError($e)) {
                    throw $e;
                }

                return $this->updateStream($name, array_merge($options, ['subjects' => $subjects]))->await();
            }
        });
    }

    /**
     * Whether a stream-create rejection means "stream name already in use" (err_code 10058), i.e. the
     * stream exists. Falls back to description wording only when the envelope carried no err_code
     * (an old server) - wording changes between server versions (#154).
     */
    private static function isStreamNameInUseError(JetStreamException $e): bool
    {
        if ($e->getErrCode() !== null) {
            return $e->getErrCode() === ApiErrCode::STREAM_NAME_IN_USE;
        }

        return stripos($e->getMessage(), 'already in use') !== false;
    }

    /**
     * Returns the names of all streams (optionally filtered to those carrying a subject), without the
     * full StreamInfo payload. Mirrors nats.go / nats.java `StreamNames` (#35).
     *
     * @param string|null $subjectFilter Optional subject the stream must carry.
     * @return Future<list<string>>
     */
    public function streamNames(?string $subjectFilter = null): Future
    {
        return async(function () use ($subjectFilter): array {
            $names = [];
            $offset = 0;
            $body = $subjectFilter !== null && $subjectFilter !== '' ? ['subject' => $subjectFilter] : [];

            do {
                $response = $this->requestJson(JetStreamApi::STREAM_NAMES, ['offset' => $offset] + $body);
                /** @var list<string> $page */
                $page = is_array($response['streams'] ?? null)
                    ? array_values(array_filter($response['streams'], 'is_string'))
                    : [];

                foreach ($page as $name) {
                    $names[] = $name;
                }

                $offset += count($page);
                $total = is_int($response['total'] ?? null) ? $response['total'] : count($names);
            } while ($page !== [] && count($names) < $total);

            return $names;
        });
    }

    /**
     * Retrieves stream metadata by name.
     *
     * @return Future<StreamInfo>
     */
    public function getStream(string $name): Future
    {
        self::assertValidJsName($name, 'stream');

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
        self::assertValidJsName($name, 'stream');

        return async(function () use ($name): bool {
            $response = $this->requestJson(JetStreamApi::STREAM_DELETE_PREFIX . $name, []);

            return (bool) ($response['success'] ?? false);
        });
    }

    /**
     * Purges all messages from a stream, optionally filtering by subject or sequence.
     *
     * @param array<string,mixed> $options Optional filter: set 'filter' for subject, 'seq' for up-to sequence.
     * @return Future<array{purged: int}>
     */
    public function purgeStream(string $name, array $options = []): Future
    {
        self::assertValidJsName($name, 'stream');

        return async(function () use ($name, $options): array {
            $response = $this->requestJson(JetStreamApi::STREAM_PURGE_PREFIX . $name, $options);

            return ['purged' => (int) ($response['purged'] ?? 0)];
        });
    }

    /**
     * Lists all streams (with optional subject filter).
     *
     * @param array<string,mixed> $options Optional: 'subject' filter.
     * @return Future<list<StreamInfo>>
     */
    public function listStreams(array $options = []): Future
    {
        return async(function () use ($options): array {
            $streams = [];
            $offset = 0;

            do {
                $response = $this->requestJson(JetStreamApi::STREAM_LIST, ['offset' => $offset] + $options);
                /** @var list<array<string,mixed>> $page */
                $page = is_array($response['streams'] ?? null) ? $response['streams'] : [];

                foreach ($page as $stream) {
                    $streams[] = StreamInfo::fromArray($stream);
                }

                $offset += count($page);
                $total = is_int($response['total'] ?? null) ? $response['total'] : count($streams);
                // Stop when the server has no more entries, or a page comes back empty (the empty-page
                // guard prevents an infinite loop if `total` is inconsistent).
            } while ($page !== [] && count($streams) < $total);

            return $streams;
        });
    }

    /**
     * Lists all consumers for a stream.
     *
     * @return Future<list<ConsumerInfo>>
     */
    public function listConsumers(string $stream): Future
    {
        self::assertValidJsName($stream, 'stream');

        return async(function () use ($stream): array {
            $consumers = [];
            $offset = 0;

            do {
                $response = $this->requestJson(JetStreamApi::CONSUMER_LIST_PREFIX . $stream, ['offset' => $offset]);
                /** @var list<array<string,mixed>> $page */
                $page = is_array($response['consumers'] ?? null) ? $response['consumers'] : [];

                foreach ($page as $consumer) {
                    $consumers[] = ConsumerInfo::fromArray($consumer);
                }

                $offset += count($page);
                $total = is_int($response['total'] ?? null) ? $response['total'] : count($consumers);
            } while ($page !== [] && count($consumers) < $total);

            return $consumers;
        });
    }

    /**
     * Fetches a message from a stream by sequence number.
     *
     * @return Future<NatsMessage>
     */
    public function getStreamMessage(string $stream, int $seq): Future
    {
        self::assertValidJsName($stream, 'stream');

        return async(function () use ($stream, $seq): NatsMessage {
            $response = $this->requestJson(
                JetStreamApi::STREAM_MSG_GET_PREFIX . $stream,
                ['seq' => $seq],
            );

            return $this->streamMessageFromResponse($response);
        });
    }

    /**
     * Fetches the LAST message stored on a subject via the leader STREAM.MSG.GET API (`last_by_subj`).
     * Unlike {@see directGetLastMessageForSubject()} (Direct Get, requires `allow_direct`), this works
     * on any stream and is served by the leader. Mirrors nats.go / nats.java `GetLastMsgForSubject`
     * (#36). A wildcard subject is rejected; a missing subject surfaces as a 404 JetStreamException.
     *
     * @return Future<NatsMessage>
     */
    public function getLastMessageForSubject(string $stream, string $subject): Future
    {
        self::assertValidJsName($stream, 'stream');

        return async(function () use ($stream, $subject): NatsMessage {
            if ($subject === '' || str_contains($subject, '*') || str_contains($subject, '>')) {
                throw new JetStreamException('getLastMessageForSubject requires a concrete (non-wildcard) subject');
            }

            $response = $this->requestJson(
                JetStreamApi::STREAM_MSG_GET_PREFIX . $stream,
                ['last_by_subj' => $subject],
            );

            return $this->streamMessageFromResponse($response);
        });
    }

    /**
     * Builds a {@see NatsMessage} from a STREAM.MSG.GET response, decoding the base64 body and any
     * stored header block.
     *
     * @param array<string,mixed> $response
     */
    private function streamMessageFromResponse(array $response): NatsMessage
    {
        /** @var array<string,mixed> $msg */
        $msg = is_array($response['message'] ?? null) ? $response['message'] : [];

        // Use a strict false check rather than `?: ''` so a legitimate falsy body such as "0"
        // is preserved instead of being replaced with an empty string.
        $payload = '';
        if (isset($msg['data']) && is_string($msg['data'])) {
            $decoded = base64_decode($msg['data'], true);
            if ($decoded !== false) {
                $payload = $decoded;
            }
        }

        // Stored messages may carry a header block (base64 'hdrs'); preserve it on the message.
        $rawHeaders = null;
        $encodedHeaders = (isset($msg['hdrs']) && is_string($msg['hdrs'])) ? $msg['hdrs'] : '';
        if ($encodedHeaders !== '') {
            $decodedHeaders = base64_decode($encodedHeaders, true);
            if ($decodedHeaders !== false) {
                $rawHeaders = $decodedHeaders;
            }
        }

        return new NatsMessage(
            subject: (string) ($msg['subject'] ?? ''),
            sid: 0,
            replyTo: null,
            payload: $payload,
            rawHeaders: $rawHeaders,
        );
    }

    /**
     * Deletes a single message from a stream by sequence number ($JS.API.STREAM.MSG.DELETE). By default
     * this is a fast delete (the message is removed but its bytes are not overwritten - `no_erase`).
     * Pass `$secureErase = true` for a secure delete that overwrites the message data with random bytes
     * before removal (slower; mirrors nats.go `SecureDeleteMsg` / nats.java `deleteMessage(seq, true)`).
     *
     * @return Future<bool> True when the server confirms the deletion.
     */
    public function deleteMessage(string $stream, int $seq, bool $secureErase = false): Future
    {
        self::assertValidJsName($stream, 'stream');

        return async(function () use ($stream, $seq, $secureErase): bool {
            $body = ['seq' => $seq];
            if (!$secureErase) {
                // Fast delete: keep the stored bytes in place, just unlink the sequence. A secure
                // erase omits this flag so the server overwrites the data before removing it.
                $body['no_erase'] = true;
            }

            $response = $this->requestJson(JetStreamApi::STREAM_MSG_DELETE_PREFIX . $stream, $body);

            return (bool) ($response['success'] ?? false);
        });
    }

    /**
     * Fetches a message from a stream by sequence number using the JetStream Direct Get API
     * ($JS.API.DIRECT.GET), which requires the stream to be created with allow_direct enabled.
     *
     * Unlike getStreamMessage() (which uses the regular $JS.API.STREAM.MSG.GET request/response and
     * is served only by the stream leader), Direct Get can be answered by any stream replica. The
     * server returns the stored message directly: the payload is the message body and the
     * stream/sequence/subject/timestamp travel as Nats-* headers, which are preserved on the
     * returned message's rawHeaders.
     *
     * @return Future<NatsMessage>
     */
    public function directGetStreamMessage(string $stream, int $seq): Future
    {
        self::assertValidJsName($stream, 'stream');

        return $this->directGet($stream, ['seq' => $seq]);
    }

    /**
     * Fetches the last message stored on a subject using the JetStream Direct Get API
     * ($JS.API.DIRECT.GET). Requires the stream to be created with allow_direct enabled.
     *
     * @return Future<NatsMessage>
     */
    public function directGetLastMessageForSubject(string $stream, string $subject): Future
    {
        self::assertValidJsName($stream, 'stream');

        return $this->directGet($stream, ['last_by_subj' => $subject]);
    }

    /**
     * Issues a Direct Get request and normalizes the direct response (raw body + Nats-* headers)
     * into a NatsMessage, mapping a status header block (e.g. 404 Message Not Found) to a
     * JetStreamException.
     *
     * @param array<string,mixed> $body
     * @return Future<NatsMessage>
     */
    private function directGet(string $stream, array $body): Future
    {
        return async(function () use ($stream, $body): NatsMessage {
            $json = json_encode($body, JSON_THROW_ON_ERROR);

            try {
                $message = $this->client->request(JetStreamApi::STREAM_DIRECT_GET_PREFIX . $stream, $json)->await();
            } catch (JetStreamException $e) {
                throw $e;
            } catch (NatsException $e) {
                // No DIRECT.GET responder => the stream has allow_direct disabled, or the server does
                // not support Direct Get. Surface a clear, catchable 503 so callers can fall back to
                // the leader STREAM.MSG.GET path instead of leaking an opaque no-responders error.
                if (str_contains($e->getMessage(), 'No responders')) {
                    throw new JetStreamException(
                        'JetStream Direct Get is unavailable on stream ' . $stream
                        . ' (enable allow_direct on the stream, or use a server that supports Direct Get)',
                        503,
                        $e,
                    );
                }

                throw $e;
            }

            $headers = NatsHeaders::fromWireBlock($message->rawHeaders);

            // A Direct Get miss (or error) comes back as a status header block with no message body.
            $status = (int) ($headers['Status'] ?? 0);
            if ($status >= 400) {
                $description = (string) ($headers['Description'] ?? 'JetStream direct get error');
                throw new JetStreamException($description, $status);
            }

            // A valid Direct Get hit always carries the message metadata as Nats-* headers. If
            // neither a status nor those headers are present, the reply is not a usable message
            // (e.g. a non-conformant server/proxy); reject it rather than returning a garbage body.
            if (!isset($headers['Nats-Stream']) && !isset($headers['Nats-Sequence'])) {
                throw new JetStreamException('JetStream direct get returned an unrecognized response');
            }

            // The original stored subject travels in the Nats-Subject header; fall back to the reply
            // message subject only if the server omitted it.
            return new NatsMessage(
                subject: $headers['Nats-Subject'] ?? $message->subject,
                sid: 0,
                replyTo: null,
                payload: $message->payload,
                rawHeaders: $message->rawHeaders,
            );
        });
    }

    /**
     * Maximum number of exact subjects packed into one batched Direct Get (`multi_last`) request. A
     * NATS 2.11+ server caps a batched Direct Get at 1024 results and rejects a larger request ("Too
     * Many Results"), so {@see directGetLastForSubjects()} splits longer exact-subject lists into chunks
     * of at most this many subjects - held conservatively below 1024 to leave margin for the
     * end-of-batch marker. Each exact subject matches at most one message, so N subjects yield <= N
     * results and a chunk of this size never trips the cap.
     */
    private const DIRECT_GET_BATCH_MAX_SUBJECTS = 1000;

    /**
     * Bytes reserved below the negotiated `max_payload` for the batched Direct Get request envelope
     * (the `{"multi_last":[...],"batch":<int>}` structural characters plus the `batch` integer) when
     * packing subjects into a chunk, so a full chunk's published request stays within max_payload.
     */
    private const DIRECT_GET_BATCH_ENVELOPE_BYTES = 64;

    /**
     * Fetches the latest message for each of several subjects using batched Direct Get requests (ADR-31
     * `multi_last`), instead of one Direct Get per subject. Requires the stream to be created with
     * `allow_direct` and a server that supports batched Direct Get (NATS 2.11+).
     *
     * The exact-subject list is split into chunks bounded by the server's 1024-result cap and by the
     * negotiated `max_payload`, and one batched request is issued per chunk (sequentially): a bucket
     * with more subjects than a single request may carry is enumerated across several requests rather
     * than a single oversized one the server would reject with "Too Many Results". Because each exact
     * subject matches at most one message and lands in exactly one chunk, concatenating the per-chunk
     * replies yields the same result a single (permitted) request would. The common small bucket
     * produces exactly one chunk and one request.
     *
     * @param list<string> $subjects
     * @return Future<list<NatsMessage>>
     */
    public function directGetLastForSubjects(string $stream, array $subjects, int $expiresMs = 5000): Future
    {
        self::assertValidJsName($stream, 'stream');

        return async(function () use ($stream, $subjects, $expiresMs): array {
            if ($subjects === []) {
                return [];
            }

            // This convenience caps `batch` at the number of subjects, which is correct only for exact
            // subjects (one match each). A wildcard filter can match many stored subjects, so capping at
            // the filter count would silently truncate the result - reject it and point to directGetBatch().
            foreach ($subjects as $subject) {
                if (str_contains($subject, '*') || str_contains($subject, '>')) {
                    throw new JetStreamException(
                        'directGetLastForSubjects expects exact subjects; the wildcard "' . $subject
                        . '" would be truncated - use directGetBatch() with an explicit batch size instead',
                    );
                }
            }

            $messages = [];
            foreach ($this->chunkExactSubjectsForBatch($subjects) as $chunk) {
                $chunkMessages = $this->directGetBatch(
                    $stream,
                    ['multi_last' => $chunk, 'batch' => count($chunk)],
                    $expiresMs,
                )->await();

                foreach ($chunkMessages as $message) {
                    $messages[] = $message;
                }
            }

            return $messages;
        });
    }

    /**
     * Splits an exact-subject list into batched Direct Get chunks bounded by BOTH the server's
     * per-request result cap ({@see DIRECT_GET_BATCH_MAX_SUBJECTS}) AND the negotiated `max_payload`
     * (the `{"multi_last":[...],"batch":N}` request must fit one PUB). Each subject lands in exactly one
     * chunk, so concatenating the chunks' replies reproduces a single request's result. A lone subject
     * whose own encoding already exceeds the payload budget still occupies its own chunk - the resulting
     * request is then rejected by publish()'s max_payload guard, a clear failure rather than silent loss.
     *
     * @param non-empty-list<string> $subjects Non-empty list of exact (wildcard-free) subjects.
     * @return non-empty-list<non-empty-list<string>>
     */
    private function chunkExactSubjectsForBatch(array $subjects): array
    {
        $maxPayload = $this->client->maxPayload();
        // Fall back to the NATS default max_payload (1 MiB) when the server did not advertise one.
        $payloadBudget = ($maxPayload !== null && $maxPayload > 0 ? $maxPayload : 1_048_576)
            - self::DIRECT_GET_BATCH_ENVELOPE_BYTES;
        if ($payloadBudget < 1) {
            $payloadBudget = 1;
        }

        /** @var list<non-empty-list<string>> $chunks */
        $chunks = [];
        /** @var list<string> $current */
        $current = [];
        $currentBytes = 0;
        foreach ($subjects as $subject) {
            // Cost of this subject inside the JSON array, measured with the SAME encoding
            // directGetBatch() serializes the request with (json_encode, default flags): the encoded,
            // quoted, escaped string plus one byte for the separating comma. This must be an exact upper
            // bound, not an approximation - json_encode escapes '/' to '\/' (KV keys legally contain '/'),
            // so strlen + 2 quotes would UNDER-count a slash-bearing subject and let a chunk overflow
            // max_payload. Encoding each subject the way it will actually be sent captures every escape.
            $entryBytes = strlen(json_encode($subject, JSON_THROW_ON_ERROR)) + 1;
            if ($current !== []
                && (count($current) >= self::DIRECT_GET_BATCH_MAX_SUBJECTS
                    || $currentBytes + $entryBytes > $payloadBudget)
            ) {
                $chunks[] = $current;
                $current = [];
                $currentBytes = 0;
            }

            $current[] = $subject;
            $currentBytes += $entryBytes;
        }

        $chunks[] = $current;

        return $chunks;
    }

    /**
     * Issues a batched / multi Direct Get request (ADR-31) and collects the multi-response stream into
     * a list of messages. The server streams one reply per matched message to a private inbox,
     * terminated by an end-of-batch marker (a 204 status, or a final message carrying
     * `Nats-Num-Pending: 0`). The wait is bounded by `$expiresMs` so a silent server cannot hang it.
     *
     * Requires NATS server 2.11+ and a stream created with `allow_direct`.
     *
     * @param array<string,mixed> $body Direct Get batch request body (e.g. `batch`, `multi_last`, `seq`, `up_to_seq`).
     * @return Future<list<NatsMessage>>
     */
    public function directGetBatch(string $stream, array $body, int $expiresMs = 5000): Future
    {
        self::assertValidJsName($stream, 'stream');

        return async(function () use ($stream, $body, $expiresMs): array {
            if ($expiresMs <= 0) {
                throw new JetStreamException('Direct Get batch expiresMs must be greater than zero');
            }

            // NOTE: chunkExactSubjectsForBatch() sizes each subject's contribution to a `multi_last`
            // request with this exact call (json_encode, default flags) to keep a chunk within
            // max_payload. Keep the flags here and there identical - adding e.g. JSON_UNESCAPED_SLASHES
            // to one but not the other would make that budget under-count and let a chunk overflow.
            $json = json_encode($body, JSON_THROW_ON_ERROR);
            $subject = JetStreamApi::STREAM_DIRECT_GET_PREFIX . $stream;
            $inbox = Inbox::generate('_INBOX.JS.DGET');

            $messages = [];
            $done = false;
            /** @var array{code:int,description:string}|null $error */
            $error = null;

            $sid = $this->client->subscribe($inbox, static function (NatsMessage $msg) use (&$messages, &$done, &$error): void {
                $headers = NatsHeaders::fromWireBlock($msg->rawHeaders);
                $status = (int) ($headers['Status'] ?? 0);

                // End-of-batch marker (204), with no payload - the stream is complete.
                if ($status === 204) {
                    $done = true;

                    return;
                }

                if ($status >= 400) {
                    $error = [
                        'code' => $status,
                        'description' => trim((string) ($headers['Description'] ?? '')),
                    ];
                    $done = true;

                    return;
                }

                $messages[] = new NatsMessage(
                    subject: $headers['Nats-Subject'] ?? $msg->subject,
                    sid: 0,
                    replyTo: null,
                    payload: $msg->payload,
                    rawHeaders: $msg->rawHeaders,
                );

                // Some server versions mark the final data message with Nats-Num-Pending: 0 rather than
                // a separate 204; treat that as completion too.
                if (($headers['Nats-Num-Pending'] ?? null) === '0') {
                    $done = true;
                }
            })->await();

            try {
                $this->client->publish($subject, $json, $inbox)->await();

                // Progress-based bound: reset the stall clock on each batch frame, so a healthy-but-slow
                // batch that keeps making progress is never failed - only a server that makes NO
                // progress for the whole interval throws (mirrors #153's missed-heartbeat approach).
                // Each wait's cancellation cancels the underlying socket read so a silent server cannot
                // hang the batch indefinitely.
                $progressIntervalNs = ($expiresMs + 1000) * 1_000_000;
                $lastActivityNs = hrtime(true);
                $seen = count($messages);

                while (!$done) {
                    $nowNs = hrtime(true);
                    if ($nowNs - $lastActivityNs >= $progressIntervalNs) {
                        // No batch frame arrived for the whole progress interval (no 204 EOB, no
                        // Nats-Num-Pending:0, no error frame - those all set $done and exit the loop
                        // normally). Returning the collected prefix would silently truncate the result
                        // as if complete; a stalled server surfaces as an incomplete-result error (#121).
                        throw new JetStreamException(sprintf(
                            'Direct Get batch for stream "%s" stalled: no progress for %d ms '
                                . '(received %d message(s), no end-of-batch marker)',
                            $stream,
                            intdiv($progressIntervalNs, 1_000_000),
                            count($messages),
                        ));
                    }

                    $waitCancellation = new TimeoutCancellation(($progressIntervalNs - ($nowNs - $lastActivityNs)) / 1e9);
                    try {
                        $frames = $this->client->processIncoming($waitCancellation)->await();
                        if ($frames === 0) {
                            delay(0.001, cancellation: $waitCancellation);
                        }
                    } catch (CancelledException) {
                        // This wait segment ended; loop around to re-evaluate the stall deadline.
                    }

                    if (count($messages) > $seen) {
                        // A batch frame landed: progress. Rebase the stall clock from now.
                        $seen = count($messages);
                        $lastActivityNs = hrtime(true);
                    }
                }
            } finally {
                $this->client->unsubscribe($sid)->await();
            }

            if ($error !== null) {
                throw new JetStreamException(
                    $error['description'] !== '' ? $error['description'] : 'JetStream direct get batch error',
                    $error['code'],
                );
            }

            return $messages;
        });
    }

    /**
     * Creates or updates a durable consumer for a stream.
     *
     * Version-gated `$options`: `filter_subjects` requires NATS 2.10+, `priority_groups`/
     * `priority_policy` require 2.11+. An older server rejects these with an UnsupportedFeatureException.
     *
     * @param array<string,mixed> $options Additional consumer config fields (max_deliver, ack_wait, etc.).
     * @return Future<ConsumerInfo>
     */
    public function createConsumer(string $stream, string $consumer, ?string $filterSubject = null, array $options = []): Future
    {
        self::assertValidJsName($stream, 'stream');
        self::assertValidJsName($consumer, 'consumer');

        return async(function () use ($stream, $consumer, $filterSubject, $options): ConsumerInfo {
            $config = $this->applyDefaultAckPolicy($options);
            $config['durable_name'] = $consumer;
            $config = $this->applyFilterSubjects($config, $filterSubject);
            $this->assertValidPriorityConfig($config);

            $response = $this->requestJson(
                JetStreamApi::CONSUMER_CREATE_PREFIX . $stream . '.' . $consumer,
                ['stream_name' => $stream, 'config' => $config],
            );

            return ConsumerInfo::fromArray($response);
        });
    }

    /**
     * Creates a consumer from a typed {@see ConsumerConfiguration} builder (#54), the discoverable
     * alternative to the array-based {@see createConsumer()}. A durable name on the config makes the
     * consumer durable; omit it for an ephemeral consumer.
     *
     * @return Future<ConsumerInfo>
     */
    public function addConsumer(string $stream, ConsumerConfiguration $config): Future
    {
        self::assertValidJsName($stream, 'stream');
        if ($config->getName() !== null) {
            self::assertValidJsName($config->getName(), 'consumer');
        }

        return async(function () use ($stream, $config): ConsumerInfo {
            $name = $config->getName();
            $subject = JetStreamApi::CONSUMER_CREATE_PREFIX . $stream . ($name !== null ? '.' . $name : '');

            $response = $this->requestJson($subject, ['stream_name' => $stream, 'config' => $config->toArray()]);

            return ConsumerInfo::fromArray($response);
        });
    }

    /**
     * Creates or updates a durable consumer (idempotent upsert). The JetStream CONSUMER.CREATE API is
     * itself create-or-update on modern servers, so this is equivalent to {@see createConsumer()} but
     * named to document the upsert intent. Mirrors nats.go / nats.java `CreateOrUpdateConsumer` (#44).
     *
     * @param array<string,mixed> $options Additional consumer config fields.
     * @return Future<ConsumerInfo>
     */
    public function addOrUpdateConsumer(string $stream, string $consumer, ?string $filterSubject = null, array $options = []): Future
    {
        self::assertValidJsName($stream, 'stream');
        self::assertValidJsName($consumer, 'consumer');

        return $this->createConsumer($stream, $consumer, $filterSubject, $options);
    }

    /**
     * Returns the names of all consumers on a stream, without the full ConsumerInfo payload.
     * Mirrors nats.go / nats.java `ConsumerNames` (#35).
     *
     * @return Future<list<string>>
     */
    public function consumerNames(string $stream): Future
    {
        self::assertValidJsName($stream, 'stream');

        return async(function () use ($stream): array {
            $names = [];
            $offset = 0;

            do {
                $response = $this->requestJson(JetStreamApi::CONSUMER_NAMES_PREFIX . $stream, ['offset' => $offset]);
                /** @var list<string> $page */
                $page = is_array($response['consumers'] ?? null)
                    ? array_values(array_filter($response['consumers'], 'is_string'))
                    : [];

                foreach ($page as $name) {
                    $names[] = $name;
                }

                $offset += count($page);
                $total = is_int($response['total'] ?? null) ? $response['total'] : count($names);
            } while ($page !== [] && count($names) < $total);

            return $names;
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
        self::assertValidJsName($stream, 'stream');

        return async(function () use ($stream, $filterSubject, $options): ConsumerInfo {
            $config = $this->applyDefaultAckPolicy($options);
            $config = $this->applyFilterSubjects($config, $filterSubject);
            $this->assertValidPriorityConfig($config);

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
        self::assertValidJsName($stream, 'stream');
        self::assertValidJsName($consumer, 'consumer');

        return async(function () use ($stream, $consumer, $deliverSubject, $filterSubject, $options): ConsumerInfo {
            $config = $this->applyDefaultAckPolicy($options);
            $config['durable_name'] = $consumer;
            $config['deliver_subject'] = $deliverSubject;
            $config = $this->applyFilterSubjects($config, $filterSubject);

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
        self::assertValidJsName($stream, 'stream');

        return async(function () use ($stream, $deliverSubject, $filterSubject, $options): ConsumerInfo {
            $config = $this->applyDefaultAckPolicy($options);
            $config['deliver_subject'] = $deliverSubject;
            $config = $this->applyFilterSubjects($config, $filterSubject);

            // Have the server reap this ephemeral consumer once it has no interest (e.g. after the
            // caller unsubscribes, or an ordered consumer is recreated/abandoned), so long-running
            // apps that re-subscribe do not leak server-side consumers. An active subscription keeps
            // it alive. Callers can override by passing their own inactive_threshold.
            if (!array_key_exists('inactive_threshold', $config)) {
                $config['inactive_threshold'] = self::EPHEMERAL_INACTIVE_THRESHOLD_NS;
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
        self::assertValidJsName($stream, 'stream');
        self::assertValidJsName($consumer, 'consumer');

        return async(function () use ($stream, $consumer, $handler, $deliverSubject, $filterSubject, $consumerOptions): int {
            $deliver = $deliverSubject ?? Inbox::generate('_INBOX.JS.PUSH');

            $this->createPushConsumer($stream, $consumer, $deliver, $filterSubject, $consumerOptions)->await();

            // When the caller asked for idle heartbeats, guard the subscription: two silent intervals
            // mean the caller-owned consumer stopped delivering (reaped/deleted/interest lost). The
            // library cannot recreate a consumer it does not own, so it surfaces the stall (#113).
            $idleHeartbeatNs = self::idleHeartbeatOf($consumerOptions);
            $state = $idleHeartbeatNs !== null ? $this->pushWatchdogState($stream, sprintf('"%s"', $consumer)) : null;
            $consumerLabel = sprintf('"%s"', $consumer);

            $sid = $this->client->subscribe($deliver, function (NatsMessage $message) use ($handler, $state, $stream, $consumerLabel): void {
                $state?->touch();

                if ($this->handlePushControlMessage($message, $controlHeaders)) {
                    // A non-100 (4xx/5xx) status frame was intercepted and withheld from the handler.
                    // For a caller-owned consumer the library cannot recreate, so surface the terminal
                    // status through the error listener rather than silently dropping it (#121).
                    $this->surfaceCallerOwnedPushStatus($controlHeaders ?? [], $stream, $consumerLabel);

                    return;
                }

                $handler($message);
            })->await();

            if ($idleHeartbeatNs !== null && $state !== null) {
                $this->armHeartbeatWatchdog($sid, $idleHeartbeatNs, $state);
            }

            return $sid;
        });
    }

    /**
     * Creates an ephemeral push consumer and subscribes with JetStream control-frame handling.
     *
     * @param callable(NatsMessage):void $handler
     * @param array<string,mixed> $consumerOptions Additional consumer config fields.
     * @param (callable(ConsumerInfo):void)|null $onConsumerCreated Invoked with the created consumer
     *        before the subscription is established - e.g. to inspect `num_pending` and signal an
     *        end-of-initial-data when the consumer starts with nothing pending (#99).
     * @return Future<int>
     */
    public function subscribeEphemeralPushConsumer(
        string $stream,
        callable $handler,
        ?string $deliverSubject = null,
        ?string $filterSubject = null,
        array $consumerOptions = [],
        ?callable $onConsumerCreated = null,
    ): Future {
        self::assertValidJsName($stream, 'stream');

        return async(function () use ($stream, $handler, $deliverSubject, $filterSubject, $consumerOptions, $onConsumerCreated): int {
            $deliver = $deliverSubject ?? Inbox::generate('_INBOX.JS.PUSH');

            $consumer = $this->createEphemeralPushConsumer($stream, $deliver, $filterSubject, $consumerOptions)->await();

            if ($onConsumerCreated !== null) {
                $onConsumerCreated($consumer);
            }

            // When the caller asked for idle heartbeats, guard the subscription: two silent intervals
            // mean the ephemeral consumer was reaped/lost. The library does not own its lifecycle here
            // (unlike an ordered consumer, which it recreates), so it surfaces the stall (#113).
            $idleHeartbeatNs = self::idleHeartbeatOf($consumerOptions);
            $state = $idleHeartbeatNs !== null ? $this->pushWatchdogState($stream, sprintf('"%s"', $consumer->name)) : null;
            $consumerLabel = sprintf('"%s"', $consumer->name);

            $sid = $this->client->subscribe($deliver, function (NatsMessage $message) use ($handler, $state, $stream, $consumerLabel): void {
                $state?->touch();

                if ($this->handlePushControlMessage($message, $controlHeaders)) {
                    // A non-100 (4xx/5xx) status frame was intercepted and withheld from the handler.
                    // For a caller-owned ephemeral consumer the library cannot recreate, so surface the
                    // terminal status through the error listener rather than silently dropping it (#121).
                    $this->surfaceCallerOwnedPushStatus($controlHeaders ?? [], $stream, $consumerLabel);

                    return;
                }

                $handler($message);
            })->await();

            if ($idleHeartbeatNs !== null && $state !== null) {
                $this->armHeartbeatWatchdog($sid, $idleHeartbeatNs, $state);
            }

            return $sid;
        });
    }

    /**
     * Creates an ordered ephemeral push consumer with automatic recreation on sequence gaps.
     *
     * @param callable(NatsMessage):void $handler
     * @return Future<int>
     */
    public function subscribeOrderedConsumer(
        string $stream,
        callable $handler,
        ?string $filterSubject = null,
        ?int $idleHeartbeatNs = null,
    ): Future {
        self::assertValidJsName($stream, 'stream');

        if ($idleHeartbeatNs !== null && $idleHeartbeatNs <= 0) {
            throw new \InvalidArgumentException('idleHeartbeatNs must be a positive integer (nanoseconds)');
        }
        $idleHeartbeatNs ??= self::ORDERED_IDLE_HEARTBEAT_NS;

        return async(function () use ($stream, $handler, $filterSubject, $idleHeartbeatNs): int {
            $deliver = Inbox::generate('_INBOX.JS.ORD');
            // Ordered delivery is tracked by the CONSUMER sequence, which increments by one per
            // delivery even for a filtered consumer over a stream that also carries non-matching
            // messages (whose STREAM sequence would be non-contiguous). The STREAM sequence is used
            // only as the restart point when a push is missed.
            $expectedConsumerSeq = 1;
            $lastStreamSeq = 0;
            // Latch for the unparseable-$JS.ACK protocol error: emitted once per consumer instance
            // (re-armed on recreate), not once per message, so a stream of unparseable deliveries
            // cannot become an error storm (#155).
            $ackParseErrorEmitted = false;

            $consumerOptions = [
                'flow_control' => true,
                'idle_heartbeat' => $idleHeartbeatNs,
                'ack_policy' => 'none',
                'max_deliver' => 1,
                'mem_storage' => true,
                // ADR-17 / nats.go ordered.go pin R1; without it an interest-retention stream's
                // replica count would be inherited by the ephemeral consumer.
                'num_replicas' => 1,
            ];

            $consumer = $this->createEphemeralPushConsumer($stream, $deliver, $filterSubject, $consumerOptions)->await();
            $consumerName = $consumer->name;

            // Recreate the consumer starting just after the last in-order message and restart the
            // consumer-sequence count. Shared by the missed-push path and the idle-heartbeat tail-gap
            // path. The recreated consumer replays the missing range in order; if the restart point was
            // pruned it resumes from the next available message - no out-of-order/duplicate delivery
            // and no recreate storm. A failed recreate (stream pruned/deleted, leadership change,
            // transient timeout) is CONTAINED here so it cannot throw out of the shared subscription
            // dispatch loop and abort delivery for every other subscription on the connection.
            // The watchdog (armed below) and the dispatch handler both trigger $recreate; $state carries
            // the in-flight guard that serializes them, so it is built before $recreate for the closure
            // to capture. See HeartbeatWatchdogState::$recreateInFlight for the race this closes (#113).
            $state = new HeartbeatWatchdogState(hrtime(true));

            // Reassigned to the real dispatch handler just below; captured by reference in $recreate so
            // the rotation can re-subscribe the fresh deliver inbox with the SAME handler (the two
            // closures are mutually recursive - the handler triggers $recreate, $recreate re-subscribes
            // the handler). The no-op placeholder is never invoked; it only fixes the callable type.
            $deliverHandler = static function (NatsMessage $message): void {};

            $recreate = function () use (&$expectedConsumerSeq, &$lastStreamSeq, &$consumerName, &$consumerOptions, &$ackParseErrorEmitted, &$deliver, &$deliverHandler, $stream, $filterSubject, $idleHeartbeatNs, $state): void {
                if ($state->recreateInFlight) {
                    // A recreate is already running (on the dispatch fiber or the watchdog fiber); it
                    // resumes from the current lastStreamSeq+1, covering this trigger too. A second
                    // concurrent CONSUMER.CREATE would orphan an ephemeral consumer, so no-op (#113).
                    return;
                }
                $state->recreateInFlight = true;

                $consumerOptions['deliver_policy'] = 'by_start_sequence';
                $consumerOptions['opt_start_seq'] = $lastStreamSeq + 1;

                // Rotate the deliver inbox on every recreate (nats.go ordered.go parity). A fresh inbox
                // means any consumer bound to the PREVIOUS inbox - the instance being replaced, or an
                // orphan from a create whose reply was lost - can no longer reach this client once the
                // old inbox is unsubscribed: neither its data frames NOR its plain idle heartbeats
                // survive to the tail-gap check, so a non-current heartbeat cannot drive a recreate
                // storm (#122). Dropping the client's interest on the old inbox also lets the server's
                // inactive_threshold reap that orphan, so the leak self-heals; the best-effort delete
                // below is now only faster cleanup. Because only the current consumer's frames arrive
                // on the current inbox, the tail-gap check is inherently scoped without parsing any
                // control-frame subject.
                $oldSid = $state->deliverSid;
                $oldConsumerName = $consumerName;
                $newDeliver = Inbox::generate('_INBOX.JS.ORD');
                $newSid = null;

                // Names this episode asked the server to create but did NOT adopt. A create whose
                // reply we never saw (timeout / connection loss - NOT a definitive JetStream API
                // rejection) may have SUCCEEDED server-side, leaving an ephemeral bound to $newDeliver.
                // Because the name is chosen client-side, that orphan is deletable by name even without
                // its create reply (#122).
                $orphanCandidates = [];

                try {
                    try {
                        $this->deleteConsumer($stream, $oldConsumerName)->await();
                    } catch (\Throwable) {
                        // Best-effort cleanup for the ephemeral being replaced. Any failure must fall
                        // through: TimeoutException/ConnectionException are NOT JetStreamExceptions, a
                        // timed-out delete may well have succeeded server-side, and the old-inbox
                        // unsubscribe below already makes inactive_threshold reap it (#151/#122).
                    }

                    // Establish interest on the fresh inbox BEFORE the create points the consumer at
                    // it, so no delivery is lost in the create/subscribe window.
                    $newSid = $this->client->subscribe($newDeliver, $deliverHandler)->await();

                    // Retry the create: after the delete, a failed create leaves NOTHING that would
                    // ever deliver a message or heartbeat on the inbox again, so a transient failure
                    // (timeout, leadership change) must not end recovery permanently (#114).
                    for ($attempt = 1; ; $attempt++) {
                        // A FRESH client-chosen name per attempt (the server honours config.name and
                        // echoes it). Each attempt therefore uses a DISTINCT name, so a create whose
                        // reply is lost spawns a DISTINCT server-side consumer - not an idempotent
                        // no-op on the wire. That orphan is deletable by name (#122): the old-inbox
                        // unsubscribe drops its interest so inactive_threshold reaps it, and the
                        // best-effort reap below deletes it faster.
                        $candidateName = self::generateOrderedConsumerName();
                        $consumerOptions['name'] = $candidateName;

                        // Adopt the new instance BEFORE the create await, not after. The server can
                        // begin delivering the replay on $newDeliver the instant the consumer exists,
                        // and those frames are dispatched to $deliverHandler DURING this create's own
                        // read-pump - before ->await() returns. If $consumerName/$expectedConsumerSeq
                        // still named the old instance, the replayed frames would be filtered out by
                        // name and a later one would trip a spurious gap, cascading into a recreate
                        // storm under load (only the pre-drop message is ever delivered). The name is
                        // client-chosen, so it is known here; adopt it up front so the new consumer's
                        // frames are recognised and sequence-checked from cseq 1 (#122).
                        $consumerName = $candidateName;
                        $deliver = $newDeliver;
                        $expectedConsumerSeq = 1;
                        $ackParseErrorEmitted = false;

                        try {
                            $consumer = $this->createEphemeralPushConsumer($stream, $newDeliver, $filterSubject, $consumerOptions)->await();
                            // The server echoes config.name, so this equals $candidateName; keep the
                            // authoritative returned value defensively.
                            $consumerName = $consumer->name;

                            // Track the rotated inbox and re-arm the watchdog on the NEW sid (it arms on
                            // the deliver sid, and the old sid is about to be unsubscribed). The watchdog
                            // sets the miss latch before invoking onMiss and only an inbound frame's
                            // touch() clears it, so a replacement reaped again before its first heartbeat
                            // would leave the latch stuck; clear it and rebase the silence clock from
                            // now, when the new consumer is live. The in-flight guard prevents a storm,
                            // and the old watchdog self-cancels once its sid goes inactive (#113).
                            $state->deliverSid = $newSid;
                            $state->notified = false;
                            $state->lastActivityNs = hrtime(true);
                            $this->armHeartbeatWatchdog($newSid, $idleHeartbeatNs, $state);

                            // Drop interest on the old inbox: the replaced instance and any lost-reply
                            // orphan on it stop reaching this client, and their inactive_threshold can
                            // fire. Best-effort - a failed UNSUB must not undo recovery (#122).
                            if ($oldSid !== null) {
                                try {
                                    $this->client->unsubscribe($oldSid)->await();
                                } catch (\Throwable) {
                                }
                            }

                            // Faster-cleanup reap of any orphan from an earlier lost-reply attempt this
                            // episode. Best-effort: a reap failure must never undo recovery (#122).
                            $this->reapOrphanedConsumers($stream, $orphanCandidates);

                            return;
                        } catch (\Throwable $e) {
                            if (!$e instanceof JetStreamException) {
                                // Not a definitive server API rejection (timeout / connection loss): the
                                // consumer named $candidateName may exist server-side. Remember it to
                                // reap once we recover, or on terminal failure below (#122).
                                $orphanCandidates[] = $candidateName;
                            }

                            if ($attempt >= self::ORDERED_RECREATE_ATTEMPTS) {
                                throw $e;
                            }

                            delay(self::ORDERED_RECREATE_RETRY_DELAY_S * $attempt);
                        }
                    }
                } catch (\Throwable $e) {
                    // Containment: see the docblock above. Best-effort reap any orphans this episode
                    // may have leaked, then surface through the error listener (#114/#122).
                    $this->reapOrphanedConsumers($stream, $orphanCandidates);

                    $this->emitClientError(new JetStreamException(
                        sprintf(
                            'Ordered consumer recreate failed for stream "%s" after %d attempts: %s',
                            $stream,
                            self::ORDERED_RECREATE_ATTEMPTS,
                            $e->getMessage(),
                        ),
                        (int) $e->getCode(),
                        $e,
                    ));

                    // Terminal: the ordered consumer is permanently dead. Tear down BOTH the new inbox
                    // (interest created but never adopted) and the old inbox so neither drains orphan/
                    // stale traffic - "dead" is actually dead rather than a still-live inbox draining
                    // noise (#122).
                    foreach ([$newSid, $oldSid] as $sid) {
                        if ($sid !== null) {
                            try {
                                $this->client->unsubscribe($sid)->await();
                            } catch (\Throwable) {
                                // Best-effort teardown; a failed UNSUB must not escape the dispatch loop.
                            }
                        }
                    }
                    $state->deliverSid = null;

                    // Terminal teardown: the consumer is dead for good, so cancel the watchdog timer now
                    // rather than leaving it to self-cancel once it notices the sid is gone (#122).
                    if ($state->watchdogTimerId !== null) {
                        EventLoop::cancel($state->watchdogTimerId);
                        $state->watchdogTimerId = null;
                    }
                } finally {
                    // Always release the guard - on the success return, the retry-exhausted throw, or a
                    // teardown cancellation - so a legitimately-needed later recreate is never blocked (#113).
                    $state->recreateInFlight = false;
                }
            };

            // The ordered consumer always requests idle heartbeats, so the watchdog recreates it when
            // two intervals pass with no frame - the missed-heartbeat monitor nats.go's ordered.go runs
            // to detect a server-side consumer that was reaped/restarted/lost interest, which the gap
            // logic above can never see because it only runs when a frame arrives (#113).
            $state->onMiss = $recreate;

            $deliverHandler = function (NatsMessage $message) use ($handler, &$expectedConsumerSeq, &$lastStreamSeq, &$consumerName, &$ackParseErrorEmitted, $stream, $recreate, $state): void {
                // Every inbound frame - data, status-100 heartbeat, or flow-control - proves the consumer
                // is alive; rearm the watchdog before any control handling so a heartbeating-but-quiet
                // consumer is never falsely recreated (#113).
                $state->touch();

                if ($this->handlePushControlMessage($message, $controlHeaders)) {
                    $status = (int) (($controlHeaders ?? [])['Status'] ?? 0);
                    if ($status !== 100) {
                        // A non-100 status control frame (409 Consumer Deleted, 404, 408, 503, ...)
                        // means this consumer instance is terminal/gone on the server. It is NOT user
                        // data (handlePushControlMessage already withheld it); recreate from the last
                        // in-order point rather than waiting for deliveries that will never resume (#121).
                        $recreate();

                        return;
                    }

                    // Status-100 tail-gap detection: an idle heartbeat reports the server's last
                    // delivered consumer sequence. Every frame on THIS inbox is from the current
                    // consumer - the deliver inbox is rotated on each recreate and the previous inbox
                    // unsubscribed, so an orphan's / stale instance's frames never arrive here - so the
                    // check needs no per-frame consumer-name scoping (#122). If the server delivered
                    // past what we processed in order, deliveries at the tail were missed and no
                    // further message will expose the gap on its own; recreate proactively from the
                    // last in-order point (#86). The headers were already parsed by
                    // handlePushControlMessage (out-param) - do not re-parse the block (#139).
                    $lastDelivered = $this->heartbeatLastConsumerSeq($controlHeaders ?? []);
                    if ($lastDelivered !== null && $lastDelivered > $expectedConsumerSeq - 1) {
                        $recreate();
                    }

                    return;
                }

                $metadata = JsMessageMetadata::fromMessage($message);

                if ($metadata === null) {
                    if ($message->replyTo !== null && str_starts_with($message->replyTo, '$JS.ACK.')) {
                        // The reply subject claims the $JS.ACK form but does not parse (an unknown
                        // future server shape - the tolerant >= 11-token parser makes this nearly
                        // unreachable). The gap check and the stale-consumer filter below CANNOT run,
                        // so this delivery's ordering is unverified. Surface that loudly instead of
                        // silently dropping every ordering guarantee (#155), then still deliver
                        // best-effort below. No recreate: it would recover nothing (the replacement
                        // consumer would produce the same unparseable form) and costs a round trip.
                        if (!$ackParseErrorEmitted) {
                            $ackParseErrorEmitted = true;
                            $this->emitClientError(new JetStreamException(sprintf(
                                'Ordered consumer on stream "%s": unparseable $JS.ACK reply subject "%s"; delivering without ordering checks (ordering unverified)',
                                $stream,
                                $message->replyTo,
                            )));
                        }
                    }

                    // No JetStream ACK metadata to order on; deliver best-effort.
                    $handler($message);

                    return;
                }

                if ($metadata->consumer !== $consumerName) {
                    // Belt-and-suspenders after inbox rotation: a frame from a different consumer
                    // instance should no longer reach this inbox, but a within-episode lost-reply
                    // orphan (a distinct name bound to the CURRENT inbox until its best-effort delete /
                    // inactive_threshold reaps it) could still deliver here - ignore it so it cannot
                    // skew the new consumer-sequence count or trigger a spurious recreate (#86/#122).
                    return;
                }

                if ($metadata->consumerSequence !== $expectedConsumerSeq) {
                    // A push was missed (the consumer delivery sequence skipped): recreate just after
                    // the last in-order message and DISCARD this out-of-order message.
                    $recreate();

                    return;
                }

                $expectedConsumerSeq++;
                $lastStreamSeq = $metadata->streamSequence;
                $handler($message);
            };

            $sid = $this->client->subscribe($deliver, $deliverHandler)->await();

            // Hand the deliver sid to the recreate closure (via the shared state) so a TERMINAL
            // recreate failure can tear this subscription down, and so each rotation knows which old
            // inbox to unsubscribe - "dead" is actually dead (#122).
            $state->deliverSid = $sid;

            $this->armHeartbeatWatchdog($sid, $idleHeartbeatNs, $state);

            return $sid;
        });
    }

    /**
     * Retrieves consumer metadata by stream and durable name.
     *
     * @return Future<ConsumerInfo>
     */
    public function getConsumer(string $stream, string $consumer): Future
    {
        self::assertValidJsName($stream, 'stream');
        self::assertValidJsName($consumer, 'consumer');

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
        self::assertValidJsName($stream, 'stream');
        self::assertValidJsName($consumer, 'consumer');

        return async(function () use ($stream, $consumer): bool {
            $response = $this->requestJson(JetStreamApi::CONSUMER_DELETE_PREFIX . $stream . '.' . $consumer, []);

            return (bool) ($response['success'] ?? false);
        });
    }

    /**
     * Pauses a consumer until a specified time.
     *
     * @param string $pauseUntil ISO 8601 timestamp (e.g. '2026-03-12T00:00:00Z').
     * @return Future<array<string,mixed>>
     */
    public function pauseConsumer(string $stream, string $consumer, string $pauseUntil): Future
    {
        self::assertValidJsName($stream, 'stream');
        self::assertValidJsName($consumer, 'consumer');

        return async(fn(): array => $this->requestJson(
            JetStreamApi::CONSUMER_PAUSE_PREFIX . $stream . '.' . $consumer,
            ['pause_until' => $pauseUntil],
        ));
    }

    /**
     * Resumes a paused consumer immediately.
     *
     * @return Future<array<string,mixed>>
     */
    public function resumeConsumer(string $stream, string $consumer): Future
    {
        self::assertValidJsName($stream, 'stream');
        self::assertValidJsName($consumer, 'consumer');

        return async(fn(): array => $this->requestJson(
            JetStreamApi::CONSUMER_PAUSE_PREFIX . $stream . '.' . $consumer,
            [],
        ));
    }

    /**
     * Clears the active client pin for a priority group (ADR-42 `pinned_client` policy), so another
     * client can take over the pin on its next pull.
     *
     * @return Future<bool>
     */
    public function unpinConsumer(string $stream, string $consumer, string $group): Future
    {
        self::assertValidJsName($stream, 'stream');
        self::assertValidJsName($consumer, 'consumer');

        return async(function () use ($stream, $consumer, $group): bool {
            if ($group === '') {
                throw new JetStreamException('Priority group must not be empty');
            }

            $response = $this->requestJson(
                JetStreamApi::CONSUMER_UNPIN_PREFIX . $stream . '.' . $consumer,
                ['group' => $group],
            );

            return (bool) ($response['success'] ?? true);
        });
    }

    /**
     * Returns the pinned-client id (`Nats-Pin-Id`) carried by the first message delivered to a newly
     * pinned client (ADR-42), or null when the message has no pin id. Pass it back as the `id` pull
     * field on subsequent fetches to retain the pin.
     */
    public function pinIdOf(NatsMessage $message): ?string
    {
        $headers = NatsHeaders::fromWireBlock($message->rawHeaders);
        $pinId = (string) ($headers['Nats-Pin-Id'] ?? '');

        return $pinId === '' ? null : $pinId;
    }

    /**
     * Issues a JetStream API request, translating a no-responders error into a JetStreamException
     * (code 503) so callers catching JetStreamException are not surprised by a bare NatsException
     * (e.g. publishing to a subject not bound to any stream, or with JetStream disabled).
     *
     * @param array<string,string>|null $headers
     */
    private function jsRequest(string $subject, string $payload, ?array $headers = null): NatsMessage
    {
        try {
            return $headers === null
                ? $this->client->request($subject, $payload)->await()
                : $this->client->requestWithHeaders($subject, $payload, $headers)->await();
        } catch (JetStreamException $e) {
            throw $e;
        } catch (NatsException $e) {
            throw JetStreamRequest::normalizeNoResponders($e, $subject);
        }
    }

    /**
     * Publishes to a stream subject and returns the JetStream publish acknowledgment.
     *
     * Optional headers can be attached: arbitrary `$headers`, a `$msgId` for server-side
     * de-duplication within the stream's `duplicate_window` (emitted as `Nats-Msg-Id`), and a per-
     * message `$ttl` (emitted as `Nats-TTL`; requires the stream to be created with `allow_msg_ttl`).
     * The TTL accepts an integer number of seconds, a Go duration string, or "never". `$msgId` works on
     * NATS 2.2+; `$ttl` requires NATS 2.11+ (the `allow_msg_ttl` stream create call fails on older servers).
     *
     * Optimistic-concurrency preconditions can be attached so the append only succeeds when the stream
     * is in the expected state (the server rejects a mismatch with a `JetStreamException`):
     * `$expectedStream` (Nats-Expected-Stream), `$expectedLastSequence` (Nats-Expected-Last-Sequence),
     * `$expectedLastSubjectSequence` (Nats-Expected-Last-Subject-Sequence - `0` asserts "no prior
     * message on this subject"), and `$expectedLastMsgId` (Nats-Expected-Last-Msg-Id).
     *
     * @param array<string,string> $headers Additional message headers.
     * @param string|null          $msgId   Optional de-duplication id (`Nats-Msg-Id`).
     * @param int|string|null      $ttl     Optional per-message TTL (`Nats-TTL`).
     * @param string|null          $expectedStream              Optional expected target stream name.
     * @param int|null             $expectedLastSequence        Optional expected stream last sequence.
     * @param int|null             $expectedLastSubjectSequence Optional expected per-subject last sequence (0 = none).
     * @param string|null          $expectedLastMsgId           Optional expected last `Nats-Msg-Id`.
     * @return Future<PubAck>
     */
    public function publish(
        string $subject,
        string $payload,
        array $headers = [],
        ?string $msgId = null,
        int|string|null $ttl = null,
        ?string $expectedStream = null,
        ?int $expectedLastSequence = null,
        ?int $expectedLastSubjectSequence = null,
        ?string $expectedLastMsgId = null,
    ): Future {
        return async(function () use ($subject, $payload, $headers, $msgId, $ttl, $expectedStream, $expectedLastSequence, $expectedLastSubjectSequence, $expectedLastMsgId): PubAck {
            if ($msgId !== null) {
                if ($msgId === '') {
                    throw new JetStreamException('Nats-Msg-Id must not be empty');
                }

                $headers['Nats-Msg-Id'] = $msgId;
            }

            if ($ttl !== null) {
                $headers['Nats-TTL'] = MessageTtl::format($ttl);
            }

            if ($expectedStream !== null && $expectedStream !== '') {
                $headers['Nats-Expected-Stream'] = $expectedStream;
            }

            // Sequence preconditions may legitimately be 0 ("expect no prior message"), so compare to
            // null rather than truthiness.
            if ($expectedLastSequence !== null) {
                $headers['Nats-Expected-Last-Sequence'] = (string) $expectedLastSequence;
            }

            if ($expectedLastSubjectSequence !== null) {
                $headers['Nats-Expected-Last-Subject-Sequence'] = (string) $expectedLastSubjectSequence;
            }

            if ($expectedLastMsgId !== null && $expectedLastMsgId !== '') {
                $headers['Nats-Expected-Last-Msg-Id'] = $expectedLastMsgId;
            }

            $message = $this->publishWithRetry($subject, $payload, $headers === [] ? null : $headers);

            return $this->parsePublishAck($message);
        });
    }

    /**
     * Issues a JetStream publish request, retrying when the JetStream API momentarily has no responder
     * (a 503 - e.g. a brief leadership change or the API not yet wired up after reconnect). Mirrors
     * nats.go `RetryAttempts`/`RetryWait` and nats.java's publish retry (#29).
     *
     * @param array<string,string>|null $headers
     */
    private function publishWithRetry(string $subject, string $payload, ?array $headers): NatsMessage
    {
        $attempts = max(1, $this->publishRetryAttempts);

        for ($attempt = 1; ; $attempt++) {
            try {
                return $this->jsRequest($subject, $payload, $headers);
            } catch (JetStreamException $e) {
                // Only transient "no responder" failures are retried; a real publish error (bad
                // subject, precondition mismatch, ...) is surfaced immediately.
                if ($e->getCode() !== 503 || $attempt >= $attempts) {
                    throw $e;
                }

                delay(max(0, $this->publishRetryWaitMs) / 1000);
            }
        }
    }

    /**
     * Publishes a scheduled message using the NATS 2.12 scheduler headers (ADR-51). The target stream
     * must be created with `allow_msg_schedules` enabled (and `allow_msg_ttl` when a schedule TTL is
     * used). The schedule expression may be `@at <RFC3339>`, `@every <duration>`, or a 6-field cron
     * expression - build it with the {@see Schedule} helper.
     *
     * Requires NATS server 2.12+.
     *
     * @param string      $schedule    Schedule expression (@at / @every / cron).
     * @param string|null $scheduleTtl Optional Nats-Schedule-TTL (requires `allow_msg_ttl` on the stream).
     * @param string|null $source      Optional Nats-Schedule-Source identifier.
     * @param string|null $timeZone    Optional IANA time zone, valid for cron schedules only.
     * @param bool        $rollup      When true, emits Nats-Schedule-Rollup: sub (one active schedule per subject).
     * @return Future<PubAck>
     */
    public function publishScheduled(
        string $scheduleSubject,
        string $targetSubject,
        string $payload,
        string $schedule,
        ?string $scheduleTtl = null,
        ?string $source = null,
        ?string $timeZone = null,
        bool $rollup = false,
    ): Future {
        return async(function () use ($scheduleSubject, $targetSubject, $payload, $schedule, $scheduleTtl, $source, $timeZone, $rollup): PubAck {
            $this->assertSupportedSchedulePattern($schedule);

            if ($timeZone !== null && $timeZone !== '' && !$this->isCronSchedule($schedule)) {
                throw new JetStreamException('Nats-Schedule-Time-Zone is only valid for cron schedule expressions');
            }

            $headers = [
                'Nats-Schedule' => $schedule,
                'Nats-Schedule-Target' => $targetSubject,
            ];

            if ($scheduleTtl !== null && $scheduleTtl !== '') {
                $headers['Nats-Schedule-TTL'] = $scheduleTtl;
            }

            if ($source !== null && $source !== '') {
                $headers['Nats-Schedule-Source'] = $source;
            }

            if ($timeZone !== null && $timeZone !== '') {
                $headers['Nats-Schedule-Time-Zone'] = $timeZone;
            }

            if ($rollup) {
                $headers['Nats-Schedule-Rollup'] = 'sub';
            }

            return $this->parsePublishAck($this->jsRequest($scheduleSubject, $payload, $headers));
        });
    }

    /**
     * Parses a JetStream publish acknowledgement payload into a typed PubAck, mapping a malformed body
     * or an embedded API error to a JetStreamException.
     */
    private function parsePublishAck(NatsMessage $message): PubAck
    {
        try {
            /** @var array<string,mixed> $data */
            $data = json_decode($message->payload, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new JetStreamException('Malformed JetStream publish ack: ' . $e->getMessage(), 0, $e);
        }

        /** @var array<string,mixed>|null $error */
        $error = is_array($data['error'] ?? null) ? $data['error'] : null;
        if ($error !== null) {
            $this->throwApiError(
                (string) ($error['description'] ?? 'JetStream publish error'),
                (int) ($error['code'] ?? 0),
                ApiErrCode::fromEnvelope($error),
            );
        }

        // A publish ack with neither an `error` nor a `stream` is not a valid success - nats.go
        // rejects an empty-stream ack. Without this, such a reply would hydrate as PubAck('', 0)
        // and be returned as a bogus success, masking a server/protocol fault (#121).
        if (!is_string($data['stream'] ?? null) || $data['stream'] === '') {
            throw new JetStreamException('Invalid JetStream publish ack: missing "stream" field');
        }

        return PubAck::fromArray($data);
    }

    /**
     * Atomically increments a distributed counter on a stream subject (ADR-49). The target stream must
     * be created with `allow_msg_counter` enabled. The delta is a signed or unsigned integer string
     * (e.g. "+5", "-3", "10"); the returned new total is also a string so arbitrary-precision values
     * are preserved (PHP int / JSON number precision is insufficient for large counters).
     *
     * Requires NATS server 2.12+.
     *
     * @return Future<string> The new counter value.
     */
    public function incrementCounter(string $subject, string $delta): Future
    {
        return async(function () use ($subject, $delta): string {
            $delta = trim($delta);
            if (preg_match('/^[+-]?\d+$/', $delta) !== 1) {
                throw new JetStreamException('Counter increment must be an integer string (e.g. "+5", "-3", "10")');
            }

            $message = $this->jsRequest($subject, '', ['Nats-Incr' => $delta]);

            return $this->parseCounterValue($message->payload);
        });
    }

    /**
     * Reads the current value of a distributed counter via Direct Get (last message on the subject).
     * Returns "0" when the counter has no stored message yet. The value is returned as a string to
     * preserve arbitrary precision.
     *
     * @return Future<string> The current counter value, or "0" if absent.
     */
    public function counterValue(string $stream, string $subject): Future
    {
        self::assertValidJsName($stream, 'stream');

        return async(function () use ($stream, $subject): string {
            try {
                $message = $this->directGetLastMessageForSubject($stream, $subject)->await();
            } catch (JetStreamException $e) {
                if ($e->getCode() === 404) {
                    return '0';
                }

                throw $e;
            }

            return $this->parseCounterValue($message->payload);
        });
    }

    /**
     * Parses the `{"val":"<bigint>"}` body returned by a counter publish ack or Direct Get, decoding
     * with JSON_BIGINT_AS_STRING so a large value is never truncated to a float. An embedded API error
     * is mapped to a JetStreamException.
     */
    private function parseCounterValue(string $payload): string
    {
        try {
            /** @var array<string,mixed> $data */
            $data = json_decode($payload, true, 512, JSON_THROW_ON_ERROR | JSON_BIGINT_AS_STRING);
        } catch (\JsonException $e) {
            throw new JetStreamException('Malformed counter response: ' . $e->getMessage(), 0, $e);
        }

        /** @var array<string,mixed>|null $error */
        $error = is_array($data['error'] ?? null) ? $data['error'] : null;
        if ($error !== null) {
            $this->throwApiError(
                (string) ($error['description'] ?? 'JetStream counter error'),
                (int) ($error['code'] ?? 0),
                ApiErrCode::fromEnvelope($error),
            );
        }

        $val = $data['val'] ?? null;
        if (is_int($val)) {
            return (string) $val;
        }

        if (is_string($val) && $val !== '') {
            return $val;
        }

        throw new JetStreamException('Counter response did not include a value');
    }

    /**
     * Fetches the next message for a pull consumer.
     *
     * @param array<string,mixed> $pull Optional pull-request fields (see {@see fetchBatch()}).
     * @return Future<NatsMessage>
     */
    public function fetchNext(string $stream, string $consumer, int $expiresMs = 3000, array $pull = []): Future
    {
        self::assertValidJsName($stream, 'stream');
        self::assertValidJsName($consumer, 'consumer');

        return async(function () use ($stream, $consumer, $expiresMs, $pull): NatsMessage {
            $messages = $this->fetchBatch($stream, $consumer, 1, $expiresMs, $pull)->await();

            return $messages[0];
        });
    }

    /**
     * Fetches a batch of messages for a pull consumer.
     *
     * The optional `$pull` array carries ADR-42 priority-group fields and general pull options:
     * `group`, `id` (pin id), `min_pending`, `min_ack_pending`, `priority` (0-9), `max_bytes`,
     * `no_wait`, and `idle_heartbeat` (nanoseconds, ADR-13; must not exceed 50% of expires or an
     * InvalidArgumentException is thrown; the fetch loop absorbs the resulting status-100 heartbeat
     * frames and fails fast with a heartbeat-miss JetStreamException when two intervals pass with
     * no frame at all, #153). Any other key is rejected with a JetStreamException instead of
     * being silently dropped (#132). When a consumer is pinned, the first delivered message carries
     * a `Nats-Pin-Id` header (read it with {@see pinIdOf()}); a stale pin id yields a 423 status
     * surfaced as a JetStreamException with code 423.
     *
     * The priority-group `$pull` fields require NATS server 2.11+ (the `prioritized` policy 2.12+);
     * a plain `{batch, expires}` fetch works on any JetStream server.
     *
     * @param array<string,mixed> $pull Optional pull-request fields.
     * @param (callable(int $code, string $description):void)|null $onTerminalStatus Invoked once, after
     *        a non-empty (partial) batch, when the pull ended with a terminal status (>=400) that
     *        arrived mid-batch. The partial batch is still returned (nats.go parity); this only makes a
     *        non-routine termination (e.g. 409 consumer deleted, 423 stale pin) observable. Routine
     *        batch-completion codes (404/408) flow through too - inspect the code (#92).
     * @return Future<list<NatsMessage>>
     */
    public function fetchBatch(
        string $stream,
        string $consumer,
        int $batch,
        int $expiresMs = 3000,
        array $pull = [],
        ?callable $onTerminalStatus = null,
    ): Future {
        self::assertValidJsName($stream, 'stream');
        self::assertValidJsName($consumer, 'consumer');

        return async(function () use ($stream, $consumer, $batch, $expiresMs, $pull, $onTerminalStatus): array {
            if ($expiresMs <= 0) {
                throw new JetStreamException('Pull fetch expiresMs must be greater than zero');
            }

            if ($batch <= 0) {
                throw new JetStreamException('Pull fetch batch must be greater than zero');
            }

            $payload = $this->buildPullRequest($batch, $expiresMs, $pull);

            $subject = JetStreamApi::CONSUMER_MSG_NEXT_PREFIX . $stream . '.' . $consumer;
            $json = json_encode($payload, JSON_THROW_ON_ERROR);

            $inbox = Inbox::generate('_INBOX.JS.FETCH');
            $messages = [];
            /** @var array{code: int, description: string}|null $terminalStatus */
            $terminalStatus = null;

            // When idle heartbeats were requested (ADR-13), the server emits a status-100 frame at
            // least every idle_heartbeat interval, so any frame on the inbox proves it is alive.
            // Track the last arrival (monotonic) to detect a dead server/route mid-window (#153);
            // buildPullRequest() already validated the value, the is_int check only narrows the type.
            $idleHeartbeatNs = isset($pull['idle_heartbeat']) && is_int($pull['idle_heartbeat']) ? $pull['idle_heartbeat'] : null;
            $lastActivityNs = hrtime(true);

            $sid = $this->client->subscribe($inbox, static function (NatsMessage $msg) use (&$messages, &$terminalStatus, &$lastActivityNs): void {
                $lastActivityNs = hrtime(true);

                $headers = NatsHeaders::fromWireBlock($msg->rawHeaders);
                $status = (int) ($headers['Status'] ?? 0);

                if ($status === 100) {
                    return;
                }

                if ($status >= 400) {
                    $terminalStatus = [
                        'code' => $status,
                        'description' => trim((string) ($headers['Description'] ?? '')),
                    ];

                    return;
                }

                $messages[] = $msg;
            })->await();

            try {
                $this->client->publish($subject, $json, $inbox)->await();

                // Bound the pull by the server expiry (plus slack), and - when heartbeats were
                // requested - by the heartbeat-miss deadline (2 silent intervals, nats.go
                // ErrNoHeartbeat parity). Each wait's cancellation cancels the underlying socket
                // read so a silent server cannot hang the fetch indefinitely.
                $deadlineNs = hrtime(true) + ($expiresMs + 1000) * 1_000_000;
                $lastActivityNs = hrtime(true);

                while (count($messages) < $batch && $terminalStatus === null) {
                    $nowNs = hrtime(true);
                    if ($nowNs >= $deadlineNs) {
                        break;
                    }

                    $waitUntilNs = $deadlineNs;
                    if ($idleHeartbeatNs !== null) {
                        $missAtNs = $lastActivityNs + 2 * $idleHeartbeatNs;
                        if ($nowNs >= $missAtNs) {
                            // Two heartbeat intervals of silence: the server (or route) is gone.
                            // A partial batch is still returned - those messages are real - but an
                            // empty fetch fails fast instead of sitting out the full deadline.
                            if ($messages !== []) {
                                break;
                            }

                            throw new JetStreamException(sprintf(
                                'JetStream pull fetch missed idle heartbeats: no message or heartbeat received for %d ms (2 x idle_heartbeat)',
                                intdiv(2 * $idleHeartbeatNs, 1_000_000),
                            ));
                        }

                        $waitUntilNs = min($waitUntilNs, $missAtNs);
                    }

                    $waitCancellation = new TimeoutCancellation(($waitUntilNs - $nowNs) / 1e9);
                    try {
                        $frames = $this->client->processIncoming($waitCancellation)->await();
                        if ($frames === 0) {
                            delay(0.001, cancellation: $waitCancellation);
                        }
                    } catch (CancelledException) {
                        // This wait segment ended (overall deadline or a heartbeat check came due);
                        // loop around to re-evaluate the deadlines against fresh activity.
                    }
                }
            } finally {
                $this->client->unsubscribe($sid)->await();
            }

            if ($messages === []) {
                if ($terminalStatus !== null) {
                    throw new JetStreamException(
                        $this->formatPullTerminalStatusMessage($terminalStatus['code'], $terminalStatus['description']),
                        $terminalStatus['code'],
                    );
                }

                throw new JetStreamException('No messages received within timeout', 408);
            }

            // A terminal status arrived AFTER >=1 message: the partial batch is returned (nats.go
            // parity), but surface the status so a caller can observe a non-routine mid-batch
            // termination (e.g. the consumer was deleted) instead of only learning on the next pull.
            if ($terminalStatus !== null && $onTerminalStatus !== null) {
                $onTerminalStatus($terminalStatus['code'], $terminalStatus['description']);
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
     * Acknowledges a message and waits for the server to confirm the ACK was received (double-ack),
     * for exactly-once-style processing. Unlike {@see ack()} (fire-and-forget), this sends `+ACK` as a
     * request and blocks until the server's empty confirmation arrives or the timeout elapses.
     * Mirrors nats.go `Msg.DoubleAck()` / nats.java `Message.ackSync()` (#18).
     *
     * @param int|null $timeoutMs Confirmation timeout (null = the client's default request timeout).
     * @return Future<void>
     *
     * @throws JetStreamException When the message carries no reply subject.
     * @throws \IDCT\NATS\Exception\TimeoutException When no confirmation arrives in time.
     */
    public function ackSync(NatsMessage $message, ?int $timeoutMs = null): Future
    {
        return async(function () use ($message, $timeoutMs): void {
            if ($message->replyTo === null || $message->replyTo === '') {
                throw new JetStreamException('JetStream ACK requires a reply subject on the delivered message');
            }

            // A request (not a bare publish) so the server round-trips an empty confirmation that the
            // ACK was durably recorded.
            $this->client->request($message->replyTo, '+ACK', $timeoutMs)->await();
        });
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
     * Validates the schedule expression format. The NATS scheduler (ADR-51, `allow_msg_schedules`)
     * accepts three forms in the Nats-Schedule header: a one-shot "@at <RFC3339>", a recurring
     * "@every <duration>", or a 6-field (seconds-resolution) cron expression.
     */
    private function assertSupportedSchedulePattern(string $schedule): void
    {
        $schedule = trim($schedule);

        // @at <RFC3339> - UTC "Z" or a numeric timezone offset (the server normalizes to UTC), with
        // optional fractional seconds.
        if (preg_match('/^@at\s+\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/', $schedule) === 1) {
            return;
        }

        // @every <duration> (a non-empty interval token; the server validates the exact duration).
        if (preg_match('/^@every\s+\S+/', $schedule) === 1) {
            return;
        }

        // Predefined aliases (ADR-51): @hourly, @daily, @weekly, @monthly, @yearly/@annually, @midnight.
        if (preg_match('/^@(?:hourly|daily|weekly|monthly|yearly|annually|midnight)$/', $schedule) === 1) {
            return;
        }

        // Otherwise it must be a 6-field cron expression (second minute hour dom month dow).
        $fields = $schedule === '' ? [] : (preg_split('/\s+/', $schedule) ?: []);
        if (count($fields) === 6) {
            return;
        }

        throw new JetStreamException(
            'Unsupported schedule expression "' . $schedule
            . '": expected @at <RFC3339>, @every <duration>, a predefined alias (@daily, @hourly, ...),'
            . ' or a 6-field cron expression',
        );
    }

    /**
     * Whether a schedule expression is a cron-class form (a 6-field cron or a predefined alias) as
     * opposed to @at/@every. Only cron-class schedules may carry a Nats-Schedule-Time-Zone header.
     * Uses the same whitespace semantics as the validator so a non-space separator cannot misclassify.
     */
    private function isCronSchedule(string $schedule): bool
    {
        return preg_match('/^@(?:at|every)\s/', trim($schedule)) !== 1;
    }

    /**
     * Publishes an ACK protocol token to a message reply subject.
     *
     * Runs inline in the caller's fiber (#136): the only synchronous work is the replyTo guard, so
     * an async() wrapper here stacked a third fiber hop on every ack/nak/term/inProgress. The
     * guard failure is returned as Future::error - not thrown - so callers (which return or await
     * this future) keep the exact same error surface as the previous async() shape.
     *
     * @return Future<void>
     */
    private function publishAckToken(NatsMessage $message, string $token): Future
    {
        if ($message->replyTo === null || $message->replyTo === '') {
            return Future::error(new JetStreamException('JetStream ACK requires a reply subject on the delivered message'));
        }

        return $this->client->publish($message->replyTo, $token);
    }

    /**
     * Handles JetStream push-control messages (heartbeat/flow-control).
     *
     * Runs synchronously in the dispatch fiber (#139): every push delivery funnels through here, and
     * the previous async() wrapper cost a Future allocation plus an event-loop hop per message while
     * the only awaits inside (the rare flow-control/stalled replies below) almost never run. Those
     * replies still await inline; suspending the dispatch fiber is safe because the connection's
     * per-sid dispatch guard tolerates a handler that suspends mid-drain.
     *
     * @param array<string,string>|null $headers Out-param: receives the parsed header map when the
     *        header block was parsed (stays null for header-less messages), so a caller that reads a
     *        control-frame header afterwards (e.g. Nats-Last-Consumer) does not re-parse it (#139).
     * @return bool True when the message is a control message and was handled.
     */
    private function handlePushControlMessage(NatsMessage $message, ?array &$headers = null): bool
    {
        $headers = null;

        if ($message->rawHeaders === null) {
            // A data message without headers - the overwhelmingly common delivery - cannot be a
            // control frame (those always carry a status line); skip the parse entirely.
            return false;
        }

        $headers = NatsHeaders::fromWireBlock($message->rawHeaders);
        $status = (int) ($headers['Status'] ?? 0);

        if ($status === 0) {
            // No status line: an ordinary data message (possibly with user headers), not a control
            // frame - deliver it.
            return false;
        }

        if ($status !== 100) {
            // A non-100 status frame (e.g. 409 Consumer Deleted, 404, 408, 503) is a JetStream
            // control/error frame, never user payload. Intercept it so it is NOT forwarded to the
            // user handler as if it were a message; the ordered consumer inspects the status (via the
            // out-param headers) to react (recreate/surface) (#121).
            return true;
        }

        $description = strtolower(trim((string) ($headers['Description'] ?? '')));
        $normalizedDescription = preg_replace('/\s+/', ' ', $description) ?: '';
        $replyTo = $message->replyTo ?? '';

        // A flow-control REQUEST carries its reply subject in the message reply ($JS.FC.*). A
        // stalled idle heartbeat instead carries the flow-control reply subject in the
        // Nats-Consumer-Stalled header VALUE and leaves the message reply empty - answer that one,
        // otherwise the server never gets its ack and keeps the consumer stalled (delivery hangs).
        $stalledReply = (string) ($headers['Nats-Consumer-Stalled'] ?? '');

        if ($stalledReply !== '') {
            $this->client->publish($stalledReply, '')->await();
        } else {
            $isFlowControl = $normalizedDescription === 'flowcontrol request'
                || str_starts_with($replyTo, '$JS.FC.');

            if ($isFlowControl && $replyTo !== '') {
                $this->client->publish($replyTo, '')->await();
            }
        }

        // Status 100 control messages are not user payload deliveries.
        return true;
    }

    /**
     * Invokes the client's configured error listener (if any), swallowing listener exceptions. Used
     * to surface contained-but-fatal conditions such as a dead ordered consumer (#114) without
     * throwing out of the shared subscription dispatch loop.
     */
    private function emitClientError(\Throwable $error): void
    {
        $listener = $this->client->options()->errorListener;
        if ($listener === null) {
            return;
        }

        try {
            $listener($error);
        } catch (\Throwable) {
            // A throwing listener must never break dispatch.
        }
    }

    /**
     * Extracts a positive-integer idle_heartbeat (ns) from consumer options, or null when absent or
     * not a usable positive integer. A subscription only gets a heartbeat watchdog when the caller (or
     * the ordered consumer) actually requested heartbeats (#113).
     *
     * @param array<string,mixed> $consumerOptions
     */
    private static function idleHeartbeatOf(array $consumerOptions): ?int
    {
        $value = $consumerOptions['idle_heartbeat'] ?? null;

        return (is_int($value) && $value > 0) ? $value : null;
    }

    /**
     * Builds a heartbeat-watchdog state for a caller-owned push consumer: on a missed-heartbeat episode
     * it surfaces an ErrConsumerNotActive-style error through the error listener - the library cannot
     * recreate a consumer whose lifecycle it does not own, so it makes the stall observable (#113).
     */
    private function pushWatchdogState(string $stream, string $consumerLabel): HeartbeatWatchdogState
    {
        $state = new HeartbeatWatchdogState(hrtime(true));
        $state->onMiss = function () use ($stream, $consumerLabel): void {
            $this->emitClientError(new JetStreamException(sprintf(
                'Push consumer %s on stream "%s" is not active: no message or idle heartbeat for 2 x idle_heartbeat '
                    . '(consumer reaped, deleted, or interest lost); the library cannot recreate a caller-owned consumer '
                    . '- resubscribe or recreate it',
                $consumerLabel,
                $stream,
            )));
        };

        return $state;
    }

    /**
     * Surfaces a terminal (4xx/5xx) push status frame through the error listener for a CALLER-OWNED
     * push consumer (subscribePushConsumer / subscribeEphemeralPushConsumer). handlePushControlMessage()
     * intercepts EVERY non-100 status frame so it is never forwarded to the user handler as bogus data;
     * for a consumer whose lifecycle the library does not own (it cannot recreate it, unlike an ordered
     * consumer) the drop would otherwise be silent, so it is made observable here. Status 100 (idle
     * heartbeat / flow control) is not terminal and is intentionally left silent (#121).
     *
     * @param array<string,string> $headers
     */
    private function surfaceCallerOwnedPushStatus(array $headers, string $stream, string $consumerLabel): void
    {
        $status = (int) ($headers['Status'] ?? 0);
        if ($status < 400) {
            return;
        }

        $description = trim((string) ($headers['Description'] ?? ''));

        $this->emitClientError(new JetStreamException(
            sprintf(
                'Push consumer %s on stream "%s" received a terminal status %d%s; delivery has stopped '
                    . 'and the library cannot recreate a caller-owned consumer - resubscribe or recreate it',
                $consumerLabel,
                $stream,
                $status,
                $description !== '' ? ' (' . $description . ')' : '',
            ),
            $status,
        ));
    }

    /**
     * Arms a missed-heartbeat watchdog for a subscription that requested idle_heartbeat (#113).
     *
     * The server emits a status-100 idle heartbeat at least every $idleHeartbeatNs while the consumer
     * lives, so a subscription that requested heartbeats yet sees NO inbound frame (data, heartbeat, or
     * flow-control) for two intervals proves the consumer stopped delivering - reaped after an
     * inactive_threshold lapse, a mem_storage R1 ordered-consumer restart, or an interest gap - which
     * the sequence-gap logic can never observe because it only runs when a frame arrives (nats.go's
     * ErrConsumerNotActive monitor). On a miss the watchdog rebases the activity clock and invokes
     * $state->onMiss (recreate for ordered; error-listener notification for a caller-owned push) once
     * per silence episode.
     *
     * The repeat timer holds the client and the activity state WEAKLY (mirroring the #126 ping timer)
     * so it can never root an abandoned connection graph, and it self-cancels the moment the
     * subscription it guards is dropped (unsubscribe/close), so no timer outlives its subscription.
     */
    private function armHeartbeatWatchdog(int $sid, int $idleHeartbeatNs, HeartbeatWatchdogState $state): void
    {
        $missThresholdNs = 2 * $idleHeartbeatNs;
        $intervalSeconds = $idleHeartbeatNs / 1_000_000_000;
        $weakClient = \WeakReference::create($this->client);
        $weakState = \WeakReference::create($state);

        // A rotation (#122) arms a new timer for the new deliver sid; cancel the prior one explicitly
        // rather than waiting for its self-cancel tick, so a timer for a stale sid cannot linger if the
        // old inbox's best-effort UNSUB failed while the connection stayed Open.
        if ($state->watchdogTimerId !== null) {
            EventLoop::cancel($state->watchdogTimerId);
        }

        $state->watchdogTimerId = EventLoop::repeat($intervalSeconds, static function (string $timerId) use ($weakClient, $weakState, $sid, $missThresholdNs): void {
            $client = $weakClient->get();
            $state = $weakState->get();
            if ($client === null || $state === null) {
                // The connection, or the subscription whose handler holds the only strong ref to
                // $state, was released; stop guarding a subscription that no longer exists.
                EventLoop::cancel($timerId);

                return;
            }

            $connectionState = $client->state();
            if ($connectionState === ConnectionState::Closed || !$client->isSubscriptionActive($sid)) {
                // Torn down (disconnect/drain, or unsubscribe): the subscription is gone for good.
                EventLoop::cancel($timerId);
                if ($state->watchdogTimerId === $timerId) {
                    $state->watchdogTimerId = null;
                }

                return;
            }

            if ($connectionState !== ConnectionState::Open) {
                // Mid-reconnect: no frame can arrive and the consumer is not the culprit. Rebase the
                // activity clock so the silence window restarts once the connection is Open again.
                $state->lastActivityNs = hrtime(true);

                return;
            }

            if ($state->notified || hrtime(true) - $state->lastActivityNs < $missThresholdNs) {
                // Still inside the window, or this silence episode was already surfaced: wait for a
                // frame to clear the latch rather than fire onMiss every interval (no recreate/error
                // storm, and a stale heartbeat from a just-recreated consumer clears the latch).
                return;
            }

            // Two intervals of total silence. Rebase the clock and latch BEFORE invoking onMiss so a
            // recreate that suspends cannot be re-entered by the next tick.
            $state->lastActivityNs = hrtime(true);
            $state->notified = true;

            try {
                ($state->onMiss)();
            } catch (\Throwable) {
                // onMiss (recreate/error emit) contains its own failures; guard the loop from escapes.
            }
        });
    }

    /**
     * Reads the `Nats-Last-Consumer` sequence an idle-heartbeat control frame reports (the consumer
     * sequence of the last message the server delivered to this consumer), or null when absent/
     * non-numeric. Used by the ordered consumer to detect a missed tail of deliveries (#86). Takes
     * the header map already parsed by {@see handlePushControlMessage()} instead of the message, so
     * the control frame's header block is parsed exactly once per delivery (#139).
     *
     * @param array<string,string> $headers
     */
    private function heartbeatLastConsumerSeq(array $headers): ?int
    {
        $value = trim((string) ($headers['Nats-Last-Consumer'] ?? ''));

        return ($value !== '' && ctype_digit($value)) ? (int) $value : null;
    }

    /**
     * Generates a UNIQUE client-chosen ephemeral consumer name for an ordered-consumer recreate
     * attempt. Choosing the name client-side (the server honours `config.name` and echoes it) means a
     * create whose reply is lost is still deletable by name - the basis for orphan reaping (#122). The
     * name is fresh per attempt (a random 12-byte token), so a lost-reply retry is NOT an idempotent
     * no-op: it creates a DISTINCT server-side consumer. That orphan is reaped by the rotation's
     * old-inbox unsubscribe (its `inactive_threshold` fires) plus the best-effort delete-by-name. The
     * token is a valid JS name (no '.', '*', '>', '/', '\\' or whitespace) and collision-safe.
     */
    private static function generateOrderedConsumerName(): string
    {
        return 'ord-' . bin2hex(random_bytes(12));
    }

    /**
     * Best-effort-deletes ordered-consumer names a recreate episode asked the server to create but
     * never adopted (a create whose reply was lost may still have succeeded server-side, leaving a
     * live ephemeral bound to the rotated deliver inbox). Faster cleanup only: the rotation already
     * unsubscribes that inbox so the orphan's `inactive_threshold` reaps it regardless. Bounded by the
     * retry count. Each delete tolerates ANY failure - the orphan may already be gone, or a timed-out
     * delete may still have succeeded server-side - so a failed reap never undoes a successful
     * recovery (mirror #151) (#122).
     *
     * @param list<string> $names
     */
    private function reapOrphanedConsumers(string $stream, array $names): void
    {
        foreach ($names as $name) {
            try {
                $this->deleteConsumer($stream, $name)->await();
            } catch (\Throwable) {
                // Best-effort: a reap failure must never break recovery.
            }
        }
    }

    /**
     * Resolves the consumer filter configuration. A consumer may filter on a single subject (the
     * `$filterSubject` argument -> `filter_subject`) or on multiple subjects (a `filter_subjects` array
     * supplied via the options -> `filter_subjects`), but not both. Validates the array and rejects the
     * mutually-exclusive combination client-side (issue #10, NATS 2.10+).
     *
     * @param array<string,mixed> $config
     * @return array<string,mixed>
     */
    private function applyFilterSubjects(array $config, ?string $filterSubject): array
    {
        if (array_key_exists('filter_subjects', $config)) {
            $subjects = $config['filter_subjects'];
            if (!is_array($subjects) || $subjects === []) {
                throw new JetStreamException('filter_subjects must be a non-empty array of subjects');
            }

            foreach ($subjects as $subject) {
                if (!is_string($subject) || $subject === '') {
                    throw new JetStreamException('filter_subjects must contain only non-empty subject strings');
                }
            }

            // Mutually exclusive with the singular filter - whether supplied as the argument or
            // smuggled in via the options bag.
            if ($filterSubject !== null || array_key_exists('filter_subject', $config)) {
                throw new JetStreamException('Use either a single filter subject or filter_subjects, not both');
            }

            // Normalize to a positional list so the encoded JSON is a clean array.
            $config['filter_subjects'] = array_values($subjects);

            return $config;
        }

        // An empty string is a caller mistake (null omits the filter); reject it uniformly across all
        // create variants rather than silently dropping it (which would over-broadly consume).
        if ($filterSubject === '') {
            throw new JetStreamException('Consumer filter subject must not be empty (use null to omit)');
        }

        if ($filterSubject !== null) {
            $config['filter_subject'] = $filterSubject;
        }

        return $config;
    }

    /**
     * Supported optional pull-request fields (ADR-13/ADR-42) accepted by fetchBatch()/fetchNext().
     */
    private const PULL_REQUEST_FIELDS = ['group', 'id', 'min_pending', 'min_ack_pending', 'priority', 'max_bytes', 'no_wait', 'idle_heartbeat'];

    /**
     * Builds a pull-consumer CONSUMER.MSG.NEXT request body, merging supported ADR-13/ADR-42 pull
     * fields onto the mandatory batch/expires. An unknown `$pull` key is rejected loudly - silently
     * dropping it would make the caller believe the option took effect (#132). Lightly validates
     * `group` and `priority`, and enforces the ADR-13 `idle_heartbeat <= expires/2` rule (#153);
     * the server validates the rest.
     *
     * @param array<string,mixed> $pull
     * @return array<string,mixed>
     */
    private function buildPullRequest(int $batch, int $expiresMs, array $pull): array
    {
        $request = [
            'batch' => $batch,
            'expires' => $expiresMs * 1_000_000,
        ];

        foreach (array_keys($pull) as $key) {
            if (!in_array($key, self::PULL_REQUEST_FIELDS, true)) {
                throw new JetStreamException(sprintf(
                    'Unknown pull request field "%s"; supported fields: %s',
                    $key,
                    implode(', ', self::PULL_REQUEST_FIELDS),
                ));
            }
        }

        if (isset($pull['group'])) {
            $group = $pull['group'];
            if (!is_string($group) || preg_match('/^[A-Za-z0-9\-_\/=]{1,16}$/', $group) !== 1) {
                throw new JetStreamException('Pull group must be 1..16 characters of [A-Za-z0-9-_/=]');
            }
        }

        if (isset($pull['priority'])) {
            $priority = $pull['priority'];
            if (!is_int($priority) || $priority < 0 || $priority > 9) {
                throw new JetStreamException('Pull priority must be an integer between 0 and 9');
            }
        }

        if (isset($pull['idle_heartbeat'])) {
            $idleHeartbeat = $pull['idle_heartbeat'];
            if (!is_int($idleHeartbeat) || $idleHeartbeat <= 0) {
                throw new \InvalidArgumentException('Pull idle_heartbeat must be a positive integer (nanoseconds)');
            }

            // ADR-13: the heartbeat interval may not exceed half the request expiry, otherwise the
            // server cannot fit two heartbeats into the pull window. Reject client-side with a
            // clear error instead of forwarding a value the server will refuse (#153).
            $expiresNs = $expiresMs * 1_000_000;
            if ($idleHeartbeat * 2 > $expiresNs) {
                throw new \InvalidArgumentException(sprintf(
                    'Pull idle_heartbeat (%d ns) must not exceed 50%% of expires (%d ns) per ADR-13',
                    $idleHeartbeat,
                    $expiresNs,
                ));
            }
        }

        foreach (self::PULL_REQUEST_FIELDS as $field) {
            if (array_key_exists($field, $pull)) {
                $request[$field] = $pull[$field];
            }
        }

        return $request;
    }

    /**
     * Validates ADR-42 priority-group consumer configuration (when present) before a consumer-create
     * round-trip.
     *
     * @param array<string,mixed> $config
     */
    private function assertValidPriorityConfig(array $config): void
    {
        if (array_key_exists('priority_policy', $config)
            && !in_array($config['priority_policy'], ['overflow', 'pinned_client', 'prioritized'], true)
        ) {
            throw new JetStreamException('priority_policy must be one of: overflow, pinned_client, prioritized');
        }

        if (!array_key_exists('priority_groups', $config)) {
            return;
        }

        $groups = $config['priority_groups'];
        if (!is_array($groups) || $groups === []) {
            throw new JetStreamException('priority_groups must be a non-empty array of group names');
        }

        foreach ($groups as $group) {
            if (!is_string($group) || preg_match('/^[A-Za-z0-9\-_\/=]{1,16}$/', $group) !== 1) {
                throw new JetStreamException('priority_groups names must be 1..16 characters of [A-Za-z0-9-_/=]');
            }
        }
    }

    /**
     * Applies JetStream's default explicit ack policy unless the caller overrides it.
     *
     * @param array<string,mixed> $options
     * @return array<string,mixed>
     */
    private function applyDefaultAckPolicy(array $options): array
    {
        if (!array_key_exists('ack_policy', $options)) {
            $options['ack_policy'] = 'explicit';
        }

        return $options;
    }

    /**
     * Formats a terminal pull-consumer status frame into an actionable exception message.
     */
    private function formatPullTerminalStatusMessage(int $status, string $description): string
    {
        $suffix = $description !== '' ? ': ' . $description : '';

        return sprintf('JetStream pull request ended with status %d%s', $status, $suffix);
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
            $this->throwApiError(
                (string) ($error['description'] ?? 'JetStream API error'),
                (int) ($error['code'] ?? 0),
                ApiErrCode::fromEnvelope($error),
            );
        }

        return $data;
    }

    /**
     * Raises a JetStream API error, upgrading it to an {@see \IDCT\NATS\Exception\UnsupportedFeatureException}
     * when the server's response shows the failure is a version-gated feature this server is too old for
     * (e.g. an `unknown field "allow_atomic"` rejection). Reactive only - no per-request version probe.
     */
    private function throwApiError(string $description, int $code, ?int $errCode = null): never
    {
        throw FeatureSupport::unsupportedFromApiError($description, $code, $this->serverVersion(), $errCode)
            ?? new JetStreamException($description, $code, null, $errCode);
    }

    /**
     * The connected server's reported version (from the INFO handshake), or null when unknown.
     */
    private function serverVersion(): ?string
    {
        return $this->client->serverInfo()?->version;
    }

    /**
     * Whether the connected server supports batched (ADR-31 `multi_last`) Direct Get, which requires
     * NATS server 2.11+. KV `getAll()` and Object Store `list()` route through a single batched
     * Direct Get when this is true, and fall back to a per-subject Direct Get fan-out otherwise (#110).
     *
     * The check reads the INFO-advertised version's numeric `major.minor` prefix (pre-release tags
     * ignored, matching the {@see BatchPublisher} comparison). An unknown or unparseable version
     * (proxies, custom builds) returns false so the conservative fan-out fallback - which works on any
     * server - is used rather than sending a request an older server cannot honor.
     */
    public function supportsBatchedDirectGet(): bool
    {
        $version = $this->serverVersion();
        if ($version === null || preg_match('/^v?(\d+)(?:\.(\d+))?/', $version, $match) !== 1) {
            return false;
        }

        $major = (int) $match[1];
        $minor = (int) ($match[2] ?? 0);

        return $major > 2 || ($major === 2 && $minor >= 11);
    }

    /**
     * Returns the stream sequence carried by a JetStream-delivered message (from its $JS.ACK reply
     * subject), or null if the message was not delivered by a JetStream consumer. Useful to recover
     * the stream sequence (e.g. a KeyValue revision) from a push/ordered-consumer delivery.
     */
    public function streamSequenceOf(NatsMessage $message): ?int
    {
        return $this->extractStreamSequence($message);
    }

    /**
     * Returns the full JetStream delivery metadata for a consumed message - stream/consumer sequences,
     * redelivery count (`num_delivered`), pending backlog (`num_pending`), server timestamp, and the
     * JetStream domain - parsed from its `$JS.ACK` reply subject. Mirrors nats.go `Msg.Metadata()` /
     * nats.java `Message.metaData()` (#30).
     *
     * @throws JetStreamException When the message was not delivered by a JetStream consumer.
     */
    public function messageMetadata(NatsMessage $message): JsMessageMetadata
    {
        $metadata = JsMessageMetadata::fromMessage($message);
        if ($metadata === null) {
            throw new JetStreamException(
                'Message is not a JetStream delivery (no parseable $JS.ACK reply subject)',
            );
        }

        return $metadata;
    }

    /**
     * Extracts the stream sequence number from a JetStream reply subject.
     *
     * Reply subjects follow the pattern: $JS.ACK.{stream}.{consumer}.{delivered}.{sseq}.{cseq}.{tm}.{pending}
     */
    private function extractStreamSequence(NatsMessage $message): ?int
    {
        if ($message->replyTo === null) {
            return null;
        }

        $parts = explode('.', $message->replyTo);
        if ($parts[0] !== '$JS' || ($parts[1] ?? null) !== 'ACK') {
            return null;
        }

        // Two ACK reply-subject shapes exist:
        //   9 tokens:  $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending>
        //  >= 11 tokens: $JS.ACK.<domain>.<account>.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending>[.<extra>...]
        // Offsets anchor from the front and trailing tokens are ignored - servers may append them
        // (nats.go parser parity, #155). The stream sequence sits at index 5 in the short form and
        // index 7 in the domain form.
        $count = count($parts);
        $streamSeqIndex = match (true) {
            $count === 9 => 5,
            $count >= 11 => 7,
            default => null,
        };

        if ($streamSeqIndex === null) {
            return null;
        }

        $seq = filter_var($parts[$streamSeqIndex], FILTER_VALIDATE_INT);

        return ($seq !== false) ? $seq : null;
    }
}
