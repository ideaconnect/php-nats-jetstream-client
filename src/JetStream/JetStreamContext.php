<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

use Amp\CancelledException;
use Amp\Future;
use Amp\TimeoutCancellation;
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
     * Requires NATS server 2.12+. A server without batch support acknowledges the batch start/commit
     * as plain publishes; `commit()` detects that and throws an UnsupportedFeatureException instead
     * of silently storing the batch message-by-message (#130).
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
                // "stream name already in use" (err_code 10058): the stream exists, so update it instead.
                if (stripos($e->getMessage(), 'already in use') === false) {
                    throw $e;
                }

                return $this->updateStream($name, array_merge($options, ['subjects' => $subjects]))->await();
            }
        });
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
     * Fetches the latest message for each of several subjects in a single batched Direct Get request
     * (ADR-31 `multi_last`), instead of one Direct Get per subject. Requires the stream to be created
     * with `allow_direct` and a server that supports batched Direct Get (NATS 2.11+).
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

            return $this->directGetBatch(
                $stream,
                ['multi_last' => $subjects, 'batch' => count($subjects)],
                $expiresMs,
            )->await();
        });
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

                $waitCancellation = new TimeoutCancellation(($expiresMs + 1000) / 1000);
                try {
                    while (!$done) {
                        $frames = $this->client->processIncoming($waitCancellation)->await();
                        if ($frames === 0) {
                            delay(0.001, cancellation: $waitCancellation);
                        }
                    }
                } catch (CancelledException) {
                    // Deadline reached; return whatever was collected (or surface a captured error).
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

            return $this->client->subscribe($deliver, function (NatsMessage $message) use ($handler): void {
                if ($this->handlePushControlMessage($message)) {
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

            return $this->client->subscribe($deliver, function (NatsMessage $message) use ($handler): void {
                if ($this->handlePushControlMessage($message)) {
                    return;
                }

                $handler($message);
            })->await();
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
    ): Future {
        self::assertValidJsName($stream, 'stream');

        return async(function () use ($stream, $handler, $filterSubject): int {
            $deliver = Inbox::generate('_INBOX.JS.ORD');
            // Ordered delivery is tracked by the CONSUMER sequence, which increments by one per
            // delivery even for a filtered consumer over a stream that also carries non-matching
            // messages (whose STREAM sequence would be non-contiguous). The STREAM sequence is used
            // only as the restart point when a push is missed.
            $expectedConsumerSeq = 1;
            $lastStreamSeq = 0;

            $consumerOptions = [
                'flow_control' => true,
                'idle_heartbeat' => 5_000_000_000,
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
            $recreate = function () use (&$expectedConsumerSeq, &$lastStreamSeq, &$consumerName, &$consumerOptions, $stream, $deliver, $filterSubject): void {
                $consumerOptions['deliver_policy'] = 'by_start_sequence';
                $consumerOptions['opt_start_seq'] = $lastStreamSeq + 1;

                try {
                    try {
                        $this->deleteConsumer($stream, $consumerName)->await();
                    } catch (\Throwable) {
                        // Best-effort cleanup for ephemeral consumers that may already be gone. Any
                        // failure must fall through: TimeoutException/ConnectionException are NOT
                        // JetStreamExceptions, and only the create leg below restores delivery - a
                        // timed-out delete may well have succeeded server-side (#151).
                    }

                    // Retry the create: after a successful delete, a failed create leaves NOTHING that
                    // would ever deliver a message or heartbeat on the inbox again, so a transient
                    // failure (timeout, leadership change) must not end recovery permanently (#114).
                    for ($attempt = 1; ; $attempt++) {
                        try {
                            $consumer = $this->createEphemeralPushConsumer($stream, $deliver, $filterSubject, $consumerOptions)->await();
                            $consumerName = $consumer->name;
                            $expectedConsumerSeq = 1;

                            return;
                        } catch (\Throwable $e) {
                            if ($attempt >= self::ORDERED_RECREATE_ATTEMPTS) {
                                throw $e;
                            }

                            delay(self::ORDERED_RECREATE_RETRY_DELAY_S * $attempt);
                        }
                    }
                } catch (\Throwable $e) {
                    // Containment: see the docblock above. The app must still learn the ordered
                    // consumer is now permanently silent, so surface through the error listener (#114).
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
                }
            };

            return $this->client->subscribe($deliver, function (NatsMessage $message) use ($handler, &$expectedConsumerSeq, &$lastStreamSeq, &$consumerName, $recreate): void {
                if ($this->handlePushControlMessage($message, $controlHeaders)) {
                    // Tail-gap detection: an idle heartbeat reports the server's last delivered consumer
                    // sequence. If it is ahead of what we have processed in order, deliveries at the tail
                    // were missed and no further message will arrive to expose the gap on its own -
                    // recreate proactively from the last in-order point (#86). The headers were already
                    // parsed by handlePushControlMessage (out-param) - do not re-parse the block (#139).
                    $lastDelivered = $this->heartbeatLastConsumerSeq($controlHeaders ?? []);
                    if ($lastDelivered !== null && $lastDelivered > $expectedConsumerSeq - 1) {
                        $recreate();
                    }

                    return;
                }

                $metadata = JsMessageMetadata::fromMessage($message);

                if ($metadata === null) {
                    // No JetStream ACK metadata to order on; deliver best-effort.
                    $handler($message);

                    return;
                }

                if ($metadata->consumer !== $consumerName) {
                    // A stale delivery from a previous (deleted) consumer instance arriving on the
                    // reused deliver inbox after a recreate - ignore it so it cannot trigger a spurious
                    // recreate or skew the new consumer-sequence count (#86).
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
            if (str_contains($e->getMessage(), 'No responders')) {
                throw new JetStreamException(
                    'No JetStream responder for subject ' . $subject
                    . ' (the subject is not bound to a stream, or JetStream is not enabled)',
                    503,
                    $e,
                );
            }

            throw $e;
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
            );
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
     * `no_wait`, and `idle_heartbeat` (nanoseconds, ADR-13; the fetch loop absorbs the resulting
     * status-100 heartbeat frames). Any other key is rejected with a JetStreamException instead of
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

            $sid = $this->client->subscribe($inbox, static function (NatsMessage $msg) use (&$messages, &$terminalStatus): void {
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

                // Bound the pull by the server expiry (plus slack). The cancellation cancels the
                // underlying socket read so a silent server cannot hang the fetch indefinitely.
                $waitCancellation = new TimeoutCancellation(($expiresMs + 1000) / 1000);
                try {
                    while (count($messages) < $batch && $terminalStatus === null) {
                        $frames = $this->client->processIncoming($waitCancellation)->await();
                        if ($frames === 0) {
                            delay(0.001, cancellation: $waitCancellation);
                        }
                    }
                } catch (CancelledException) {
                    // Deadline reached; fall through to evaluate collected messages / terminal status.
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

        if ($status !== 100) {
            return false;
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
     * `group` and `priority`; the server validates the rest.
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
            );
        }

        return $data;
    }

    /**
     * Raises a JetStream API error, upgrading it to an {@see \IDCT\NATS\Exception\UnsupportedFeatureException}
     * when the server's response shows the failure is a version-gated feature this server is too old for
     * (e.g. an `unknown field "allow_atomic"` rejection). Reactive only - no per-request version probe.
     */
    private function throwApiError(string $description, int $code): never
    {
        throw FeatureSupport::unsupportedFromApiError($description, $code, $this->serverVersion())
            ?? new JetStreamException($description, $code);
    }

    /**
     * The connected server's reported version (from the INFO handshake), or null when unknown.
     */
    private function serverVersion(): ?string
    {
        return $this->client->serverInfo()?->version;
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

        // Three ACK reply-subject shapes exist:
        //   9 tokens:  $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending>
        //  11 tokens:  $JS.ACK.<domain>.<account>.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending>
        //  12 tokens:  ...the 11-token domain form plus a trailing random token.
        // The stream sequence sits at index 5 in the short form and index 7 in both domain forms.
        $streamSeqIndex = match (count($parts)) {
            9 => 5,
            11, 12 => 7,
            default => null,
        };

        if ($streamSeqIndex === null) {
            return null;
        }

        $seq = filter_var($parts[$streamSeqIndex], FILTER_VALIDATE_INT);

        return ($seq !== false) ? $seq : null;
    }
}
