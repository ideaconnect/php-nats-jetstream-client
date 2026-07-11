<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

use Amp\Future;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\UnsupportedFeatureException;
use IDCT\NATS\JetStream\Models\PubAck;

use function Amp\async;

/**
 * Builder for an atomic (all-or-nothing) JetStream publish batch (ADR-50, NATS 2.12). The target
 * stream must be created with `allow_atomic` enabled.
 *
 * Messages are staged with `add()` and sent on `commit()`: every message carries a shared
 * `Nats-Batch-Id` and an incrementing `Nats-Batch-Sequence`; the final message carries
 * `Nats-Batch-Commit: 1`, on which the server atomically commits the whole batch and returns a single
 * PubAck (with the batch id and committed `count`). A consistency-check failure aborts the entire
 * batch - nothing is stored.
 *
 * Usage:
 *   $ack = $js->batch()
 *       ->add('orders.created', $a)
 *       ->add('orders.created', $b)
 *       ->commit()
 *       ->await();
 */
final class BatchPublisher
{
    /**
     * Client-side cap on the number of messages staged in one batch. ADR-50's 1000 is the server
     * DEFAULT batch limit, not a protocol constant - operators may configure other values, and the
     * server's error reply stays authoritative for the actual limit; this cap only fails
     * obviously oversized batches before any bytes are sent.
     */
    public const MAX_MESSAGES = 1000;

    /** @var list<array{subject:string,payload:string,headers:array<string,string>}> */
    private array $messages = [];

    private bool $committed = false;

    public function __construct(
        private readonly NatsClient $client,
        private readonly string $batchId,
    ) {}

    /**
     * Stages a message in the batch. Messages are sent in the order they are added.
     *
     * @param array<string,string> $headers Optional per-message headers.
     * @return $this
     */
    public function add(string $subject, string $payload, array $headers = []): self
    {
        if ($this->committed) {
            throw new JetStreamException('Cannot add to an already-committed batch');
        }

        if (count($this->messages) >= self::MAX_MESSAGES) {
            throw new JetStreamException('Atomic batch is limited to ' . self::MAX_MESSAGES . ' messages');
        }

        $this->messages[] = ['subject' => $subject, 'payload' => $payload, 'headers' => $headers];

        return $this;
    }

    /**
     * Number of messages currently staged. commit() releases the staged payloads, so this returns
     * 0 once the batch is committed (#133).
     */
    public function count(): int
    {
        return count($this->messages);
    }

    /**
     * The shared batch id sent on every message in this batch.
     */
    public function batchId(): string
    {
        return $this->batchId;
    }

    /**
     * Sends the staged messages and commits the batch atomically, returning the commit PubAck.
     *
     * @return Future<PubAck>
     */
    public function commit(): Future
    {
        return async(function (): PubAck {
            if ($this->committed) {
                throw new JetStreamException('Batch already committed');
            }

            if ($this->messages === []) {
                throw new JetStreamException('Cannot commit an empty batch');
            }

            // Pre-flight (#152): a pre-2.12 server stores batch messages as plain publishes, and
            // the reply-shape detection below fires only AFTER the start message is durably
            // stored - one orphan message from an all-or-nothing API. When INFO advertises a
            // parseable version below 2.12, fail before anything reaches the wire. Unparseable
            // versions (proxies, custom builds) and mixed-version clusters (JS leader older than
            // the connected server) still fall through to the reply-shape detection. Nothing was
            // sent, so the batch stays uncommitted and intact.
            $version = self::versionComponents($this->client->serverInfo()?->version);
            if ($version !== null && ($version[0] < 2 || ($version[0] === 2 && $version[1] < 12))) {
                throw $this->atomicBatchUnsupported(preflight: true);
            }

            $this->committed = true;

            // Release the staged payloads before sending: once committed they are pure dead weight
            // (add() rejects a committed batch), yet an app retaining the publisher - e.g. keyed by
            // batchId() to correlate acks - would otherwise pin up to MAX_MESSAGES full payloads for
            // the object's lifetime (#133). count() therefore reports 0 after commit(). The local
            // copy preserves the exact send behavior below.
            $messages = $this->messages;
            $this->messages = [];

            $total = count($messages);
            $lastIndex = $total - 1;

            // Per ADR-50 the batch START (sequence 1) is a request: the server replies with a zero-byte
            // ack on success or an error if the batch is rejected (e.g. allow_atomic disabled),
            // so the client learns immediately instead of blindly publishing the whole batch. For a
            // single-message batch the lone message is both start and commit (handled below).
            if ($total > 1) {
                $first = $messages[0];
                $startReply = $this->client->requestWithHeaders(
                    $first['subject'],
                    $first['payload'],
                    $this->batchHeaders($first['headers'], 1, false),
                )->await();
                $this->assertStartAccepted($startReply->payload);

                // Intermediate messages (2..n-1) are fire-and-forget; the server stages them by batch
                // id, and nothing is awaited from it between them. They are therefore coalesced into
                // bounded segment writes - one wire operation carrying many frames - instead of one
                // awaited publish per message (#138). Frames stay byte-identical to per-message
                // publishes; writes are serialized on the single connection, so they arrive in
                // sequence order.
                $intermediates = [];
                for ($i = 1; $i < $lastIndex; ++$i) {
                    $message = $messages[$i];
                    $intermediates[] = [
                        'subject' => $message['subject'],
                        'payload' => $message['payload'],
                        'headers' => $this->batchHeaders($message['headers'], $i + 1, false),
                    ];
                }

                if ($intermediates !== []) {
                    $this->client->publishHeaderBlock($intermediates)->await();
                }
            }

            // The final message carries Nats-Batch-Commit and is sent request/reply; the server commits
            // the whole batch and returns a single PubAck (batch id + committed count).
            $final = $messages[$lastIndex];
            $reply = $this->client->requestWithHeaders(
                $final['subject'],
                $final['payload'],
                $this->batchHeaders($final['headers'], $total, true),
            )->await();

            // A single-message batch is trivially atomic, so a plain PubAck (no batch fields) is
            // acceptable there; a multi-message commit MUST carry the batch acknowledgement.
            return $this->parseCommitAck($reply->payload, requireBatchAck: $total > 1);
        });
    }

    /**
     * Validates the reply to the batch-start request: an empty (zero-byte) reply means the server
     * accepted the batch; a reply carrying an error (e.g. atomic publish not enabled on the stream)
     * aborts the batch before the remaining messages are published.
     */
    private function assertStartAccepted(string $payload): void
    {
        if (trim($payload) === '') {
            return;
        }

        try {
            /** @var array<string,mixed> $data */
            $data = json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            // A non-empty, non-JSON start reply is unexpected but not an error payload; treat as accepted.
            return;
        }

        /** @var array<string,mixed>|null $error */
        $error = is_array($data['error'] ?? null) ? $data['error'] : null;
        if ($error !== null) {
            throw new JetStreamException(
                (string) ($error['description'] ?? 'Atomic batch rejected at start'),
                (int) ($error['code'] ?? 0),
                null,
                ApiErrCode::fromEnvelope($error),
            );
        }

        // A batching-aware server answers the start with a ZERO-BYTE ack (ADR-50). A normal PubAck
        // here means the message was stored as a plain publish - the answering server does not
        // understand Nats-Batch-* headers (pre-2.12), and continuing would store the "batch"
        // message-by-message, silently breaking the all-or-nothing guarantee (#130). Abort loudly
        // instead. The commit() version pre-flight keeps a directly-connected old server off this
        // path; it still fires for unparseable versions and mixed-version clusters where the JS
        // leader is older than the connected server - the start message is already stored by then,
        // a residual window the pre-flight cannot close from the client (#152).
        if (isset($data['stream']) || isset($data['seq'])) {
            throw $this->atomicBatchUnsupported();
        }
    }

    /**
     * Numeric [major, minor] prefix of an INFO-advertised server version, or null when the string
     * has no leading number (proxies and custom builds advertise arbitrary strings). Pre-release
     * tags are ignored - a 2.12.0-beta server understands batch semantics - matching nats.go's
     * numeric-prefix comparison rather than full semver precedence.
     *
     * @return array{int,int}|null
     */
    private static function versionComponents(?string $version): ?array
    {
        if ($version === null || preg_match('/^v?(\d+)(?:\.(\d+))?/', $version, $match) !== 1) {
            return null;
        }

        return [(int) $match[1], (int) ($match[2] ?? 0)];
    }

    /**
     * Builds the "server too old for atomic batches" failure with the connected server's version.
     * $preflight distinguishes the version gate (nothing was sent) from the reply-shape detection
     * (the start message was already stored by the answering server).
     */
    private function atomicBatchUnsupported(bool $preflight = false): UnsupportedFeatureException
    {
        $serverVersion = $this->client->serverInfo()?->version;
        $connected = $serverVersion !== null && $serverVersion !== '' ? ' ' . $serverVersion : '';

        return new UnsupportedFeatureException(
            'allow_atomic',
            '2.12',
            $serverVersion,
            $preflight
                ? sprintf('Atomic batch publish requires NATS server 2.12+ (connected server%s; nothing was published)', $connected)
                : sprintf('Atomic batch publish requires NATS server 2.12+ (connected server%s treated the batch as plain publishes)', $connected),
        );
    }

    /**
     * Builds the per-message header block, adding the batch id, sequence, and (for the final message)
     * the commit marker.
     *
     * @param array<string,string> $headers
     * @return array<string,string>
     */
    private function batchHeaders(array $headers, int $sequence, bool $commit): array
    {
        $headers['Nats-Batch-Id'] = $this->batchId;
        $headers['Nats-Batch-Sequence'] = (string) $sequence;

        if ($commit) {
            $headers['Nats-Batch-Commit'] = '1';
        }

        return $headers;
    }

    /**
     * Parses the atomic-batch commit acknowledgement, mapping a malformed body or an embedded API
     * error (an aborted batch) to a JetStreamException. With $requireBatchAck (multi-message
     * batches), a PubAck lacking the batch id/count means the server committed nothing as a batch
     * - it stored the messages individually - and must fail as unsupported (#130).
     */
    private function parseCommitAck(string $payload, bool $requireBatchAck): PubAck
    {
        try {
            /** @var array<string,mixed> $data */
            $data = json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new JetStreamException('Malformed atomic batch commit ack: ' . $e->getMessage(), 0, $e);
        }

        /** @var array<string,mixed>|null $error */
        $error = is_array($data['error'] ?? null) ? $data['error'] : null;
        if ($error !== null) {
            $description = (string) ($error['description'] ?? 'JetStream atomic batch error');
            $code = (int) ($error['code'] ?? 0);
            throw new JetStreamException($description, $code, null, ApiErrCode::fromEnvelope($error));
        }

        $ack = PubAck::fromArray($data);

        if ($requireBatchAck && $ack->batchId === null && $ack->batchCount === null) {
            throw $this->atomicBatchUnsupported();
        }

        return $ack;
    }
}
