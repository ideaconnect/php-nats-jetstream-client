<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

use Amp\Future;
use IDCT\NATS\Core\NatsClient;
use IDCT\NATS\Exception\JetStreamException;
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
    /** Server-enforced upper bound on the number of messages in a single atomic batch. */
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
     * Number of messages currently staged.
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

            $this->committed = true;

            $total = count($this->messages);
            $lastIndex = $total - 1;

            // Per ADR-50 the batch START (sequence 1) is a request: the server replies with a zero-byte
            // ack on success or an error if the batch is rejected (e.g. allow_atomic disabled),
            // so the client learns immediately instead of blindly publishing the whole batch. For a
            // single-message batch the lone message is both start and commit (handled below).
            if ($total > 1) {
                $first = $this->messages[0];
                $startReply = $this->client->requestWithHeaders(
                    $first['subject'],
                    $first['payload'],
                    $this->batchHeaders($first['headers'], 1, false),
                )->await();
                $this->assertStartAccepted($startReply->payload);

                // Intermediate messages (2..n-1) are fire-and-forget; the server stages them by batch
                // id. Writes are serialized on the single connection, so they arrive in sequence order.
                for ($i = 1; $i < $lastIndex; ++$i) {
                    $message = $this->messages[$i];
                    $this->client->publishWithHeaders(
                        $message['subject'],
                        $message['payload'],
                        $this->batchHeaders($message['headers'], $i + 1, false),
                    )->await();
                }
            }

            // The final message carries Nats-Batch-Commit and is sent request/reply; the server commits
            // the whole batch and returns a single PubAck (batch id + committed count).
            $final = $this->messages[$lastIndex];
            $reply = $this->client->requestWithHeaders(
                $final['subject'],
                $final['payload'],
                $this->batchHeaders($final['headers'], $total, true),
            )->await();

            return $this->parseCommitAck($reply->payload);
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
            );
        }
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
     * error (an aborted batch) to a JetStreamException.
     */
    private function parseCommitAck(string $payload): PubAck
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
            throw new JetStreamException($description, $code);
        }

        return PubAck::fromArray($data);
    }
}
