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
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\FastBatchException;
use IDCT\NATS\Exception\JetStreamException;
use IDCT\NATS\Exception\NatsException;
use IDCT\NATS\Exception\SubscriptionLimitException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Exception\UnsupportedFeatureException;
use IDCT\NATS\JetStream\Models\FastPubAck;
use IDCT\NATS\JetStream\Models\PubAck;

use function Amp\async;
use function Amp\delay;

/**
 * Fast-ingest batch publisher (ADR-50, NATS server 2.14+): a flow-controlled, non-atomic batch whose messages are
 * stored as they arrive. The target stream must be created with `allow_batched` enabled. Made by
 * {@see JetStreamContext::fastPublisher()}; one publisher is one batch.
 *
 * The batch's state travels in each message's reply subject, `<inbox>.<flow>.<gap>.<seq>.<op>.$FI`, where `<inbox>`
 * is an inbox of the batch's own (its last token is the batch id) and the publisher's subscription on `<inbox>.>` is
 * the batch's control channel. The server answers the first message with a flow ack that starts the batch, sends a
 * flow ack every `msgs` messages after that, and answers the commit with a PubAck. add() does not wait for each
 * message's ack: it waits only once `ackSequence + msgs * maxOutstandingAcks <= sequence`, pinging the batch every
 * third of the ack timeout to recover a lost ack, and gives up after the ack timeout.
 *
 * Gap mode `fail` (the default) ends the batch at the first lost message or refused per-message check: every message
 * up to the end was stored, in order, and the next call throws a {@see FastBatchException} carrying the PubAck the
 * server ended the batch with. Gap mode `ok` (`continueOnGap: true`) lets the batch go on past them: each gap and
 * each refused message is handed to `onError` as a {@see FastBatchException}, and some messages may be missing.
 *
 * Not safe for concurrent use: await each add(), commit() and close() before the next one (one fiber per
 * publisher). A call made while another is still running fails without sending anything. A batch the server has not
 * heard of for 10 seconds is abandoned; the next message, commit or ping then fails with err_code 10208.
 *
 * Usage:
 *   $fast = $js->fastPublisher();
 *   $fast->add('metrics.cpu', $a)->await();
 *   $ack = $fast->commit('metrics.cpu', $last)->await();   // or $fast->close()->await()
 */
final class FastPublisher
{
    /** Default flow: the server is asked for at most one flow ack every this many messages (Orbit's default). */
    public const DEFAULT_FLOW = 100;

    /** Default number of flow acks that may be outstanding before add() waits (Orbit's default). */
    public const DEFAULT_MAX_OUTSTANDING_ACKS = 2;

    /** The highest flow: the server's `msgs` is a uint16. */
    public const MAX_FLOW = 65535;

    /** The most outstanding acks a client may allow (ADR-50: "Clients must not allow more than 3"). */
    public const MAX_OUTSTANDING_ACKS = 3;

    private const OP_START = 0;
    private const OP_APPEND = 1;
    private const OP_COMMIT = 2;
    private const OP_COMMIT_EOB = 3;
    private const OP_PING = 4;

    /** The atomic batch headers: a fast-ingest message must not carry them. */
    private const ATOMIC_BATCH_HEADERS = ['nats-batch-id', 'nats-batch-sequence', 'nats-batch-commit'];

    private readonly string $inbox;
    private readonly string $batchId;

    /** `<inbox>.<flow>.<gap>.`: the flow asked for stays in every reply subject, whatever the server's acks say. */
    private readonly string $replyPrefix;

    private readonly string $gapMode;

    /** The highest batch sequence sent (a ping does not advance it). */
    private int $sequence = 0;

    /** The highest batch sequence a flow ack acknowledged. */
    private int $ackSequence = 0;

    /** The `msgs` of the latest flow ack: the server acks every this many messages. */
    private int $serverFlow;

    /** Whether the server's first flow ack has started the batch. */
    private bool $started = false;

    /** The subject of the batch's first message, which pings and the EOB commit go to. */
    private ?string $batchSubject = null;

    /** The subject of the commit message, once one was sent. */
    private ?string $commitSubject = null;

    private ?int $sid = null;

    /** The -ERR the server rejected the control inbox with, when it did. */
    private ?string $inboxRejection = null;

    /**
     * The reply without a `type` that ended the batch: the commit's PubAck, or the PubAck or error the server ended
     * it with before.
     *
     * @var array<string,mixed>|null
     */
    private ?array $endReply = null;

    /** The status of a status-only reply to the start or the commit (503: nothing captures the subject). */
    private ?string $endStatus = null;

    /** In gap mode `fail`: the gap or error notice that ends the batch; its PubAck follows. */
    private ?FastBatchException $endingNotice = null;

    /** @var list<FastBatchException> Notices waiting for the next call to hand them to onError. */
    private array $notices = [];

    private bool $closed = false;
    private string $closedReason = '';
    private ?\Throwable $closedBy = null;
    private bool $busy = false;

    /** How long one call waits for the server in all, in milliseconds. */
    private readonly int $ackTimeoutMs;

    /**
     * @param int $flow The flow to ask for: at most one flow ack every this many messages (1..65535). The server may
     *        ack more often; it never acks less often.
     * @param int $maxOutstandingAcks How many flow acks may be outstanding before add() waits (1..3).
     * @param bool $continueOnGap Gap mode `ok` (true) or `fail` (false).
     * @param int|null $ackTimeoutMs How long one call waits for the server in all, in milliseconds (> 0); null = the
     *        client's {@see \IDCT\NATS\Connection\NatsOptions::$requestTimeoutMs}.
     * @param (\Closure(FastBatchException): void)|null $onError Receives the notices that do not end the batch, in the
     *        fiber of the next add(), commit() or close(). What it throws goes to the client's error listener.
     * @throws JetStreamException When a setting is out of range (nothing is sent).
     */
    public function __construct(
        private readonly NatsClient $client,
        int $flow = self::DEFAULT_FLOW,
        private readonly int $maxOutstandingAcks = self::DEFAULT_MAX_OUTSTANDING_ACKS,
        private readonly bool $continueOnGap = false,
        ?int $ackTimeoutMs = null,
        private readonly ?\Closure $onError = null,
    ) {
        if ($flow < 1 || $flow > self::MAX_FLOW) {
            throw new JetStreamException(sprintf('Fast batch flow must be between 1 and %d, %d given', self::MAX_FLOW, $flow));
        }

        if ($maxOutstandingAcks < 1 || $maxOutstandingAcks > self::MAX_OUTSTANDING_ACKS) {
            throw new JetStreamException(sprintf(
                'Fast batch maxOutstandingAcks must be between 1 and %d, %d given',
                self::MAX_OUTSTANDING_ACKS,
                $maxOutstandingAcks,
            ));
        }

        if ($ackTimeoutMs !== null && $ackTimeoutMs <= 0) {
            throw new JetStreamException(sprintf('Fast batch ackTimeoutMs must be greater than zero, %d given', $ackTimeoutMs));
        }

        $this->ackTimeoutMs = $ackTimeoutMs ?? $client->options()->requestTimeoutMs;
        $this->inbox = Inbox::generate($client->options()->inboxPrefix);
        $this->batchId = substr($this->inbox, (int) strrpos($this->inbox, '.') + 1);
        $this->gapMode = $continueOnGap ? 'ok' : 'fail';
        $this->replyPrefix = $this->inbox . '.' . $flow . '.' . $this->gapMode . '.';
        $this->serverFlow = $flow;
    }

    /**
     * The batch id: the last token of the batch's control inbox, which the server echoes in the commit's PubAck.
     */
    public function batchId(): string
    {
        return $this->batchId;
    }

    /**
     * Whether the batch is over: committed, closed, ended by the server, or given up after an ack timeout. Every
     * later add(), commit() and close() fails without sending anything.
     */
    public function isClosed(): bool
    {
        return $this->closed;
    }

    /**
     * Publishes one message of the batch. The first one starts the batch: add() waits for the server's first reply
     * before it returns, and fails when the server does not start the batch (the stream has no `allow_batched`, the
     * subject is captured by no stream, or the server predates fast ingest and stored the message as a plain
     * publish). Every later message is sent at once; add() waits only when the outstanding acks are used up.
     *
     * @param array<string,string|list<string>> $headers Message headers; not the atomic batch's Nats-Batch-* headers.
     * @return Future<FastPubAck>
     */
    public function add(string $subject, string $payload, array $headers = []): Future
    {
        return async(function () use ($subject, $payload, $headers): FastPubAck {
            $this->enter();
            try {
                self::assertNoAtomicBatchHeaders($headers);
                $this->assertOpen();

                if ($this->sequence === 0) {
                    return $this->start($subject, $payload, $headers);
                }

                $sequence = ++$this->sequence;
                $this->send($subject, $payload, $headers, $sequence, self::OP_APPEND);

                if ($this->mustWaitForAcks()) {
                    $this->waitFor(
                        $this->stallIsOver(...),
                        true,
                        sprintf('message %d waited %d ms for a flow ack', $sequence, $this->ackTimeoutMs),
                    );
                }

                $this->throwIfEnded();
                $this->deliverNotices();

                return new FastPubAck($sequence, $this->ackSequence);
            } finally {
                $this->busy = false;
            }
        });
    }

    /**
     * Publishes the batch's last message and commits the batch: the server stores the message, ends the batch and
     * answers with a PubAck whose `batchId` is the batch id and whose `batchCount` is the commit's batch sequence (in
     * gap mode `ok` that is not the number of messages stored). A commit without an earlier add() is a batch of one.
     * Waits for the PubAck, pinging the batch every third of the ack timeout. The publisher is closed afterwards.
     *
     * @param array<string,string|list<string>> $headers Message headers; not the atomic batch's Nats-Batch-* headers.
     * @return Future<PubAck>
     */
    public function commit(string $subject, string $payload, array $headers = []): Future
    {
        return async(function () use ($subject, $payload, $headers): PubAck {
            $this->enter();
            try {
                self::assertNoAtomicBatchHeaders($headers);
                $this->assertOpen();

                if ($this->sid === null) {
                    $this->batchSubject = $subject;
                    $this->subscribeControlInbox();
                }

                $this->commitSubject = $subject;

                return $this->sendCommit($subject, $payload, $headers, self::OP_COMMIT);
            } finally {
                $this->busy = false;
            }
        });
    }

    /**
     * Ends the batch without storing another message (an EOB commit, operation 3, on the subject of the batch's
     * first message) and returns the server's PubAck, whose `batchCount` is the batch sequence of the last message.
     * Fails before the first add(). The publisher is closed afterwards.
     *
     * @return Future<PubAck>
     */
    public function close(): Future
    {
        return async(function (): PubAck {
            $this->enter();
            try {
                if ($this->sequence === 0 && !$this->closed) {
                    throw new JetStreamException(sprintf('Fast batch %s: nothing to close, no message was added', $this->batchId));
                }

                $this->assertOpen();
                $subject = (string) $this->batchSubject;
                $this->commitSubject = $subject;

                return $this->sendCommit($subject, '', [], self::OP_COMMIT_EOB);
            } finally {
                $this->busy = false;
            }
        });
    }

    /**
     * Sends the batch's first message on a control inbox of its own and waits for the server's first reply (ADR-50:
     * the client MUST wait for it before it sends anything else).
     *
     * @param array<string,string|list<string>> $headers
     */
    private function start(string $subject, string $payload, array $headers): FastPubAck
    {
        $this->batchSubject = $subject;
        $this->subscribeControlInbox();
        $this->sequence = 1;

        try {
            $this->send($subject, $payload, $headers, 1, self::OP_START);
        } catch (\Throwable $e) {
            $this->shutDown('its first message could not be sent', $e);

            throw $e;
        }

        $this->waitFor(
            fn(): bool => $this->started || $this->endingNotice !== null || $this->hasEnded(),
            false,
            sprintf('no reply to its first message within %d ms', $this->ackTimeoutMs),
        );

        $this->throwIfEnded();
        $this->deliverNotices();

        return new FastPubAck(1, $this->ackSequence);
    }

    /**
     * Sends a commit (operation 2 with a message, 3 without) and waits for the PubAck that ends the batch.
     *
     * @param array<string,string|list<string>> $headers
     */
    private function sendCommit(string $subject, string $payload, array $headers, int $operation): PubAck
    {
        $sequence = ++$this->sequence;

        try {
            $this->send($subject, $payload, $headers, $sequence, $operation);
        } catch (\Throwable $e) {
            $this->shutDown('its commit could not be sent', $e);

            throw $e;
        }

        $this->waitFor(
            fn(): bool => $this->hasEnded(),
            true,
            sprintf('no PubAck for the commit at batch sequence %d within %d ms', $sequence, $this->ackTimeoutMs),
        );

        // An EOB commit stores no message, so the server counts the batch up to the message before it. A PubAck with
        // another count ended the batch before the commit: a gap or a refused message in gap mode `fail`.
        $expectedCount = $operation === self::OP_COMMIT_EOB ? $sequence - 1 : $sequence;
        $reply = $this->endReply;
        if ($this->endStatus === null && $reply !== null && !isset($reply['error'])) {
            $ack = PubAck::fromArray($reply);
            if ($ack->batchCount === $expectedCount) {
                $this->shutDown('it was committed');
                $this->deliverNotices();

                return $ack;
            }
        }

        $this->endBatch();
    }

    /**
     * Subscribes the batch's control inbox, `<inbox>.>`: an inbox of its own, not the muxed request inbox, so that
     * interest is dropped as soon as the batch ends (ADR-50).
     */
    private function subscribeControlInbox(): void
    {
        try {
            $this->sid = $this->client->subscribeGuarded(
                $this->inbox . '.>',
                $this->onControlMessage(...),
                function (string $serverError): void {
                    $this->inboxRejection ??= $serverError;
                },
            )->await();
        } catch (ConnectionException $e) {
            $this->throwIfInboxRejected($e);

            throw $e;
        }

        // A dropped flow ack is recovered by the next one, but a dropped PubAck is not: never drop a control reply.
        $this->client->markSubscriptionUnbounded($this->sid);
    }

    /**
     * @param array<string,string|list<string>> $headers
     */
    private function send(string $subject, string $payload, array $headers, int $sequence, int $operation): void
    {
        $reply = $this->replyPrefix . $sequence . '.' . $operation . '.$FI';

        if ($headers === []) {
            $this->client->publish($subject, $payload, $reply)->await();

            return;
        }

        $this->client->publishWithHeaders($subject, $payload, $headers, $reply)->await();
    }

    /**
     * Pings the batch (operation 4) at the highest batch sequence sent, which the ping does not advance: the server
     * answers with its latest flow ack, so that a lost ack does not stall the batch.
     */
    private function sendPing(): void
    {
        $this->client->publish((string) $this->batchSubject, '', $this->replyPrefix . $this->sequence . '.' . self::OP_PING . '.$FI')->await();
    }

    private function mustWaitForAcks(): bool
    {
        return $this->ackSequence + $this->serverFlow * $this->maxOutstandingAcks <= $this->sequence;
    }

    /**
     * Whether a stalled add() may stop waiting: an ack opened the window, or the server is ending the batch.
     */
    private function stallIsOver(): bool
    {
        return !$this->mustWaitForAcks() || $this->endingNotice !== null || $this->hasEnded();
    }

    /**
     * Reads the connection until $satisfied, within one ack timeout in all. With $ping, pings the batch every third
     * of the timeout. On the timeout the batch is given up: closed, its inbox released.
     *
     * @param \Closure(): bool $satisfied
     */
    private function waitFor(\Closure $satisfied, bool $ping, string $timeoutWhat): void
    {
        $timeoutNs = $this->ackTimeoutMs * 1_000_000;
        $pingIntervalNs = intdiv($timeoutNs, 3);
        $startNs = hrtime(true);
        $deadlineNs = $startNs + $timeoutNs;
        $nextPingNs = $startNs + $pingIntervalNs;

        while (!$satisfied()) {
            $this->throwIfInboxRejected();

            $nowNs = hrtime(true);
            if ($nowNs >= $deadlineNs) {
                $timeout = new TimeoutException(
                    sprintf('Fast batch %s: %s; ack sequence %d', $this->batchId, $timeoutWhat, $this->ackSequence),
                    0,
                    $this->endingNotice,
                );
                $this->shutDown('it timed out waiting for the server', $timeout);

                throw $timeout;
            }

            if ($ping && $nowNs >= $nextPingNs) {
                $this->sendPing();
                $nextPingNs = $nowNs + $pingIntervalNs;

                continue;
            }

            $untilNs = $ping ? min($deadlineNs, $nextPingNs) : $deadlineNs;
            $wait = new TimeoutCancellation(max(0.001, ($untilNs - $nowNs) / 1_000_000_000));
            try {
                $read = $this->client->readIncomingForOperation($wait, $this->sid)->await();
                if (!$read->consumedBytes) {
                    delay(0.001, cancellation: $wait);
                }
            } catch (CancelledException) {
                // This wait segment is over: look at the deadline and the ping again.
            } catch (NatsException $e) {
                // The read met the -ERR that rejected the control inbox: fail with the rejection, the batch given up.
                $this->throwIfInboxRejected($e);

                throw $e;
            }
        }
    }

    /**
     * The subscription handler of the control inbox. Runs inside whichever fiber's read delivers the reply: it only
     * records what the reply says, and never suspends.
     */
    private function onControlMessage(NatsMessage $message): void
    {
        if ($this->hasEnded()) {
            // Whatever follows the end (10208 answers to messages sent before it was read) is moot.
            return;
        }

        $status = NatsHeaders::fromWireBlock($message->rawHeaders)['Status'] ?? null;
        if ($message->payload === '' && $status !== null) {
            $this->onStatus($message->subject, $status);

            return;
        }

        $reply = json_decode($message->payload, true);
        if (!is_array($reply)) {
            $this->notices[] = new FastBatchException(sprintf('Fast batch %s: malformed control reply: %s', $this->batchId, $message->payload));

            return;
        }

        /** @var array<string,mixed> $reply */
        $type = $reply['type'] ?? null;
        if ($type === 'ack') {
            $sequence = self::intField($reply, 'seq');
            if ($sequence > $this->ackSequence) {
                $this->ackSequence = $sequence;
            }

            $messages = self::intField($reply, 'msgs');
            if ($messages >= 1) {
                $this->serverFlow = $messages;
            }

            $this->started = true;

            return;
        }

        if ($type === 'gap') {
            $lastSequence = self::intField($reply, 'last_seq');
            $sequence = self::intField($reply, 'seq');
            $this->notice(new FastBatchException(
                sprintf('Fast batch %s: gap at batch sequence %d, messages %d to %d were lost', $this->batchId, $sequence, $lastSequence, $sequence - 1),
                0,
                null,
                null,
                null,
                $sequence,
            ));

            return;
        }

        if ($type === 'err') {
            $sequence = self::intField($reply, 'seq');
            /** @var array<string,mixed> $error */
            $error = is_array($reply['error'] ?? null) ? $reply['error'] : [];
            $this->notice(new FastBatchException(
                sprintf('Fast batch %s: message %d was refused: %s', $this->batchId, $sequence, (string) ($error['description'] ?? 'unknown error')),
                self::intField($error, 'code'),
                null,
                ApiErrCode::fromEnvelope($error),
                null,
                $sequence,
            ));

            return;
        }

        if ($type !== null) {
            $this->notices[] = new FastBatchException(sprintf('Fast batch %s: control reply of unknown type: %s', $this->batchId, $message->payload));

            return;
        }

        // No type: a PubAck, which always ends the batch.
        $this->endReply = $reply;
    }

    /**
     * A status-only reply (no responders: no stream captures the message's subject). For the start or the commit it
     * ends the batch; for an appended message or a ping it is a lost message, a notice, and the gap mode decides.
     */
    private function onStatus(string $replySubject, string $status): void
    {
        $tokens = explode('.', $replySubject);
        $count = count($tokens);
        $operation = $count >= 3 ? $tokens[$count - 2] : '';
        $sequence = $count >= 3 ? (int) $tokens[$count - 3] : 0;

        if ($operation === (string) self::OP_APPEND || $operation === (string) self::OP_PING) {
            $this->notices[] = new FastBatchException(
                sprintf('Fast batch %s: message %d got status %s (no stream captures its subject)', $this->batchId, $sequence, $status),
                (int) $status,
                null,
                null,
                null,
                $sequence,
            );

            return;
        }

        $this->endStatus = $status;
    }

    /**
     * Records a gap or a refused message: in gap mode `fail` it ends the batch (its PubAck follows), in gap mode `ok`
     * it is a notice for onError.
     */
    private function notice(FastBatchException $notice): void
    {
        if ($this->continueOnGap) {
            $this->notices[] = $notice;

            return;
        }

        $this->endingNotice ??= $notice;
    }

    private function hasEnded(): bool
    {
        return $this->endReply !== null || $this->endStatus !== null;
    }

    /**
     * Throws when the server has ended the batch, or is ending it (a gap or a refused message in gap mode `fail`,
     * whose PubAck is then waited for): the publisher is closed, its inbox released.
     */
    private function throwIfEnded(): void
    {
        if ($this->endingNotice !== null && !$this->hasEnded()) {
            $this->waitFor(
                fn(): bool => $this->hasEnded(),
                true,
                sprintf('no PubAck within %d ms after the server ended the batch', $this->ackTimeoutMs),
            );
        }

        if (!$this->hasEnded()) {
            return;
        }

        $this->endBatch();
    }

    /**
     * Ends the publisher after the server ended the batch: closed, its inbox released, its notices delivered, and the
     * reason thrown.
     */
    private function endBatch(): never
    {
        $failure = $this->endedException();
        $this->shutDown('the server ended it', $failure);
        $this->deliverNotices();

        throw $failure;
    }

    /**
     * Why the batch ended without being committed, from the reply that ended it.
     */
    private function endedException(): JetStreamException
    {
        if ($this->endStatus !== null) {
            $subject = $this->commitSubject ?? (string) $this->batchSubject;
            if ($this->endStatus === '503') {
                // The same failure a JetStream publish to a subject no stream captures gets (#161).
                $failure = JetStreamRequest::normalizeNoResponders(new NatsException('No responders for subject ' . $subject), $subject);

                return $failure instanceof JetStreamException ? $failure : new JetStreamException($failure->getMessage(), 503, $failure);
            }

            return new JetStreamException(
                sprintf('Fast batch %s: the server answered a message to %s with status %s', $this->batchId, $subject, $this->endStatus),
                (int) $this->endStatus,
            );
        }

        /** @var array<string,mixed> $reply */
        $reply = $this->endReply;
        if (is_array($reply['error'] ?? null)) {
            /** @var array<string,mixed> $error */
            $error = $reply['error'];

            return new JetStreamException(
                (string) ($error['description'] ?? 'JetStream fast batch error'),
                self::intField($error, 'code'),
                $this->endingNotice,
                ApiErrCode::fromEnvelope($error),
            );
        }

        $ack = PubAck::fromArray($reply);
        if ($ack->batchId === null && $ack->batchCount === null) {
            // A plain PubAck: the server that answered predates fast ingest and stored the message as a plain publish.
            $serverVersion = $this->client->serverInfo()?->version;

            return new UnsupportedFeatureException(
                'allow_batched',
                '2.14',
                $serverVersion,
                sprintf(
                    'Fast-ingest batch publish requires NATS server 2.14+: the server stored the message as a plain publish'
                    . ' (stream "%s", seq %d) instead of starting a batch (connected server %s)',
                    $ack->stream,
                    $ack->seq,
                    $serverVersion ?? 'unknown',
                ),
            );
        }

        return new FastBatchException(
            sprintf(
                'Fast batch %s ended at batch sequence %d before its commit%s',
                $this->batchId,
                (int) $ack->batchCount,
                $this->endingNotice !== null ? ': ' . $this->endingNotice->getMessage() : '',
            ),
            0,
            $this->endingNotice,
            null,
            $ack,
        );
    }

    /**
     * Fails a call on a closed publisher, before anything is sent. A batch the server ended keeps its PubAck in the
     * exception.
     */
    private function assertOpen(): void
    {
        if (!$this->closed) {
            if ($this->endingNotice !== null || $this->hasEnded()) {
                $this->throwIfEnded();
            }

            return;
        }

        $message = sprintf('Fast batch %s is closed: %s', $this->batchId, $this->closedReason);
        $cause = $this->closedBy;
        if ($cause instanceof FastBatchException) {
            throw new FastBatchException($message, $cause->getCode(), $cause, $cause->getErrCode(), $cause->pubAck);
        }

        throw new JetStreamException($message, 0, $cause);
    }

    /**
     * Closes the publisher and releases its control inbox, at once: once the batch is over the server's further
     * replies are of no use, and dropping interest lets the server stop sending them (ADR-50).
     */
    private function shutDown(string $reason, ?\Throwable $cause = null): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;
        $this->closedReason = $reason;
        $this->closedBy = $cause;

        if ($this->sid !== null) {
            $sid = $this->sid;
            $this->sid = null;
            $this->client->releaseSubscriptionWithin($sid, new TimeoutCancellation($this->ackTimeoutMs / 1000))->await();
        }
    }

    /**
     * Hands the queued notices to onError, in this call's fiber. What onError throws goes to the client's error
     * listener, so that it does not mask the call's own result.
     */
    private function deliverNotices(): void
    {
        while ($this->notices !== []) {
            $notice = array_shift($this->notices);
            if ($this->onError === null) {
                continue;
            }

            try {
                ($this->onError)($notice);
            } catch (\Throwable $e) {
                $this->client->emitError($e);
            }
        }
    }

    /**
     * Fails the call when the server rejected the control inbox's subscription, as its rejection handler recorded:
     * no reply can come on it.
     */
    private function throwIfInboxRejected(?\Throwable $previous = null): void
    {
        if ($this->inboxRejection === null) {
            return;
        }

        $subject = $this->inbox . '.>';
        $failure = $this->client->isSubscriptionLimitError($this->inboxRejection)
            ? new SubscriptionLimitException(
                sprintf(
                    'Fast batch publish failed: the server may have rejected its control-inbox subscription "%s" because the'
                    . ' connection is at its subscription limit (maximum subscriptions exceeded). A new batch subscribes a new one.',
                    $subject,
                ),
                0,
                $previous,
            )
            : new ConnectionException(
                sprintf('Fast batch publish failed: the server rejected its control-inbox subscription "%s": %s', $subject, $this->inboxRejection),
                0,
                $previous,
            );

        $this->shutDown('the server rejected its control inbox', $failure);

        throw $failure;
    }

    private function enter(): void
    {
        if ($this->busy) {
            throw new JetStreamException(sprintf(
                'Fast batch %s: another add(), commit() or close() is still running; a FastPublisher is not safe for concurrent use',
                $this->batchId,
            ));
        }

        $this->busy = true;
    }

    /**
     * @param array<string,string|list<string>> $headers
     */
    private static function assertNoAtomicBatchHeaders(array $headers): void
    {
        foreach (array_keys($headers) as $name) {
            if (in_array(strtolower((string) $name), self::ATOMIC_BATCH_HEADERS, true)) {
                throw new JetStreamException(sprintf(
                    'A fast-ingest batch message must not carry the atomic batch header %s; use batch() for an atomic batch',
                    $name,
                ));
            }
        }
    }

    /**
     * @param array<string,mixed> $data
     */
    private static function intField(array $data, string $field): int
    {
        $value = $data[$field] ?? 0;

        return is_int($value) ? $value : (is_numeric($value) ? (int) $value : 0);
    }
}
