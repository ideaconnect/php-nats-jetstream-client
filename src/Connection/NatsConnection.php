<?php

declare(strict_types=1);

namespace IDCT\NATS\Connection;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use IDCT\NATS\Connection\Enum\ConnectionEvent;
use IDCT\NATS\Connection\Enum\ConnectionState;
use IDCT\NATS\Connection\Enum\SlowConsumerPolicy;
use IDCT\NATS\Core\Inbox;
use IDCT\NATS\Core\NatsHeaders;
use IDCT\NATS\Core\NatsMessage;
use IDCT\NATS\Exception\AuthenticationException;
use IDCT\NATS\Exception\ConnectionException;
use IDCT\NATS\Exception\NatsException;
use IDCT\NATS\Exception\ProtocolException;
use IDCT\NATS\Exception\TimeoutException;
use IDCT\NATS\Protocol\Enum\ProtocolFrameType;
use IDCT\NATS\Protocol\ProtocolCodec;
use IDCT\NATS\Protocol\ProtocolFrame;
use IDCT\NATS\Protocol\ProtocolParser;
use IDCT\NATS\Protocol\ServerInfo;
use IDCT\NATS\Transport\TlsAwareTransportInterface;
use IDCT\NATS\Transport\TransportClosedException;
use IDCT\NATS\Transport\TransportInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Revolt\EventLoop;
use SplQueue;

use function Amp\async;
use function Amp\delay;
use function Amp\Future\awaitFirst;

/**
 * Manages low-level NATS protocol connection lifecycle and frame processing.
 */
final class NatsConnection
{
    /** Cap on the validated-subject memo (#136); hitting it resets the memo to stay bounded. */
    private const VALIDATED_SUBJECTS_MAX = 512;

    /**
     * Max bytes per coalesced publish segment in {@see publishHeaderBlock()} (#138). Frames are
     * concatenated up to this cap and flushed as one transport write, bounding peak buffer memory
     * (a 1000 x 1MB batch must not concatenate into a ~1GB string). A single frame larger than the
     * cap is written alone - frames cannot be split.
     */
    private const PUBLISH_BLOCK_SEGMENT_BYTES = 512 * 1024;

    private ConnectionState $state = ConnectionState::Idle;
    private ?ServerInfo $serverInfo = null;
    private ProtocolParser $parser;
    private int $nextSid = 1;
    private int $serverCursor = 0;
    /** @var array<int, callable(NatsMessage):void> */
    private array $subscriptions = [];
    /** @var array<int, array{subject: string, queue: ?string}> */
    private array $subscriptionMeta = [];
    /** @var array<int, SplQueue<NatsMessage>> */
    private array $pendingMessages = [];
    /**
     * Messages RECEIVED from the server per sid, counted at intake (before any slow-consumer drop),
     * for auto-unsubscribe accounting. Counting at receive - not at handler delivery - mirrors the
     * server's own `UNSUB <sid> <max>` counter: a message the slow-consumer policy drops still counts
     * toward the max, exactly as the server counts a message it sent. Counting at delivery instead
     * would let a dropped message stall the counter below max forever (the terminal cleanup never
     * fires, so the subscription leaks), and a reconnect would then re-arm with a positive remaining
     * and over-deliver live messages past the intended max (#112).
     *
     * @var array<int, int>
     */
    private array $receivedCounts = [];
    /**
     * Messages actually DELIVERED to each subscription's handler, used to cap handler delivery at the
     * auto-unsubscribe max so the client never over-delivers past it - even if a server (or a replayed
     * read after reconnect) sends an extra frame. Distinct from {@see $receivedCounts}: under a
     * slow-consumer drop, delivered can lag received, so cleanup keys on received while the delivery
     * cap keys on delivered (#112).
     *
     * @var array<int, int>
     */
    private array $deliveredCounts = [];
    /**
     * Per-sid auto-unsubscribe limits armed via unsubscribe($sid, $max): the handler stays registered
     * until this many TOTAL messages have been received, matching the server-side UNSUB count (#112).
     *
     * @var array<int, int>
     */
    private array $autoUnsubMax = [];
    /**
     * SIDs whose queue is currently being delivered. A subscription handler may await on the
     * connection (e.g. an ordered consumer recreating itself), which suspends the dispatch fiber
     * with readInProgress already cleared; a heartbeat tick or nested request() self-pump would then
     * re-enter the drain for the SAME sid and deliver its next message on top of the suspended one.
     * This per-sid guard makes same-sid delivery non-reentrant (FIFO preserved - the suspended loop
     * resumes and continues), while leaving OTHER sids deliverable so nested requests still complete.
     *
     * @var array<int, true>
     */
    private array $dispatchingSids = [];
    private int $outstandingPings = 0;
    private ?string $pingTimerId = null;
    private bool $drainFlushPending = false;
    /** Set while flush() awaits its PONG; cleared by the PONG handler. */
    private bool $flushPending = false;
    /**
     * In-progress reconnect, so concurrent callers wait for it instead of starting a second one.
     *
     * @var ?DeferredFuture<void>
     */
    private ?DeferredFuture $reconnecting = null;
    /** Guards against two overlapping socket reads (user read vs heartbeat self-read). */
    private bool $readInProgress = false;
    /**
     * Completed and rotated whenever the shared read slot frees, so request waiters parked behind
     * another fiber's read wake to take over pumping instead of polling on 1ms timers (#135).
     *
     * @var DeferredFuture<null>
     */
    private DeferredFuture $readSlotReleased;
    /**
     * Set by disconnect()/drain() to signal user close-intent. The reconnect paths bail when it is set
     * so an in-flight heartbeat/read-path recovery cannot re-open a connection the user just closed
     * (#84). Cleared on a fresh connect().
     */
    private bool $closing = false;
    /**
     * Publish callback bound onto every delivered {@see NatsMessage} so it can reply to its own
     * reply subject via {@see NatsMessage::respond()}. Built once and reused for all messages.
     *
     * @var \Closure(string,string,array<string,string>|null):Future<void>
     */
    private readonly \Closure $messageResponder;
    /** Whether a LameDuck event has already been emitted for the current server, to avoid repeats. */
    private bool $lameDuckAnnounced = false;
    /**
     * The last set of discovered cluster endpoints, so a DiscoveredServers event fires only when the
     * advertised `connect_urls` actually change. Also merged into the reconnect server pool.
     *
     * @var list<string>
     */
    private array $knownConnectUrls = [];
    /** The server URL the transport is currently attached to (set on each successful connect). */
    private ?string $connectedServer = null;
    /** Traffic counters surfaced via {@see statistics()}. */
    private int $inMsgs = 0;
    private int $outMsgs = 0;
    private int $inBytes = 0;
    private int $outBytes = 0;
    private int $reconnectCount = 0;
    /** Encoded publishes buffered while reconnecting (flushed on a successful reconnect); see #49. */
    private string $reconnectBuffer = '';
    /**
     * Subjects that already passed publish-path validation (#136), so repeat publishes skip the
     * regex + per-token scan. Bounded by {@see self::VALIDATED_SUBJECTS_MAX} with a full reset at
     * the cap: unique $JS.ACK reply subjects flow through here as ack publish subjects and would
     * otherwise grow it without limit.
     *
     * @var array<string,true>
     */
    private array $validatedSubjects = [];
    /**
     * Configured servers in dial order - shuffled once when {@see NatsOptions::$randomizeServers} is
     * set (#55), otherwise the configured order. Discovered peers are appended in {@see serverPool()}.
     *
     * @var list<string>
     */
    private readonly array $orderedServers;

    /** Structured logger for lifecycle/error events; NullLogger when none is configured (#69). */
    private readonly LoggerInterface $logger;

    /**
     * Creates a connection runtime with transport and protocol dependencies.
     *
     * @param NatsOptions $options Connection/runtime settings controlling handshake flags, auth, reconnect, heartbeat,
     *                             TLS, request defaults, and subscription buffering policies.
     * @param TransportInterface $transport Byte-stream transport implementation responsible for socket I/O.
     * @param ProtocolCodec $codec Encoder used to serialize NATS wire commands (CONNECT, PUB/HPUB, SUB, UNSUB, PING/PONG).
     */
    public function __construct(
        private readonly NatsOptions $options,
        private readonly TransportInterface $transport,
        private readonly ProtocolCodec $codec = new ProtocolCodec(),
    ) {
        $this->parser = new ProtocolParser();
        $this->readSlotReleased = new DeferredFuture();
        $this->readSlotReleased->getFuture()->ignore();

        $servers = $this->options->servers;
        if ($this->options->randomizeServers && count($servers) > 1) {
            shuffle($servers);
        }
        $this->orderedServers = $servers;
        $this->logger = $this->options->logger ?? new NullLogger();

        $this->messageResponder = fn(string $subject, string $payload, ?array $headers): Future => $headers === null
                ? $this->publish($subject, $payload)
                : $this->publishWithHeaders($subject, $payload, $headers);
    }

    /**
     * Returns the current connection state.
     */
    public function state(): ConnectionState
    {
        return $this->state;
    }

    /**
     * Returns server capabilities discovered during handshake.
     */
    public function serverInfo(): ?ServerInfo
    {
        return $this->serverInfo;
    }

    /**
     * The server URL the connection is currently attached to, or null when not connected.
     */
    public function connectedUrl(): ?string
    {
        return $this->state === ConnectionState::Open ? $this->connectedServer : null;
    }

    /**
     * Additional cluster endpoints advertised by the server (INFO `connect_urls`).
     *
     * @return list<string>
     */
    public function discoveredServers(): array
    {
        return $this->knownConnectUrls;
    }

    /**
     * The server's maximum accepted payload size (`max_payload`), or null when unknown.
     */
    public function maxPayload(): ?int
    {
        return $this->serverInfo?->maxPayload;
    }

    /**
     * Returns a snapshot of traffic counters for this connection.
     */
    public function statistics(): ConnectionStats
    {
        return new ConnectionStats(
            inMsgs: $this->inMsgs,
            outMsgs: $this->outMsgs,
            inBytes: $this->inBytes,
            outBytes: $this->outBytes,
            reconnects: $this->reconnectCount,
        );
    }

    /**
     * Measures the round-trip time to the server by timing a PING/PONG exchange.
     *
     * @return Future<float> Round-trip time in seconds.
     */
    public function rtt(): Future
    {
        return async(function (): float {
            if ($this->state !== ConnectionState::Open) {
                throw new ConnectionException('Connection is not open');
            }

            $start = $this->monotonicSeconds();
            $this->flush()->await();

            return $this->monotonicSeconds() - $start;
        });
    }

    /**
     * Monotonic clock in seconds (hrtime-based) for deadline/elapsed math, immune to wall-clock
     * jumps (NTP steps, suspend/resume) that make the wall clock non-monotonic (#70). The hard
     * timeout bound on every wait is already a monotonic TimeoutCancellation; this keeps the
     * surrounding loop-guard/elapsed arithmetic monotonic too.
     */
    private function monotonicSeconds(): float
    {
        return hrtime(true) / 1e9;
    }

    /**
     * Opens a transport connection and completes NATS CONNECT/PING handshake.
     *
     * Subscriptions do not survive a terminal close (user disconnect()/drain(), auth failure, or
     * an exhausted reconnect): connecting the same instance again starts from a clean slate, and
     * the application must re-create its subscriptions (nats.go parity, #127).
     *
     * @return Future<void>
     */
    public function connect(): Future
    {
        return async(function (): void {
            if ($this->state === ConnectionState::Open) {
                return;
            }

            // A fresh connect re-arms the recovery paths after a prior disconnect()/drain() (#84).
            $this->closing = false;

            try {
                $this->connectOnce();
                $this->emitEvent(ConnectionEvent::Connected);
            } catch (AuthenticationException $e) {
                // An auth failure will not resolve by retrying: fail fast instead of entering reconnect.
                $this->state = ConnectionState::Closed;
                $this->closeTransportBestEffort();
                $this->releaseRuntimeState();
                $this->emitEvent(ConnectionEvent::Closed, $e);

                throw $e;
            } catch (\Throwable $e) {
                if ($this->options->reconnectEnabled && $this->options->maxReconnectAttempts > 0) {
                    $this->recoverConnection();

                    return;
                }

                // retry-on-failed-initial-connect (#56): keep retrying the first connect even when
                // ongoing reconnect is disabled.
                if ($this->options->retryOnFailedInitialConnect
                    && $this->options->maxReconnectAttempts > 0
                    && $this->retryInitialConnect()
                ) {
                    return;
                }

                $this->state = ConnectionState::Closed;
                $this->closeTransportBestEffort();
                $this->releaseRuntimeState();
                $this->emitEvent(ConnectionEvent::Closed, $e);
                throw new ConnectionException($e->getMessage(), (int) $e->getCode(), $e);
            }
        });
    }

    /**
     * Closes the transport and marks the runtime as closed.
     *
     * Locally queued, undelivered messages (already parsed and counted in `inMsgs`, awaiting
     * dispatch) are discarded without being delivered - nats.go Close() parity (#134). Use
     * {@see drain()} for the lossless path: it delivers the buffered backlog before closing.
     *
     * @return Future<void>
     */
    public function disconnect(): Future
    {
        return async(function (): void {
            // Signal close-intent BEFORE closing the socket so an in-flight reconnect/heartbeat read
            // cannot race to re-open the connection after the user asked to close it (#84).
            $this->closing = true;
            $this->cancelPingTimer();
            $this->transport->close()->await();
            $this->state = ConnectionState::Closed;

            // Release per-connection state so a long-lived/pooled client (or one disconnect()ed and
            // later reused) does not retain handler closures, buffered messages, parser bytes, or the
            // reconnect buffer until the whole object is GC'd (#85). Mirrors drain()'s teardown.
            $this->releaseRuntimeState();

            $this->emitEvent(ConnectionEvent::Closed);
        });
    }

    /**
     * Releases per-connection runtime state: subscription registry and handler closures, queued
     * messages, delivery counters, parser bytes, and the reconnect buffer. Must run on every
     * terminal transition to Closed - not just user disconnect() - so payloads and closures never
     * outlive the connection, and a later manual connect() starts from a clean slate instead of
     * resurrecting sids from a dead epoch when a future recovery replays subscriptionMeta (#127).
     */
    private function releaseRuntimeState(): void
    {
        $this->subscriptions = [];
        $this->subscriptionMeta = [];
        $this->pendingMessages = [];
        $this->receivedCounts = [];
        $this->deliveredCounts = [];
        $this->autoUnsubMax = [];
        $this->reconnectBuffer = '';
        $this->parser = new ProtocolParser();
    }

    /**
     * Best-effort transport close for terminal failure exits, mirroring disconnect(). connectOnce()
     * opens the socket before the handshake can fail, so every path that gives up (terminal connect
     * failure, exhausted or auth-aborted recovery) must close it - otherwise the fd stays pinned by
     * the transport until the client object itself is GC'd (#133). Close failures are irrelevant
     * here: the socket may already be gone, and the terminal state transition is what matters.
     */
    private function closeTransportBestEffort(): void
    {
        try {
            $this->transport->close()->await();
        } catch (\Throwable) {
            // Ignore: already closed/broken sockets must not mask the original failure.
        }
    }

    /**
     * Gracefully drains all subscriptions, flushes pending messages, then closes.
     *
     * @return Future<void>
     */
    public function drain(): Future
    {
        return async(function (): void {
            if ($this->state !== ConnectionState::Open) {
                throw new ConnectionException('Connection is not open');
            }

            $this->state = ConnectionState::Draining;
            // Close-intent: a recovery triggered mid-drain must not re-open the connection (#84).
            $this->closing = true;
            $this->cancelPingTimer();

            // Send UNSUB for all active subscriptions so no new messages arrive.
            foreach (array_keys($this->subscriptionMeta) as $sid) {
                $this->transport->write($this->codec->encodeUnsubscribe($sid))->await();
            }

            // Flush in-flight deliveries already emitted by the server before closing.
            $this->drainFlushPending = true;
            $this->transport->write($this->codec->encodePing())->await();

            // Read until the server's PONG confirms the flush (handleFrame clears drainFlushPending),
            // bounded by a deadline so a slow/wedged server cannot hang drain() forever. A partial
            // chunk (0 complete frames yet) must NOT end the flush early - only the PONG or the
            // deadline does.
            $flushCancellation = new TimeoutCancellation(max(0.1, $this->options->requestTimeoutMs / 1000));
            try {
                while (!$flushCancellation->isRequested()) {
                    $frames = $this->processIncoming($flushCancellation)->await();

                    if (!$this->drainFlushPending) {
                        // The server's PONG arrived (handleFrame cleared the flag): flush complete.
                        break;
                    }

                    if ($frames === 0) {
                        // No complete frame this read. Yield so the event loop advances and the
                        // deadline can fire - processIncoming() returns 0 synchronously on an empty
                        // read, so without this the loop would busy-spin and starve the timer forever.
                        delay(0.001, cancellation: $flushCancellation);
                    }
                }
            } catch (CancelledException) {
                // Flush deadline reached; close with whatever was delivered.
            } catch (\Throwable) {
                // A fatal frame (e.g. a server -ERR) surfaced mid-flush. Stop flushing and fall through
                // to the cleanup below so drain() still closes the socket and clears state rather than
                // leaving the connection wedged in Draining with the socket open.
            }

            // Drain any remaining buffered messages to callbacks.
            $this->drainAllPending();

            // Clear subscription state.
            $this->releaseRuntimeState();
            $this->drainFlushPending = false;

            $this->transport->close()->await();
            $this->state = ConnectionState::Closed;
        });
    }

    /**
     * Publishes payload bytes to the given subject.
     *
     * @return Future<void>
     */
    public function publish(string $subject, string $payload, ?string $replyTo = null): Future
    {
        return async(function () use ($subject, $payload, $replyTo): void {
            $this->validateSubjectCached($subject);
            if ($replyTo !== null) {
                // Not cached: a publish replyTo is typically a per-request unique inbox, so caching
                // it would only churn the memo (see validateSubjectCached()).
                $this->validateSubject($replyTo);
            }
            $this->enforceMaxPayload(strlen($payload));

            $frame = $this->codec->encodePublish($subject, $payload, $replyTo);

            if ($this->state !== ConnectionState::Open) {
                // Buffer while a reconnect is in flight (flushed on reconnect); otherwise unusable.
                if (!$this->bufferFrame($frame)) {
                    throw new ConnectionException('Connection is not open');
                }

                $this->recordOutbound($payload);

                return;
            }

            try {
                $this->transport->write($frame)->await();
            } catch (\Throwable) {
                $this->recoverConnection();
                $this->transport->write($frame)->await();
            }

            $this->recordOutbound($payload);
        });
    }

    /**
     * Publishes payload bytes with NATS headers to the given subject. A header value may be a single
     * string or a list of strings for multi-value (multimap) headers (ADR-4).
     *
     * @param array<string,string|list<string>> $headers
     * @return Future<void>
     */
    public function publishWithHeaders(
        string $subject,
        string $payload,
        array $headers,
        ?string $replyTo = null,
    ): Future {
        return async(function () use ($subject, $payload, $headers, $replyTo): void {
            $this->validateSubjectCached($subject);
            if ($replyTo !== null) {
                // Not cached: a publish replyTo is typically a per-request unique inbox, so caching
                // it would only churn the memo (see validateSubjectCached()).
                $this->validateSubject($replyTo);
            }
            // A server that does not advertise the `headers` capability treats HPUB as an unknown
            // protocol operation and closes the connection; fail client-side instead (nats.go
            // ErrHeadersNotSupported parity, #132).
            if ($this->serverInfo?->headersSupported === false) {
                throw new ConnectionException('Server does not advertise headers support; cannot publish with headers (HPUB)');
            }
            // Build (and CR/LF-validate) the header wire block once, then reuse it for sizing and for
            // each write attempt, instead of re-running toWireBlock() per call.
            $headerBlock = NatsHeaders::toWireBlock($headers);
            $this->enforceMaxPayload(strlen($headerBlock) + strlen($payload));

            $frame = $this->codec->encodeHeaderPublishBlock($subject, $payload, $headerBlock, $replyTo);

            if ($this->state !== ConnectionState::Open) {
                if (!$this->bufferFrame($frame)) {
                    throw new ConnectionException('Connection is not open');
                }

                $this->recordOutbound($payload);

                return;
            }

            try {
                $this->transport->write($frame)->await();
            } catch (\Throwable) {
                $this->recoverConnection();
                $this->transport->write($frame)->await();
            }

            $this->recordOutbound($payload);
        });
    }

    /**
     * Publishes a block of header-carrying messages (no reply subjects) coalesced into bounded
     * segment writes: every message is validated up front, then consecutive HPUB frames are
     * concatenated and flushed in segments of at most {@see self::PUBLISH_BLOCK_SEGMENT_BYTES}, so
     * one transport write carries many frames instead of one awaited write per message (#138).
     * Each frame is byte-identical to a publishWithHeaders() call for the same message, and frame
     * order is preserved; segments are consecutive writes with no server round-trip in between.
     *
     * @internal Used by BatchPublisher to send atomic-batch intermediates; not part of the
     *           supported API.
     *
     * @param list<array{subject:string,payload:string,headers:array<string,string|list<string>>}> $messages
     * @return Future<void>
     */
    public function publishHeaderBlock(array $messages): Future
    {
        return async(function () use ($messages): void {
            if ($messages === []) {
                return;
            }

            // A server that does not advertise the `headers` capability treats HPUB as an unknown
            // protocol operation and closes the connection; fail client-side instead (nats.go
            // ErrHeadersNotSupported parity, #132).
            if ($this->serverInfo?->headersSupported === false) {
                throw new ConnectionException('Server does not advertise headers support; cannot publish with headers (HPUB)');
            }

            // Validate EVERY message (subject rules + max_payload) before any bytes hit the wire:
            // an invalid message must abort the whole block with nothing written, not fail midway
            // through a partially-sent block. The validated header wire blocks are kept for the
            // encode pass below - they are small next to the payloads, which stay referenced in
            // $messages either way (PHP strings are refcounted, not copied).
            $headerBlocks = [];
            foreach ($messages as $index => $message) {
                $this->validateSubjectCached($message['subject']);
                $headerBlock = NatsHeaders::toWireBlock($message['headers']);
                $this->enforceMaxPayload(strlen($headerBlock) + strlen($message['payload']));
                $headerBlocks[$index] = $headerBlock;
            }

            $segment = '';
            /** @var list<string> $segmentPayloads */
            $segmentPayloads = [];
            foreach ($messages as $index => $message) {
                $frame = $this->codec->encodeHeaderPublishBlock(
                    $message['subject'],
                    $message['payload'],
                    $headerBlocks[$index],
                );

                // Flush BEFORE appending would overflow the cap, so a segment never exceeds it
                // unless a single frame alone does (an unsplittable frame is written by itself).
                if ($segment !== '' && strlen($segment) + strlen($frame) > self::PUBLISH_BLOCK_SEGMENT_BYTES) {
                    $this->writePublishSegment($segment, $segmentPayloads);
                    $segment = '';
                    $segmentPayloads = [];
                }

                $segment .= $frame;
                $segmentPayloads[] = $message['payload'];
            }

            $this->writePublishSegment($segment, $segmentPayloads);
        });
    }

    /**
     * Writes one coalesced publish segment with the same shape publish() uses for a single frame:
     * while not Open the WHOLE segment goes through the reconnect buffer (or fails), otherwise one
     * transport write with a single recover-and-retry on failure. Outbound stats are recorded per
     * message once the segment is buffered or written, matching per-message publishes.
     *
     * @param list<string> $payloads The payloads carried by the segment, in frame order.
     */
    private function writePublishSegment(string $segment, array $payloads): void
    {
        if ($this->state !== ConnectionState::Open) {
            // Buffer while a reconnect is in flight (flushed on reconnect); otherwise unusable.
            if (!$this->bufferFrame($segment)) {
                throw new ConnectionException('Connection is not open');
            }
        } else {
            try {
                $this->transport->write($segment)->await();
            } catch (\Throwable) {
                $this->recoverConnection();
                $this->transport->write($segment)->await();
            }
        }

        foreach ($payloads as $payload) {
            $this->recordOutbound($payload);
        }
    }

    /**
     * Buffers an encoded publish while a reconnect is in flight (flushed on reconnect). Returns false
     * when buffering does not apply - no active reconnect, buffering disabled, or the buffer is full.
     */
    private function bufferFrame(string $frame): bool
    {
        if ($this->reconnecting === null || $this->options->reconnectBufferSize <= 0) {
            return false;
        }

        // A terminal path can flip to Closed and then suspend in its transport-close await while
        // $reconnecting is still set; accepting bytes there would report success for a publish
        // that releaseRuntimeState() is about to discard with no error signal (#146). Refusing
        // keeps the failure loud and immediate on every terminal path (#123 invariant).
        if ($this->state === ConnectionState::Closed) {
            return false;
        }

        if (strlen($this->reconnectBuffer) + strlen($frame) > $this->options->reconnectBufferSize) {
            return false;
        }

        $this->reconnectBuffer .= $frame;

        return true;
    }

    /**
     * Records an outbound message in the traffic counters.
     */
    private function recordOutbound(string $payload): void
    {
        $this->outMsgs++;
        $this->outBytes += strlen($payload);
    }

    /**
     * Registers a subscription callback and sends a SUB command.
     *
     * @param callable(NatsMessage):void $handler
     * @return Future<int>
     */
    public function subscribe(string $subject, callable $handler, ?string $queue = null): Future
    {
        return async(function () use ($subject, $handler, $queue): int {
            if ($this->state !== ConnectionState::Open) {
                throw new ConnectionException('Connection is not open');
            }

            $this->validateSubject($subject, allowWildcards: true);
            if ($queue !== null) {
                $this->validateQueueGroup($queue);
            }
            $sid = $this->nextSid++;
            // Register before the write: once SUB hits the wire another fiber's read may deliver for
            // this sid immediately, so the handler must already be routable.
            $this->subscriptions[$sid] = $handler;
            $this->subscriptionMeta[$sid] = ['subject' => $subject, 'queue' => $queue];
            $this->pendingMessages[$sid] = new SplQueue();

            try {
                $this->transport->write($this->codec->encodeSubscribe($subject, $sid, $queue))->await();
            } catch (\Throwable $e) {
                // The SUB never reached the wire; roll back so the registry does not retain an entry
                // whose sid the caller never learns (and resubscribeAll() cannot revive it) (#116).
                $this->dropSubscriptionState($sid);

                throw $e;
            }

            return $sid;
        });
    }

    /**
     * Removes a subscription callback and sends an UNSUB command.
     *
     * With $maxMessages, arms auto-unsubscribe instead of removing immediately: the server keeps
     * delivering until $maxMessages TOTAL messages (counting those already delivered) have been sent
     * on the sid, and the local handler stays registered until that point so the remaining deliveries
     * reach the application instead of the unknown-sid discard (#112). Mirrors nats.go AutoUnsubscribe.
     *
     * On a connection that is not open the server cannot be told, but local state is still released
     * without throwing: finally-based inbox cleanup runs on broken connections and must neither leak
     * the subscription entry nor mask the caller's original error (#116).
     *
     * A plain unsubscribe (no $maxMessages) discards the sid's locally queued, undelivered backlog -
     * messages already parsed and counted in `inMsgs` but not yet dispatched to the handler - matching
     * nats.go Unsubscribe() (#134). Use {@see drainSubscription()} (or a full {@see drain()}) for the
     * lossless path that delivers the backlog first.
     *
     * @return Future<void>
     */
    public function unsubscribe(int $sid, ?int $maxMessages = null): Future
    {
        return async(function () use ($sid, $maxMessages): void {
            if (!isset($this->subscriptionMeta[$sid])) {
                return;
            }

            if ($maxMessages !== null) {
                // Auto-unsubscribe: keep the subscription registered so the remaining deliveries up to
                // the max still reach the handler - INCLUDING across a reconnect, where resubscribeAll()
                // replays SUB + re-arms UNSUB with the remaining allowance. This is checked BEFORE the
                // not-open guard on purpose: arming while a reconnect is in flight must defer the arm to
                // recovery, never destroy the subscription (which would silently lose the remaining
                // deliveries with the caller still believing they will arrive) (#112).
                $this->autoUnsubMax[$sid] = $maxMessages;

                if ($this->state === ConnectionState::Open) {
                    // On a broken connection the server cannot be told now; recovery re-arms it. A write
                    // failure propagates but leaves the arm state intact for that recovery.
                    $this->transport->write($this->codec->encodeUnsubscribe($sid, $maxMessages))->await();
                }

                // Already satisfied (max <= messages already received) with nothing left to deliver:
                // complete immediately rather than waiting for a delivery that will never come.
                $this->completeAutoUnsubIfSatisfied($sid);

                return;
            }

            if ($this->state !== ConnectionState::Open) {
                // Plain unsubscribe on a broken connection: release local state without throwing, so
                // finally-based inbox cleanup neither leaks the entry nor masks the caller's error (#116).
                $this->dropSubscriptionState($sid);

                return;
            }

            try {
                $this->transport->write($this->codec->encodeUnsubscribe($sid, $maxMessages))->await();
            } finally {
                // Drop local state even when the UNSUB write fails: the connection is heading into
                // recovery anyway, and retaining the entry would leak it and re-SUB it later (#116).
                $this->dropSubscriptionState($sid);
            }
        });
    }

    /**
     * Drains a single subscription: sends UNSUB so the server stops delivering, flushes so any
     * messages already in flight are received and dispatched to the handler, then removes the local
     * subscription state. Mirrors nats.go / nats.java per-subscription `Drain()` (#43).
     *
     * @return Future<void>
     */
    public function drainSubscription(int $sid): Future
    {
        return async(function () use ($sid): void {
            if ($this->state !== ConnectionState::Open) {
                // Nothing to drain on a connection that is not open; just drop any local state.
                $this->dropSubscriptionState($sid);

                return;
            }

            if (!isset($this->subscriptionMeta[$sid])) {
                return;
            }

            // Stop new deliveries for this sid, then flush so in-flight messages are received...
            $this->transport->write($this->codec->encodeUnsubscribe($sid))->await();
            try {
                $this->flush()->await();
            } catch (\Throwable) {
                // A flush failure (timeout/closed) still leaves us safe to drop the subscription below.
            }

            // ...deliver whatever arrived for it, then remove the handler and local state.
            $this->drainPendingForSid($sid);
            $this->dropSubscriptionState($sid);
        });
    }

    /**
     * Flushes the outbound buffer and waits for the server to round-trip a PONG, confirming the server
     * has processed everything written so far. Useful to ensure a SUBSCRIBE is registered server-side
     * before relying on it (e.g. before publishing a request to a freshly-subscribed responder).
     * Bounded by the configured request timeout.
     *
     * @return Future<void>
     */
    public function flush(): Future
    {
        return async(function (): void {
            if ($this->state !== ConnectionState::Open) {
                throw new ConnectionException('Connection is not open');
            }

            $this->flushPending = true;
            $this->transport->write($this->codec->encodePing())->await();

            $cancellation = new TimeoutCancellation(max(0.1, $this->options->requestTimeoutMs / 1000));
            try {
                while ($this->flushPending) {
                    $frames = $this->processIncoming($cancellation)->await();

                    // A read that produced no complete frame must not busy-spin: yield so the deadline
                    // can fire (processIncoming() returns 0 synchronously on an empty read).
                    if ($frames === 0 && $this->flushPending) {
                        delay(0.001, cancellation: $cancellation);
                    }
                }
            } catch (CancelledException) {
                throw new TimeoutException('Flush timed out waiting for server PONG');
            } finally {
                $this->flushPending = false;
            }
        });
    }

    /**
     * Reads one transport chunk, parses frames, and dispatches message callbacks.
     *
     * @param Cancellation|null $cancellation Optional token that cancels the underlying socket read,
     *                                        so a timed-out caller does not orphan an in-flight read.
     * @return Future<int>
     *
     * @phpstan-impure Mutates connection state (e.g. clears drainFlushPending / outstandingPings via
     *                 handled frames), so callers must not assume remembered property values persist.
     */
    public function processIncoming(?Cancellation $cancellation = null): Future
    {
        return async(function () use ($cancellation): int {
            if ($this->state !== ConnectionState::Open && $this->state !== ConnectionState::Draining) {
                throw new ConnectionException('Connection is not open');
            }

            if ($this->readInProgress) {
                // A concurrent read (e.g. the heartbeat timer) owns the socket; avoid a second
                // overlapping read which the transport would reject with a pending-read error.
                return 0;
            }

            $this->readInProgress = true;

            try {
                $chunk = $this->transport->readLine($cancellation)->await();
            } catch (CancelledException $cancelledException) {
                throw $cancelledException;
            } catch (\Throwable $readError) {
                // During drain() a read failure means the flush is finished, not a fault to recover
                // from: recovering would reconnect and re-SUBscribe the very subscriptions drain()
                // just UNSUBbed (and could re-deliver). Treat it as end-of-flush instead.
                if ($this->state !== ConnectionState::Draining) {
                    $this->emitError($readError);
                    $this->recoverConnection();
                }

                return 0;
            } finally {
                $this->readInProgress = false;
                $this->signalReadSlotFree();
            }

            if ($chunk === '') {
                return 0;
            }

            try {
                $frames = $this->parser->push($chunk);
            } catch (ProtocolException) {
                // An unparseable/corrupt stream is a transport-level failure: reconnect rather than
                // letting the exception escape the caller's processing loop. The parser has already
                // resynced past the offending bytes, so a recovery-disabled retry will not re-throw.
                $this->recoverConnection();

                return 0;
            }

            // Note: the outstanding-ping counter is reset only when an actual PONG is handled (see
            // handleFrame), not on any inbound bytes - otherwise a server that stops answering PINGs
            // but still trickles data would never trip maxPingsOut and the watchdog could not escalate.
            try {
                $this->dispatchFrames($frames);
            } finally {
                // Drain buffered deliveries after each chunk to preserve wire-order delivery - even
                // when a frame failed to dispatch: the messages are already enqueued and their bytes
                // consumed, so they must not wait behind (or be lost to) the surfacing error.
                $this->drainAllPending();
            }

            return count($frames);
        });
    }

    /**
     * Dispatches parsed frames, containing per-frame failures so one frame cannot abort delivery
     * of the frames parsed from the same chunk: the parser has already consumed the bytes, so an
     * undispatched trailing frame is unrecoverable (core NATS does not resend, and a reconnect
     * replays SUBs, not missed messages) (#128). The first failure is rethrown after every frame
     * has been dispatched, preserving fatal -ERR / write-failure semantics for the caller.
     *
     * @param list<ProtocolFrame> $frames
     */
    private function dispatchFrames(array $frames): void
    {
        $firstError = null;

        foreach ($frames as $frame) {
            try {
                $this->handleFrame($frame);
            } catch (\Throwable $e) {
                $firstError ??= $e;
            }
        }

        if ($firstError !== null) {
            throw $firstError;
        }
    }

    /**
     * Wakes request waiters parked behind another fiber's socket read (#135): the current
     * broadcast future completes (all parked waiters resume, re-check their completion, and one
     * takes over the read slot) and a fresh one is armed for the next read cycle.
     */
    private function signalReadSlotFree(): void
    {
        $released = $this->readSlotReleased;
        $this->readSlotReleased = new DeferredFuture();
        $this->readSlotReleased->getFuture()->ignore();
        $released->complete();
    }

    /**
     * Sends a request and awaits the first response on an auto-generated inbox subject.
     *
     * @param Cancellation|null $cancellation Optional external cancellation token.
     * @return Future<NatsMessage>
     */
    public function request(
        string $subject,
        string $payload,
        ?int $timeoutMs = null,
        ?Cancellation $cancellation = null,
    ): Future {
        return async(function () use ($subject, $payload, $timeoutMs, $cancellation): NatsMessage {
            // Cached: request targets repeat (unlike the per-request inbox, which is validated
            // uncached as publish()'s replyTo).
            $this->validateSubjectCached($subject);

            return $this->requestInternal($subject, $payload, null, $timeoutMs, $cancellation);
        });
    }

    /**
     * Sends a request with headers and awaits the first response.
     *
     * @param array<string,string> $headers
     * @param Cancellation|null $cancellation Optional external cancellation token.
     * @return Future<NatsMessage>
     */
    public function requestWithHeaders(
        string $subject,
        string $payload,
        array $headers,
        ?int $timeoutMs = null,
        ?Cancellation $cancellation = null,
    ): Future {
        return async(function () use ($subject, $payload, $headers, $timeoutMs, $cancellation): NatsMessage {
            // Cached: request targets repeat (unlike the per-request inbox, which is validated
            // uncached as publish()'s replyTo).
            $this->validateSubjectCached($subject);

            return $this->requestInternal($subject, $payload, $headers, $timeoutMs, $cancellation);
        });
    }

    /**
     * Executes request/reply flow using plain publish or header publish variants.
     *
     * @param array<string,string>|null $headers
     */
    private function requestInternal(
        string $subject,
        string $payload,
        ?array $headers,
        ?int $timeoutMs,
        ?Cancellation $cancellation,
    ): NatsMessage {
        if ($this->state !== ConnectionState::Open) {
            throw new ConnectionException('Connection is not open');
        }

        $inbox = Inbox::generate($this->options->inboxPrefix);
        /** @var DeferredFuture<NatsMessage> $deferred */
        $deferred = new DeferredFuture();
        // Set by the handler when the reply is delivered. The wait loop checks this rather than
        // $deferred->isComplete() so a reply delivered in the same tick the deadline fires is
        // returned instead of being discarded as a spurious timeout.
        $replyReceived = false;

        $sid = $this->subscribe($inbox, static function (NatsMessage $message) use ($deferred, &$replyReceived): void {
            if (!$deferred->isComplete()) {
                $deferred->complete($message);
                $replyReceived = true;
            }
        })->await();

        try {
            if ($headers === null) {
                $this->publish($subject, $payload, $inbox)->await();
            } else {
                $this->publishWithHeaders($subject, $payload, $headers, $inbox)->await();
            }

            $deadlineMs = $timeoutMs ?? $this->options->requestTimeoutMs;
            if ($deadlineMs <= 0) {
                throw new TimeoutException('Request timeout must be greater than zero');
            }

            $timeoutCancellation = new TimeoutCancellation($deadlineMs / 1000);
            $waitCancellation = $cancellation === null
                ? $timeoutCancellation
                : new CompositeCancellation($cancellation, $timeoutCancellation);

            while (true) {
                // Completion is checked BEFORE the deadline so a reply delivered in the same tick the
                // deadline fires (by this loop's read or a concurrent heartbeat read) is returned
                // rather than discarded as a spurious timeout.
                if ($replyReceived) {
                    break;
                }

                if ($waitCancellation->isRequested()) {
                    if ($cancellation !== null && $cancellation->isRequested()) {
                        throw new CancelledException();
                    }

                    throw new TimeoutException('Request timed out for subject ' . $subject);
                }

                if ($this->readInProgress) {
                    // Another fiber owns the socket read. Park on our reply or the read slot
                    // freeing instead of re-polling on a 1ms timer: N concurrent requests used to
                    // burn O(N x 1000/s) wakeups, each allocating a Future (#135). The slot future
                    // is captured before the re-check inside awaitFirst, so a release between the
                    // flag check and the await completes it immediately - no lost wakeup.
                    try {
                        awaitFirst([$deferred->getFuture(), $this->readSlotReleased->getFuture()], $waitCancellation);
                    } catch (CancelledException) {
                        // Deadline or external cancellation while parked; the top-of-loop checks
                        // return the reply delivered in the same tick or throw.
                    }

                    continue;
                }

                try {
                    $frames = $this->processIncoming($waitCancellation)->await();
                } catch (CancelledException $e) {
                    if ($cancellation !== null && $cancellation->isRequested()) {
                        throw $e;
                    }

                    // The deadline fired during the read. Loop once more: the top-of-loop check
                    // returns the reply if it was delivered in the same tick, otherwise the deadline
                    // check there throws the timeout.
                    continue;
                }

                if ($frames === 0) {
                    // No data available from transport; yield to avoid a tight spin.
                    delay(0.001);
                }
            }

            $response = $deferred->getFuture()->await();

            if ($this->isNoRespondersStatus($response)) {
                throw new NatsException('No responders for subject ' . $subject);
            }

            return $response;
        } finally {
            $this->cleanupRequestSubscription($sid);
        }
    }

    /**
     * Sends a single request and collects MULTIPLE replies (scatter-gather), terminating on the
     * first of: {@see $maxResponses} collected, a no-responders (503) sentinel, the per-message
     * stall interval elapsing, or the total timeout.
     *
     * @param array<string,string>|null $headers Optional request headers (null = plain PUB).
     * @param int|null $maxResponses Stop after this many replies (null = unbounded, bounded only by time).
     * @param int|null $totalTimeoutMs Overall budget in ms (null = the configured request timeout).
     * @param int|null $stallMs If set, stop once this long passes after the most recent reply.
     * @param Cancellation|null $cancellation Optional external cancellation token.
     * @return Future<list<NatsMessage>>
     */
    public function requestMany(
        string $subject,
        string $payload,
        ?array $headers = null,
        ?int $maxResponses = null,
        ?int $totalTimeoutMs = null,
        ?int $stallMs = null,
        ?Cancellation $cancellation = null,
    ): Future {
        return async(function () use ($subject, $payload, $headers, $maxResponses, $totalTimeoutMs, $stallMs, $cancellation): array {
            // Cached: request targets repeat (unlike the per-request inbox, which is validated
            // uncached as publish()'s replyTo).
            $this->validateSubjectCached($subject);

            if ($maxResponses !== null && $maxResponses < 1) {
                throw new \InvalidArgumentException('maxResponses must be at least 1 when provided');
            }
            if ($stallMs !== null && $stallMs <= 0) {
                throw new \InvalidArgumentException('stallMs must be greater than zero when provided');
            }

            return $this->requestManyInternal($subject, $payload, $headers, $maxResponses, $totalTimeoutMs, $stallMs, $cancellation);
        });
    }

    /**
     * Executes the scatter-gather collection loop.
     *
     * @param array<string,string>|null $headers
     * @return list<NatsMessage>
     */
    private function requestManyInternal(
        string $subject,
        string $payload,
        ?array $headers,
        ?int $maxResponses,
        ?int $totalTimeoutMs,
        ?int $stallMs,
        ?Cancellation $cancellation,
    ): array {
        if ($this->state !== ConnectionState::Open) {
            throw new ConnectionException('Connection is not open');
        }

        $totalMs = $totalTimeoutMs ?? $this->options->requestTimeoutMs;
        if ($totalMs <= 0) {
            throw new TimeoutException('Request timeout must be greater than zero');
        }

        $inbox = Inbox::generate($this->options->inboxPrefix);
        /** @var list<NatsMessage> $messages */
        $messages = [];
        $lastAt = null;
        $noResponders = false;

        // Rotated on every delivery so a waiter parked behind another fiber's read wakes to
        // re-evaluate its termination conditions (count/stall/no-responders) (#135).
        /** @var DeferredFuture<null> $replyTick */
        $replyTick = new DeferredFuture();
        $replyTick->getFuture()->ignore();

        $sid = $this->subscribe($inbox, function (NatsMessage $message) use (&$messages, &$lastAt, &$noResponders, &$replyTick): void {
            if ($this->isNoRespondersStatus($message)) {
                // The server's 503 sentinel: no service is listening. Stop immediately with whatever
                // (typically nothing) was collected.
                $noResponders = true;
            } else {
                $messages[] = $message;
                $lastAt = $this->monotonicSeconds();
            }

            $tick = $replyTick;
            $replyTick = new DeferredFuture();
            $replyTick->getFuture()->ignore();
            $tick->complete();
        })->await();

        try {
            if ($headers === null) {
                $this->publish($subject, $payload, $inbox)->await();
            } else {
                $this->publishWithHeaders($subject, $payload, $headers, $inbox)->await();
            }

            $deadline = $this->monotonicSeconds() + $totalMs / 1000;
            $totalCancellation = new TimeoutCancellation($totalMs / 1000);
            $waitCancellation = $cancellation === null
                ? $totalCancellation
                : new CompositeCancellation($cancellation, $totalCancellation);

            while (true) {
                if ($noResponders) {
                    break;
                }

                if ($maxResponses !== null && count($messages) >= $maxResponses) {
                    break;
                }

                $now = $this->monotonicSeconds();

                // Stall: stop once the gap since the last reply exceeds the configured interval.
                if ($stallMs !== null && $lastAt !== null && ($now - $lastAt) * 1000 >= $stallMs) {
                    break;
                }

                $remainingTotal = $deadline - $now;
                if ($remainingTotal <= 0 || $waitCancellation->isRequested()) {
                    if ($cancellation !== null && $cancellation->isRequested()) {
                        throw new CancelledException();
                    }

                    break;
                }

                // Wake at the earlier of the total deadline and the next stall checkpoint, so the
                // stall interval is honored even while the socket is idle.
                $slice = $remainingTotal;
                if ($stallMs !== null && $lastAt !== null) {
                    $slice = min($slice, $stallMs / 1000 - ($now - $lastAt));
                }
                $sliceCancellation = new CompositeCancellation(
                    $waitCancellation,
                    new TimeoutCancellation(max(0.001, $slice)),
                );

                if ($this->readInProgress) {
                    // Another fiber owns the socket read: park on the next delivery or the read
                    // slot freeing, bounded by the same slice so stall/total still fire (#135).
                    try {
                        awaitFirst([$replyTick->getFuture(), $this->readSlotReleased->getFuture()], $sliceCancellation);
                    } catch (CancelledException) {
                        // Slice/total deadline while parked; loop re-evaluates at the top.
                    }

                    continue;
                }

                try {
                    $frames = $this->processIncoming($sliceCancellation)->await();
                } catch (CancelledException $e) {
                    if ($cancellation !== null && $cancellation->isRequested()) {
                        throw $e;
                    }

                    // Slice or total deadline fired during the read; loop to re-evaluate the
                    // termination conditions (stall/total) at the top.
                    continue;
                }

                if ($frames === 0) {
                    delay(0.001);
                }
            }

            return $messages;
        } finally {
            $this->cleanupRequestSubscription($sid);
        }
    }

    /**
     * Checks whether a response message carries a 503 No Responders status.
     */
    private function isNoRespondersStatus(NatsMessage $message): bool
    {
        if ($message->rawHeaders === null) {
            return false;
        }

        $firstLine = explode("\r\n", $message->rawHeaders, 2)[0];
        if ($firstLine === '') {
            return false;
        }

        // Status line format: "NATS/1.0 503" or "NATS/1.0 503 No Responders".
        return (bool) preg_match('/^NATS\/1\.0\s+503\b/', $firstLine);
    }

    /**
     * Determines whether the connection must be upgraded to TLS, based on the configured options
     * (explicit {@see NatsOptions::$tlsRequired} or a supplied {@see NatsOptions::$tlsContext}), the
     * server URL scheme, and the server's advertised TLS requirement.
     *
     * A configured `tlsContext` implies TLS-required (per its documented contract): without this, a
     * `tlsContext`-only configuration over a `nats://` DSN to a server that does not advertise
     * `tls_required` would skip the upgrade and write CONNECT (credentials) in cleartext.
     */
    private function requiresTls(string $server, ServerInfo $serverInfo): bool
    {
        return $this->options->tlsRequired
            || $this->options->tlsContext !== null
            || str_starts_with($server, 'tls://')
            || $serverInfo->tlsRequired;
    }

    /**
     * Normalizes NATS DSN scheme to the transport-compatible scheme.
     */
    private function normalizeDsn(string $server): string
    {
        // Strip URL-embedded credentials (user:pass@ / token@): they are applied to the CONNECT
        // payload (see extractUrlCredentials()), not dialed by the socket transport.
        $stripped = preg_replace('#^([a-z][a-z0-9+.\-]*://)[^@/]*@#i', '$1', $server);
        $server = $stripped ?? $server;

        $normalized = preg_replace('#^nats://#', 'tcp://', $server);
        if ($normalized === null) {
            throw new ConnectionException('Invalid server DSN');
        }

        return $normalized;
    }

    /**
     * Extracts credentials embedded in a server URL's userinfo (#37): `user:pass@host` yields a
     * user/password pair, a single `token@host` component yields a token. Returns an empty array when
     * the URL carries no credentials.
     *
     * @return array{user?:string,pass?:string,token?:string}
     */
    private function extractUrlCredentials(string $server): array
    {
        $user = parse_url($server, PHP_URL_USER);
        if (!is_string($user) || $user === '') {
            return [];
        }

        $user = rawurldecode($user);
        $pass = parse_url($server, PHP_URL_PASS);
        if (is_string($pass) && $pass !== '') {
            return ['user' => $user, 'pass' => rawurldecode($pass)];
        }

        // A lone userinfo component (no password) is a token.
        return ['token' => $user];
    }

    /**
     * Establishes a fresh connection against the next available server.
     */
    private function connectOnce(): void
    {
        $this->state = ConnectionState::Connecting;
        // A fresh connection is not (yet) draining; allow a new lame-duck signal to be observed.
        $this->lameDuckAnnounced = false;
        // Framing state is per TCP connection: a previous connection that died mid-frame leaves the
        // parser expecting the rest of a payload, which would swallow this handshake's INFO/PONG as
        // phantom payload bytes and fail every reconnect attempt against a healthy server (#125).
        // The post-handshake reset below still re-couples the bound to the negotiated max_payload.
        $this->parser = new ProtocolParser();

        $server = $this->nextServer();
        $this->connectedServer = $server;
        $urlCredentials = $this->extractUrlCredentials($server);
        $dsn = $this->normalizeDsn($server);
        $this->transport->connect($dsn, $this->options->connectTimeoutMs)->await();

        $this->serverInfo = $this->awaitServerInfo();

        // Standard NATS TLS upgrade: after the plaintext INFO, upgrade the socket to TLS (unless the
        // handshake-first path already negotiated TLS during connect()).
        if (!$this->options->tlsHandshakeFirst && $this->requiresTls($server, $this->serverInfo)) {
            $this->transport->upgradeTls()->await();
        }

        // Never write CONNECT (which carries credentials) over a socket that is still plaintext when
        // TLS is required - regardless of which path was meant to establish it. This guard runs for the
        // handshake-first path too, so a misconfiguration (tlsHandshakeFirst=true but no TLS materials
        // or a nats:// DSN, while the server's INFO advertises tls_required) fails fast instead of
        // leaking credentials in cleartext.
        if ($this->requiresTls($server, $this->serverInfo)
            && $this->transport instanceof TlsAwareTransportInterface
            && !$this->transport->tlsActive()
        ) {
            throw new ConnectionException(
                'Server requires TLS but the TLS handshake was not established; '
                . 'configure TLS materials (NatsOptions tlsRequired / tlsCaFile / tlsCertFile) for this connection',
            );
        }

        $this->transport->write($this->codec->encodeConnect($this->options, $this->serverInfo->nonce, $urlCredentials))->await();
        $this->transport->write($this->codec->encodePing())->await();

        $this->awaitInitialPong();
        // Reset parser state after handshake to avoid carrying partial bootstrap chunks, and couple the
        // inbound frame bound to the server's negotiated max_payload so a legitimately large message
        // (on a server with a raised max_payload) is receivable instead of being rejected as an
        // oversized frame - which would throw and force a reconnect (#94).
        $this->parser = new ProtocolParser($this->inboundFrameBound());
        // Seed the discovered-servers set from the initial INFO (without emitting a discovery event -
        // that is reserved for subsequent async INFO changes), so failover can use the cluster peers.
        if ($this->serverInfo !== null && $this->serverInfo->connectUrls !== []) {
            $this->knownConnectUrls = $this->serverInfo->connectUrls;
        }
        $this->state = ConnectionState::Open;
        $this->startPingTimer();
    }

    /**
     * Returns the next server endpoint, round-robin over the configured servers plus any cluster peers
     * discovered from INFO `connect_urls`.
     */
    private function nextServer(): string
    {
        $pool = $this->serverPool();
        if ($pool === []) {
            return NatsOptions::DEFAULT_SERVER;
        }

        $index = $this->serverCursor % count($pool);
        $this->serverCursor++;

        return $pool[$index];
    }

    /**
     * The dial pool: configured servers followed by discovered cluster peers (deduped, normalized to a
     * `nats://` scheme when the advertised entry is a bare host:port).
     *
     * @return list<string>
     */
    private function serverPool(): array
    {
        $pool = $this->orderedServers;
        foreach ($this->knownConnectUrls as $url) {
            $normalized = str_contains($url, '://') ? $url : 'nats://' . $url;
            if (!in_array($normalized, $pool, true)) {
                $pool[] = $normalized;
            }
        }

        return $pool;
    }

    /**
     * Reconnects using retry policy and restores subscription state.
     *
     * Concurrent callers are coalesced: while one reconnect is running, others (e.g. a ping-timer
     * callback resuming after its write while the read path already began recovering) await the same
     * attempt and share its outcome, rather than racing on the parser, state, and socket.
     */
    private function recoverConnection(): void
    {
        // The user asked to close (disconnect/drain): never start or join a reconnect that would
        // re-open the connection (#84).
        if ($this->closing) {
            return;
        }

        $inProgress = $this->reconnecting;
        if ($inProgress !== null) {
            $inProgress->getFuture()->await();

            return;
        }

        $deferred = new DeferredFuture();
        // Suppress unhandled-error reporting for the no-waiter case; awaiting callers still receive
        // the error from await().
        $deferred->getFuture()->ignore();
        $this->reconnecting = $deferred;

        try {
            $this->performRecovery();
            $deferred->complete();
        } catch (\Throwable $e) {
            $deferred->error($e);

            throw $e;
        } finally {
            $this->reconnecting = null;
        }

        // Deliver any messages buffered during subscription replay now that recovery has finished and
        // we are OUT of the critical section: `reconnecting` is cleared, so a callback that publishes
        // and hits a write failure starts a fresh recovery instead of deadlocking on the in-progress
        // one, and the per-sid dispatch guard keeps it non-reentrant. (Only reached on success; the
        // catch above rethrows on failure.)
        $this->drainAllPending();
    }

    /**
     * Retries the initial connect (the first attempt has already failed) up to maxReconnectAttempts
     * with backoff, WITHOUT enabling ongoing reconnect (#56). Returns true on success. An auth failure
     * aborts immediately.
     */
    private function retryInitialConnect(): bool
    {
        $maxAttempts = max(1, $this->options->maxReconnectAttempts);

        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            delay($this->backoffDelayMs($attempt) / 1000);

            try {
                $this->transport->close()->await();
            } catch (\Throwable) {
                // Ignore close failures between attempts.
            }

            try {
                $this->connectOnce();
                $this->emitEvent(ConnectionEvent::Connected);

                return true;
            } catch (AuthenticationException $e) {
                $this->state = ConnectionState::Closed;
                $this->closeTransportBestEffort();
                $this->emitEvent(ConnectionEvent::Closed, $e);

                throw $e;
            } catch (\Throwable) {
                // Keep retrying until attempts are exhausted.
            }
        }

        // Attempts exhausted: the last attempt's socket may still be open (connectOnce() dials
        // before the handshake can fail) - release it before reporting failure (#133).
        $this->closeTransportBestEffort();

        return false;
    }

    /**
     * Performs the actual reconnect + subscription replay, serialized by {@see recoverConnection()}.
     */
    private function performRecovery(): void
    {
        // User close-intent set before/while recovery began: do not re-open (#84).
        if ($this->closing) {
            $this->state = ConnectionState::Closed;

            return;
        }

        if (!$this->options->reconnectEnabled) {
            $this->state = ConnectionState::Closed;
            // Terminal close: same invariant as the exhaustion/auth paths - release the socket and
            // runtime state so a later manual connect() starts clean (#127/#133, missed here: #146).
            $this->closeTransportBestEffort();
            $this->releaseRuntimeState();
            $this->emitEvent(ConnectionEvent::Closed);
            throw new ConnectionException('Reconnect is disabled');
        }

        // Leave Open before the first await: the state check is what routes concurrent publishes
        // into the reconnect buffer, so staying Open across the transport close would let them
        // race a socket that is about to disappear (#124).
        $this->state = ConnectionState::Connecting;

        $this->cancelPingTimer();
        $this->emitEvent(ConnectionEvent::Disconnected);

        $maxAttempts = max(1, $this->options->maxReconnectAttempts);
        $lastError = null;

        for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
            // disconnect()/drain() may have been called between attempts: stop reopening (#84).
            if ($this->closing) {
                $this->state = ConnectionState::Closed;

                return;
            }

            try {
                $this->transport->close()->await();
            } catch (\Throwable) {
                // Ignore close failures during reconnect transitions.
            }

            try {
                $this->connectOnce();

                // The user closed while this attempt was connecting: tear the new socket back down
                // instead of flipping to Open/Reconnected (#84).
                if ($this->closing) {
                    try {
                        $this->transport->close()->await();
                    } catch (\Throwable) {
                        // Already gone; the Closed state below is what matters.
                    }
                    $this->state = ConnectionState::Closed;

                    return;
                }

                $this->resubscribeAll();
                $this->reconnectCount++;
                $this->flushReconnectBuffer();
                $this->emitEvent(ConnectionEvent::Reconnected);

                return;
            } catch (AuthenticationException $e) {
                // Credentials will not become valid by retrying: stop the reconnect loop immediately
                // rather than hammering the server until attempts are exhausted (#46).
                $this->state = ConnectionState::Closed;
                $this->closeTransportBestEffort();
                $this->releaseRuntimeState();
                $this->emitError($e);
                $this->emitEvent(ConnectionEvent::Closed, $e);

                throw $e;
            } catch (\Throwable $e) {
                $lastError = $e;
                $delayMs = $this->backoffDelayMs($attempt);
                $this->logger->warning(
                    sprintf('NATS reconnect attempt %d/%d failed; retrying in %dms', $attempt, $maxAttempts, $delayMs),
                    ['attempt' => $attempt, 'maxAttempts' => $maxAttempts, 'delayMs' => $delayMs, 'exception' => $e],
                );
                delay($delayMs / 1000);
            }
        }

        $this->state = ConnectionState::Closed;

        // Publishes buffered during the outage already reported success to their callers;
        // abandoning them must be loud, and the buffer must not survive into a later manual
        // connect() where a future recovery would replay frames from this dead epoch (#123).
        if ($this->reconnectBuffer !== '') {
            $abandonedBytes = strlen($this->reconnectBuffer);
            $this->reconnectBuffer = '';
            $this->emitError(new NatsException(
                sprintf('Reconnect exhausted: %d bytes of buffered publishes were discarded', $abandonedBytes),
            ));
        }

        // The last attempt's socket may still be open (each attempt closes only at its START, and
        // connectOnce() dials before the handshake can fail) - release it now (#133).
        $this->closeTransportBestEffort();

        // The connection is terminally closed: subscriptions do not survive it. Releasing here
        // keeps handler closures/payloads from outliving the connection and stops a later manual
        // connect() + recovery from resurrecting this epoch's sids as ghost subscriptions (#127).
        $this->releaseRuntimeState();

        $this->emitEvent(ConnectionEvent::Closed, $lastError);
        throw new ConnectionException(
            'Reconnect attempts exhausted',
            0,
            $lastError,
        );
    }

    /**
     * Writes any publishes buffered while the connection was down, then clears the buffer (#49).
     */
    private function flushReconnectBuffer(): void
    {
        if ($this->reconnectBuffer === '') {
            return;
        }

        // Clear only after the write succeeds: publish() already reported success for these
        // frames when they were buffered, so a flush failure must leave them in place for the
        // next reconnect attempt to replay - clearing first silently lost every publish
        // accepted during the reconnect window (#123). A partially transmitted flush can
        // duplicate frames on the retry; duplication beats loss (nats.go pending-buffer
        // semantics).
        $this->transport->write($this->reconnectBuffer)->await();
        $this->reconnectBuffer = '';
    }

    /**
     * Replays existing SUB registrations after a reconnect.
     *
     * The whole replay is coalesced into ONE transport write followed by ONE bounded drain:
     * a per-sid awaited write plus a ~5ms drain poll (the server sends nothing after a
     * successful SUB with verbose off) made reconnect latency scale at ~5ms x subscription
     * count inside the reconnect critical section, where publishes buffer and nothing
     * dispatches (#137). The byte stream is identical to the per-sid version - each SUB is
     * immediately followed by its UNSUB re-arm, in registration order.
     */
    private function resubscribeAll(): void
    {
        $buffer = '';

        foreach ($this->subscriptionMeta as $sid => $meta) {
            $max = $this->autoUnsubMax[$sid] ?? null;
            $remaining = $max === null ? null : $max - ($this->receivedCounts[$sid] ?? 0);

            if ($remaining !== null && $remaining <= 0) {
                // The auto-unsubscribe max was already reached (all counted at receive, so slow-consumer
                // drops are included); nothing remains to deliver on this sid, and re-SUBbing would
                // over-deliver live messages past the intended max. Drop it instead of replaying (#112).
                $this->dropSubscriptionState($sid);

                continue;
            }

            $buffer .= $this->codec->encodeSubscribe($meta['subject'], $sid, $meta['queue']);

            if ($remaining !== null) {
                // A fresh SUB resets the server's per-sid count, so re-arm auto-unsubscribe with the
                // REMAINING allowance; the cumulative local counter still ends delivery at the
                // original max (#112). Mirrors nats.go resendSubscriptions.
                $buffer .= $this->codec->encodeUnsubscribe($sid, $remaining);
            }
        }

        // Nothing to replay (no subscriptions, or all were dropped above): no write, no drain.
        if ($buffer === '') {
            return;
        }

        // A single large buffer is fine here: write() runs inline and suspends on backpressure
        // (#136) - the same path flushReconnectBuffer() takes.
        $this->transport->write($buffer)->await();

        // One bounded poll for the whole replay so prompt -ERR responses (e.g. permission
        // violations) still abort this reconnect attempt instead of leaving silently rejected
        // subscriptions (#137 keeps the detection, drops the per-sid latency floor).
        $this->drainImmediateServerFrames();
    }

    /**
     * Polls for any immediate frames emitted by the server after a protocol write.
     *
     * This is primarily used during reconnect subscription replay so prompt `-ERR`
     * responses do not leave the connection open with silently rejected subscriptions.
     *
     * It deliberately does NOT drain message deliveries to user callbacks: this runs inside the
     * reconnect critical section (with state already Open and `reconnecting` set), and a callback that
     * publishes and hits a write failure would re-enter recoverConnection(), await the in-progress
     * reconnect deferred, and deadlock. Message frames captured here are buffered via handleFrame() and
     * delivered by the normal processIncoming()/heartbeat drain once recovery has completed.
     */
    private function drainImmediateServerFrames(): void
    {
        $maxPolls = 16;
        $pollTimeoutMs = 5;

        for ($poll = 0; $poll < $maxPolls; $poll++) {
            try {
                $chunk = $this->transport->readLine(new TimeoutCancellation($pollTimeoutMs / 1000))->await();
            } catch (CancelledException) {
                return;
            }

            if ($chunk === '') {
                return;
            }

            // Per-frame containment (#128): a prompt -ERR still aborts this reconnect attempt (the
            // first failure rethrows after the loop), but sibling MSG frames from the same chunk
            // are enqueued first instead of being discarded. handleFrame() ignores +OK frames.
            $this->dispatchFrames($this->parser->push($chunk));
        }
    }

    /**
     * Computes reconnect delay with exponential backoff, capped at reconnectMaxDelayMs.
     */
    private function backoffDelayMs(int $attempt): int
    {
        $base = max(1, $this->options->reconnectDelayMs);
        $exponential = (int) ($base * (2 ** ($attempt - 1)));
        $capped = min($exponential, max($base, $this->options->reconnectMaxDelayMs));
        $jitter = $this->options->reconnectJitterMs > 0 ? random_int(0, $this->options->reconnectJitterMs) : 0;

        return $capped + $jitter;
    }

    /**
     * Waits for initial PONG while handling expected intermediary control lines.
     */
    private function awaitInitialPong(): void
    {
        $deadline = $this->handshakeDeadline();
        $remainingPolls = $this->handshakePollBudget();

        while ($remainingPolls-- > 0 && $this->monotonicSeconds() < $deadline) {
            $chunk = $this->readHandshakeChunk($deadline);
            if ($chunk === null || $chunk === '') {
                continue;
            }

            $frames = $this->parser->push($chunk);

            foreach ($frames as $frame) {
                if ($frame->type === ProtocolFrameType::Ok) {
                    continue;
                }

                if ($frame->type === ProtocolFrameType::Ping) {
                    $this->transport->write($this->codec->encodePong())->await();
                    continue;
                }

                if ($frame->type === ProtocolFrameType::Info && $frame->infoPayload !== null) {
                    $this->serverInfo = $this->decodeServerInfoPayload($frame->infoPayload);

                    continue;
                }

                if ($frame->type === ProtocolFrameType::Pong) {
                    return;
                }

                if ($frame->type === ProtocolFrameType::Err) {
                    throw $this->connectErrorFromFrame($frame->error);
                }
            }
        }

        throw new ConnectionException('Expected PONG after CONNECT');
    }

    /**
     * Waits for and parses the initial INFO frame sent by the server.
     */
    private function awaitServerInfo(): ServerInfo
    {
        $deadline = $this->handshakeDeadline();
        $remainingPolls = $this->handshakePollBudget();

        while ($remainingPolls-- > 0 && $this->monotonicSeconds() < $deadline) {
            $chunk = $this->readHandshakeChunk($deadline);
            if ($chunk === null || $chunk === '') {
                continue;
            }

            $frames = $this->parser->push($chunk);

            foreach ($frames as $frame) {
                if ($frame->type === ProtocolFrameType::Info && $frame->infoPayload !== null) {
                    /** @var array<string,mixed> $data */
                    $data = json_decode($frame->infoPayload, true, 512, JSON_THROW_ON_ERROR);

                    return ServerInfo::fromInfoPayload($data);
                }

                if ($frame->type === ProtocolFrameType::Ping) {
                    $this->transport->write($this->codec->encodePong())->await();

                    continue;
                }

                if ($frame->type === ProtocolFrameType::Err) {
                    throw $this->connectErrorFromFrame($frame->error);
                }
            }
        }

        throw new ConnectionException('Expected INFO during connect');
    }

    /**
     * Returns the absolute handshake deadline based on connect timeout.
     */
    private function handshakeDeadline(): float
    {
        $timeoutSeconds = max(0.001, $this->options->connectTimeoutMs / 1000);

        return $this->monotonicSeconds() + $timeoutSeconds;
    }

    /**
     * Bounds handshake polling for transports that may return empty chunks immediately.
     */
    private function handshakePollBudget(): int
    {
        return max(16, (int) ceil(max(1, $this->options->connectTimeoutMs) / 10));
    }

    /**
     * Reads the next handshake chunk within the remaining timeout budget.
     */
    private function readHandshakeChunk(float $deadline): ?string
    {
        $remainingMs = (int) ceil(($deadline - $this->monotonicSeconds()) * 1000);
        if ($remainingMs <= 0) {
            return null;
        }

        $sliceMs = min($remainingMs, 50);

        try {
            return $this->transport->readLine(new TimeoutCancellation(max(1, $sliceMs) / 1000))->await();
        } catch (CancelledException) {
            return null;
        }
    }

    /**
     * Handles non-message frames immediately and queues message frames for delivery.
     */
    private function handleFrame(ProtocolFrame $frame): void
    {
        if ($frame->type === ProtocolFrameType::Ping) {
            $this->transport->write($this->codec->encodePong())->await();

            return;
        }

        if ($frame->type === ProtocolFrameType::Pong) {
            $this->outstandingPings = 0;
            $this->drainFlushPending = false;
            $this->flushPending = false;

            return;
        }

        if ($frame->type === ProtocolFrameType::Info && $frame->infoPayload !== null) {
            try {
                $this->serverInfo = $this->decodeServerInfoPayload($frame->infoPayload);
            } catch (\JsonException $e) {
                // A malformed async INFO (corruption in flight, or a non-conformant server push) must not
                // throw out of the shared read loop and abort delivery of the MSG frames parsed from the
                // same chunk - mirrors the #97 dispatch-containment principle. Skip the bad update and keep
                // the last known serverInfo; surface it to the error listener. (Handshake INFO is decoded
                // separately in awaitServerInfo() and still fails the connect on bad JSON.)
                $this->emitError(new NatsException('Discarding malformed async INFO frame: ' . $e->getMessage()));

                return;
            }
            $this->handleServerInfoUpdate();

            return;
        }

        if ($frame->type === ProtocolFrameType::Err) {
            $error = $frame->error ?? 'unknown';
            if ($this->isRecoverableServerError($error)) {
                // Non-fatal server error (e.g. a per-subscription permissions violation): surface it
                // to the async error listener instead of tearing down the connection.
                $this->emitError(new NatsException('Server sent recoverable error frame: ' . $error));

                return;
            }

            throw new ConnectionException('Server sent error frame: ' . $error);
        }

        if ($frame->type === ProtocolFrameType::Msg || $frame->type === ProtocolFrameType::HMsg) {
            $sid = $frame->sid;
            if ($sid === null || !isset($this->subscriptions[$sid])) {
                return;
            }

            [$rawHeaders, $payload] = $this->extractHeadersAndPayload($frame);
            $message = new NatsMessage(
                subject: $frame->subject ?? '',
                sid: $sid,
                replyTo: $frame->replyTo,
                payload: $payload,
                rawHeaders: $rawHeaders,
                responder: $this->messageResponder,
            );

            $this->inMsgs++;
            $this->inBytes += strlen($payload);
            $this->enqueueMessage($sid, $message);
        }
    }

    /**
     * Splits HMSG combined data into raw headers and payload body bytes.
     *
     * @return array{0: ?string, 1: string}
     */
    private function extractHeadersAndPayload(ProtocolFrame $frame): array
    {
        $payload = $frame->payload ?? '';

        if ($frame->type !== ProtocolFrameType::HMsg || $frame->headerBytes === null || $frame->headerBytes <= 0) {
            return [null, $payload];
        }

        if ($frame->headerBytes > strlen($payload)) {
            throw new ProtocolException('Malformed HMSG frame: header bytes exceed payload length');
        }

        // Header bytes include only the wire header block; remainder is message body.
        $headerBytes = $frame->headerBytes;
        $headers = substr($payload, 0, $headerBytes);
        $body = substr($payload, $headerBytes);

        return [$headers, $body];
    }

    /**
     * Adds a message to a subscription queue and applies slow-consumer policy when full.
     */
    private function enqueueMessage(int $sid, NatsMessage $message): void
    {
        // Count the message toward auto-unsubscribe accounting at intake - before any slow-consumer
        // drop below - so a dropped message still advances toward the max exactly as it does on the
        // server (which counts messages it SENT, not messages the client managed to deliver) (#112).
        $this->receivedCounts[$sid] = ($this->receivedCounts[$sid] ?? 0) + 1;

        if (!isset($this->pendingMessages[$sid])) {
            // Defensive only: subscribe() creates the queue and it persists (empty between drains,
            // #139) until dropSubscriptionState() removes it, so a routable sid always has one.
            $this->pendingMessages[$sid] = new SplQueue();
        }

        $queue = $this->pendingMessages[$sid];
        $limit = max(1, $this->options->maxPendingMessagesPerSubscription);

        if ($queue->count() >= $limit) {
            if ($this->options->slowConsumerPolicy === SlowConsumerPolicy::DropOldest) {
                $queue->dequeue();
                $this->emitError(new NatsException('Slow consumer on sid ' . $sid . ': dropped oldest message'), 'debug');
            } elseif ($this->options->slowConsumerPolicy === SlowConsumerPolicy::DropNewest) {
                $this->emitError(new NatsException('Slow consumer on sid ' . $sid . ': dropped newest message'), 'debug');

                return;
            } else {
                $overflow = new ConnectionException('Subscription queue overflow for sid ' . $sid);
                $this->emitError($overflow);

                throw $overflow;
            }
        }

        $queue->enqueue($message);
    }

    /**
     * Drains all queued subscription messages in SID order.
     */
    private function drainAllPending(): void
    {
        foreach (array_keys($this->pendingMessages) as $sid) {
            $this->drainPendingForSid($sid);
        }
    }

    /**
     * Computes the maximum inbound MSG/HMSG frame size to accept, derived from the server's negotiated
     * `max_payload`. Inbound payloads never exceed `max_payload`; a margin is added for the HMSG header
     * block, and the bound never drops below {@see ProtocolParser::DEFAULT_MAX_FRAME_SIZE} so small-
     * `max_payload` servers keep the historical headroom. When `max_payload` is not advertised, a
     * generous bound is used rather than the conservative default. (#94)
     */
    private function inboundFrameBound(): int
    {
        $serverInfo = $this->serverInfo;
        if ($serverInfo === null || $serverInfo->maxPayload <= 0) {
            return 64 * 1024 * 1024; // 64 MiB
        }

        return max(ProtocolParser::DEFAULT_MAX_FRAME_SIZE, $serverInfo->maxPayload + 1024 * 1024);
    }

    /**
     * Validates that payload size does not exceed server max_payload.
     */
    private function enforceMaxPayload(int $totalBytes): void
    {
        if ($this->serverInfo === null) {
            return;
        }

        $max = $this->serverInfo->maxPayload;
        if ($max > 0 && $totalBytes > $max) {
            throw new ProtocolException(sprintf(
                'Payload size %d exceeds server max_payload of %d',
                $totalBytes,
                $max,
            ));
        }
    }

    /**
     * Parses an INFO payload JSON fragment into ServerInfo.
     */
    private function decodeServerInfoPayload(string $infoPayload): ServerInfo
    {
        /** @var array<string,mixed> $data */
        $data = json_decode($infoPayload, true, 512, JSON_THROW_ON_ERROR);

        return ServerInfo::fromInfoPayload($data);
    }

    /**
     * Builds the exception for a server -ERR received during the connect handshake, classifying
     * authorization/authentication failures as {@see AuthenticationException} so the reconnect loop
     * does not retry them (#46).
     */
    private function connectErrorFromFrame(?string $error): ConnectionException
    {
        $error ??= 'unknown';
        $normalized = strtolower($error);

        if (str_contains($normalized, 'authorization') || str_contains($normalized, 'authentication')) {
            return new AuthenticationException('Server rejected authentication during connect: ' . $error);
        }

        return new ConnectionException('Server error during connect: ' . $error);
    }

    /**
     * Returns true when a server -ERR is documented as connection-nonfatal.
     */
    private function isRecoverableServerError(string $error): bool
    {
        $normalized = strtolower(trim($error, " '\t\r\n\0\x0B"));

        if ($normalized === 'invalid subject') {
            return true;
        }

        return str_starts_with($normalized, 'permissions violation for subscription to ')
            || str_starts_with($normalized, 'permissions violation for publish to ');
    }

    /**
     * Starts the periodic ping timer based on configured interval.
     */
    private function startPingTimer(): void
    {
        $this->cancelPingTimer();
        $this->outstandingPings = 0;

        $intervalSeconds = $this->options->pingIntervalSeconds;
        if ($intervalSeconds <= 0) {
            return;
        }

        // The repeat closure must not bind $this strongly: Revolt's driver holds it until cancel,
        // and none of the cancel paths fire for a healthy connection the application simply stops
        // referencing - the timer would root the whole connection graph (open socket, handler
        // closures, buffers) in the event loop forever, PINGing and even delivering messages to
        // abandoned handlers (#126).
        $weakSelf = \WeakReference::create($this);
        $this->pingTimerId = EventLoop::repeat($intervalSeconds, static function (string $timerId) use ($weakSelf): void {
            $self = $weakSelf->get();
            if ($self === null) {
                EventLoop::cancel($timerId);

                return;
            }

            $self->pingTimerTick();
        });
    }

    /**
     * One heartbeat tick: verify the liveness budget, send PING, and consume the PONG.
     */
    private function pingTimerTick(): void
    {
        if ($this->state !== ConnectionState::Open) {
            $this->cancelPingTimer();

            return;
        }

        $this->outstandingPings++;

        if ($this->outstandingPings > $this->options->maxPingsOut) {
            $this->cancelPingTimer();

            try {
                $this->recoverConnection();
            } catch (\Throwable) {
                $this->state = ConnectionState::Closed;
            }

            return;
        }

        try {
            $this->transport->write($this->codec->encodePing())->await();
        } catch (\Throwable) {
            $this->cancelPingTimer();

            try {
                $this->recoverConnection();
            } catch (\Throwable) {
                $this->state = ConnectionState::Closed;
            }

            return;
        }

        // Consume the server PONG ourselves so liveness detection does not depend on the
        // application actively calling processIncoming(). If a user read is already running,
        // it will consume the PONG instead and reset the counter.
        $this->consumeHeartbeatResponse();
    }

    /**
     * Safety net for clients abandoned without disconnect()/drain(): stop the heartbeat and close
     * the socket best-effort. Only reachable because the ping timer holds $this weakly (#126);
     * transport teardown is deferred via EventLoop::queue because spawning fibers inside a
     * destructor (possibly during GC) is unsafe.
     */
    public function __destruct()
    {
        $this->cancelPingTimer();

        if ($this->state !== ConnectionState::Open) {
            return;
        }

        $transport = $this->transport;
        EventLoop::queue(static function () use ($transport): void {
            try {
                $transport->close()->await();
            } catch (\Throwable) {
                // Best-effort teardown of an abandoned connection.
            }
        });
    }

    /**
     * Performs a short, bounded read to consume the heartbeat PONG (and any other control frames)
     * without colliding with an in-flight user read. Any message frames captured during this read
     * are delivered immediately via drainAllPending(); control frames (PONG/PING/INFO) are handled
     * inline.
     */
    private function consumeHeartbeatResponse(): void
    {
        if ($this->readInProgress) {
            return;
        }

        $timeoutSeconds = min(2.0, max(0.05, (float) $this->options->pingIntervalSeconds));

        $this->readInProgress = true;

        $closed = false;
        try {
            $chunk = $this->transport->readLine(new TimeoutCancellation($timeoutSeconds))->await();
        } catch (TransportClosedException) {
            // The peer closed the socket during the heartbeat read. Recover, but only after the
            // finally clears readInProgress (recoverConnection -> connectOnce reads the socket).
            $closed = true;
            $chunk = '';
        } catch (\Throwable) {
            // No PONG within the window (or a transient read error); leave escalation to the next
            // tick or to the application's own processIncoming() loop.
            return;
        } finally {
            $this->readInProgress = false;
            $this->signalReadSlotFree();
        }

        if ($closed) {
            try {
                $this->recoverConnection();
            } catch (\Throwable) {
                $this->state = ConnectionState::Closed;
            }

            return;
        }

        if ($chunk === '') {
            return;
        }

        $dispatchError = null;
        try {
            $frames = $this->parser->push($chunk);

            try {
                $this->dispatchFrames($frames);
            } catch (\Throwable $e) {
                // Contained per frame (#128): the sibling MSG frames are already enqueued below.
                $dispatchError = $e;
            }

            // The PONG handled above (dispatchFrames) resets the outstanding-ping counter; do not
            // reset on any other frame, so an unresponsive server still trips maxPingsOut.

            // Deliver any message frames captured during the heartbeat read instead of leaving
            // them buffered until the next processIncoming(), mirroring processIncoming().
            $this->drainAllPending();
        } catch (\Throwable) {
            // A parse failure or a handler error during the drain; leave escalation to the next
            // user read / tick rather than throwing out of the event-loop timer.
        }

        if ($dispatchError !== null) {
            // Previously a fatal frame (e.g. a server -ERR) observed during the heartbeat read was
            // swallowed whole. Surface it through the error listener (#128); escalation still
            // belongs to the next user read / tick, not to the event-loop timer.
            $this->emitError($dispatchError);
        }
    }

    /**
     * Cancels the active ping timer if running.
     */
    private function cancelPingTimer(): void
    {
        if ($this->pingTimerId !== null) {
            EventLoop::cancel($this->pingTimerId);
            $this->pingTimerId = null;
        }
    }

    /**
     * Invokes the configured connection lifecycle listener, swallowing any exception it raises so a
     * faulty handler cannot wedge the connection runtime.
     */
    private function emitEvent(ConnectionEvent $event, ?\Throwable $error = null): void
    {
        // Log every lifecycle transition regardless of whether a connection listener is configured (#69).
        if ($error !== null) {
            $this->logger->warning('NATS connection ' . $event->name, ['event' => $event->name, 'exception' => $error]);
        } else {
            $this->logger->info('NATS connection ' . $event->name, ['event' => $event->name]);
        }

        $listener = $this->options->connectionListener;
        if ($listener === null) {
            return;
        }

        try {
            $listener($event, $error);
        } catch (\Throwable) {
            // A throwing listener must never break connection handling.
        }
    }

    /**
     * Invokes the configured asynchronous-error listener, swallowing any exception it raises.
     *
     * @internal Public only so client-side buffers (SubscriptionQueue slow-consumer drops, #134)
     *           can report through the same listener/logger; not part of the supported API.
     */
    public function emitError(\Throwable $error, string $logLevel = 'error'): void
    {
        // Routine, high-frequency conditions (slow-consumer drops) log at debug so they cannot flood
        // error logs on a per-message hot path; genuine errors stay at error level. The error listener
        // is always notified regardless of level (callers opted in and can throttle themselves).
        $this->logger->log($logLevel, 'NATS connection error: ' . $error->getMessage(), ['exception' => $error]);

        $listener = $this->options->errorListener;
        if ($listener === null) {
            return;
        }

        try {
            $listener($error);
        } catch (\Throwable) {
            // A throwing listener must never break connection handling.
        }
    }

    /**
     * Reacts to an async INFO update by emitting discovery / lame-duck lifecycle events when the
     * advertised cluster topology or shutdown state changes.
     */
    private function handleServerInfoUpdate(): void
    {
        $info = $this->serverInfo;
        if ($info === null) {
            return;
        }

        if ($info->connectUrls !== [] && $info->connectUrls !== $this->knownConnectUrls) {
            // Update the discovery pool first so a lame-duck failover can dial a freshly-advertised peer.
            $this->knownConnectUrls = $info->connectUrls;
            $this->emitEvent(ConnectionEvent::DiscoveredServers);
        }

        if ($info->lameDuckMode && !$this->lameDuckAnnounced) {
            $this->lameDuckAnnounced = true;
            $this->emitEvent(ConnectionEvent::LameDuck);

            // The server is draining and will close this connection; proactively fail over to another
            // pool member now (rather than waiting for the eventual EOF) when reconnect is enabled and
            // more than one endpoint is available to move to (#47).
            if ($this->options->reconnectEnabled && count($this->serverPool()) > 1) {
                try {
                    $this->recoverConnection();
                } catch (\Throwable $e) {
                    $this->emitError($e);
                }
            }
        }
    }

    /**
     * Validates a NATS subject string against protocol rules.
     *
     * @param bool $allowWildcards Whether * and > tokens are permitted (subscribe only).
     */
    private function validateSubject(string $subject, bool $allowWildcards = false): void
    {
        if ($subject === '') {
            throw new ProtocolException('Subject must not be empty');
        }

        if (preg_match('/[\s\r\n]/', $subject)) {
            throw new ProtocolException('Subject must not contain whitespace');
        }

        $tokens = explode('.', $subject);
        foreach ($tokens as $i => $token) {
            if ($token === '') {
                throw new ProtocolException('Subject must not contain empty tokens');
            }

            if ($token === '*' || $token === '>') {
                if (!$allowWildcards) {
                    throw new ProtocolException('Wildcards are not allowed in publish subjects');
                }

                // ">" must be the last token.
                if ($token === '>' && $i !== count($tokens) - 1) {
                    throw new ProtocolException('Wildcard ">" must be the last token');
                }

                continue;
            }

            if (str_contains($token, '*') || str_contains($token, '>')) {
                throw new ProtocolException('Wildcards must occupy an entire token');
            }
        }
    }

    /**
     * Validates a publish-path subject, memoizing subjects that already passed (#136).
     *
     * Validation is pure (protocol rules on the string only), so a subject that passed once never
     * needs re-scanning - repeat publishes and request targets skip the regex + token walk. Must
     * NOT be used for per-request reply inboxes (unique per request - pure cache pollution) nor
     * for subscribe subjects (those validate with allowWildcards, a laxer rule set that must not
     * leak into publish validation). Unique $JS.ACK reply subjects do flow through here as ack
     * publish subjects and churn the memo; the full reset at the cap bounds that churn.
     */
    private function validateSubjectCached(string $subject): void
    {
        if (isset($this->validatedSubjects[$subject])) {
            return;
        }

        $this->validateSubject($subject);

        if (count($this->validatedSubjects) >= self::VALIDATED_SUBJECTS_MAX) {
            $this->validatedSubjects = [];
        }

        $this->validatedSubjects[$subject] = true;
    }

    /**
     * Validates a NATS queue group name against protocol rules.
     *
     * Queue groups are interpolated into the SUB control line, so they must not
     * be empty or contain whitespace/CR/LF that could break or inject wire frames.
     */
    private function validateQueueGroup(string $queue): void
    {
        if ($queue === '') {
            throw new ProtocolException('Queue group must not be empty');
        }

        if (preg_match('/[\s\r\n]/', $queue)) {
            throw new ProtocolException('Queue group must not contain whitespace');
        }
    }

    /**
     * Delivers buffered messages to a single subscription callback in FIFO order.
     */
    private function drainPendingForSid(int $sid): void
    {
        $queue = $this->pendingMessages[$sid] ?? null;
        if ($queue === null) {
            return;
        }

        if (!isset($this->subscriptions[$sid])) {
            // The subscription is gone; its backlog is undeliverable. Drop it instead of retaining an
            // entry that drainAllPending() would re-scan on every chunk.
            unset($this->pendingMessages[$sid]);

            return;
        }

        if ($queue->isEmpty()) {
            // Nothing buffered. The queue persists empty until the subscription is dropped (#139) -
            // the previous unset-on-empty meant one SplQueue alloc/free per delivered message in the
            // promptly-drained common case - so this cheap check is the whole per-chunk scan cost
            // for an idle subscription.
            return;
        }

        if (isset($this->dispatchingSids[$sid])) {
            // Already delivering this sid further up the stack (a handler awaited and suspended). Do
            // not re-enter: the suspended loop resumes and drains whatever we enqueued meanwhile, so
            // ordering holds and a handler is never invoked on top of itself.
            return;
        }

        $this->dispatchingSids[$sid] = true;

        try {
            while (!$queue->isEmpty()) {
                if (!array_key_exists($sid, $this->subscriptions)) {
                    break;
                }

                /** @var NatsMessage $message */
                $message = $queue->dequeue();
                $this->deliveredCounts[$sid] = ($this->deliveredCounts[$sid] ?? 0) + 1;
                $this->subscriptions[$sid]($message);

                $max = $this->autoUnsubMax[$sid] ?? null;
                if ($max !== null && $this->deliveredCounts[$sid] >= $max) {
                    // Cap handler delivery at the auto-unsubscribe max: stop and drop now rather than
                    // deliver a batched-in frame past the max (the server-side UNSUB may not have taken
                    // effect yet, and a replayed read after reconnect can carry an extra frame) (#112).
                    $this->dropSubscriptionState($sid);

                    break;
                }
            }
        } finally {
            unset($this->dispatchingSids[$sid]);

            // Run in finally so a handler that throws mid-drain still triggers the terminal cleanup
            // rather than stranding the subscription (#112). completeAutoUnsub handles the
            // slow-consumer case where delivered stays below max (dropped messages) but the
            // server-side max was still reached: it drops on received>=max once the backlog is
            // drained. The drained queue itself is deliberately kept (empty) for reuse - see the
            // early return above (#139); dropSubscriptionState() removes it with the subscription.
            $this->completeAutoUnsubIfSatisfied($sid);
        }
    }

    /**
     * Completes an armed auto-unsubscribe once the server-side max has been received and the local
     * backlog is fully drained: drops the subscription so no reconnect re-arms it and no further frame
     * is dispatched. Counting at receive (see {@see $receivedCounts}) means this fires even when the
     * slow-consumer policy dropped some of the counted messages, so the subscription cannot leak (#112).
     */
    private function completeAutoUnsubIfSatisfied(int $sid): void
    {
        $max = $this->autoUnsubMax[$sid] ?? null;
        if ($max === null || ($this->receivedCounts[$sid] ?? 0) < $max) {
            return;
        }

        // Wait for the backlog to drain before dropping, so queued-but-undelivered messages that the
        // server already counted toward the max still reach the handler.
        if (isset($this->pendingMessages[$sid]) && !$this->pendingMessages[$sid]->isEmpty()) {
            return;
        }

        $this->dropSubscriptionState($sid);
    }

    private function cleanupRequestSubscription(int $sid): void
    {
        // Clean up based on the subscription itself, not on a pending message queue: the queue is
        // delivery bookkeeping that lives and dies with the subscription state (#139), and keying
        // the UNSUB on its presence would skip cleanup whenever the entry is missing.
        if (!isset($this->subscriptionMeta[$sid], $this->subscriptions[$sid])) {
            return;
        }

        if ($this->state === ConnectionState::Open) {
            try {
                $this->unsubscribe($sid)->await();

                return;
            } catch (\Throwable) {
                // Preserve the original request failure and fall back to local cleanup.
            }
        }

        $this->dropSubscriptionState($sid);
    }

    private function dropSubscriptionState(int $sid): void
    {
        unset($this->subscriptions[$sid]);
        unset($this->subscriptionMeta[$sid]);
        unset($this->pendingMessages[$sid]);
        unset($this->receivedCounts[$sid]);
        unset($this->deliveredCounts[$sid]);
        unset($this->autoUnsubMax[$sid]);
    }
}
