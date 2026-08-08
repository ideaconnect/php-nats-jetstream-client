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

    /**
     * Hard cap on {@see flushReconnectBuffer()} drain passes before it SEALS the reconnect buffer
     * (#165). Each pass writes the whole pending buffer; a fiber publishing during a write's
     * suspension re-fills the buffer, so an uncapped loop can defer the Open flip indefinitely (the
     * connection stays Connecting and subscribe/request/flush/rtt/processIncoming all throw). After
     * this many passes the flush stops admitting new buffered frames - late publishers park on
     * {@see $reconnectFlushGate} and write directly once Open - so the drain converges within one more
     * pass. Sized above a normal multi-publisher burst (which drains in one or two passes) so the seal
     * only ever engages under sustained publish pressure, leaving the common path unchanged.
     */
    private const RECONNECT_FLUSH_MAX_PASSES = 8;

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
     * Dirty set: the sids whose pending queue currently holds at least one undelivered message. Since
     * #139 keeps a subscription's SplQueue allocated (EMPTY) for the subscription's whole lifetime to
     * avoid a per-message alloc/free, iterating {@see $pendingMessages} to drain would scan O(all live
     * subscriptions) on every inbound chunk - including message-free heartbeat self-reads - re-inverting
     * the #139 win for many-idle-subscription workloads. drainAllPending() iterates this set instead, so
     * the per-chunk drain cost is O(sids-with-backlog), independent of the idle-subscription count (#162).
     *
     * Invariant: a sid is present here IFF its queue is non-empty. enqueueMessage() adds on enqueue;
     * drainPendingForSid()/dropSubscriptionState() remove once the queue empties or the sid is dropped.
     * Only the SCAN set changes - the empty queue objects are still retained (the #139 no-realloc win).
     *
     * @var array<int, true>
     */
    private array $pendingDirty = [];
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
    /**
     * Muxed request inbox base subject for the current connection epoch (e.g. "_INBOX.<24hex>"), null
     * until the first request establishes it. One long-lived wildcard subscription "<base>.*" serves
     * EVERY request()/requestMany() reply instead of a fresh inbox + SUB/UNSUB per request (#118).
     * Retained across reconnect; only releaseRuntimeState() clears it, so a fresh connect() starts a
     * new random base.
     */
    private ?string $muxBase = null;
    /**
     * sid of the long-lived "<muxBase>.*" subscription, null until established. Stable across reconnect:
     * it is a normal subscribe() registration in subscriptionMeta[$muxSid], replayed by resubscribeAll()
     * with the same sid and subject (the map key is reused, nextSid is not consulted) (#118).
     */
    private ?int $muxSid = null;
    /**
     * Latched once the mux inbox subscription "<muxBase>.*" is rejected by server permissions (#167).
     * The -ERR is async, so it lands after ensureMuxInbox() cached the (now dead) muxSid/muxBase. On
     * detection the mux state is dropped (so a reconnect does not replay the rejected SUB) and every
     * request()/requestMany() then fails fast with a clear, catchable error instead of a silent
     * permanent timeout. Reset by releaseRuntimeState() so a fresh connect() re-attempts the mux inbox.
     */
    private bool $muxRejected = false;
    /**
     * In-flight request waiters keyed by per-request suffix token. Each value routes one reply: a single
     * request completes its DeferredFuture; requestMany appends to its collector. Removed on EVERY request
     * exit - the no-late-leak mechanism that replaces per-request sid removal (#118).
     *
     * @var array<string, callable(NatsMessage):void>
     */
    private array $muxWaiters = [];
    /**
     * Strictly-increasing per-connection token counter. Guarantees pairwise-distinct request tokens
     * within an epoch by construction (never reset); the random $muxBase provides unguessability (#118).
     */
    private int $muxTokenSeq = 0;
    /**
     * Serializes concurrent first-request mux establishment so exactly one "<base>.*" SUB is written
     * even when several requests race before any completes (subscribe()->await() suspends) (#118).
     *
     * @var Future<void>|null
     */
    private ?Future $muxInboxSetup = null;
    /**
     * SIDs exempt from the per-subscription slow-consumer count bound - only the mux inbox sid. Every
     * in-flight reply shares that single sid's queue, and a dropped reply silently breaks whichever
     * request owns it with no backpressure path, so the mux queue must never drop (#118).
     *
     * @var array<int, true>
     */
    private array $unboundedSids = [];
    /**
     * Per-sid callbacks invoked when the server rejects THAT subscription with an async permissions
     * -ERR (generalizing the #167 mux latch): a long-lived reply inbox (the pull-pipeline engine's
     * "_INBOX.JS.PULL.<nuid>.*") whose SUB is rejected can never deliver a reply, so every pull
     * retires as a silent client-side deadline and the engine would spin forever with zero signal.
     * The owning layer registers a handler to fail fast instead. Cleared with the sid's state.
     *
     * @var array<int, \Closure(string):void>
     */
    private array $subscriptionRejectionHandlers = [];
    private int $outstandingPings = 0;
    private ?string $pingTimerId = null;
    /**
     * FIFO pong-correlation queue (#117, nats.go `nc.pongs` parity): every outbound PING except
     * the connect handshake's enqueues one slot, and the PONG handler completes the OLDEST slot -
     * TCP delivers PONGs in PING order, so head-of-queue is exactly the PING this PONG answers.
     * Heartbeat PINGs enqueue a slot nobody awaits purely to hold their queue position; a
     * timed-out flush leaves its slot queued because its PONG is still owed and must consume that
     * slot (not a later waiter's) when it arrives. Epoch ends - the reconnect handshake and every
     * terminal close - error out and clear all slots so none survives into a new TCP connection.
     * The handshake PING is excluded because awaitInitialPong() consumes its PONG before frames
     * ever reach handleFrame().
     *
     * @var list<DeferredFuture<null>>
     */
    private array $pongWaiters = [];
    /**
     * In-progress reconnect, so concurrent callers wait for it instead of starting a second one.
     *
     * @var ?DeferredFuture<void>
     */
    private ?DeferredFuture $reconnecting = null;
    /**
     * The fiber that owns the in-flight recovery (the only one that can complete {@see $reconnecting}).
     * Lifecycle/error listeners run synchronously inside it, so a listener-initiated connect() that
     * joined the recovery would await a deferred its own fiber must complete - a permanent deadlock.
     * connect() compares its caller's fiber against this to refuse such a join loudly (#145).
     *
     * @var ?\Fiber<mixed, mixed, mixed, mixed>
     */
    private ?\Fiber $recoveryFiber = null;
    /**
     * In-progress user connect(), so a concurrent connect() shares its outcome instead of starting
     * a parallel dial chain against the same transport and parser (#145).
     *
     * @var ?DeferredFuture<void>
     */
    private ?DeferredFuture $connecting = null;
    /**
     * The fiber running the in-flight user connect()'s performConnect(). Lifecycle/error listeners
     * fire synchronously inside that fiber, so a listener-initiated connect() would run on it too: a
     * join of the still-pending $connecting deferred (or a fresh dial after the deferred was settled)
     * awaits an outcome only the suspended emitting fiber can produce - a permanent deadlock. connect()
     * compares its caller's fiber against this to refuse such a re-entry loudly, mirroring the
     * recovery-fiber guard. Held until connect()'s finally so the refusal stays armed through the
     * terminal Closed emission, where $connecting has already been settled (#145).
     *
     * @var ?\Fiber<mixed, mixed, mixed, mixed>
     */
    private ?\Fiber $connectFiber = null;
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
     * Monotonic deadline of the drain in progress; set when drain() computes its budget, cleared in
     * its teardown. Shared with {@see writePublishFrame()}'s Draining branch and
     * {@see drainPendingForSid()} so a handler ack/reply published while drain delivers backlog is
     * bounded by the REMAINING drain budget - not a fresh full request timeout per publish - and a
     * delivery pass stops delivering (past its head message) once the budget is exhausted: K queued
     * messages whose handlers each publish could otherwise serially extend drain() to
     * ~K x requestTimeoutMs, far past its documented single ~requestTimeoutMs bound (#149).
     */
    private ?float $drainDeadline = null;
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
    /**
     * Whether the connection has ever completed a handshake and gone Open. Set once by
     * {@see markConnectionOpen()}. Lets the recovery loop tell a FAILED initial connect() (which
     * hands off to recoverConnection() and reaches the first-ever open through the reconnect path)
     * apart from a genuine reconnect: the former must emit Connected, not a spurious
     * Disconnected/Reconnected, and must not count as a reconnect (#161).
     */
    private bool $everConnected = false;
    /** Encoded publishes buffered while reconnecting (flushed on a successful reconnect); see #49. */
    private string $reconnectBuffer = '';
    /**
     * Set while {@see flushReconnectBuffer()} has SEALED the reconnect buffer after
     * {@see self::RECONNECT_FLUSH_MAX_PASSES} drain passes (#165): a publish that would otherwise keep
     * re-filling the buffer parks on this future in {@see writePublishFrame()} instead, then writes
     * directly once the connection flips Open (or fails loudly once it flips Closed). Completed and
     * cleared by {@see recoverConnection()}'s finally after the state is finalized, so a parked
     * publisher always wakes to a settled state. The buffered frames are fully flushed before the flip,
     * so the parked (later) writes always land after them - buffered-before-direct ordering (#148).
     *
     * @var ?DeferredFuture<void>
     */
    private ?DeferredFuture $reconnectFlushGate = null;
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
     * Dial-chain ownership (nats.go conn.mu parity, #145) - what is enforced, exactly: connect()
     * during an in-flight recovery joins the recovery (throwing when the recovery ends without the
     * connection Open, and refusing outright when called from inside the recovery fiber - see the
     * guards below); connect() while another connect() is dialing awaits that dial (same not-Open
     * rule); connect() during drain() throws; and {@see recoverConnection()} ignores recovery
     * requests from stale failure continuations while a user connect() is dialing. A recovery that
     * starts BETWEEN user connects (no connect() in flight) still owns its own dial loop.
     *
     * @return Future<void>
     */
    public function connect(): Future
    {
        // Captured on the CALLER's fiber: the closure below runs on its own async() fiber, so a
        // connect() issued from a listener inside the recovery fiber is only recognizable by the
        // fiber connect() was called from.
        $caller = \Fiber::getCurrent();

        return async(function () use ($caller): void {
            if ($this->state === ConnectionState::Open) {
                return;
            }

            // An in-flight recovery owns the dial loop: join it and share its outcome instead of
            // racing it with a second connectOnce() chain - the recovery closes the transport at
            // the start of every attempt, which would tear down the socket a concurrent connect()
            // just established and silently drop subscriptions created on that epoch (#145).
            // Checked before the state checks because a concurrent disconnect() may already have
            // moved state to Closed while the recovery fiber is still winding down.
            $recovery = $this->reconnecting;
            if ($recovery !== null) {
                // A connection/error listener runs synchronously inside the recovery fiber - the
                // only fiber that can complete the recovery deferred. Joining from there would
                // await that deferred forever: refuse loudly instead of deadlocking (#145).
                if ($caller !== null && $caller === $this->recoveryFiber) {
                    throw new ConnectionException(
                        'connect() cannot join the in-flight recovery from a connection/error listener: '
                        . 'the listener runs inside the recovery fiber, and awaiting the join there '
                        . 'deadlocks. Schedule supervision reconnects with Revolt\EventLoop::queue() and '
                        . 'do not await the scheduled connect from inside the listener (that only moves '
                        . 'the same dependency cycle one fiber away).',
                    );
                }

                $recovery->getFuture()->await();
                $this->throwUnlessOpenAfterJoin('Recovery was aborted before the connection opened');

                return;
            }

            // A user connect()'s performConnect() emits lifecycle events (Connected/Closed)
            // synchronously on its own fiber, and a listener that calls connect() from there runs on
            // that same fiber. Joining the still-pending $connecting deferred - or dialing afresh once
            // it was settled - would await an outcome only the suspended emitting fiber can produce: a
            // permanent deadlock. Refuse loudly, mirroring the recovery-fiber guard above (#145).
            if ($caller !== null && $caller === $this->connectFiber) {
                throw new ConnectionException(
                    'connect() cannot be re-entered from a connection/error listener: the listener runs '
                    . 'inside the connecting fiber, and awaiting the re-entrant connect there deadlocks. '
                    . 'Schedule supervision reconnects with Revolt\EventLoop::queue() and do not await '
                    . 'the scheduled connect from inside the listener (that only moves the same '
                    . 'dependency cycle one fiber away).',
                );
            }

            if ($this->state === ConnectionState::Draining) {
                throw new ConnectionException('Cannot connect: drain in progress');
            }

            // Coalesce concurrent user connects the same way recoverConnection() coalesces
            // recoveries: the second caller awaits the first dial's outcome (#145).
            $inFlight = $this->connecting;
            if ($inFlight !== null) {
                $inFlight->getFuture()->await();
                $this->throwUnlessOpenAfterJoin('Connect was aborted before the connection opened');

                return;
            }

            $deferred = new DeferredFuture();
            // Suppress unhandled-error reporting for the no-waiter case; awaiting callers still
            // receive the error from await().
            $deferred->getFuture()->ignore();
            $this->connecting = $deferred;
            // The fiber that runs performConnect() below, so the caller-fiber guard above can refuse a
            // listener-initiated re-entry. Held until the finally (not settleConnecting()) so it stays
            // armed through the terminal Closed emission, where $connecting is already settled (#145).
            $this->connectFiber = \Fiber::getCurrent();

            // A fresh connect re-arms the recovery paths after a prior disconnect()/drain() (#84).
            // Only this fresh-dial path resets close-intent: the joining paths above must not
            // disarm a concurrent disconnect() (#145).
            $this->closing = false;

            try {
                $this->performConnect();
            } catch (\Throwable $e) {
                // performConnect() settles $connecting before each of its own emissions; this covers
                // the owned-recovery hand-off, whose failure leaves $connecting pending. Idempotent.
                $this->settleConnecting($e);

                throw $e;
            } finally {
                $this->connectFiber = null;
            }

            // A direct success already settled $connecting before emitting Connected; a completed
            // owned recovery hand-off leaves it pending until here.
            $this->settleConnecting(null);
            // The dial can RESOLVE without the connection opening: an owned recovery aborted by a
            // concurrent disconnect()/drain() returns without throwing, leaving state Closed. Callers
            // treat a resolved connect() as "connected", so a not-Open outcome must surface as a
            // failure - for the owner exactly as it already does for joiners (#145).
            $this->throwUnlessOpenAfterJoin('Connect was aborted before the connection opened');
        });
    }

    /**
     * A joined dial can RESOLVE without the connection opening: performRecovery() completes (rather
     * than errors) its deferred when a concurrent disconnect()/drain() set close-intent mid-recovery,
     * and the connecting deferred inherits that outcome through the recovery hand-off. Callers of
     * connect() treat resolution as "connected", so a join whose outcome is not Open must surface as
     * a failure, not as success (#145).
     *
     * @phpstan-impure Reads state mutated across the caller's suspension points.
     */
    private function throwUnlessOpenAfterJoin(string $message): void
    {
        if ($this->state !== ConnectionState::Open) {
            throw new ConnectionException($message);
        }
    }

    /**
     * Runs one user-initiated connect - dial + handshake with the standing failure policy (auth
     * failures fail fast; other failures hand off to recovery or the initial-connect retry loop;
     * otherwise the connection closes terminally). Serialized by {@see connect()}.
     */
    private function performConnect(): void
    {
        try {
            $this->connectOnce();
            $this->markConnectionOpen();
            // Settle $connecting before running the listener: the deferred must never be pending
            // while user code runs, or a listener-initiated connect() join would await an outcome
            // only this (now suspended-in-the-listener) fiber can produce - a deadlock - and a
            // concurrent live-epoch failure could be swallowed by recoverConnection()'s guard (#145).
            $this->settleConnecting(null);
            $this->emitEvent(ConnectionEvent::Connected);
        } catch (AuthenticationException $e) {
            // An auth failure will not resolve by retrying: fail fast instead of entering reconnect.
            $this->state = ConnectionState::Closed;
            $this->closeTransportBestEffort();
            $this->releaseRuntimeState();
            // Settle before emitting Closed so the deferred is never pending under a listener (#145).
            $this->settleConnecting($e);
            $this->emitEvent(ConnectionEvent::Closed, $e);

            throw $e;
        } catch (\Throwable $e) {
            if ($this->options->reconnectEnabled && $this->options->maxReconnectAttempts > 0) {
                // ownedByConnect: this hand-off runs inside the connect fiber while $connecting is
                // still set - it is the one recovery request the in-flight-connect guard must admit.
                $this->recoverConnection(ownedByConnect: true);

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
            // Settle before emitting Closed (deferred never pending under a listener, #145) and with
            // the SAME wrapped exception the direct caller receives, so joiners see one error type.
            $wrapped = new ConnectionException($e->getMessage(), (int) $e->getCode(), $e);
            $this->settleConnecting($wrapped);
            $this->emitEvent(ConnectionEvent::Closed, $e);
            throw $wrapped;
        }
    }

    /**
     * Resolves and clears the in-flight connect() deferred exactly once. Called before every
     * synchronous lifecycle emission on a direct performConnect() exit path so the deferred is never
     * pending while user code (a connection/error listener) runs: a pending $connecting under a
     * listener re-opens the join deadlock (a listener's connect() awaiting a deferred only the
     * suspended emitting fiber can complete) and the swallowed-recovery window (#145). Idempotent -
     * the owned-recovery hand-off leaves it for connect() to settle after performConnect() returns.
     */
    private function settleConnecting(?\Throwable $error): void
    {
        $deferred = $this->connecting;
        if ($deferred === null) {
            return;
        }

        $this->connecting = null;
        if ($error === null) {
            $deferred->complete();
        } else {
            $deferred->error($error);
        }
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
        $this->pendingDirty = [];
        $this->receivedCounts = [];
        $this->deliveredCounts = [];
        $this->autoUnsubMax = [];
        // Mux request inbox (#118): a terminal close ends the epoch. Clearing muxSid/muxBase forces a
        // fresh ensureMuxInbox() (new random base + sid) on the next connect(); clearing muxWaiters drops
        // any still-registered in-flight waiter - its request fiber terminates via the wait-loop state
        // gate / recovery exception, not via the map. muxTokenSeq is intentionally NOT reset (the new
        // random base makes any cross-epoch token undeliverable regardless).
        $this->muxBase = null;
        $this->muxSid = null;
        $this->muxWaiters = [];
        // A fresh connect() may target an account/server that permits the reply-inbox wildcard, so a
        // terminal close clears the rejection latch and lets ensureMuxInbox() re-attempt the mux (#167).
        $this->muxRejected = false;
        $this->unboundedSids = [];
        $this->subscriptionRejectionHandlers = [];
        $this->reconnectBuffer = '';
        $this->parser = new ProtocolParser();
        // Terminal close: no PONG will ever arrive for a queued PING, so parked flush/rtt waiters
        // must observe the close instead of idling out their deadlines (#117).
        $this->failPongWaiters(new ConnectionException('Connection closed before the server answered the PING'));
    }

    /**
     * Emits a loud error naming the count of parsed-but-undelivered inbound messages a terminal close is
     * about to discard, mirroring the outbound reconnect-buffer discard (#123) so an inbound backlog
     * dropped at close is never silent - the same observable-drop principle as #134. Called BEFORE
     * releaseRuntimeState() clears the queues. drain() reports its own bounded-deadline discard (#149)
     * and disconnect() is the documented lossy nats.go Close() path, so neither routes through here (#158).
     */
    private function reportDiscardedInboundBacklog(): void
    {
        $undelivered = 0;
        foreach ($this->pendingMessages as $queue) {
            $undelivered += $queue->count();
        }

        if ($undelivered > 0) {
            // emitErrorSafely, not emitError: this runs on terminal-close paths immediately before
            // releaseRuntimeState()/emitEvent(Closed), and emitError() logs BEFORE its listener-throw
            // guard - a throwing user logger here would otherwise skip the cleanup that follows (#158).
            $this->emitErrorSafely(new NatsException(sprintf(
                'Connection closed: %d parsed inbound message(s) were discarded undelivered',
                $undelivered,
            )));
        }
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
     * Writes raw bytes with the WAIT bounded by $cancellation. A transport write suspends the
     * calling fiber on backpressure (a peer stalling with a full send buffer) and cannot be
     * cancelled mid-flight, so the write runs on its own fiber and only the wait is bounded: on
     * cancellation the caller proceeds - drain() treats it as a wedged transport and tears down
     * (its socket close then errors the abandoned write out), flush() times out - while the
     * fiber's late outcome is ignored. Only bounded-time contracts (drain, flush) pay the extra
     * fiber; hot paths keep the direct single-fiber write.
     */
    private function writeBounded(string $bytes, Cancellation $cancellation): void
    {
        $write = async(function () use ($bytes): void {
            $this->transport->write($bytes)->await();
        });
        $write->ignore();
        $write->await($cancellation);
    }

    /**
     * The remaining budget until $deadline as a cancellation for one bounded drain write; never
     * zero so a deadline razor-edge still surfaces as a cancellation, not an invalid timeout.
     */
    private function remainingBudgetCancellation(float $deadline): Cancellation
    {
        return new TimeoutCancellation(max(0.001, $deadline - $this->monotonicSeconds()));
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

            // One overall drain budget (monotonic), computed once at entry, bounds BOTH the flush-wait
            // and the backlog-wait phases: total drain time cannot exceed ~requestTimeoutMs. A second
            // sequential deadline for the backlog wait would roughly double worst-case drain latency;
            // #149 requires the wait be bounded by the existing (singular) drain deadline.
            $drainDeadline = $this->monotonicSeconds() + max(0.1, $this->options->requestTimeoutMs / 1000);
            // Recorded on the instance so mid-drain handler publishes (writePublishFrame's Draining
            // branch) and the per-sid delivery loop (drainPendingForSid) share this same budget.
            $this->drainDeadline = $drainDeadline;

            try {
                // Send UNSUB for all active subscriptions so no new messages arrive. Each write's
                // WAIT is bounded by the remaining drain budget: a peer stalling with a full send
                // buffer suspends transport writes indefinitely (they cannot be cancelled, only
                // abandoned), and drain() has already cancelled the heartbeat - the one escalation
                // that could otherwise break such a wedge - so an unbounded write here hung drain()
                // forever in violation of its documented ~requestTimeoutMs bound (#149).
                foreach (array_keys($this->subscriptionMeta) as $sid) {
                    $this->writeBounded(
                        $this->codec->encodeUnsubscribe($sid),
                        $this->remainingBudgetCancellation($drainDeadline),
                    );
                }

                // Flush in-flight deliveries already emitted by the server before closing. The FIFO
                // pong slot pairs this PING with ITS pong (#117): a stale PONG answering an earlier
                // heartbeat PING (whose bounded self-read timed out without consuming it) completes
                // that older slot instead of ending this flush early - ending early would close the
                // socket with in-flight MSGs unread, silent loss on the documented lossless path.
                $flushSlot = $this->enqueuePongSlot();
                try {
                    $this->writeBounded($this->codec->encodePing(), $this->remainingBudgetCancellation($drainDeadline));
                } catch (CancelledException $wedged) {
                    // The PING write wedged past the deadline but its bytes may still reach the wire
                    // when the queue drains (or die with the socket close below); either way the
                    // epoch teardown clears the slot, so leaving it queued stays FIFO-correct.
                    throw $wedged;
                } catch (\Throwable $writeError) {
                    // The PING never hit the wire: drop its slot so correlation stays aligned.
                    $this->discardPongSlot($flushSlot);

                    throw $writeError;
                }

                // Read until the server's PONG for THIS ping confirms the flush (handleFrame completes
                // the slot), bounded by the REMAINING drain budget (shared with the backlog wait below)
                // so a slow/wedged server cannot hang drain() forever. A partial chunk (0 complete frames
                // yet) must NOT end the flush early - only the PONG or the deadline does.
                $flushCancellation = new TimeoutCancellation(max(0.001, $drainDeadline - $this->monotonicSeconds()));
                try {
                    while (!$flushCancellation->isRequested()) {
                        $read = $this->readIncoming($flushCancellation)->await();

                        if ($flushSlot->isComplete()) {
                            // The PONG answering the drain PING arrived (or a concurrent teardown
                            // errored the slot - close-and-clean-up below is right either way).
                            break;
                        }

                        if (!$read->consumedBytes) {
                            // Genuinely idle read (empty socket, or another fiber owns the read). Yield so
                            // the event loop advances and the deadline can fire - without this the loop
                            // would busy-spin and starve the timer forever. A read that consumed bytes but
                            // did not complete this drain PONG frame (a large payload arriving in chunks)
                            // loops immediately: the rest is already buffered, so no idle sleep is paid per
                            // partial chunk (#119).
                            delay(0.001, cancellation: $flushCancellation);
                        }
                    }
                } catch (CancelledException) {
                    // Flush deadline reached; close with whatever was delivered.
                }
            } catch (CancelledException) {
                // A drain WRITE wedged past the deadline (backpressure-stalled peer): the socket is
                // unusable, so skip the flush phase - the teardown below closes it, which errors the
                // abandoned write's fiber out. The drain bound now covers the write phase too (#149).
            } catch (\Throwable $flushError) {
                // A failed drain write (dead socket), a fatal frame (e.g. a server -ERR), or a handler
                // that threw/published while the flush-phase read delivered backlog surfaced here.
                // Route it to the error listener (a swallowed failure during the lossless path was
                // invisible before) and fall through to the cleanup below so drain() still closes
                // rather than leaving the connection wedged in Draining with the socket open (#150).
                $this->emitErrorSafely($flushError);
            }

            // Deliver the remaining buffered backlog before closing. A handler may await mid-delivery
            // (suspending on ANOTHER fiber, its sid guarded by dispatchingSids with messages still
            // queued) or publish an ack/reply (which now reaches the wire during Draining, #150).
            // Wait - bounded by the single drain deadline computed at entry - for every sid's queue to
            // empty and every in-flight dispatch to finish before releasing state, so a suspended
            // dispatch loop cannot resume into a cleared registry and silently drop its remainder on
            // the lossless path (#149). Each delivery pass is contained so a handler exception is
            // surfaced rather than stranding the connection in Draining - drain() always reaches
            // Closed (#150).
            while (true) {
                try {
                    $this->drainAllPending();
                } catch (\Throwable $handlerError) {
                    $this->emitErrorSafely($handlerError);
                }

                if (!$this->hasUndeliveredDrainBacklog()) {
                    break;
                }

                if ($this->monotonicSeconds() >= $drainDeadline) {
                    // Deadline reached with backlog still undelivered (a handler suspended past it):
                    // releaseRuntimeState() below clears the registry, and the resumed dispatch loop
                    // then breaks on the missing subscription and discards the remainder. Make that
                    // discard LOUD, never silent (#149 acceptance: either delivered or an error naming
                    // the count; mirrors the #123/#134 observable-drop principle).
                    $undelivered = $this->countUndeliveredDrainBacklog();
                    if ($undelivered > 0) {
                        $this->emitErrorSafely(new ConnectionException(
                            'drain deadline exceeded: ' . $undelivered
                            . ' buffered message(s) were not delivered before close',
                        ));
                    }

                    break;
                }

                // Yield so a dispatch loop suspended on another fiber can resume and drain its sid's
                // queue; without this the loop would spin while that fiber is never scheduled.
                delay(0.001);
            }

            // The drain budget ends with the delivery phases above; writePublishFrame()'s Draining
            // branch falls back to its fixed per-publish bound once cleared.
            $this->drainDeadline = null;

            // Clear subscription state (also errors out any still-parked pong slots, e.g. this
            // drain's own slot when the flush ended via the deadline).
            $this->releaseRuntimeState();

            // Best-effort like every other terminal path: TransportInterface::close() gives no
            // no-throw guarantee, and drain deliberately routes known-broken sockets here (the
            // dead-socket write failures contained above), so a throwing close() must not rethrow
            // out of drain() and strand the state in Draining - drain() always reaches Closed (#150).
            $this->closeTransportBestEffort();
            $this->state = ConnectionState::Closed;
        });
    }

    /**
     * True while drain() still has backlog to deliver: a sid has queued messages, or a dispatch loop
     * is suspended mid-delivery on another fiber (dispatchingSids non-empty) and may yet enqueue-drain
     * more for its sid. drain() waits on this - bounded by its deadline - before tearing down (#149).
     */
    private function hasUndeliveredDrainBacklog(): bool
    {
        if ($this->dispatchingSids !== []) {
            return true;
        }

        // The dirty set holds exactly the sids with a non-empty queue (its maintained invariant), so a
        // non-empty dirty set is precisely "a sid still has buffered messages" - equivalent to the old
        // full-queue scan but O(1), and consistent with what drainAllPending() iterates (#162).
        return $this->pendingDirty !== [];
    }

    /**
     * Sums the messages still buffered across every sid's queue - the backlog drain() would discard
     * if its deadline is reached before delivery completes. A sid guarded by a suspended dispatch
     * keeps its not-yet-delivered messages in its queue, so summing all queue sizes counts them too.
     * Used to name the count in the loud deadline-exceeded error so the drop is never silent (#149).
     */
    private function countUndeliveredDrainBacklog(): int
    {
        $count = 0;
        foreach ($this->pendingMessages as $queue) {
            $count += $queue->count();
        }

        return $count;
    }

    /**
     * Reports an asynchronous error through the listener/logger, swallowing a throw from a
     * user-supplied logger/listener: emitError() guards the listener but logs before that guard, so a
     * throwing logger could otherwise re-open the very escape a containment closes (#150). Used wherever
     * a secondary/handler error must be surfaced WITHOUT masking a primary exception already in flight
     * (drain teardown, and the per-chunk dispatch/drain containment in processIncoming/dispatchFrames, #158).
     */
    private function emitErrorSafely(\Throwable $error): void
    {
        try {
            $this->emitError($error);
        } catch (\Throwable) {
            // A throwing user logger/listener must never break drain's teardown.
        }
    }

    /**
     * Publishes payload bytes to the given subject.
     *
     * At-least-once, nats.go-parity semantics while reconnecting: a publish issued while a reconnect is
     * in flight is normally buffered and reports success IMMEDIATELY - the returned Future resolves as
     * soon as the frame is queued, before it has reached any server. If the reconnect then succeeds the
     * buffered frames are flushed in publish order; if it exhausts every attempt, those frames are
     * DISCARDED and this reported success is retroactively void. That exhaustion loss is signalled only
     * out-of-band - via the connection-level {@see ConnectionEvent::Closed} event and the
     * "Reconnect exhausted: N bytes ... discarded" async error (#123) - never through this Future. A
     * caller needing end-to-end delivery confirmation must use a JetStream publish-with-ack rather than
     * treating this success as proof the bytes were transmitted.
     *
     * The one exception to "resolves immediately" is sustained publish pressure during the reconnect
     * flush: once the buffer has been drained {@see self::RECONNECT_FLUSH_MAX_PASSES} times it is sealed
     * so recovery can reach Open in a bounded number of passes (#165). A publish that arrives after the
     * seal does NOT buffer - it parks on the flush gate until the connection flips Open (then writes
     * directly, still ordered after the buffered frames) or Closed (then throws ConnectionException
     * through this Future). So under that narrow window this Future can block, and can surface the
     * connection-closed failure directly, rather than resolving success out of hand.
     *
     * @return Future<void>
     */
    public function publish(string $subject, string $payload, ?string $replyTo = null): Future
    {
        return async(function () use ($subject, $payload, $replyTo): void {
            $this->validatePublishSubjects($subject, $replyTo);
            $this->enforceMaxPayload(strlen($payload));

            $frame = $this->codec->encodePublish($subject, $payload, $replyTo);

            $this->writePublishFrame($frame);
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
            $this->validatePublishSubjects($subject, $replyTo);
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

            $this->writePublishFrame($frame);
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
        $this->writePublishFrame($segment);

        foreach ($payloads as $payload) {
            $this->recordOutbound($payload);
        }
    }

    /**
     * Writes an already-encoded publish frame with the state-appropriate delivery:
     *   - Open: write to the socket, with a single recover-and-retry on a transient write failure.
     *   - Draining: write straight to the still-live socket. A draining connection keeps its socket
     *     open until drain() closes it, and a handler's ack/reply (a JetStream ack, respond(), a
     *     request reply) MUST reach the wire - nats.go drains by publishing then closing. Buffering
     *     is wrong (no reconnect will flush it) and refusing would redeliver the just-acked message;
     *     recovery is not attempted (drain is tearing down). #150
     *   - otherwise: buffer while a reconnect is in flight (flushed on reconnect), else fail loudly -
     *     a publish after the connection has Closed still throws (#146).
     */
    /**
     * Validates a publish subject (cached) and its optional reply subject. The reply is validated
     * uncached: a publish replyTo is typically a per-request unique inbox, so caching it would only
     * churn the memo (see validateSubjectCached()). Shared by publish() and publishWithHeaders().
     */
    private function validatePublishSubjects(string $subject, ?string $replyTo): void
    {
        $this->validateSubjectCached($subject);
        if ($replyTo !== null) {
            $this->validateSubject($replyTo);
        }
    }

    private function writePublishFrame(string $frame): void
    {
        if ($this->state === ConnectionState::Open) {
            try {
                $this->transport->write($frame)->await();
            } catch (\Throwable) {
                // The direct write failed mid-flight: recover, then re-send on the fresh socket. The
                // re-send stays AFTER recovery (it is NOT seeded into the flush) on purpose: recovery
                // success must be independent of this frame - a persistently rejected publish would
                // otherwise cascade the whole recovery into exhaustion (#145) - and the post-recovery
                // delivery drain must run before the retry write (#144). This frame therefore lands
                // after any frames other publishers buffered during the outage: that is the
                // buffered-before-direct ordering #148/#165 make global, and the at-least-once
                // consequence documented on publish() (#121). Per-publisher order still holds: this
                // publish() does not return until the frame is (re-)written, so the same publisher's
                // next frame follows it on the wire.
                $this->recoverConnection();
                $this->transport->write($frame)->await();
            }

            return;
        }

        if ($this->state === ConnectionState::Draining) {
            // Bounded by drain()'s own REMAINING budget, not a fresh full request timeout: a handler
            // ack/reply published while drain delivers backlog rides a socket whose heartbeat is
            // already cancelled, so an unbounded write on a backpressure-stalled peer would wedge
            // the drain with no escalation left - and a fresh per-publish bound would let K queued
            // handlers serially extend drain() to ~K x requestTimeoutMs past its documented single
            // budget (#149). The fixed fallback covers a Draining state with no recorded deadline
            // (defensive: drain() always records one before delivering). On the wedge the caller's
            // failure is contained by drain's handler-error paths.
            $drainDeadline = $this->drainDeadline;
            try {
                $this->writeBounded(
                    $frame,
                    $drainDeadline !== null
                        ? $this->remainingBudgetCancellation($drainDeadline)
                        : new TimeoutCancellation(max(0.1, $this->options->requestTimeoutMs / 1000)),
                );
            } catch (CancelledException) {
                throw new TimeoutException('Publish during drain timed out (transport backpressure)');
            }

            return;
        }

        // A sealed reconnect flush (#165) parks late publishers here so they stop re-filling the
        // buffer: once the flush completes the connection is Open (retry writes directly) or Closed
        // (bufferFrame() refuses -> throw), and either way these frames are ordered AFTER the buffered
        // frames the flush already emitted. The gate is always completed by recoverConnection()'s
        // finally with the state finalized, so this await cannot outlive the recovery.
        $flushGate = $this->reconnectFlushGate;
        if ($flushGate !== null) {
            $flushGate->getFuture()->await();
            $this->writePublishFrame($frame);

            return;
        }

        if (!$this->bufferFrame($frame)) {
            throw new ConnectionException('Connection is not open');
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
     * Reports whether a subscription with the given sid is still registered. False once it has been
     * dropped by unsubscribe/drain or a terminal close (which clears the registry). Used by the
     * JetStream idle-heartbeat watchdog to self-cancel the moment the subscription it guards is torn
     * down, so no timer outlives its subscription (#113).
     */
    public function isSubscriptionActive(int $sid): bool
    {
        return isset($this->subscriptions[$sid]);
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
     * Exempts a subscription from the per-subscription slow-consumer pending-queue bound, so its
     * replies are never dropped by {@see enqueueMessage()}'s overflow policy. Used for the muxed
     * request inbox (#118) and the pipelined pull inbox (#120), where every in-flight reply shares
     * one sid's queue and a dropped reply silently breaks a request/pull with no backpressure path.
     * Synchronous (no await) so it takes effect in the same tick the caller received the sid from
     * {@see subscribe()}, before any reply can be enqueued. Memory is then bounded by the caller's
     * in-flight concurrency, not by the queue cap (#159).
     *
     * @internal Low-level mechanism for the JetStream request/pull inboxes; not general API.
     */
    public function markSubscriptionUnbounded(int $sid): void
    {
        $this->unboundedSids[$sid] = true;
    }

    /**
     * Registers a callback invoked (with the raw -ERR text) when the server rejects this sid's
     * subscription with an async permissions violation. See {@see $subscriptionRejectionHandlers}.
     *
     * @internal Low-level mechanism for long-lived JetStream reply inboxes; not general API.
     */
    public function markSubscriptionRejectionHandler(int $sid, \Closure $handler): void
    {
        $this->subscriptionRejectionHandlers[$sid] = $handler;
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

            // FIFO pong correlation (#117): completion of THIS slot means "the server processed
            // everything written before THIS flush's PING". A stale PONG answering an earlier
            // (heartbeat or timed-out) PING completes that PING's slot, never this one, and a
            // concurrent flush timing out cannot release this waiter.
            $slot = $this->enqueuePongSlot();
            try {
                // The WAIT is bounded (the write itself cannot be cancelled): flush() documents a
                // request-timeout bound, but a backpressure-suspended write held it forever before
                // the read phase's deadline could even start (#149's write-phase twin).
                $this->writeBounded(
                    $this->codec->encodePing(),
                    new TimeoutCancellation(max(0.1, $this->options->requestTimeoutMs / 1000)),
                );
            } catch (CancelledException) {
                // Write wedged past the flush budget. The PING may still reach the wire whenever
                // the queue drains, so its slot deliberately STAYS queued (same rule as the
                // read-phase timeout below); epoch teardown clears it if the PONG never comes.
                throw new TimeoutException('Flush timed out writing PING (transport backpressure)');
            } catch (\Throwable $writeError) {
                // The PING never hit the wire: drop its slot so correlation stays aligned with
                // wire order (nats.go removePongFromList parity).
                $this->discardPongSlot($slot);

                throw $writeError;
            }

            $cancellation = new TimeoutCancellation(max(0.1, $this->options->requestTimeoutMs / 1000));
            try {
                while (!$slot->isComplete()) {
                    $read = $this->readIncoming($cancellation)->await();

                    // Only a genuinely idle read yields: without it the loop would busy-spin and
                    // starve the deadline. A read that consumed bytes but produced no complete frame
                    // yet (this flush's PONG arriving behind a large payload split across chunks) loops
                    // immediately - the rest is already buffered, so no 1 ms sleep per chunk (#119). A
                    // slot completed during any read exits at the loop head regardless.
                    if (!$read->consumedBytes) {
                        delay(0.001, cancellation: $cancellation);
                    }
                }
            } catch (CancelledException) {
                // The slot deliberately STAYS queued on timeout: its PONG is still owed and must
                // consume this slot when it lands - skipping an abandoned head keeps the FIFO
                // aligned, where removing it mid-queue would desynchronize later waiters. Epoch
                // teardown clears it if the PONG never comes.
                if (!$slot->isComplete()) {
                    throw new TimeoutException('Flush timed out waiting for server PONG');
                }
            }

            // Completed: the PONG for THIS ping resolves the flush; an epoch end (reconnect or
            // terminal close) surfaces its ConnectionException to the waiter here.
            $slot->getFuture()->await();
        });
    }

    /**
     * Reads one transport chunk, parses frames, and dispatches message callbacks, reporting BOTH the
     * number of complete frames dispatched and whether the read consumed bytes off the wire (#119).
     *
     * A wait loop uses {@see IncomingChunkResult::$consumedBytes} to tell partial-frame progress (a
     * large payload arriving in socket-sized chunks: bytes consumed, frame not yet complete) apart
     * from a genuinely idle read (empty socket / another fiber owns the read): it loops immediately in
     * the former case and yields 1 ms only in the latter. {@see processIncoming()} exposes the same
     * cycle as a frame count only, preserving its existing contract.
     *
     * @param Cancellation|null $cancellation Optional token that cancels the underlying socket read,
     *                                        so a timed-out caller does not orphan an in-flight read.
     * @return Future<IncomingChunkResult>
     *
     * @phpstan-impure Mutates connection state (e.g. completes queued pong slots / resets
     *                 outstandingPings via handled frames), so callers must not assume remembered
     *                 property values persist.
     */
    public function readIncoming(?Cancellation $cancellation = null): Future
    {
        return async(function () use ($cancellation): IncomingChunkResult {
            if ($this->state !== ConnectionState::Open && $this->state !== ConnectionState::Draining) {
                throw new ConnectionException('Connection is not open');
            }

            if ($this->readInProgress) {
                // A concurrent read (e.g. the heartbeat timer) owns the socket; avoid a second
                // overlapping read which the transport would reject with a pending-read error.
                return new IncomingChunkResult(0, false);
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

                return new IncomingChunkResult(0, false);
            } finally {
                $this->readInProgress = false;
                $this->signalReadSlotFree();
            }

            if ($chunk === '') {
                return new IncomingChunkResult(0, false);
            }

            try {
                $frames = $this->parser->push($chunk);
            } catch (ProtocolException $parseError) {
                // An unparseable/corrupt stream is a transport-level failure: reconnect rather than
                // letting the exception escape the caller's processing loop. The parser has already
                // resynced past the offending bytes, so a recovery-disabled retry will not re-throw.
                // Frames that parsed before the failure are dispatched first - their bytes are
                // consumed, so recovery would lose them permanently (#147, parse-layer twin of #128)
                // - and the failure surfaces via the error listener before recovery runs.
                $recovered = $this->parser->takeParsedFrames();

                try {
                    try {
                        $this->dispatchFrames($recovered);
                    } finally {
                        // Deliver the enqueued messages even when a frame failed to dispatch,
                        // mirroring the clean-path drain below.
                        $this->drainAllPending();
                    }
                } finally {
                    // A handler that throws while the recovered frames are delivered must not leave
                    // the connection Open on a corrupt stream with the failure unobservable: the
                    // error emission and the recovery run regardless, and the handler's own
                    // exception then propagates to the caller (#128 rethrow-after-containment).
                    try {
                        $this->emitError($parseError);
                    } catch (\Throwable) {
                        // emitError() swallows listener throws, but a user-supplied logger can
                        // still throw; recovery must run regardless.
                    }

                    $this->recoverConnection();
                }

                // A non-empty chunk was read and pushed, so bytes were consumed even though the
                // parse failed partway - the caller made wire progress and should not idle-sleep.
                return new IncomingChunkResult(count($recovered), true);
            }

            // Note: the outstanding-ping counter is reset only when an actual PONG is handled (see
            // handleFrame), not on any inbound bytes - otherwise a server that stops answering PINGs
            // but still trickles data would never trip maxPingsOut and the watchdog could not escalate.
            $dispatchError = null;
            try {
                $this->dispatchFrames($frames);
            } catch (\Throwable $e) {
                // Held, not rethrown yet: the already-enqueued backlog must still drain (wire-order
                // delivery, #128) before this fatal frame error (a server -ERR / PONG-write failure)
                // escalates to the caller below.
                $dispatchError = $e;
            }

            try {
                // Drain buffered deliveries after each chunk to preserve wire-order delivery - even
                // when a frame failed to dispatch: the messages are already enqueued and their bytes
                // consumed, so they must not wait behind (or be lost to) the surfacing error.
                $this->drainAllPending();
            } catch (\Throwable $drainError) {
                if ($dispatchError === null) {
                    // No primary error in flight: surface the handler failure to the caller unchanged.
                    throw $drainError;
                }

                // A fatal dispatch exception is already in flight. A handler throwing during this drain
                // must NOT replace it - PHP would propagate the later throw and swallow the fatal -ERR,
                // which then never reaches the caller's escalation path. Route the handler failure to the
                // error listener and let the primary exception propagate below (#158).
                $this->emitErrorSafely($drainError);
            }

            if ($dispatchError !== null) {
                throw $dispatchError;
            }

            // Bytes were consumed off the wire this read (the chunk was non-empty); $frames may still
            // be 0 when a large frame spans several chunks and this one did not complete it (#119).
            return new IncomingChunkResult(count($frames), true);
        });
    }

    /**
     * Reads one transport chunk, parses frames, and dispatches message callbacks, returning the
     * number of complete frames dispatched. Frame-count-only view of {@see readIncoming()} (whose
     * result also reports whether bytes were consumed); its contract is unchanged (#119).
     *
     * @param Cancellation|null $cancellation Optional token that cancels the underlying socket read,
     *                                        so a timed-out caller does not orphan an in-flight read.
     * @return Future<int>
     *
     * @phpstan-impure Mutates connection state (e.g. completes queued pong slots / resets
     *                 outstandingPings via handled frames), so callers must not assume remembered
     *                 property values persist.
     */
    public function processIncoming(?Cancellation $cancellation = null): Future
    {
        return $this->readIncoming($cancellation)->map(static fn(IncomingChunkResult $result): int => $result->frames);
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
                if ($firstError === null) {
                    $firstError = $e;

                    continue;
                }

                // Only the FIRST failure is rethrown (below) for the caller's escalation. A 2nd+ failure
                // from the same chunk would otherwise vanish - neither thrown nor observable - hiding the
                // corresponding message loss from the error listener and from tests. Surface it (contained
                // so a throwing logger cannot mask the first error being rethrown) (#158).
                $this->emitErrorSafely($e);
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
     * Establishes the per-connection mux request inbox on first use: one wildcard subscription
     * "<base>.*" whose handler ({@see dispatchMuxReply()}) routes every reply to its per-token waiter.
     * Idempotent - a no-op once established, so it survives reconnect (muxSid/muxBase are retained,
     * only {@see releaseRuntimeState()} nulls them). Serialized via $muxInboxSetup so concurrent first
     * requests write exactly one SUB even though subscribe()->await() suspends (#118).
     *
     * Precondition: the caller has checked state === Open; subscribe() re-checks and throws otherwise.
     */
    private function ensureMuxInbox(): void
    {
        if ($this->muxSid !== null) {
            return;
        }

        if ($this->muxInboxSetup !== null) {
            // Another fiber is mid-establishment; join it instead of writing a second SUB.
            $this->muxInboxSetup->await();

            return;
        }

        $deferred = new DeferredFuture();
        $this->muxInboxSetup = $deferred->getFuture();
        $this->muxInboxSetup->ignore();

        try {
            $base = Inbox::generate($this->options->inboxPrefix);
            // dispatchMuxReply is non-suspending, so it is safe to invoke inside drainPendingForSid()'s
            // dequeue loop. The handler lives in subscriptions[$muxSid] and persists across reconnect
            // (resubscribeAll replays only the SUB bytes; only releaseRuntimeState clears the handler).
            $sid = $this->subscribe($base . '.*', $this->dispatchMuxReply(...))->await();
            $this->muxBase = $base;
            $this->muxSid = $sid;
            // Slow-consumer exemption: the mux queue must never drop a reply (breaks a request).
            $this->unboundedSids[$sid] = true;
            $deferred->complete();
        } catch (\Throwable $e) {
            $deferred->error($e);

            throw $e;
        } finally {
            // Cleared so a failed establishment retries on the next request.
            $this->muxInboxSetup = null;
        }
    }

    /**
     * Mints a per-request suffix token unique within the connection epoch. Sequential (not random) so
     * uniqueness is guaranteed by construction, not probabilistically; the random $muxBase provides
     * unguessability - an attacker cannot address "<base>.<n>" without knowing the base (#118).
     */
    private function newMuxToken(): string
    {
        return dechex($this->muxTokenSeq++);
    }

    /**
     * Registers a request's reply-dispatch callback under its suffix token, before its PUB is written.
     *
     * @param callable(NatsMessage):void $handler
     */
    private function registerMuxWaiter(string $token, callable $handler): void
    {
        $this->muxWaiters[$token] = $handler;
    }

    /**
     * Removes a request's waiter. Runs in the finally of both request methods, on every exit path -
     * the removal that makes a later/duplicate reply for a terminated request undeliverable (#118).
     */
    private function removeMuxWaiter(string $token): void
    {
        unset($this->muxWaiters[$token]);
    }

    /**
     * Mux subscription handler: routes one reply to the request that owns it, keyed on the suffix token
     * in the reply subject "<muxBase>.<token>". A reply whose token has no live waiter (the owning
     * request already completed / timed out / was cancelled, or it is a duplicate) is discarded - the
     * no-late-leak guard that replaces the per-request unknown-sid discard (#118).
     *
     * MUST NOT suspend: it runs inside drainPendingForSid()'s dequeue loop over the single mux sid, so
     * suspending here would stall every other reply behind the same-sid dispatch guard.
     */
    private function dispatchMuxReply(NatsMessage $message): void
    {
        $base = $this->muxBase;
        if ($base === null) {
            // Torn down (terminal close) between enqueue and drain.
            return;
        }

        $prefix = $base . '.';
        $subject = $message->subject;
        if (strncmp($subject, $prefix, strlen($prefix)) !== 0) {
            // Not addressed to this epoch's base (defensive; the server only delivers "<base>.*").
            return;
        }

        $token = substr($subject, strlen($prefix));
        $waiter = $this->muxWaiters[$token] ?? null;
        if ($waiter === null) {
            // Late / duplicate / stale reply for a terminated request -> drop.
            return;
        }

        $waiter($message);
    }

    /**
     * The clear, catchable error raised by request()/requestMany() once the mux reply-inbox
     * subscription has been permission-rejected (#167), replacing the pre-fix silent timeout. Names the
     * reply-inbox wildcard the account must be allowed to subscribe to.
     */
    private function muxRejectedException(): ConnectionException
    {
        $wildcard = $this->options->inboxPrefix . '.>';

        return new ConnectionException(
            'request/reply is unavailable on this connection: the shared reply-inbox subscription "'
            . $this->options->inboxPrefix . '.<inbox>.*" was rejected by the server (permissions violation). '
            . 'Grant the account subscribe permission for the reply-inbox wildcard "' . $wildcard
            . '" to use request()/requestMany().',
        );
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

        if ($this->muxRejected) {
            throw $this->muxRejectedException();
        }

        $this->ensureMuxInbox();
        $token = $this->newMuxToken();
        $replyTo = $this->muxBase . '.' . $token;

        /** @var DeferredFuture<NatsMessage> $deferred */
        $deferred = new DeferredFuture();
        // Set by the handler when the reply is delivered. The wait loop checks this rather than
        // $deferred->isComplete() so a reply delivered in the same tick the deadline fires is
        // returned instead of being discarded as a spurious timeout.
        $replyReceived = false;

        // Registered by token on the shared mux inbox instead of a fresh per-request SUB (#118). The
        // handler body is unchanged and idempotent: a coalesced duplicate arriving in the same drain
        // batch (before the finally removes the waiter) is ignored by the isComplete() guard.
        $this->registerMuxWaiter($token, static function (NatsMessage $message) use ($deferred, &$replyReceived): void {
            if (!$deferred->isComplete()) {
                $deferred->complete($message);
                $replyReceived = true;
            }
        });

        try {
            if ($headers === null) {
                $this->publish($subject, $payload, $replyTo)->await();
            } else {
                $this->publishWithHeaders($subject, $payload, $headers, $replyTo)->await();
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

                // The mux reply inbox was permission-rejected mid-flight (the async -ERR was just read);
                // no reply can arrive on it, so fail fast with the clear error rather than waiting out
                // the deadline (#167).
                if ($this->muxRejected) {
                    throw $this->muxRejectedException();
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
                    $read = $this->readIncoming($waitCancellation)->await();
                } catch (CancelledException $e) {
                    if ($cancellation !== null && $cancellation->isRequested()) {
                        throw $e;
                    }

                    // The deadline fired during the read. Loop once more: the top-of-loop check
                    // returns the reply if it was delivered in the same tick, otherwise the deadline
                    // check there throws the timeout.
                    continue;
                }

                if (!$read->consumedBytes) {
                    // No data available from transport (genuinely idle read); yield to avoid a tight
                    // spin. A read that consumed bytes but did not complete the reply frame yet loops
                    // immediately - the rest is already buffered, so no 1 ms sleep per chunk (#119).
                    delay(0.001);
                }
            }

            $response = $deferred->getFuture()->await();

            if ($this->isNoRespondersStatus($response)) {
                throw new NatsException('No responders for subject ' . $subject);
            }

            return $response;
        } finally {
            $this->removeMuxWaiter($token);
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

        if ($this->muxRejected) {
            throw $this->muxRejectedException();
        }

        $this->ensureMuxInbox();
        $token = $this->newMuxToken();
        $replyTo = $this->muxBase . '.' . $token;

        /** @var list<NatsMessage> $messages */
        $messages = [];
        $lastAt = null;
        $noResponders = false;

        // Rotated on every delivery so a waiter parked behind another fiber's read wakes to
        // re-evaluate its termination conditions (count/stall/no-responders) (#135).
        /** @var DeferredFuture<null> $replyTick */
        $replyTick = new DeferredFuture();
        $replyTick->getFuture()->ignore();

        // Registered by token on the shared mux inbox instead of a fresh per-request SUB (#118); the
        // collector body (incl. the #160 cap and #135 tick rotation) is unchanged.
        $this->registerMuxWaiter($token, function (NatsMessage $message) use (&$messages, &$lastAt, &$noResponders, &$replyTick, $maxResponses): void {
            if ($this->isNoRespondersStatus($message)) {
                // The server's 503 sentinel: no service is listening. Stop immediately with whatever
                // (typically nothing) was collected.
                $noResponders = true;
            } elseif ($maxResponses === null || count($messages) < $maxResponses) {
                // Cap the collection here, not only in the wait loop below: replies coalesced into a
                // single chunk are ALL dispatched to this collector before the loop regains control,
                // so an unconditional append could return more than maxResponses. Extra replies past
                // the cap are dropped here (the loop breaks on count >= maxResponses regardless) (#160).
                $messages[] = $message;
                $lastAt = $this->monotonicSeconds();
            }

            $tick = $replyTick;
            $replyTick = new DeferredFuture();
            $replyTick->getFuture()->ignore();
            $tick->complete();
        });

        try {
            if ($headers === null) {
                $this->publish($subject, $payload, $replyTo)->await();
            } else {
                $this->publishWithHeaders($subject, $payload, $headers, $replyTo)->await();
            }

            $deadline = $this->monotonicSeconds() + $totalMs / 1000;
            $totalCancellation = new TimeoutCancellation($totalMs / 1000);
            $waitCancellation = $cancellation === null
                ? $totalCancellation
                : new CompositeCancellation($cancellation, $totalCancellation);

            while (true) {
                // The mux reply inbox was permission-rejected (the async -ERR was just read); no further
                // reply can arrive. Return whatever this scatter-gather has already collected rather than
                // discarding it; the clear permission error surfaces only when nothing was collected -
                // e.g. a rejection at first use, or a reconnect re-SUB rejected mid-collection (#167/review).
                if ($this->muxRejected) {
                    if ($messages !== []) {
                        break;
                    }

                    throw $this->muxRejectedException();
                }

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
                    $read = $this->readIncoming($sliceCancellation)->await();
                } catch (CancelledException $e) {
                    if ($cancellation !== null && $cancellation->isRequested()) {
                        throw $e;
                    }

                    // Slice or total deadline fired during the read; loop to re-evaluate the
                    // termination conditions (stall/total) at the top.
                    continue;
                }

                if (!$read->consumedBytes) {
                    // Genuinely idle read: yield to avoid a tight spin. A read that consumed bytes but
                    // completed no reply frame yet loops immediately - the rest of a chunked reply is
                    // already buffered, so no 1 ms sleep is paid per partial chunk (#119).
                    delay(0.001);
                }
            }

            return $messages;
        } finally {
            $this->removeMuxWaiter($token);
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
     *
     * Leaves state Connecting: the caller flips Open via {@see markConnectionOpen()}. The initial
     * connect paths flip immediately after the handshake; recovery flips only after the
     * subscription replay and reconnect-buffer flush complete, so nothing that keys off
     * state === Open (publish routing, user reads, the ping timer) treats the connection as live
     * while the replay is still in progress (#148, nats.go RECONNECTING parity).
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
        // Pong correlation is per TCP connection, like the parser: a slot parked by a previous
        // epoch's PING must never be completed by a PONG from this new connection, and its own
        // PONG died with the old socket - error the waiters (flush/rtt) out instead (#117).
        $this->failPongWaiters(new ConnectionException('Connection lost before the server answered the PING'));

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

        $trailingFrames = $this->awaitInitialPong();
        // Couple the inbound frame bound to the server's negotiated max_payload so a legitimately large
        // message (on a server with a raised max_payload) is receivable instead of being rejected as an
        // oversized frame - which would throw and force a reconnect (#94). Do it IN PLACE on the existing
        // parser rather than replacing it: replacing discarded any bytes the handshake segment buffered
        // behind a partial trailing frame, resuming the stream at an arbitrary offset and raising a
        // spurious ProtocolException on the next read (#157). The connect-start reset (#125) already
        // gave this epoch a fresh parser, so no cross-connection framing state can leak here.
        $this->parser->setMaxFrameSize($this->inboundFrameBound());
        // Seed the discovered-servers set from the initial INFO (without emitting a discovery event -
        // that is reserved for subsequent async INFO changes), so failover can use the cluster peers.
        // serverInfo is non-null here: awaitServerInfo() set it above and awaitInitialPong() only ever
        // reassigns it to a non-null ServerInfo.
        if ($this->serverInfo->connectUrls !== []) {
            $this->knownConnectUrls = $this->serverInfo->connectUrls;
        }

        // Dispatch frames the server coalesced behind the handshake PONG in the same TCP segment (an
        // async INFO with connect_urls/lame-duck, an -ERR, or MSGs for a replayed subscription): their
        // bytes are already consumed, so dropping them would lose them permanently (#157). Routed through
        // the normal enqueue/dispatch+drain path so an INFO updates the discovered pool and a MSG reaches
        // its handler; a fatal -ERR here surfaces as a connect failure the caller's policy then handles.
        if ($trailingFrames !== []) {
            try {
                $this->dispatchFrames($trailingFrames);
            } finally {
                $this->drainAllPending();
            }
        }
    }

    /**
     * Flips the connection live: publishes stop buffering and route to the socket, user reads are
     * admitted, and the heartbeat starts. Must run only once the socket is fully usable - after
     * the handshake on the initial connect paths, and after the subscription replay plus
     * reconnect-buffer flush on the recovery path (#148).
     */
    private function markConnectionOpen(): void
    {
        $this->state = ConnectionState::Open;
        // Records the first-ever successful open so the recovery loop can emit Connected (not
        // Reconnected) for a failed initial connect that recovered here (#161).
        $this->everConnected = true;
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
     *
     * @param bool $ownedByConnect True only for the hand-off from {@see performConnect()}, which
     *                             runs inside the connect fiber while {@see $connecting} is set and
     *                             must bypass the in-flight-connect guard below.
     */
    private function recoverConnection(bool $ownedByConnect = false): void
    {
        // The user asked to close (disconnect/drain): never start or join a reconnect that would
        // re-open the connection (#84).
        if ($this->closing) {
            return;
        }

        // A user-initiated connect() owns the dial while $connecting is set. A stale failure
        // continuation from the previous epoch (a write/read that suspended before a terminal
        // close and resumed failing after connect() started dialing) must not start a recovery
        // here: its first attempt would close the fresh dial's socket - the #145 race in reverse.
        // Only the connect fiber's own failure policy (performConnect()) may hand off to recovery.
        // The state !== Open clause keeps this from swallowing a GENUINE current-epoch failure: once
        // the connection is Open, a $connecting deferred still pending (a Connected listener parked
        // mid-emission) is stale bookkeeping, and a live publish/heartbeat/read failure there must
        // start a recovery rather than be dropped onto a dead socket (#145).
        if ($this->connecting !== null && !$ownedByConnect && $this->state !== ConnectionState::Open) {
            return;
        }

        $inProgress = $this->reconnecting;
        if ($inProgress !== null) {
            // A recover requested re-entrantly from WITHIN the active recovery fiber must not await its
            // own in-progress future - that would suspend this fiber on a deferred only this fiber can
            // complete (a deadlock). This reaches here when a lame-duck INFO is dispatched from inside
            // the recovery fiber - coalesced behind the reconnect handshake PONG during connectOnce
            // (#157), or read during the subscription-replay poll drainImmediateServerFrames (#166):
            // the recovery is already underway on this very fiber, so the nested request is a no-op.
            if ($this->recoveryFiber !== null && $this->recoveryFiber === \Fiber::getCurrent()) {
                return;
            }

            $inProgress->getFuture()->await();

            return;
        }

        $deferred = new DeferredFuture();
        // Suppress unhandled-error reporting for the no-waiter case; awaiting callers still receive
        // the error from await().
        $deferred->getFuture()->ignore();
        $this->reconnecting = $deferred;
        // Recorded so connect() can refuse a join issued from inside this fiber (a listener call),
        // which could never complete: this fiber is the one that resolves $reconnecting (#145).
        $this->recoveryFiber = \Fiber::getCurrent();

        try {
            $this->performRecovery();
            $deferred->complete();
        } catch (\Throwable $e) {
            $deferred->error($e);

            throw $e;
        } finally {
            $this->recoveryFiber = null;
            $this->reconnecting = null;
            // Wake any publishers parked on a sealed flush (#165). The connection state is finalized
            // by now - Open on success, Closed on every failure/close/auth path - so on wake they route
            // by it: write directly if Open, or fail loudly via bufferFrame() if Closed. Completing
            // AFTER $reconnecting is cleared keeps a Closed-path wake from re-buffering onto a dead
            // epoch (bufferFrame() then sees no active reconnect and refuses, #146).
            $flushGate = $this->reconnectFlushGate;
            if ($flushGate !== null) {
                $this->reconnectFlushGate = null;
                $flushGate->complete();
            }
        }

        // Deliver any messages buffered during subscription replay now that recovery has finished and
        // we are OUT of the critical section: `reconnecting` is cleared, so a callback that publishes
        // and hits a write failure starts a fresh recovery instead of deadlocking on the in-progress
        // one, and the per-sid dispatch guard keeps it non-reentrant. (Only reached on success; the
        // catch above rethrows on failure.)
        try {
            $this->drainAllPending();
        } catch (\Throwable $handlerError) {
            // Recovery itself already succeeded; only a handler(-triggered) failure can escape this
            // drain. It must not reach the recovery callers, whose catch blocks treat anything thrown
            // here as a FAILED recovery - the heartbeat paths would flip a healthy connection to
            // Closed without a Closed event or state release, and publish()'s retry would surface an
            // unrelated exception for a frame that was never written (#144). Report it as an async
            // error instead (nats.go parity: handler errors during post-reconnect delivery are
            // reported, not fatal).
            try {
                $this->emitError($handlerError);
            } catch (\Throwable) {
                // emitError() swallows listener throws but logs BEFORE that guard: a throwing
                // user-supplied logger would otherwise re-open the exact escape this catch closes.
            }
        }
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
                $this->markConnectionOpen();
                // Settle the in-flight connect() before the listener runs: this retry loop is still
                // inside performConnect() (the deferred is set), so a pending $connecting under the
                // Connected/Closed listener would re-open the join deadlock (#145).
                $this->settleConnecting(null);
                $this->emitEvent(ConnectionEvent::Connected);

                return true;
            } catch (AuthenticationException $e) {
                $this->state = ConnectionState::Closed;
                $this->closeTransportBestEffort();
                $this->settleConnecting($e);
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
            // Name any parsed inbound backlog being discarded, mirroring the outbound path (#123/#158).
            $this->reportDiscardedInboundBacklog();
            $this->releaseRuntimeState();
            $this->emitEvent(ConnectionEvent::Closed);
            throw new ConnectionException('Reconnect is disabled');
        }

        // A FAILED initial connect() hands off here (ownedByConnect) before the connection was ever
        // Open. That first-ever open must surface as Connected, never a spurious Disconnected (nothing
        // was up to disconnect) then Reconnected, and must not bump the reconnect count. everConnected
        // is captured before markConnectionOpen() sets it below (#161).
        $firstConnect = !$this->everConnected;

        // Leave Open before the first await: the state check is what routes concurrent publishes
        // into the reconnect buffer, so staying Open across the transport close would let them
        // race a socket that is about to disappear (#124).
        $this->state = ConnectionState::Connecting;

        $this->cancelPingTimer();
        if (!$firstConnect) {
            $this->emitEvent(ConnectionEvent::Disconnected);
        }

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

                // The replay window: state stays Connecting through the subscription replay and
                // the buffered-publish flush, so a concurrent publish keeps buffering (and flushes
                // in order below) instead of jumping the queue on the wire, and a failed leg falls
                // to the catch with no Open state / armed ping timer on a dead socket (#148).
                $this->resubscribeAll();
                // A failed initial connect recovering here is not a reconnect - do not bump the
                // reconnect count for the first-ever open (#161).
                if (!$firstConnect) {
                    $this->reconnectCount++;
                }
                $this->flushReconnectBuffer();

                // Re-check close-intent after the replay's suspension points: flipping Open here
                // would resurrect a connection disconnect()/drain() just closed (#84).
                if ($this->closing) {
                    try {
                        $this->transport->close()->await();
                    } catch (\Throwable) {
                        // Already gone; the Closed state below is what matters.
                    }
                    $this->state = ConnectionState::Closed;

                    return;
                }

                $this->markConnectionOpen();
                // The first-ever open reached through the recovery loop (a failed initial connect)
                // fires Connected, not Reconnected, so listeners keyed on Connected still fire (#161).
                $this->emitEvent($firstConnect ? ConnectionEvent::Connected : ConnectionEvent::Reconnected);

                return;
            } catch (AuthenticationException $e) {
                // Credentials will not become valid by retrying: stop the reconnect loop immediately
                // rather than hammering the server until attempts are exhausted (#46).
                $this->state = ConnectionState::Closed;
                $this->closeTransportBestEffort();
                // Name any parsed inbound backlog being discarded, mirroring the outbound path (#123/#158).
                $this->reportDiscardedInboundBacklog();
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

        // Parsed-but-undelivered inbound messages are about to be cleared with the runtime state. Name
        // the count in a loud error first (mirror of the outbound discard above, #123) so an inbound
        // backlog dropped at terminal close is never silent, the same as #134's observable drops (#158).
        $this->reportDiscardedInboundBacklog();

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
     *
     * Drains in a loop: the flush runs while state is still Connecting (#148), so a concurrent
     * fiber's publish can append to the buffer while a write below is suspended - those frames
     * must also reach the wire, in publish order, before the connection flips Open.
     */
    private function flushReconnectBuffer(): void
    {
        $passes = 0;
        while ($this->reconnectBuffer !== '') {
            // Bound the drain (#165): a fiber publishing during each write's suspension re-fills the
            // buffer, so an uncapped loop can defer the Open flip indefinitely (connection stuck
            // Connecting). After RECONNECT_FLUSH_MAX_PASSES, SEAL the buffer - writePublishFrame()
            // then parks late publishers on the gate instead of appending - so with no further
            // appends the remaining bytes drain in one final pass and recovery reaches Open. The
            // already-buffered frames are still written before the flip, so buffered-before-direct
            // ordering (#148) holds. Sealed at most once per recovery; the gate is completed by
            // recoverConnection()'s finally.
            if ($passes >= self::RECONNECT_FLUSH_MAX_PASSES && $this->reconnectFlushGate === null) {
                $this->reconnectFlushGate = new DeferredFuture();
            }

            $pending = $this->reconnectBuffer;

            // Remove the transmitted prefix only after the write succeeds: publish() already
            // reported success for these frames when they were buffered, so a flush failure must
            // leave them in place for the next reconnect attempt to replay - clearing first
            // silently lost every publish accepted during the reconnect window (#123). A
            // partially transmitted flush can duplicate frames on the retry; duplication beats
            // loss (nats.go pending-buffer semantics). Frames appended while the write was
            // suspended survive as the remainder and go out on the next iteration.
            $this->transport->write($pending)->await();
            $this->reconnectBuffer = substr($this->reconnectBuffer, strlen($pending));
            $passes++;
        }
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
     * reconnect critical section (state still Connecting, `reconnecting` set), and a callback that
     * publishes and hits a write failure would re-enter recoverConnection(), await the in-progress
     * reconnect deferred, and deadlock. Message frames captured here are buffered via handleFrame() and
     * delivered by the normal processIncoming()/heartbeat drain once recovery has completed.
     *
     * These reads run without taking the shared read slot, which is safe because state is not Open
     * for the whole replay window (#148): every reader that takes the slot is state-gated -
     * processIncoming() requires Open/Draining and consumeHeartbeatResponse() requires Open - so no
     * user or heartbeat read can start against the new socket until the recovery flips Open. (The
     * slot may even be legitimately held here: a read-failure-triggered recovery runs inside
     * processIncoming()'s catch, before its finally releases the slot.)
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
            try {
                $this->dispatchFrames($this->parser->push($chunk));
            } catch (ProtocolException $parseError) {
                // A mid-chunk parse failure fails this attempt, and the retry's connectOnce()
                // replaces the parser - which would drop the frames it retained (#147). Enqueue
                // them first (the post-recovery drainAllPending() delivers them), then rethrow so
                // attempt-failure semantics stay unchanged.
                try {
                    $this->dispatchFrames($this->parser->takeParsedFrames());
                } catch (\Throwable) {
                    // The rethrow below already fails this attempt; dispatchFrames() enqueued the
                    // recovered MSG frames per frame before rethrowing (#128).
                }

                throw $parseError;
            }
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
     * Waits for initial PONG while handling expected intermediary control lines. Returns any frames the
     * server coalesced BEHIND the PONG in the same TCP segment (an async INFO, a lame-duck notice, an
     * -ERR, or MSGs for a replayed subscription): they parsed in the same batch and their bytes are
     * already consumed, so returning them lets connectOnce() dispatch them instead of dropping them
     * when this loop returns at the PONG (#157).
     *
     * @return list<ProtocolFrame>
     */
    /**
     * Runs the bounded connect-handshake read loop shared by awaitInitialPong() and awaitServerInfo():
     * polls readHandshakeChunk() within the handshake deadline / poll budget, parses each chunk, and
     * feeds every frame to $onFrame - after auto-handling the two frames both callers treat identically,
     * PING (reply with PONG) and -ERR (throw the connect error). $onFrame returns a non-null value to end
     * the loop with that result, or null to keep polling. Throws a ConnectionException carrying
     * $timeoutMessage if the poll budget or deadline is exhausted first.
     *
     * @template T
     * @param callable(ProtocolFrame, int, list<ProtocolFrame>): (T|null) $onFrame
     * @return T
     */
    private function pollHandshake(string $timeoutMessage, callable $onFrame): mixed
    {
        $deadline = $this->handshakeDeadline();
        $remainingPolls = $this->handshakePollBudget();

        while ($remainingPolls-- > 0 && $this->monotonicSeconds() < $deadline) {
            $chunk = $this->readHandshakeChunk($deadline);
            if ($chunk === null || $chunk === '') {
                continue;
            }

            $frames = $this->parser->push($chunk);

            foreach ($frames as $index => $frame) {
                if ($frame->type === ProtocolFrameType::Ping) {
                    $this->transport->write($this->codec->encodePong())->await();

                    continue;
                }

                if ($frame->type === ProtocolFrameType::Err) {
                    throw $this->connectErrorFromFrame($frame->error);
                }

                $result = $onFrame($frame, $index, $frames);
                if ($result !== null) {
                    return $result;
                }
            }
        }

        throw new ConnectionException($timeoutMessage);
    }

    /**
     * @return list<ProtocolFrame>
     */
    private function awaitInitialPong(): array
    {
        return $this->pollHandshake('Expected PONG after CONNECT', function (ProtocolFrame $frame, int $index, array $frames): ?array {
            if ($frame->type === ProtocolFrameType::Ok) {
                return null;
            }

            if ($frame->type === ProtocolFrameType::Info && $frame->infoPayload !== null) {
                $this->serverInfo = $this->decodeServerInfoPayload($frame->infoPayload);

                return null;
            }

            if ($frame->type === ProtocolFrameType::Pong) {
                // Hand back the frames parsed after the PONG in this batch so they are not lost;
                // connectOnce() dispatches them once the parser bound is coupled to max_payload.
                // array_slice with default keys re-indexes, so the result is already a list.
                return array_slice($frames, $index + 1);
            }

            return null;
        });
    }

    /**
     * Waits for and parses the initial INFO frame sent by the server.
     */
    private function awaitServerInfo(): ServerInfo
    {
        return $this->pollHandshake('Expected INFO during connect', function (ProtocolFrame $frame): ?ServerInfo {
            if ($frame->type === ProtocolFrameType::Info && $frame->infoPayload !== null) {
                return $this->decodeServerInfoPayload($frame->infoPayload);
            }

            return null;
        });
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
     * Queues a pong-correlation slot for a PING that is about to be written (see {@see $pongWaiters}).
     * Enqueue happens synchronously before the write suspends, so queue order matches the order the
     * PINGs reach the wire even when several fibers send concurrently.
     *
     * @return DeferredFuture<null>
     */
    private function enqueuePongSlot(): DeferredFuture
    {
        /** @var DeferredFuture<null> $slot */
        $slot = new DeferredFuture();
        // Slots without a live waiter (heartbeat placeholders, timed-out flushes) are errored at
        // epoch teardown; suppress unhandled-error reporting - waiters still get the error from
        // their own await().
        $slot->getFuture()->ignore();
        $this->pongWaiters[] = $slot;

        return $slot;
    }

    /**
     * Removes a slot whose PING never reached the wire (the write failed). Only that case may
     * remove mid-queue: no PONG is owed for an unwritten PING, so removal REPAIRS alignment,
     * whereas removing a timed-out-but-written PING's slot would desynchronize it.
     *
     * @param DeferredFuture<null> $slot
     */
    private function discardPongSlot(DeferredFuture $slot): void
    {
        $remaining = [];
        foreach ($this->pongWaiters as $queued) {
            if ($queued !== $slot) {
                $remaining[] = $queued;
            }
        }

        $this->pongWaiters = $remaining;
    }

    /**
     * Errors out every parked pong slot and empties the queue. Must run whenever a connection
     * epoch ends - the reconnect handshake ({@see connectOnce()}) and every terminal close
     * ({@see releaseRuntimeState()}) - so no slot survives into a new TCP connection: a PONG from
     * the new socket must never complete a wait pinned to the old one, and the old PING's answer
     * can no longer arrive (#117, nats.go clearPendingFlushCalls parity).
     */
    private function failPongWaiters(\Throwable $error): void
    {
        $waiters = $this->pongWaiters;
        $this->pongWaiters = [];

        foreach ($waiters as $slot) {
            if (!$slot->isComplete()) {
                $slot->error($error);
            }
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
            // ANY pong proves the server is alive, so the watchdog resets unconditionally; flush
            // completion is per-PING (#117): the oldest queued slot is the PING this PONG answers
            // (TCP preserves order), so a stale heartbeat PONG completes the heartbeat's
            // placeholder - never a later flush()/drain() waiter's slot.
            $this->outstandingPings = 0;

            $slot = array_shift($this->pongWaiters);
            if ($slot !== null && !$slot->isComplete()) {
                $slot->complete();
            }

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
                // A permissions violation naming the mux inbox subscription "<muxBase>.*" means this
                // account cannot subscribe to the reply-inbox wildcard, so no request reply can ever be
                // delivered (#167). Drop the dead mux state (so a reconnect does not replay the rejected
                // SUB) and latch $muxRejected: request()/requestMany() then fail fast with a clear,
                // catchable error instead of a silent permanent timeout. The random muxBase makes the
                // substring match unambiguous.
                if ($this->muxSid !== null && $this->muxBase !== null && str_contains($error, $this->muxBase)) {
                    // dropSubscriptionState() also clears the sid's slow-consumer exemption flag.
                    $this->dropSubscriptionState($this->muxSid);
                    $this->muxSid = null;
                    $this->muxBase = null;
                    $this->muxInboxSetup = null;
                    $this->muxRejected = true;
                }

                // Generalized #167: a permissions violation naming a subscription subject starves
                // whichever layer routes replies through that sid. Notify the owning layer's
                // registered rejection handler (the pull-pipeline engine) so it fails fast with a
                // clear error instead of spinning on silent deadline retires forever. The subject
                // may appear quoted or bare in the server's -ERR text.
                if (preg_match('/subscription to "?([^\s"\']+)"?/i', $error, $subjectMatch) === 1) {
                    $rejectedSubject = $subjectMatch[1];
                    foreach ($this->subscriptionMeta as $rejectedSid => $meta) {
                        if ($meta['subject'] === $rejectedSubject && isset($this->subscriptionRejectionHandlers[$rejectedSid])) {
                            try {
                                ($this->subscriptionRejectionHandlers[$rejectedSid])($error);
                            } catch (\Throwable) {
                                // A throwing handler must never break frame dispatch.
                            }
                        }
                    }
                }

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
     *
     * The queue is bounded by message COUNT only ({@see NatsOptions::$maxPendingMessagesPerSubscription});
     * there is no byte-based bound, so N large payloads can pin proportional memory per slow
     * subscription (nats.go's pending limits are both count- and byte-based) (#159).
     */
    private function enqueueMessage(int $sid, NatsMessage $message): void
    {
        // Count the message toward auto-unsubscribe accounting at intake - before any slow-consumer
        // drop below - so a dropped message still advances toward the max exactly as it does on the
        // server (which counts messages it SENT, not messages the client managed to deliver). This
        // holds for EVERY policy, including Error: the server decremented its auto-unsub max when it
        // wrote the message, so a client-side slow-consumer drop must not roll this back - doing so
        // desynchronises the count and completeAutoUnsubIfSatisfied() would never fire, leaking the
        // subscription (the #112 invariant it relies on) (#112/#159).
        $this->receivedCounts[$sid] = ($this->receivedCounts[$sid] ?? 0) + 1;

        if (!isset($this->pendingMessages[$sid])) {
            // Defensive only: subscribe() creates the queue and it persists (empty between drains,
            // #139) until dropSubscriptionState() removes it, so a routable sid always has one.
            $this->pendingMessages[$sid] = new SplQueue();
        }

        $queue = $this->pendingMessages[$sid];

        if (isset($this->unboundedSids[$sid])) {
            // Mux request inbox (#118): every in-flight reply shares this single sid's queue, so the
            // per-subscription count bound must NOT apply - dropping here would silently break whichever
            // request owns the dropped reply, and there is no backpressure path (the server already sent
            // it). Memory is bounded by outstanding request concurrency: one read cycle enqueues at most
            // a chunk's worth of replies, all drained before the next read, so the queue does not grow
            // across reads; a pinned reply is strictly better than a lost one (#159 caveat).
            $queue->enqueue($message);
            $this->pendingDirty[$sid] = true;

            return;
        }

        $limit = max(1, $this->options->maxPendingMessagesPerSubscription);

        if ($queue->count() >= $limit) {
            if ($this->options->slowConsumerPolicy === SlowConsumerPolicy::DropOldest) {
                $queue->dequeue();
                $this->emitError(new NatsException('Slow consumer on sid ' . $sid . ': dropped oldest message'), 'debug');
            } elseif ($this->options->slowConsumerPolicy === SlowConsumerPolicy::DropNewest) {
                $this->emitError(new NatsException('Slow consumer on sid ' . $sid . ': dropped newest message'), 'debug');

                return;
            } else {
                // Error policy: the overflowing message is dropped (core NATS will not resend it) and
                // the loss is surfaced loudly by THROWING - the single surfacing point. dispatchFrames()
                // rethrows the first such error to the caller and emitErrorSafely()s any subsequent one
                // (#158), so an extra emitError() here would report the SAME exception to the listener
                // twice. The intake count charged above is deliberately NOT rolled back: the server
                // already counted this message toward the auto-unsub max, so it must count here too or
                // completeAutoUnsubIfSatisfied() never fires and the subscription leaks (#159/#112).
                throw new ConnectionException('Subscription queue overflow for sid ' . $sid);
            }
        }

        $queue->enqueue($message);
        // The queue is now non-empty: record the sid in the dirty set so drainAllPending() scans it.
        // Setting an existing key is idempotent and does NOT change its position, so the set's contents
        // stay a subset of the sids; drainAllPending() re-imposes ascending (registration) order (#162).
        $this->pendingDirty[$sid] = true;
    }

    /**
     * Drains all queued subscription messages in SID order.
     */
    private function drainAllPending(): void
    {
        if ($this->pendingDirty === []) {
            // No sid has a buffered message. Return before allocating so a message-free inbound chunk -
            // every heartbeat self-read, and any chunk that carried only control frames - costs O(1)
            // instead of the pre-#162 O(all live subscriptions) scan plus a fresh array_keys() copy.
            return;
        }

        // Iterate ONLY sids with a non-empty queue (the dirty set), so the per-chunk cost is
        // O(sids-with-backlog), independent of the idle-subscription count (#162).
        if (count($this->pendingDirty) === 1) {
            // The common single-active chunk: one sid has backlog. Deliver it directly - no array_keys()
            // copy and no sort() (a single sid is already ordered), so this path is cheaper than the old
            // array_keys(pendingMessages) scan even at one subscription, not just at many idle ones.
            $this->drainPendingForSid(array_key_first($this->pendingDirty));

            return;
        }

        // Two or more sids have backlog: sort ascending so cross-sid delivery keeps the registration-order
        // sequencing of the old array_keys(pendingMessages) scan (subscribe() inserts sids ascending).
        $sids = array_keys($this->pendingDirty);
        sort($sids);
        foreach ($sids as $sid) {
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
    /**
     * From the heartbeat timer: cancel the ping timer and attempt recovery, forcing the connection
     * Closed if recovery itself throws. Shared by the missed-PONG (maxPingsOut) and PING-write-failure
     * paths of pingTimerTick().
     */
    private function recoverFromHeartbeatFailure(): void
    {
        $this->cancelPingTimer();

        try {
            $this->recoverConnection();
        } catch (\Throwable) {
            $this->state = ConnectionState::Closed;
        }
    }

    private function pingTimerTick(): void
    {
        if ($this->state !== ConnectionState::Open) {
            $this->cancelPingTimer();

            return;
        }

        $this->outstandingPings++;

        if ($this->outstandingPings > $this->options->maxPingsOut) {
            $this->recoverFromHeartbeatFailure();

            return;
        }

        // The heartbeat PING occupies a pong-correlation slot even though nothing awaits it
        // (#117): its PONG must consume ITS queue position - otherwise a heartbeat PONG left
        // unconsumed by the bounded self-read below would complete the next flush()/drain()
        // waiter's slot one PING early.
        $slot = $this->enqueuePongSlot();

        try {
            $this->transport->write($this->codec->encodePing())->await();
        } catch (\Throwable) {
            // The PING never hit the wire: drop its slot so correlation stays aligned (the
            // recovery below clears the rest on the epoch change anyway).
            $this->discardPongSlot($slot);
            $this->recoverFromHeartbeatFailure();

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
        // The tick's entry guard checked Open, but the PING write above is a suspension point: a
        // recovery entered meanwhile owns the socket (possibly mid-handshake/replay on a fresh
        // one), and a heartbeat read here would collide with its reads (#148).
        if ($this->state !== ConnectionState::Open) {
            return;
        }

        if ($this->readInProgress) {
            return;
        }

        $timeoutSeconds = min(2.0, max(0.05, (float) $this->options->pingIntervalSeconds));

        $this->readInProgress = true;

        $closed = false;
        $protocolViolation = null;
        try {
            $chunk = $this->transport->readLine(new TimeoutCancellation($timeoutSeconds))->await();
        } catch (TransportClosedException) {
            // The peer closed the socket during the heartbeat read. Recover, but only after the
            // finally clears readInProgress (recoverConnection -> connectOnce reads the socket).
            $closed = true;
            $chunk = '';
        } catch (ProtocolException $violation) {
            // A transport-level protocol violation surfaced on the TIMER's read. Some of these are
            // ONE-SHOT (the WebSocket transport's deferred processFrames terminals - inflate
            // failure, RSV1 gate, fragment bound - whose offending bytes are already consumed):
            // swallowing here would drop the violation forever and leave a corrupt stream running
            // silently. Surface + recover after the finally, mirroring the user-read path.
            $protocolViolation = $violation;
            $chunk = '';
        } catch (\Throwable) {
            // No PONG within the window (or a transient read error); leave escalation to the next
            // tick or to the application's own processIncoming() loop.
            return;
        } finally {
            $this->readInProgress = false;
            $this->signalReadSlotFree();
        }

        if ($protocolViolation !== null) {
            // emitErrorSafely, not emitError: emitError() logs BEFORE its listener-throw guard, so a
            // throwing user logger would skip the recovery below and escape into the event-loop
            // timer - and the violation is ONE-SHOT (the WS transport nulls it before throwing), so
            // the skipped recovery would never be retried from a re-raised violation, leaving the
            // corrupt stream running as Open (#150 containment).
            $this->emitErrorSafely($protocolViolation);
            try {
                $this->recoverConnection();
            } catch (\Throwable) {
                $this->state = ConnectionState::Closed;
            }

            return;
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
        } catch (ProtocolException $parseError) {
            // A mid-chunk parse failure: frames parsed before it are already consumed from the
            // wire and would otherwise vanish (#147). Deliver them through the normal
            // enqueue/dispatch path and surface the failure; escalation (recovery) stays with the
            // ping watchdog / next user read, not with the event-loop timer.
            try {
                try {
                    $this->dispatchFrames($this->parser->takeParsedFrames());
                } finally {
                    $this->drainAllPending();
                }
            } catch (\Throwable $e) {
                // Contained like the clean-path dispatch above: surfaced below, never thrown
                // out of the timer.
                $dispatchError = $e;
            }

            $this->emitError($parseError);
        } catch (\Throwable) {
            // A handler error during the drain; leave escalation to the next user read / tick
            // rather than throwing out of the event-loop timer.
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
            // The subscription is gone; its backlog is undeliverable. Drop it (and its dirty-set entry)
            // instead of retaining state that drainAllPending() would re-scan.
            unset($this->pendingMessages[$sid], $this->pendingDirty[$sid]);

            return;
        }

        if ($queue->isEmpty()) {
            // Nothing buffered. The queue persists empty until the subscription is dropped (#139) - the
            // previous unset-on-empty meant one SplQueue alloc/free per delivered message in the
            // promptly-drained common case. Clear any dirty-set entry so the invariant (dirty IFF
            // non-empty) holds even when a reentrant drain emptied this sid earlier in the same pass.
            unset($this->pendingDirty[$sid]);

            return;
        }

        if (isset($this->dispatchingSids[$sid])) {
            // Already delivering this sid further up the stack (a handler awaited and suspended). Do
            // not re-enter: the suspended loop resumes and drains whatever we enqueued meanwhile, so
            // ordering holds and a handler is never invoked on top of itself.
            return;
        }

        $this->dispatchingSids[$sid] = true;

        // Deliveries made by THIS pass, for the mid-drain budget check below: the head delivery of a
        // pass is always permitted (drain()'s dedicated final pass keeps its pinned deliver-what-the-
        // flush-could-not contract), so only iteration 2+ consults the deadline.
        $deliveredThisPass = 0;

        try {
            while (!$queue->isEmpty()) {
                if (
                    $deliveredThisPass > 0
                    && $this->drainDeadline !== null
                    && $this->state === ConnectionState::Draining
                    && $this->monotonicSeconds() >= $this->drainDeadline
                ) {
                    // The drain budget ran out mid-pass: stop before the NEXT delivery, so a handler
                    // that consumes budget per message (e.g. a bounded ack publish timing out per
                    // delivery) cannot serially extend drain() a whole pass (~K x the per-publish
                    // bound) past its deadline. The remainder stays queued (the sid stays dirty via
                    // the finally below) and drain()'s own deadline check then reports the discard
                    // loudly, exactly as it does for a backlog that never got a pass (#149).
                    break;
                }

                if (!array_key_exists($sid, $this->subscriptions)) {
                    break;
                }

                $max = $this->autoUnsubMax[$sid] ?? null;
                if ($max !== null && ($this->deliveredCounts[$sid] ?? 0) >= $max) {
                    // Gate DELIVERY at the cap, not just the aftermath: an auto-unsub armed with
                    // max <= already-delivered while a backlog was still queued (completeAutoUnsub
                    // deferred on the non-empty backlog) would otherwise dequeue and deliver exactly
                    // one message past the max before the post-delivery check below fired. Drop the
                    // sid here without invoking the handler once more (#156). The post-delivery check
                    // still handles the #112 flush case where max > delivered on entry.
                    $this->dropSubscriptionState($sid);

                    break;
                }

                /** @var NatsMessage $message */
                $message = $queue->dequeue();
                $this->deliveredCounts[$sid] = ($this->deliveredCounts[$sid] ?? 0) + 1;
                $deliveredThisPass++;
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

            // Keep the dirty set = sids with a non-empty queue: once this sid's queue has fully drained
            // (or the subscription was dropped by an auto-unsub cap / completeAutoUnsub above), remove it
            // so drainAllPending() no longer scans it. A handler that threw mid-drain leaves messages
            // queued with the subscription still alive - that stays dirty and is re-drained next pass (#162).
            if (!isset($this->pendingMessages[$sid]) || $this->pendingMessages[$sid]->isEmpty()) {
                unset($this->pendingDirty[$sid]);
            }
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

    private function dropSubscriptionState(int $sid): void
    {
        unset($this->subscriptions[$sid]);
        unset($this->subscriptionMeta[$sid]);
        unset($this->pendingMessages[$sid]);
        unset($this->pendingDirty[$sid]);
        unset($this->receivedCounts[$sid]);
        unset($this->deliveredCounts[$sid]);
        unset($this->autoUnsubMax[$sid]);
        // Hygiene: the slow-consumer exemption flag must never outlive its sid (#118).
        unset($this->unboundedSids[$sid]);
        unset($this->subscriptionRejectionHandlers[$sid]);
    }
}
